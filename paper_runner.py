"""Paper-mode orchestration: market data -> engine -> risk -> execution."""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from backtesting import replay_positioning_frames
from engine import CurrentPosition, MarketFrame, SourceFreshness, StrategyConfig, StrategyEngine
from binance_client import ClientConfig, FuturesPublicClient
from execution import (
    BinanceExecutor,
    ExecutionConfig,
    Executor,
    MarketSnapshot,
    PaperExecutor,
    executor_from_env,
)
from reconciliation import Reconciler, apply_user_stream_event
from risk import ExchangeRules, RiskContext, RiskGate, RiskLimits
from runtime_gate import GateResult, evaluate_runtime_gate, max_data_age_sec
from scripts import bian_market
from trade_intent import TradeIntent
from trading_store import TradingStore


def _decimal(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"invalid market numeric value: {value}") from exc
    if not result.is_finite() or result <= 0:
        raise ValueError(f"market numeric value must be positive: {value}")
    return result


def _int_env(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.environ.get(name, default)))
    except ValueError:
        return default


def _decimal_env(name: str, default: str) -> Decimal:
    try:
        return Decimal(os.environ.get(name, default))
    except InvalidOperation:
        return Decimal(default)


def _symbols(value: str) -> list[str]:
    symbols = [item.strip().upper() for item in value.split(",") if item.strip()]
    if not symbols:
        raise ValueError("BIAN_PAPER_SYMBOLS must contain at least one symbol")
    if any(not symbol.endswith("USDT") for symbol in symbols):
        raise ValueError("BIAN_PAPER_SYMBOLS must contain USDT pairs")
    return list(dict.fromkeys(symbols))


def _test_only_signal_injection(
    frame: MarketFrame,
    *,
    mode: str,
) -> TradeIntent | None:
    """Build a validation-only intent without changing the strategy engine."""
    enabled = os.environ.get("TEST_ONLY_SIGNAL_INJECTION", "false").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        return None
    environment = os.environ.get("BIAN_ENVIRONMENT", "development").strip().lower()
    if mode != "paper":
        raise RuntimeError("TEST_ONLY_SIGNAL_INJECTION requires BIAN_MODE=paper")
    if environment in {"production", "prod", "live"}:
        raise RuntimeError(
            "TEST_ONLY_SIGNAL_INJECTION is blocked in production environments"
        )

    side = os.environ.get(
        "TEST_ONLY_SIGNAL_DIRECTION",
        os.environ.get("TEST_ONLY_SIGNAL_SIDE", "LONG"),
    ).strip().upper()
    order_type = os.environ.get("TEST_ONLY_SIGNAL_ORDER_TYPE", "MARKET").strip().upper()
    if side in {"BUY", "LONG"}:
        side = "LONG"
    elif side in {"SELL", "SHORT"}:
        side = "SHORT"
    else:
        raise ValueError("TEST_ONLY_SIGNAL_DIRECTION must be LONG or SHORT")
    if order_type not in {"MARKET", "LIMIT"}:
        raise ValueError("TEST_ONLY_SIGNAL_ORDER_TYPE must be MARKET or LIMIT")

    quantity_text = os.environ.get("TEST_ONLY_SIGNAL_QUANTITY", "0.01")
    price_text = os.environ.get("TEST_ONLY_SIGNAL_PRICE")
    quantity = Decimal(quantity_text)
    price = Decimal(price_text) if price_text else frame.closes[-1]
    if order_type == "LIMIT" and price_text is None:
        raise ValueError("TEST_ONLY_SIGNAL_PRICE is required for LIMIT injection")
    direction = side
    return TradeIntent(
        symbol=frame.symbol,
        direction=direction,  # type: ignore[arg-type]
        action="OPEN",
        reduce_only=False,
        leverage=Decimal(os.environ.get("DEFAULT_LEVERAGE", "1")),
        order_type=order_type,  # type: ignore[arg-type]
        quantity=quantity,
        price=price if order_type == "LIMIT" else None,
        confidence=Decimal("1"),
        reason="TEST_ONLY_SIGNAL_INJECTION",
        strategy_version="test-only-injection",
        created_at=frame.captured_at,
    )


def _market_frame(
    report: dict[str, Any],
    *,
    positioning_events: list[dict[str, Any]] | None = None,
    universe_context: dict[str, Any] | None = None,
) -> MarketFrame:
    raw_klines = report.get("klines") or []
    closes = tuple(_decimal(row[4]) for row in raw_klines if len(row) > 4)
    if not closes:
        raise ValueError("Binance kline response contained no closes")
    captured_at = datetime.fromisoformat(str(report["captured_at"]))
    source_timestamp = datetime.fromisoformat(
        str(report.get("source_timestamp", report["captured_at"]))
    )
    received_timestamp = datetime.fromisoformat(
        str(report.get("received_timestamp", report["captured_at"]))
    )
    freshness = SourceFreshness(
        source="binance_futures_klines",
        source_timestamp=source_timestamp,
        received_timestamp=received_timestamp,
        max_age_sec=max_data_age_sec(),
        now=received_timestamp,
    )
    features = bian_market.positioning_feature_values(
        positioning_events or [], as_of=captured_at
    )
    universe_context = dict(universe_context or {})
    universe_source_timestamps = universe_context.pop("source_timestamps", {})
    features.update({
        name: value
        for name, value in universe_context.items()
        if name in {
            "relative_strength", "relative_strength_1m", "relative_strength_5m",
            "relative_strength_15m", "relative_strength_1h", "breadth_score",
            "advance_decline_ratio", "market_regime", "meme_risk_tier",
        }
    })
    source_timestamps = {
        "futures_klines": {
            "source_timestamp": source_timestamp.isoformat(),
            "received_timestamp": received_timestamp.isoformat(),
            "latency_ms": int((received_timestamp - source_timestamp).total_seconds() * 1000),
        },
        **features.pop("source_timestamps"),
        **universe_source_timestamps,
    }
    source_freshness = [freshness]
    for source, timestamps in source_timestamps.items():
        if source == "futures_klines":
            continue
        source_at = datetime.fromisoformat(str(timestamps["source_timestamp"]))
        received_at = datetime.fromisoformat(str(timestamps["received_timestamp"]))
        source_freshness.append(
            SourceFreshness(
                source=source,
                source_timestamp=source_at,
                received_timestamp=received_at,
                max_age_sec=max_data_age_sec(),
                now=captured_at,
            )
        )
    return MarketFrame(
        symbol=str(report["symbol"]),
        closes=closes,
        captured_at=captured_at,
        quote_volume=_decimal(raw_klines[-1][7]) if len(raw_klines[-1]) > 7 else None,
        freshness=tuple(source_freshness),
        source_timestamps=source_timestamps,
        **features,
    )


def _market_snapshot(frame: MarketFrame) -> MarketSnapshot:
    last_price = frame.last_price or frame.closes[-1]
    mark_price = frame.mark_price
    return MarketSnapshot(
        last_price=last_price,
        mark_price=mark_price,
        index_price=frame.index_price,
        bid_price=frame.bid_price,
        ask_price=frame.ask_price,
        available_liquidity_notional_usdt=(
            frame.depth_25bps * mark_price
            if frame.depth_25bps is not None and mark_price is not None
            else None
        ),
        funding_rate=frame.funding_rate,
        funding_timestamp=frame.funding_timestamp,
        settlement_timestamp=frame.funding_settlement_timestamp,
        current_timestamp=frame.captured_at,
    )


def _positioning_frame_for_symbol(
    symbol: str,
    store: TradingStore,
    *,
    strict: bool,
) -> MarketFrame:
    """Build one timestamp-bounded decision frame from public observations."""
    report = bian_market.get_klines(
        symbol,
        interval=os.environ.get("BIAN_PAPER_INTERVAL", "1m"),
        limit=_int_env("BIAN_PAPER_KLINE_LIMIT", 100, minimum=1),
    )
    captured_at = datetime.fromisoformat(str(report["captured_at"]))
    positioning_events: list[dict[str, Any]] = []
    universe_context: dict[str, Any] = {}
    if hasattr(store, "positioning_events"):
        try:
            positioning_events = store.positioning_events(
                symbol,
                as_of=captured_at,
                lookback_seconds=_int_env(
                    "POSITIONING_EVENT_LOOKBACK_SEC", 86_400, minimum=300
                ),
            )
        except Exception:
            if strict:
                raise
    if hasattr(store, "market_universe_context"):
        try:
            universe_context = store.market_universe_context(symbol, as_of=captured_at)
        except Exception:
            if strict:
                raise
    return _market_frame(
        report,
        positioning_events=positioning_events,
        universe_context=universe_context,
    )


def _record_shadow(
    frame: MarketFrame,
    *,
    store: TradingStore,
    engine: StrategyEngine,
    strict: bool,
) -> tuple[Any | None, dict[str, Any] | None]:
    """Persist one legacy-versus-positioning comparison without execution."""
    if not hasattr(engine, "positioning_decision"):
        return None, None
    previous_state = None
    if hasattr(store, "latest_positioning_state"):
        previous_state = store.latest_positioning_state(
            frame.symbol, before=frame.captured_at
        )
    positioning = engine.positioning_decision(
        frame,
        now=frame.captured_at,
        previous_state=previous_state,
    )
    try:
        store.record_positioning_snapshot(
            positioning,
            strategy_version=getattr(
                engine.config, "positioning_strategy_version", "positioning-v1"
            ),
        )
        shadow = engine.evaluate_shadow(frame, positioning=positioning)
        store.record_system_event(
            event_type="POSITIONING_SHADOW_DECISION",
            severity="INFO",
            message="legacy and positioning decisions compared",
            payload={
                "symbol": frame.symbol,
                "legacy_decision": shadow["legacy_decision"],
                "positioning_decision": positioning.as_dict(),
                "agreement": shadow["agreement"],
                "why_different": shadow["why_different"],
            },
        )
        return positioning, shadow
    except Exception:
        if strict:
            raise
        # Shadow persistence must never turn a valid legacy paper cycle into an order.
        return positioning, None


def run_shadow_cycle(
    symbol: str,
    *,
    store: TradingStore | None = None,
    engine: StrategyEngine | None = None,
) -> dict[str, Any]:
    """Persist a positioning shadow decision without Risk or execution calls."""
    store = store or TradingStore()
    engine = engine or StrategyEngine(StrategyConfig.from_env())
    frame = _positioning_frame_for_symbol(symbol, store, strict=True)
    positioning, shadow = _record_shadow(
        frame,
        store=store,
        engine=engine,
        strict=True,
    )
    if positioning is None or shadow is None:
        raise RuntimeError("positioning shadow engine is unavailable")
    return {
        "status": "shadow_recorded",
        "symbol": frame.symbol,
        "captured_at": frame.captured_at.isoformat(),
        "legacy_decision": shadow["legacy_decision"],
        "positioning_decision": positioning.direction,
        "positioning_state": positioning.state,
        "evidence_snapshot_id": str(positioning.evidence_snapshot_id),
    }


def paper_shadow_attribution(
    symbol: str,
    *,
    store: TradingStore,
    engine: StrategyEngine,
) -> dict[str, Any]:
    """Attribute persisted Shadow decisions without changing Paper execution."""
    frames = store.positioning_replay_frames(
        symbol,
        source_ttl_sec=engine.config.source_ttl_sec,
    )
    replay = replay_positioning_frames(frames, engine=engine)
    payload = replay.as_dict()
    store.record_system_event(
        event_type="PAPER_SHADOW_ATTRIBUTION",
        severity="INFO",
        message="positioning, legacy SMA, and momentum attribution calculated",
        payload={"symbol": symbol.upper(), **payload},
    )
    return {"symbol": symbol.upper(), **payload}


def _current_position(store: TradingStore, symbol: str) -> CurrentPosition:
    position = store.get_position(symbol) if hasattr(store, "get_position") else None
    if not position:
        return CurrentPosition()
    quantity = Decimal(str(position.get("quantity") or "0"))
    side = str(position.get("position_side") or "FLAT")
    if quantity == 0:
        side = "FLAT"
    elif side not in {"LONG", "SHORT"}:
        side = "LONG"
    leverage = Decimal(str(position.get("leverage") or "1"))
    entry = position.get("entry_price") or position.get("average_price")
    return CurrentPosition(
        direction=side,  # type: ignore[arg-type]
        quantity=quantity,
        entry_price=Decimal(str(entry)) if entry is not None else None,
        leverage=leverage if leverage > 0 else Decimal("1"),
    )


def _risk_context(
    store: TradingStore,
    intent: TradeIntent,
    market: MarketSnapshot,
    *,
    public_client: FuturesPublicClient | None = None,
    executor: Executor | None = None,
) -> RiskContext:
    if executor is not None and hasattr(executor, "account_state"):
        account = executor.account_state()
    else:
        quote = store.get_balance("USDT") or {}
        wallet = Decimal(str(quote.get("wallet_balance") or quote.get("free") or "0"))
        used = Decimal(str(quote.get("used_margin") or quote.get("locked") or "0"))
        account = {
            "wallet_balance": wallet,
            "available_balance": Decimal(str(quote.get("available_balance") or quote.get("free") or "0")),
            "used_margin": used,
            "equity": wallet,
            "unrealized_pnl": Decimal(str(quote.get("unrealized_pnl") or "0")),
            "realized_pnl": Decimal("0"),
            "funding_pnl": Decimal(str(quote.get("funding_pnl") or "0")),
        }
    position = store.get_position(intent.symbol) if hasattr(store, "get_position") else None
    position = position or {}
    quantity = Decimal(str(position.get("quantity") or "0"))
    direction = str(position.get("position_side") or "FLAT")
    if quantity == 0:
        direction = "FLAT"
    mark = market.mark_price
    rules = ExchangeRules(symbol=intent.symbol)
    if public_client is not None:
        raw_rules = public_client.get_symbol_rules(intent.symbol)
        max_qty = Decimal(raw_rules["max_qty"])
        rules = ExchangeRules(
            symbol=intent.symbol,
            status=raw_rules["status"],
            min_qty=Decimal(raw_rules["min_qty"]),
            max_qty=max_qty if max_qty > 0 else None,
            step_size=Decimal(raw_rules["step_size"]),
            tick_size=Decimal(raw_rules["tick_size"]),
            min_notional=Decimal(raw_rules["min_notional"]),
        )
    liquidation_price = (
        Decimal(str(position["liquidation_price"]))
        if position.get("liquidation_price") is not None
        else None
    )
    if (
        liquidation_price is None
        and intent.action == "OPEN"
        and direction == "FLAT"
        and isinstance(executor, PaperExecutor)
        and mark is not None
    ):
        liquidation_price = executor.liquidation_price_for(
            intent.symbol,
            intent.direction,
            mark,
            intent.leverage,
        )
    return RiskContext(
        wallet_balance=Decimal(str(account.get("wallet_balance") or "0")),
        available_balance=Decimal(str(account.get("available_balance") or "0")),
        equity=Decimal(str(account.get("equity") or "0")),
        used_margin=Decimal(str(account.get("used_margin") or "0")),
        position_direction=direction,  # type: ignore[arg-type]
        position_quantity=quantity,
        position_notional=abs(quantity * mark) if mark is not None else Decimal("0"),
        entry_price=(
            Decimal(str(position["entry_price"]))
            if position.get("entry_price") is not None
            else Decimal(str(position.get("average_price") or "0"))
        ),
        mark_price=mark,
        index_price=market.index_price,
        unrealized_pnl=Decimal(str(account.get("unrealized_pnl") or position.get("unrealized_pnl") or "0")),
        realized_pnl=Decimal(str(account.get("realized_pnl") or position.get("realized_pnl") or "0")),
        funding_pnl=Decimal(str(account.get("funding_pnl") or position.get("funding_pnl") or "0")),
        leverage=Decimal(str(position.get("leverage") or intent.leverage)),
        account_leverage=(
            Decimal(str(position["leverage"]))
            if position.get("leverage") is not None
            else None
        ),
        margin_type="ISOLATED",
        position_mode="ONE_WAY",
        liquidation_price=liquidation_price,
        liquidation_distance_percent=(
            abs(mark - liquidation_price) / mark * Decimal("100")
            if mark is not None and mark > 0 and liquidation_price is not None
            else None
        ),
        exchange_rules=rules,
        positioning_confidence=intent.confidence,
        crowding_score=intent.crowding_score,
        liquidity_score=intent.liquidity_score,
        data_quality_score=intent.data_quality_score,
        evidence_conflict=intent.positioning_state == "CONFLICTED",
        # An intent without an explicit, persisted Futures universe
        # classification is observe-only; it must not default to tradeable.
        meme_risk_tier=intent.meme_risk_tier or "OBSERVE",
    )


def run_cycle(
    symbol: str,
    *,
    store: TradingStore | None = None,
    engine: StrategyEngine | None = None,
    risk_gate: RiskGate | None = None,
    executor: Executor | None = None,
    public_client: FuturesPublicClient | None = None,
    mode: str = "paper",
    runtime_gate: GateResult | None = None,
) -> dict[str, Any]:
    """Run one cycle through the shared strategy, risk, and executor path."""
    if mode != "paper":
        if runtime_gate is None:
            raise RuntimeError("non-paper cycles require the canonical runtime gate")
        if not runtime_gate.trading_enabled:
            raise RuntimeError(
                "canonical runtime gate blocks trading: "
                + "; ".join(runtime_gate.reasons)
            )
    store = store or TradingStore()
    engine = engine or StrategyEngine(StrategyConfig.from_env())
    risk_gate = risk_gate or RiskGate(limits=RiskLimits.from_env())
    executor = executor or executor_from_env(
        store=store,
        config=ExecutionConfig.from_env(mode=mode),
    )
    frame = _positioning_frame_for_symbol(symbol, store, strict=False)
    market = _market_snapshot(frame)
    positioning, shadow = _record_shadow(
        frame,
        store=store,
        engine=engine,
        strict=False,
    )
    injected_intent = _test_only_signal_injection(frame, mode=mode)
    current = _current_position(store, symbol)
    evaluate = getattr(engine, "evaluate")
    try:
        intent = injected_intent or evaluate(frame, current_position=current)
    except TypeError:
        intent = injected_intent or evaluate(frame)
    if intent is None:
        if isinstance(executor, PaperExecutor):
            if market.settlement_timestamp is not None:
                executor.apply_funding(symbol, market)
            if market.mark_price is not None:
                executor.mark_to_market(symbol, market.mark_price)
        store.record_system_event(
            event_type="NO_SIGNAL",
            severity="INFO",
            message="strategy produced no trade signal",
            payload={
                "symbol": symbol,
                "captured_at": frame.captured_at.isoformat(),
                "strategy_version": engine.config.strategy_version,
                "shadow": positioning.as_dict() if positioning is not None else None,
            },
        )
        return {
            "status": "no_signal",
            "symbol": symbol,
            "captured_at": frame.captured_at.isoformat(),
        }
    positioning_intent = intent.direction is not None
    signal = None if injected_intent is not None or positioning_intent else engine.signal(frame)
    if signal is None and injected_intent is None and not positioning_intent:
        raise RuntimeError("engine returned intent without signal")
    store.record_signal(
        symbol=intent.symbol,
        side=intent.exchange_side(),
        confidence=(
            intent.confidence if injected_intent is not None or positioning_intent else signal.confidence
        ),
        reason=(
            intent.reason if injected_intent is not None or positioning_intent else signal.reason
        ),
        strategy_version=intent.strategy_version,
        observed_at=intent.created_at,
        payload={
            "price": str(
                intent.price
                or (signal.price if signal is not None else frame.closes[-1])
            ),
            "test_only": injected_intent is not None,
        },
    )
    store.record_intent(intent, status="CREATED")
    decision = risk_gate.evaluate(
        intent,
        _risk_context(
            store,
            intent,
            market,
            public_client=public_client,
            executor=executor,
        ),
    )
    result = executor.submit(intent, decision, market=market)
    if isinstance(executor, PaperExecutor):
        if market.settlement_timestamp is not None:
            executor.apply_funding(symbol, market)
        if market.mark_price is not None:
            executor.mark_to_market(symbol, market.mark_price)
    store.update_intent_status(intent.id, result.status)
    return {
        "status": result.status.lower(),
        "symbol": intent.symbol,
        "intent_id": str(intent.id),
        "order_id": str(result.order_id),
        "client_order_id": result.client_order_id,
        "executed_quantity": str(result.executed_quantity),
        "executed_price": str(result.executed_price),
        "risk_decision": decision.decision,
        "shadow": positioning.as_dict() if positioning is not None else None,
    }


def _assert_account_risk_config(client: Any) -> None:
    """Compatibility wrapper for callers that still expect a preflight check."""
    gate = evaluate_runtime_gate(
        mode=os.environ.get("BIAN_MODE", "testnet"),
        client=client,
        symbols=_symbols(os.environ.get("BIAN_PAPER_SYMBOLS", "BTCUSDT")),
        probe_account=True,
    )
    if not gate.account_mode_ok or not gate.margin_mode_ok:
        raise SystemExit("runtime gate blocked account configuration: " + "; ".join(gate.reasons))


def _startup_recovery(mode: str, store: TradingStore) -> GateResult:
    if mode == "live" and os.environ.get("BIAN_MARKET", "").strip().upper() != "FUTURES":
        raise SystemExit("live mode requires BIAN_MARKET=FUTURES")
    if mode == "live":
        expected = os.environ.get("LIVE_CONFIRMATION_TOKEN")
        confirmed = os.environ.get("BIAN_LIVE_CONFIRMATION")
        if not expected or confirmed != expected:
            raise SystemExit("live mode requires explicit confirmation token")
    store.initialize()
    symbols = _symbols(os.environ.get("BIAN_PAPER_SYMBOLS", "BTCUSDT"))
    client = None
    if mode != "paper":
        from binance_client import FuturesPrivateClient

        # The client constructor enforces mode-specific credentials and live
        # configuration before any authenticated request can be attempted.
        client = FuturesPrivateClient(ClientConfig.from_env(mode))
    preflight = evaluate_runtime_gate(
        mode=mode,
        store=store,
        client=client,
        symbols=symbols,
        probe_account=mode != "paper",
    )
    if mode == "live" and not preflight.confirmation_ok:
        raise SystemExit("runtime gate blocked live confirmation")
    if not preflight.credentials_ok or not preflight.account_reachable and mode != "paper":
        raise SystemExit("runtime gate blocked startup: " + "; ".join(preflight.reasons))
    if mode == "paper":
        result = Reconciler(store, mode=mode).recover()
    else:
        assert client is not None
        result = Reconciler(store, client=client, mode=mode).recover()
    if not result.safe_to_trade:
        raise SystemExit(f"startup reconciliation blocked trading: {result.status}")
    if mode in {"testnet", "live"} and not preflight.trading_enabled:
        raise SystemExit("runtime gate blocked startup: " + "; ".join(preflight.reasons))
    gate = replace(preflight, reconciliation_ok=result.safe_to_trade)
    if not result.safe_to_trade:
        gate = replace(
            gate,
            reasons=tuple(dict.fromkeys((*gate.reasons, "RECONCILIATION_NOT_VERIFIED"))),
        )
    if mode == "live" and not gate.live_allowed:
        raise SystemExit("runtime gate hard-blocked live: " + "; ".join(gate.reasons))
    if mode == "testnet" and not gate.data_health_ok:
        raise SystemExit("runtime gate blocked testnet data health")
    return gate


async def _run_private_forever(
    symbols: list[str],
    *,
    mode: str,
    store: TradingStore,
    executor: Executor,
    public_client: FuturesPublicClient,
    runtime_gate: GateResult,
) -> None:
    from user_stream import UserStreamClient

    reconciler = Reconciler(
        store,
        client=executor.client if isinstance(executor, BinanceExecutor) else None,
        mode=mode,
    )
    stream = UserStreamClient(
        ClientConfig.from_env(mode),
        on_event=lambda event: apply_user_stream_event(store, event),
        on_reconcile=lambda: reconciler.recover(),
    )
    stream_task = asyncio.create_task(stream.run_forever())
    interval = max(1, int(os.environ.get("BIAN_PAPER_POLL_SEC", "60")))
    try:
        while True:
            for symbol in symbols:
                try:
                    result = await asyncio.to_thread(
                        run_cycle,
                        symbol,
                        store=store,
                        executor=executor,
                        public_client=public_client,
                        mode=mode,
                        runtime_gate=runtime_gate,
                    )
                    print(result, flush=True)
                except Exception as exc:
                    print({"status": "cycle_failed", "symbol": symbol, "error": str(exc)}, flush=True)
            await asyncio.sleep(interval)
    finally:
        stream_task.cancel()
        await stream.close()
        await asyncio.gather(stream_task, return_exceptions=True)


def run_forever(symbols: list[str], *, mode: str | None = None) -> None:
    """Run the configured mode until interrupted."""
    resolved_mode = (mode or os.environ.get("BIAN_MODE", "paper")).strip().lower()
    if resolved_mode not in {"paper", "testnet", "live"}:
        raise SystemExit("BIAN_MODE must be paper, testnet, or live")
    store = TradingStore()
    runtime_gate = _startup_recovery(resolved_mode, store)
    client_config = ClientConfig.from_env(resolved_mode)
    public_client = FuturesPublicClient(client_config)
    executor = executor_from_env(
        store=store,
        config=ExecutionConfig.from_env(mode=resolved_mode),
        client_config=client_config,
    )
    if resolved_mode == "paper":
        interval = max(1, int(os.environ.get("BIAN_PAPER_POLL_SEC", "60")))
        while True:
            for symbol in symbols:
                try:
                    print(
                        run_cycle(
                            symbol,
                            store=store,
                            executor=executor,
                            public_client=public_client,
                            mode=resolved_mode,
                            runtime_gate=runtime_gate,
                        ),
                        flush=True,
                    )
                except Exception as exc:
                    print({"status": "cycle_failed", "symbol": symbol, "error": str(exc)}, flush=True)
            time.sleep(interval)
        return
    asyncio.run(
        _run_private_forever(
            symbols,
            mode=resolved_mode,
            store=store,
            executor=executor,
            public_client=public_client,
            runtime_gate=runtime_gate,
        )
    )


def run_shadow_forever(symbols: list[str]) -> None:
    """Record public positioning comparisons without entering the trade loop."""
    store = TradingStore()
    store.initialize()
    engine = StrategyEngine(StrategyConfig.from_env())
    interval = _int_env("POSITIONING_SHADOW_POLL_SEC", 60, minimum=1)
    while True:
        for symbol in symbols:
            try:
                print(run_shadow_cycle(symbol, store=store, engine=engine), flush=True)
            except Exception as exc:
                print(
                    {"status": "shadow_cycle_failed", "symbol": symbol, "error": str(exc)},
                    flush=True,
                )
        time.sleep(interval)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run bian paper trading.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="run one cycle per configured symbol and exit",
    )
    parser.add_argument(
        "--symbols",
        default=os.environ.get("BIAN_PAPER_SYMBOLS", "BTCUSDT"),
    )
    parser.add_argument("--mode", default=os.environ.get("BIAN_MODE", "paper"))
    parser.add_argument(
        "--attribution",
        action="store_true",
        help="calculate research-only Shadow attribution from persisted evidence",
    )
    shadow_mode = parser.add_mutually_exclusive_group()
    shadow_mode.add_argument(
        "--shadow-only",
        action="store_true",
        help="record one positioning shadow decision per symbol without execution",
    )
    shadow_mode.add_argument(
        "--shadow-forever",
        action="store_true",
        help="continuously record positioning shadow decisions without execution",
    )
    args = parser.parse_args(argv)
    symbols = _symbols(args.symbols)
    if args.attribution:
        store = TradingStore()
        strategy = StrategyEngine(StrategyConfig.from_env())
        for symbol in symbols:
            print(paper_shadow_attribution(symbol, store=store, engine=strategy), flush=True)
        return 0
    if args.shadow_only:
        store = TradingStore()
        store.initialize()
        strategy = StrategyEngine(StrategyConfig.from_env())
        for symbol in symbols:
            print(run_shadow_cycle(symbol, store=store, engine=strategy), flush=True)
        return 0
    if args.shadow_forever:
        run_shadow_forever(symbols)
        return 0
    if args.once:
        store = TradingStore()
        runtime_gate = _startup_recovery(args.mode, store)
        client_config = ClientConfig.from_env(args.mode)
        public_client = FuturesPublicClient(client_config)
        executor = executor_from_env(
            store=store,
            config=ExecutionConfig.from_env(mode=args.mode),
            client_config=client_config,
        )
        for symbol in symbols:
            print(
                run_cycle(
                    symbol,
                    store=store,
                    executor=executor,
                    public_client=public_client,
                    mode=args.mode,
                    runtime_gate=runtime_gate,
                ),
                flush=True,
            )
        return 0
    run_forever(symbols, mode=args.mode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
