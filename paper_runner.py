"""Paper-mode orchestration: market data -> engine -> risk -> execution."""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, replace
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

from backtesting import replay_positioning_frames
from engine import CurrentPosition, MarketFrame, PositioningDecision, StrategyConfig, StrategyEngine
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
from risk import ExchangeRules, FuturesAccountSnapshot, RiskContext, RiskGate, RiskLimits
from runtime_gate import GateResult, evaluate_runtime_gate, max_data_age_sec
from scripts import bian_market
from trade_intent import TradeIntent
from trading_store import TradingStore


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


def _record_shadow(
    frame: MarketFrame,
    *,
    store: TradingStore,
    engine: StrategyEngine,
 ) -> tuple[PositioningDecision, dict[str, Any]]:
    """Persist the positioning decision before any intent can exist."""
    previous_state = store.latest_positioning_state(
        frame.symbol, before=frame.captured_at
    )
    positioning = engine.positioning_decision(
        frame,
        now=frame.captured_at,
        previous_state=previous_state,
    )
    positioning = _with_persisted_episode(positioning, store=store, engine=engine)
    store.record_positioning_snapshot(
        positioning,
        strategy_version=engine.config.positioning_strategy_version,
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


def _episode_config_hash(engine: StrategyEngine) -> str:
    payload = json.dumps(asdict(engine.config), default=str, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _episode_uuid(row: dict[str, Any]) -> UUID:
    return UUID(str(row["episode_id"]))


def _episode_timestamp(row: dict[str, Any], field: str) -> datetime:
    value = row[field]
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _with_persisted_episode(
    decision: PositioningDecision,
    *,
    store: TradingStore,
    engine: StrategyEngine,
) -> PositioningDecision:
    """Attach the Store-owned lifecycle to a pure positioning decision."""
    active = store.get_active_episode(decision.symbol)
    direction = engine.episode_direction(decision.state)
    metadata = {
        "positioning_state": decision.state,
        "reason_codes": list(decision.reason_codes),
    }
    if decision.state in {"UNKNOWN", "CONFLICTED"}:
        if active is None:
            return decision
        episode = store.update_episode(
            _episode_uuid(active),
            state=decision.state,
            status="UNRESOLVED",
            observed_at=decision.timestamp,
            metadata=metadata,
        )
        return replace(
            decision,
            episode_id=_episode_uuid(episode),
            episode_direction=episode["direction"],
            episode_status="UNRESOLVED",
            episode_started_at=_episode_timestamp(episode, "started_at"),
        )
    if decision.state in {"LONG_UNWIND", "SHORT_COVERING", "FORCED_DELEVERAGING"}:
        if active is None:
            return decision
        episode = store.close_episode(
            _episode_uuid(active),
            state=decision.state,
            observed_at=decision.timestamp,
            metadata=metadata,
        )
        return replace(
            decision,
            episode_id=_episode_uuid(episode),
            episode_direction=episode["direction"],
            episode_status="CLOSED",
            episode_started_at=_episode_timestamp(episode, "started_at"),
            episode_ended_at=_episode_timestamp(episode, "ended_at"),
        )
    if direction == "FLAT":
        if active is None:
            return decision
        return replace(
            decision,
            episode_id=_episode_uuid(active),
            episode_direction=active["direction"],
            episode_status=active["status"],
            episode_started_at=_episode_timestamp(active, "started_at"),
        )
    if active is not None and active["direction"] != direction:
        store.close_episode(
            _episode_uuid(active),
            state="CONFIRMED_REVERSAL",
            observed_at=decision.timestamp,
            metadata=metadata,
        )
        active = None
    if active is None:
        episode = store.start_episode(
            symbol=decision.symbol,
            direction=direction,
            state=decision.state,
            observed_at=decision.timestamp,
            strategy_version=engine.config.positioning_strategy_version,
            config_hash=_episode_config_hash(engine),
            metadata=metadata,
        )
    else:
        episode = store.update_episode(
            _episode_uuid(active),
            state=decision.state,
            status="OPEN",
            observed_at=decision.timestamp,
            metadata=metadata,
        )
    return replace(
        decision,
        episode_id=_episode_uuid(episode),
        episode_direction=episode["direction"],
        episode_status=episode["status"],
        episode_started_at=_episode_timestamp(episode, "started_at"),
    )


def run_shadow_cycle(
    symbol: str,
    *,
    store: TradingStore | None = None,
    engine: StrategyEngine | None = None,
) -> dict[str, Any]:
    """Persist a positioning shadow decision without Risk or execution calls."""
    store = store or TradingStore()
    engine = engine or StrategyEngine(StrategyConfig.from_env())
    frame = store.latest_market_observation(
        symbol,
        source_ttl_sec=engine.config.source_ttl_sec,
    )
    positioning, shadow = _record_shadow(
        frame,
        store=store,
        engine=engine,
    )
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


def _current_position(
    snapshot: FuturesAccountSnapshot,
    symbol: str,
) -> CurrentPosition:
    position = next(
        (
            dict(row)
            for row in snapshot.positions
            if str(row.get("symbol", "")).upper() == symbol.upper()
        ),
        None,
    )
    if position is None:
        return CurrentPosition()
    raw_quantity = Decimal(str(
        position.get("quantity", position.get("positionAmt", "0"))
    ))
    quantity = abs(raw_quantity)
    side = str(
        position.get("position_side", position.get("positionSide", ""))
    ).upper()
    if quantity == 0:
        side = "FLAT"
    elif side in {"BOTH", ""}:
        side = "LONG" if raw_quantity > 0 else "SHORT" if raw_quantity < 0 else "FLAT"
    elif side not in {"LONG", "SHORT"}:
        raise ValueError(f"unrecognized futures position side: {side}")
    leverage = Decimal(str(position.get("leverage") or "1"))
    entry = position.get("entry_price") or position.get("average_price")
    return CurrentPosition(
        direction=side,  # type: ignore[arg-type]
        quantity=quantity,
        entry_price=Decimal(str(entry)) if entry is not None else None,
        leverage=leverage if leverage > 0 else Decimal("1"),
    )


def _risk_context(
    intent: TradeIntent,
    market: MarketSnapshot,
    *,
    account_snapshot: FuturesAccountSnapshot,
    mode: str,
    public_client: FuturesPublicClient | None = None,
    paper_executor: PaperExecutor | None = None,
) -> RiskContext:
    if not isinstance(account_snapshot, FuturesAccountSnapshot):
        raise RuntimeError("canonical account snapshot is required")
    account_state_error: str | None = None
    try:
        current = _current_position(account_snapshot, intent.symbol)
    except ValueError as exc:
        current = CurrentPosition()
        account_state_error = str(exc)
    position = next(
        (
            dict(row)
            for row in account_snapshot.positions
            if str(row.get("symbol", "")).upper() == intent.symbol.upper()
        ),
        {},
    )
    quantity = current.quantity
    direction = current.direction
    mark = market.mark_price
    if mode in {"testnet", "live"}:
        if public_client is None:
            raise RuntimeError("Binance Futures exchange rules are required")
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
        if rules.min_qty <= 0 or rules.step_size <= 0 or rules.tick_size <= 0:
            raise RuntimeError("Binance Futures exchange rules are incomplete")
    else:
        rules = ExchangeRules(
            symbol=intent.symbol,
            status="TRADING",
            min_qty=_decimal_env("PAPER_MIN_QTY", "0.001"),
            step_size=_decimal_env("PAPER_STEP_SIZE", "0.001"),
            tick_size=_decimal_env("PAPER_TICK_SIZE", "0.01"),
            min_notional=_decimal_env("PAPER_MIN_NOTIONAL", "5"),
        )
        if rules.min_qty <= 0 or rules.step_size <= 0 or rules.tick_size <= 0:
            raise RuntimeError("paper exchange rules must be explicit and positive")
    liquidation_price = (
        Decimal(str(position["liquidation_price"]))
        if position.get("liquidation_price") is not None
        else None
    )
    if (
        liquidation_price is None
        and intent.action == "OPEN"
        and direction == "FLAT"
        and mode == "paper"
        and mark is not None
    ):
        if paper_executor is None:
            raise RuntimeError("paper liquidation model is unavailable")
        liquidation_price = paper_executor.liquidation_price_for(
            intent.symbol,
            intent.direction,
            mark,
            intent.leverage,
        )
    return RiskContext(
        mode=mode,  # type: ignore[arg-type]
        wallet_balance=account_snapshot.wallet_balance,
        available_balance=account_snapshot.available_balance,
        equity=account_snapshot.total_margin,
        used_margin=account_snapshot.used_margin,
        position_direction=direction,  # type: ignore[arg-type]
        position_quantity=quantity,
        position_notional=abs(quantity * mark) if mark is not None else None,
        entry_price=(
            Decimal(str(position["entry_price"]))
            if position.get("entry_price") is not None
            else Decimal(str(position.get("average_price") or "0"))
        ),
        mark_price=mark,
        index_price=market.index_price,
        unrealized_pnl=account_snapshot.unrealized_pnl,
        realized_pnl=account_snapshot.realized_pnl,
        funding_pnl=Decimal("0"),
        leverage=Decimal(str(position.get("leverage") or intent.leverage)),
        account_leverage=(
            Decimal(str(account_snapshot.symbol_leverage[intent.symbol.upper()]))
            if intent.symbol.upper() in account_snapshot.symbol_leverage
            else None
        ),
        margin_type=account_snapshot.margin_mode,
        position_mode=account_snapshot.position_mode,
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
        is_meme=intent.is_meme,
        meme_require_classification=os.environ.get(
            "MEME_REQUIRE_CLASSIFICATION", "true"
        ).strip().lower() in {"1", "true", "yes", "on"},
        account_snapshot=account_snapshot,
        account_state_error=account_state_error,
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
    frame = store.latest_market_observation(
        symbol,
        source_ttl_sec=engine.config.source_ttl_sec,
    )
    market = _market_snapshot(frame)
    try:
        positioning, shadow = _record_shadow(frame, store=store, engine=engine)
    except Exception as exc:
        try:
            store.set_halt(
                True,
                reason=f"positioning evidence persistence failed: {type(exc).__name__}",
                source="paper_runner",
            )
        except Exception as halt_exc:
            raise RuntimeError(
                "positioning evidence persistence failed and halt recording failed"
            ) from halt_exc
        raise RuntimeError("positioning evidence persistence failed; trading halted") from exc
    injected_intent = _test_only_signal_injection(frame, mode=mode)
    if injected_intent is not None:
        injected_intent = injected_intent.model_copy(
            update={"evidence_snapshot_id": positioning.evidence_snapshot_id}
        )
    try:
        account_snapshot = executor.account_snapshot()
        current = _current_position(account_snapshot, symbol)
    except ValueError as exc:
        reason = f"INVALID_ACCOUNT_STATE: {exc}"
        store.set_halt(True, reason=reason, source="paper_runner")
        return {"status": "halted", "symbol": symbol.upper(), "reason": reason}
    except Exception as exc:
        reason = f"ACCOUNT_UNAVAILABLE: {type(exc).__name__}"
        store.set_halt(True, reason=reason, source="paper_runner")
        return {"status": "halted", "symbol": symbol.upper(), "reason": reason}
    intent = injected_intent
    if intent is None and engine.config.positioning_decision_enabled:
        intent = engine._intent_from_positioning(
            positioning,
            frame,
            current_position=current,
        )
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
                "shadow": positioning.as_dict(),
            },
        )
        return {
            "status": "no_signal",
            "symbol": symbol,
            "captured_at": frame.captured_at.isoformat(),
        }
    store.record_intent(intent, status="CREATED")
    decision = risk_gate.evaluate(
        intent,
        _risk_context(
            intent,
            market,
            account_snapshot=account_snapshot,
            mode=mode,
            public_client=public_client,
            paper_executor=executor if isinstance(executor, PaperExecutor) else None,
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
        "shadow": positioning.as_dict(),
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
