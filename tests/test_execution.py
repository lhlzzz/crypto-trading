from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from execution import (
    BinanceExecutor,
    ExecutionConfig,
    ExecutionRejected,
    MarketSnapshot,
    PaperExecutor,
)
from binance_client import BinanceConnectionError, ClientConfig, FuturesRiskRules
from risk import ExchangeRules, FuturesAccountSnapshot, RiskContext, RiskGate
from trade_intent import TradeIntent


class MemoryStore:
    def __init__(self) -> None:
        self.intents: list[tuple[TradeIntent, str]] = []
        self.orders: dict[UUID, dict[str, object]] = {}
        self.events: list[tuple[UUID, str, str]] = []
        self.trades: list[dict[str, object]] = []
        self.positions: list[dict[str, object]] = []
        self.risk_events: list[dict[str, object]] = []
        self.balances: dict[str, dict[str, object]] = {}
        self.halted = False
        self.system_events: list[dict[str, object]] = []

    def initialize(self) -> None:
        return None

    def record_intent(self, intent: TradeIntent, *, status: str = "CREATED") -> None:
        self.intents.append((intent, status))

    def create_order(self, intent: TradeIntent, *, mode: str, status: str, **fields: object) -> UUID:
        order_id = UUID(int=len(self.orders) + 1)
        self.orders[order_id] = {
            "order_id": order_id,
            "intent_id": intent.id,
            "symbol": intent.symbol,
            "client_order_id": intent.client_order_id,
            "executed_quantity": Decimal("0"),
            "intent": intent,
            "side": intent.exchange_side(),
            "order_type": intent.order_type,
            "quantity": intent.quantity,
            "price": intent.price,
            "position_side": intent.direction,
            "position_action": intent.action,
            "reduce_only": intent.reduce_only,
            "leverage": intent.leverage,
            "mode": mode,
            "status": status,
            **fields,
        }
        return order_id

    def update_order(self, order_id: UUID, *, status: str, **fields: object) -> None:
        self.orders[order_id].update({"status": status, **fields})

    def append_order_event(
        self,
        order_id: UUID,
        *,
        event_type: str,
        status: str,
        payload: dict[str, object] | None = None,
    ) -> None:
        del payload
        self.events.append((order_id, event_type, status))

    def record_trade(self, order_id: UUID, **fields: object) -> UUID:
        self.trades.append({"order_id": order_id, **fields})
        return UUID(int=len(self.trades))

    def upsert_position(self, symbol: str, **fields: object) -> None:
        self.positions.append({"symbol": symbol, **fields})

    def get_position(self, symbol: str) -> dict[str, object] | None:
        for position in reversed(self.positions):
            if position["symbol"] == symbol:
                return position
        return None

    def list_positions(self, *, market: str = "FUTURES"):
        del market
        latest: dict[str, dict[str, object]] = {}
        for position in self.positions:
            latest[str(position["symbol"])] = position
        return list(latest.values())

    def upsert_balance(self, asset: str, **fields: object) -> None:
        self.balances[asset] = {"asset": asset, **fields}

    def get_balance(self, asset: str) -> dict[str, object] | None:
        return self.balances.get(asset)

    def record_system_event(self, **fields: object) -> UUID:
        self.system_events.append(fields)
        self.events.append((UUID(int=0), str(fields.get("event_type", "SYSTEM_EVENT")), str(fields)))
        return UUID(int=len(self.events))

    def is_halted(self) -> bool:
        return self.halted

    def set_halt(self, halted: bool, *, reason: str, source: str) -> None:
        self.halted = halted
        self.record_system_event(event_type="HALT", reason=reason, source=source)

    def get_order(self, order_id: UUID):
        return self.orders.get(order_id)

    def list_open_local_orders(self):
        return [
            order
            for order in self.orders.values()
            if order["status"] in {
                "CREATED",
                "RISK_APPROVED",
                "SUBMITTED",
                "ACKNOWLEDGED",
                "PARTIALLY_FILLED",
                "UNKNOWN",
            }
        ]

    def record_risk_event(self, **fields: object) -> UUID:
        self.risk_events.append(fields)
        return UUID(int=len(self.risk_events))

    def get_order_by_client_order_id(self, client_order_id: str):
        return next(
            (
                order
                for order in self.orders.values()
                if order["client_order_id"] == client_order_id
            ),
            None,
        )


def _intent(**updates: object) -> TradeIntent:
    values: dict[str, object] = {
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "action": "OPEN",
        "reduce_only": False,
        "leverage": Decimal("2"),
        "order_type": "MARKET",
        "quantity": Decimal("0.1"),
        "confidence": Decimal("0.8"),
        "reason": "test signal",
        "strategy_version": "test-1",
    }
    values.update(updates)
    return TradeIntent(**values)


def _market(**updates: object) -> MarketSnapshot:
    values: dict[str, object] = {
        "last_price": Decimal("100"),
        "mark_price": Decimal("100"),
        "index_price": Decimal("100"),
        "bid_price": Decimal("99.9"),
        "ask_price": Decimal("100.1"),
    }
    values.update(updates)
    return MarketSnapshot(**values)  # type: ignore[arg-type]


def _risk(intent: TradeIntent, **updates: object):
    values: dict[str, object] = {
        "wallet_balance": Decimal("1000"),
        "available_balance": Decimal("1000"),
        "equity": Decimal("1000"),
        "mark_price": Decimal("100"),
        "leverage": intent.leverage,
        "margin_type": "ISOLATED",
        "position_mode": "ONE_WAY",
        "data_quality_score": Decimal("1"),
        "liquidity_score": Decimal("1"),
        "positioning_confidence": Decimal("1"),
        "is_meme": True,
        "exchange_rules": ExchangeRules(
            symbol=intent.symbol,
            min_qty=Decimal("0.001"),
            step_size=Decimal("0.001"),
            tick_size=Decimal("0.01"),
            min_notional=Decimal("5"),
        ),
        "liquidation_price": Decimal("50"),
        "liquidation_distance_percent": Decimal("50"),
        "account_snapshot": FuturesAccountSnapshot(
            mode="paper",
            wallet_balance=Decimal("1000"),
            available_balance=Decimal("1000"),
            total_margin=Decimal("1000"),
            used_margin=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            realized_pnl=Decimal("0"),
            positions=(),
            open_orders=(),
            leverage={},
            margin_mode="ISOLATED",
            position_mode="ONE_WAY",
            captured_at=datetime.now(timezone.utc),
            source="test",
        ),
    }
    if intent.action != "OPEN":
        values["position_direction"] = intent.direction
        values["position_quantity"] = intent.quantity
        values["position_notional"] = intent.quantity * Decimal("100")
    values.update(updates)
    return RiskGate().evaluate(intent, RiskContext(**values))  # type: ignore[arg-type]


def test_paper_executor_runs_shared_order_lifecycle() -> None:
    store = MemoryStore()
    intent = _intent()
    result = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", fee_rate=Decimal("0.001"), slippage_bps=Decimal("0")),
    ).submit(intent, _risk(intent), market=_market())

    assert result.status == "FILLED"
    assert result.executed_quantity == Decimal("0.1")
    assert [event[1] for event in store.events][:5] == [
        "ORDER_CREATED",
        "ORDER_RISK_APPROVED",
        "ORDER_SUBMITTED",
        "ORDER_ACKNOWLEDGED",
        "ORDER_FILLED",
    ]
    assert len(store.trades) == 1
    assert store.get_position("BTCUSDT")["position_side"] == "LONG"
    assert store.get_position("BTCUSDT")["quantity"] == Decimal("0.1")
    assert store.get_balance("BTC") is None


def test_paper_account_snapshot_uses_shared_contract() -> None:
    executor = PaperExecutor(
        store=MemoryStore(),
        config=ExecutionConfig(mode="paper", initial_usdt=Decimal("250")),
    )

    snapshot = executor.account_snapshot()

    assert snapshot.mode == "paper"
    assert snapshot.source == "paper_executor"
    assert snapshot.wallet_balance == Decimal("250")
    assert snapshot.available_balance == Decimal("250")
    assert snapshot.margin_mode == "ISOLATED"
    assert snapshot.position_mode == "ONE_WAY"
    assert snapshot.fresh is True
    assert snapshot.symbol_leverage == {}
    assert snapshot.leverage == {}


def test_paper_account_snapshot_separates_configured_and_position_leverage() -> None:
    store = MemoryStore()
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(
            mode="paper",
            fee_rate=Decimal("0"),
            slippage_bps=Decimal("0"),
            default_leverage=Decimal("1"),
        ),
    )
    intent = _intent(leverage=Decimal("2"))
    executor.submit(intent, _risk(intent), market=_market())

    snapshot = executor.account_snapshot()

    assert snapshot.leverage["BTCUSDT"] == Decimal("2")
    assert snapshot.symbol_leverage["BTCUSDT"] == Decimal("1")


def test_paper_open_and_close_long() -> None:
    store = MemoryStore()
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", fee_rate=Decimal("0.001"), slippage_bps=Decimal("0")),
    )
    open_intent = _intent()
    executor.submit(open_intent, _risk(open_intent), market=_market())
    close_intent = _intent(action="CLOSE", reduce_only=True)
    result = executor.submit(
        close_intent,
        _risk(close_intent),
        market=_market(last_price=Decimal("110"), mark_price=Decimal("110"), bid_price=Decimal("110"), ask_price=Decimal("110.1")),
    )

    position = store.get_position("BTCUSDT")
    assert result.status == "FILLED"
    assert position["position_side"] == "FLAT"
    assert position["quantity"] == Decimal("0")
    assert position["realized_pnl"] > 0
    assert "BTC" not in store.balances


def test_paper_exit_remains_possible_while_store_is_halted() -> None:
    store = MemoryStore()
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(
            mode="paper",
            fee_rate=Decimal("0"),
            slippage_bps=Decimal("0"),
        ),
    )
    opened = _intent(quantity=Decimal("0.1"))
    executor.submit(opened, _risk(opened), market=_market())
    store.halted = True

    close = _intent(
        action="CLOSE",
        reduce_only=True,
        quantity=Decimal("0.1"),
        positioning_state="UNKNOWN",
        meme_risk_tier="BLOCK",
    )
    decision = _risk(
        close,
        halted=True,
        evidence_conflict=True,
        data_quality_score=Decimal("0"),
        liquidity_score=Decimal("0"),
        positioning_confidence=Decimal("0"),
        meme_risk_tier="BLOCK",
    )

    result = executor.submit(close, decision, market=_market())

    assert decision.decision == "ALLOW"
    assert result.status == "FILLED"


def test_paper_close_realized_pnl_updates_wallet_and_equity() -> None:
    store = MemoryStore()
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", fee_rate=Decimal("0"), slippage_bps=Decimal("0")),
    )
    opened = _intent(quantity=Decimal("1"), leverage=Decimal("2"))
    executor.submit(opened, _risk(opened), market=_market(last_price=Decimal("100")))
    closed = _intent(
        action="CLOSE",
        reduce_only=True,
        quantity=Decimal("1"),
        created_at=datetime(2026, 8, 1, 0, 1, tzinfo=timezone.utc),
    )
    result = executor.submit(
        closed,
        _risk(closed, position_quantity=Decimal("1"), position_notional=Decimal("110")),
        market=_market(last_price=Decimal("110"), mark_price=Decimal("110"), bid_price=Decimal("110"), ask_price=Decimal("110")),
    )

    assert result.status == "FILLED"
    account = executor.account_state()
    assert account["wallet_balance"] == Decimal("1009.9")
    assert account["equity"] == Decimal("1009.9")
    assert account["available_balance"] == Decimal("1009.9")


def test_paper_open_and_close_short() -> None:
    store = MemoryStore()
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", fee_rate=Decimal("0"), slippage_bps=Decimal("0")),
    )
    open_intent = _intent(direction="SHORT")
    executor.submit(open_intent, _risk(open_intent), market=_market())
    close_intent = _intent(direction="SHORT", action="CLOSE", reduce_only=True)
    executor.submit(
        close_intent,
        _risk(close_intent),
        market=_market(last_price=Decimal("90"), mark_price=Decimal("90"), bid_price=Decimal("89.9"), ask_price=Decimal("90")),
    )
    position = store.get_position("BTCUSDT")
    assert position["position_side"] == "FLAT"
    assert position["realized_pnl"] > 0


def test_paper_partial_reduce() -> None:
    store = MemoryStore()
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", fee_rate=Decimal("0"), slippage_bps=Decimal("0")),
    )
    open_intent = _intent(quantity=Decimal("0.2"))
    executor.submit(open_intent, _risk(open_intent), market=_market())
    reduce_intent = _intent(action="REDUCE", reduce_only=True, quantity=Decimal("0.1"))
    executor.submit(
        reduce_intent,
        _risk(reduce_intent, position_quantity=Decimal("0.2")),
        market=_market(),
    )
    position = store.get_position("BTCUSDT")
    assert position["position_side"] == "LONG"
    assert position["quantity"] == Decimal("0.1")


def test_paper_executor_supports_partial_fill() -> None:
    store = MemoryStore()
    intent = _intent()
    result = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", partial_fill_ratio=Decimal("0.5")),
    ).submit(intent, _risk(intent), market=_market())

    assert result.status == "PARTIALLY_FILLED"
    assert result.executed_quantity == Decimal("0.05")
    assert store.orders[result.order_id]["status"] == "PARTIALLY_FILLED"


def test_paper_low_liquidity_cannot_fully_fill() -> None:
    store = MemoryStore()
    intent = _intent()
    result = PaperExecutor(store=store).submit(
        intent,
        _risk(intent),
        market=_market(available_liquidity_notional_usdt=Decimal("4")),
    )
    assert result.status == "PARTIALLY_FILLED"
    assert result.executed_quantity == Decimal("0.03996003996003996003996003996")


def test_paper_executor_rejects_denied_risk_decision() -> None:
    store = MemoryStore()
    intent = _intent()
    denied = _risk(intent, halted=True)

    with pytest.raises(ExecutionRejected, match="halted"):
        PaperExecutor(store=store).submit(intent, denied, market=_market())

    assert store.orders == {}
    assert store.risk_events[0]["decision"] == "HALT"


def test_paper_limit_order_can_rest_and_expire() -> None:
    store = MemoryStore()
    intent = _intent(order_type="LIMIT", quantity=Decimal("0.1"), price=Decimal("99"))
    result = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", order_expiry_sec=10),
    ).submit(intent, _risk(intent), market=_market())

    assert result.status == "ACKNOWLEDGED"
    order = store.orders[result.order_id]
    order["expires_at"] = datetime.now(timezone.utc).replace(year=2020)

    recovered = PaperExecutor(store=store).get_open_orders()

    assert recovered == []
    assert store.orders[result.order_id]["status"] == "EXPIRED"


def test_paper_partial_order_can_fill_on_a_later_market_cycle() -> None:
    store = MemoryStore()
    intent = _intent(order_type="LIMIT", price=Decimal("100.1"))
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", partial_fill_ratio=Decimal("0.5")),
    )
    first = executor.submit(
        intent,
        _risk(intent),
        market=_market(available_liquidity_notional_usdt=Decimal("10")),
    )
    second = executor.process_market(
        first.order_id,
        _market(available_liquidity_notional_usdt=Decimal("10")),
    )
    assert first.status == "PARTIALLY_FILLED"
    assert second.status == "FILLED"
    assert second.executed_quantity == Decimal("0.1")
    assert len(store.trades) == 2


def test_paper_duplicate_submit_is_idempotent() -> None:
    store = MemoryStore()
    intent = _intent()
    executor = PaperExecutor(store=store)
    first = executor.submit(intent, _risk(intent), market=_market())
    second = executor.submit(intent, _risk(intent), market=_market())
    assert second.order_id == first.order_id
    assert len(store.trades) == 1


def test_paper_restart_recovers_partial_order_without_duplicate_trade() -> None:
    store = MemoryStore()
    intent = _intent(order_type="LIMIT", price=Decimal("100.1"))
    first_executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", partial_fill_ratio=Decimal("0.5")),
    )
    first = first_executor.submit(
        intent,
        _risk(intent),
        market=_market(available_liquidity_notional_usdt=Decimal("10")),
    )
    restarted_executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", partial_fill_ratio=Decimal("0.5")),
    )
    recovered = restarted_executor.recover()
    final = restarted_executor.process_market(
        first.order_id,
        _market(available_liquidity_notional_usdt=Decimal("10")),
    )
    assert len(recovered) == 1
    assert recovered[0].status == "PARTIALLY_FILLED"
    assert final.status == "FILLED"
    assert len(store.trades) == 2


def test_paper_mark_to_market_uses_mark_price() -> None:
    store = MemoryStore()
    intent = _intent()
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", fee_rate=Decimal("0"), slippage_bps=Decimal("0")),
    )
    executor.submit(intent, _risk(intent), market=_market(ask_price=Decimal("100"), bid_price=Decimal("100")))
    executor.mark_to_market("BTCUSDT", Decimal("110"))
    position = store.get_position("BTCUSDT")
    assert position is not None
    assert position["unrealized_pnl"] == Decimal("1.0")
    assert position["mark_price"] == Decimal("110")


def test_paper_funding_is_recorded_separately() -> None:
    store = MemoryStore()
    intent = _intent()
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", fee_rate=Decimal("0"), slippage_bps=Decimal("0")),
    )
    executor.submit(intent, _risk(intent), market=_market(ask_price=Decimal("100"), bid_price=Decimal("100")))
    payment = executor.apply_funding(
        "BTCUSDT",
        _market(
            funding_rate=Decimal("0.01"),
            funding_timestamp=datetime(2026, 8, 29, tzinfo=timezone.utc),
            settlement_timestamp=datetime(2026, 8, 29, tzinfo=timezone.utc),
        ),
    )
    position = store.get_position("BTCUSDT")
    assert payment < 0
    assert position["funding_pnl"] == payment
    assert executor.account_state()["funding_pnl"] == payment


def test_paper_funding_is_not_double_recorded_after_mark() -> None:
    store = MemoryStore()
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", fee_rate=Decimal("0"), slippage_bps=Decimal("0")),
    )
    intent = _intent(quantity=Decimal("1"), leverage=Decimal("2"))
    executor.submit(intent, _risk(intent), market=_market(last_price=Decimal("100")))
    funding_market = _market(
        last_price=Decimal("100"),
        funding_rate=Decimal("0.01"),
        funding_timestamp=datetime(2026, 8, 29, tzinfo=timezone.utc),
        settlement_timestamp=datetime(2026, 8, 29, tzinfo=timezone.utc),
    )

    first = executor.apply_funding("BTCUSDT", funding_market)
    executor.mark_to_market("BTCUSDT", Decimal("100"))
    second = executor.apply_funding("BTCUSDT", funding_market)

    assert first == Decimal("-1")
    assert second == Decimal("0")
    assert executor.account_state()["funding_pnl"] == Decimal("-1")
    settlements = [
        event for event in store.system_events
        if event.get("event_type") == "FUNDING_SETTLED"
    ]
    assert len(settlements) == 1
    assert settlements[0]["payload"]["funding_timestamp"] == "2026-08-29T00:00:00+00:00"


def test_paper_liquidation_halts() -> None:
    store = MemoryStore()
    intent = _intent(leverage=Decimal("5"), quantity=Decimal("1"))
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(
            mode="paper",
            fee_rate=Decimal("0"),
            slippage_bps=Decimal("0"),
            risk_rules=FuturesRiskRules(
                symbol="BTCUSDT", maintenance_margin_rate=Decimal("0.5")
            ),
            initial_usdt=Decimal("1000"),
        ),
    )
    executor.submit(
        intent,
        _risk(intent, available_balance=Decimal("1000")),
        market=_market(ask_price=Decimal("100"), bid_price=Decimal("100")),
    )
    executor.mark_to_market("BTCUSDT", Decimal("40"))
    assert store.halted is True
    assert store.get_position("BTCUSDT")["position_side"] == "FLAT"
    assert any(event.get("event_type") == "LIQUIDATED" for event in store.system_events)


def test_paper_fees_are_deducted() -> None:
    store = MemoryStore()
    intent = _intent()
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", fee_rate=Decimal("0.001"), slippage_bps=Decimal("0")),
    )
    result = executor.submit(intent, _risk(intent), market=_market(ask_price=Decimal("100"), bid_price=Decimal("100")))
    assert result.fee > 0
    assert executor.account_state()["wallet_balance"] < Decimal("1000")


def test_paper_limit_cancel_has_no_trade_side_effect() -> None:
    store = MemoryStore()
    intent = _intent(order_type="LIMIT", price=Decimal("99"))
    executor = PaperExecutor(store=store)
    result = executor.submit(intent, _risk(intent), market=_market())
    cancelled = executor.cancel(result.order_id)
    assert cancelled.status == "CANCELLED"
    assert store.trades == []


def test_paper_rejects_when_margin_is_insufficient() -> None:
    store = MemoryStore()
    intent = _intent(quantity=Decimal("0.1"), leverage=Decimal("1"))
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", initial_usdt=Decimal("1")),
    )
    with pytest.raises(ExecutionRejected, match="margin"):
        executor.submit(intent, _risk(intent, available_balance=Decimal("1")), market=_market())
    assert store.orders == {}


def test_paper_equity_identity_after_open() -> None:
    store = MemoryStore()
    intent = _intent()
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", fee_rate=Decimal("0.001"), slippage_bps=Decimal("0")),
    )
    executor.submit(intent, _risk(intent), market=_market(ask_price=Decimal("100"), bid_price=Decimal("100")))
    account = executor.account_state()
    assert account["equity"] == account["wallet_balance"] + account["unrealized_pnl"]
    assert account["used_margin"] <= account["equity"]


def test_reduce_only_cannot_increase_position() -> None:
    store = MemoryStore()
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", fee_rate=Decimal("0"), slippage_bps=Decimal("0")),
    )
    open_intent = _intent(quantity=Decimal("0.1"))
    executor.submit(open_intent, _risk(open_intent), market=_market())
    reduce_intent = _intent(action="REDUCE", reduce_only=True, quantity=Decimal("0.05"))
    executor.submit(
        reduce_intent,
        _risk(reduce_intent, position_quantity=Decimal("0.1")),
        market=_market(),
    )
    assert store.get_position("BTCUSDT")["quantity"] == Decimal("0.05")


def test_futures_observation_to_paper_position_path() -> None:
    from datetime import timezone
    from engine import CurrentPosition, MarketFrame, SourceFreshness, StrategyConfig, StrategyEngine

    captured = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
    source_timestamps = {
        source: {
            "source_timestamp": captured.isoformat(),
            "received_timestamp": captured.isoformat(),
            "latency_ms": 0,
        }
        for source in (
            "futures_open_interest",
            "futures_funding",
            "futures_trade_flow",
            "futures_taker_ratio",
            "futures_mark_price",
            "futures_orderbook",
            "futures_book_ticker",
        )
    }
    frame = MarketFrame(
        symbol="BTCUSDT",
        closes=(Decimal("100"), Decimal("101")),
        captured_at=captured,
            net_spot_flow=Decimal("8"),
            futures_trade_flow=Decimal("8"),
            cvd_change=Decimal("8"),
        taker_buy_volume=Decimal("10"),
        taker_sell_volume=Decimal("3"),
        oi_change=Decimal("0.03"),
        funding_rate=Decimal("0.0001"),
        spread_bps=Decimal("2"),
        depth_25bps=Decimal("100"),
        market_regime="RISK_ON",
        meme_risk_tier="TRADEABLE",
        evidence_status={"orderbook": "VALID"},
        is_meme=True,
        freshness=(SourceFreshness("futures_trade_flow", captured, captured, 900, captured),),
        source_timestamps=source_timestamps,
    )
    engine = StrategyEngine(StrategyConfig(positioning_decision_enabled=True))
    intent = engine.evaluate(frame, current_position=CurrentPosition())
    assert intent is not None
    assert intent.direction == "LONG"
    assert intent.action == "OPEN"
    store = MemoryStore()
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", fee_rate=Decimal("0"), slippage_bps=Decimal("0")),
    )
    result = executor.submit(intent, _risk(intent), market=_market(ask_price=Decimal("100"), bid_price=Decimal("100")))
    assert result.status == "FILLED"
    position = store.get_position("BTCUSDT")
    assert position["position_side"] == "LONG"
    assert position["quantity"] > 0


def test_order_timeout_never_resubmits(monkeypatch) -> None:
    class Client:
        def __init__(self):
            self.create_calls = 0
            self.lookup_calls = 0

        def create_order(self, **kwargs):
            del kwargs
            self.create_calls += 1
            raise TimeoutError("timed out")

        def get_order(self, **kwargs):
            del kwargs
            self.lookup_calls += 1
            return {"orderId": "42", "status": "FILLED", "executedQty": "0.1"}

    client = Client()
    monkeypatch.setattr("execution.FuturesPrivateClient", lambda config: client)
    store = MemoryStore()
    intent = _intent()
    executor = BinanceExecutor(
        store=store,
        config=ExecutionConfig(mode="testnet"),
        client_config=ClientConfig(mode="testnet", api_key="key", api_secret="secret"),
    )

    result = executor.submit(intent, _risk(intent), market=_market())

    assert result.status == "FILLED"
    assert client.create_calls == 1
    assert client.lookup_calls == 1
    assert store.orders[result.order_id]["exchange_order_id"] == "42"


def test_order_5xx_never_resubmits_and_halts_when_unresolved(monkeypatch) -> None:
    class Client:
        def __init__(self):
            self.create_calls = 0
            self.lookup_calls = 0

        def create_order(self, **kwargs):
            del kwargs
            self.create_calls += 1
            raise BinanceConnectionError("server failure")

        def get_order(self, **kwargs):
            del kwargs
            self.lookup_calls += 1
            raise BinanceConnectionError("lookup unavailable")

    client = Client()
    monkeypatch.setattr("execution.FuturesPrivateClient", lambda config: client)
    store = MemoryStore()
    intent = _intent()
    executor = BinanceExecutor(
        store=store,
        config=ExecutionConfig(mode="testnet"),
        client_config=ClientConfig(mode="testnet", api_key="key", api_secret="secret"),
    )

    result = executor.submit(intent, _risk(intent), market=_market())

    assert result.status == "UNKNOWN"
    assert client.create_calls == 1
    assert client.lookup_calls == 1
    assert store.halted is True
    assert store.orders[result.order_id]["status"] == "UNKNOWN"
