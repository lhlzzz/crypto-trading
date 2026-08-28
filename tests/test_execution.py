from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from execution import (
    ExecutionConfig,
    ExecutionRejected,
    MarketSnapshot,
    PaperExecutor,
)
from risk import RiskContext, RiskGate
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

    def initialize(self) -> None:
        return None

    def record_intent(self, intent: TradeIntent, *, status: str = "CREATED") -> None:
        self.intents.append((intent, status))

    def create_order(self, intent: TradeIntent, *, mode: str, status: str, **fields: object) -> UUID:
        order_id = UUID(int=len(self.orders) + 1)
        self.orders[order_id] = {
            "order_id": order_id,
            "intent_id": intent.id,
            "client_order_id": intent.client_order_id,
            "executed_quantity": Decimal("0"),
            "intent": intent,
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

    def upsert_balance(self, asset: str, **fields: object) -> None:
        self.balances[asset] = {"asset": asset, **fields}

    def get_balance(self, asset: str) -> dict[str, object] | None:
        return self.balances.get(asset)

    def record_system_event(self, **fields: object) -> UUID:
        self.events.append((UUID(int=0), "SYSTEM_EVENT", str(fields)))
        return UUID(int=len(self.events))

    def is_halted(self) -> bool:
        return self.halted

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
        "side": "BUY",
        "order_type": "MARKET",
        "quantity": Decimal("0.1"),
        "confidence": Decimal("0.8"),
        "reason": "test signal",
        "strategy_version": "test-1",
    }
    values.update(updates)
    return TradeIntent(**values)


def _risk(intent: TradeIntent):
    return RiskGate().evaluate(
        intent,
        RiskContext(
            available_quote_usdt=Decimal("1000"),
            market_price=Decimal("100"),
        ),
    )


def test_paper_executor_runs_shared_order_lifecycle() -> None:
    store = MemoryStore()
    intent = _intent()
    result = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", fee_rate=Decimal("0.001")),
    ).submit(
        intent,
        _risk(intent),
        market=MarketSnapshot(
            last_price=Decimal("100"),
            bid_price=Decimal("99.9"),
            ask_price=Decimal("100.1"),
        ),
    )

    assert result.status == "FILLED"
    assert result.executed_quantity == Decimal("0.1")
    assert [event[1] for event in store.events] == [
        "ORDER_CREATED",
        "ORDER_RISK_APPROVED",
        "ORDER_SUBMITTED",
        "ORDER_ACKNOWLEDGED",
        "ORDER_FILLED",
    ]
    assert len(store.trades) == 1
    assert store.positions[-1]["quantity"] == Decimal("0.1")


def test_paper_executor_supports_partial_fill() -> None:
    store = MemoryStore()
    intent = _intent()
    result = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", partial_fill_ratio=Decimal("0.5")),
    ).submit(intent, _risk(intent), market=MarketSnapshot(last_price=Decimal("100")))

    assert result.status == "PARTIALLY_FILLED"
    assert result.executed_quantity == Decimal("0.05")
    assert store.events[-1][1] == "ORDER_PARTIALLY_FILLED"


def test_paper_executor_converts_quote_quantity_using_market_price() -> None:
    store = MemoryStore()
    intent = _intent(quantity=None, quote_quantity=Decimal("10"))
    result = PaperExecutor(store=store).submit(
        intent,
        _risk(intent),
        market=MarketSnapshot(last_price=Decimal("100")),
    )

    assert result.status == "FILLED"
    assert result.executed_quantity == Decimal("0.1")


def test_paper_executor_rejects_denied_risk_decision() -> None:
    store = MemoryStore()
    intent = _intent()
    denied = RiskGate().evaluate(
        intent,
        RiskContext(available_quote_usdt=Decimal("1000"), halted=True),
    )

    with pytest.raises(ExecutionRejected, match="halted"):
        PaperExecutor(store=store).submit(
            intent,
            denied,
            market=MarketSnapshot(last_price=Decimal("100")),
        )

    assert store.orders == {}
    assert store.risk_events[0]["decision"] == "HALT"


def test_paper_limit_order_can_rest_and_expire() -> None:
    store = MemoryStore()
    intent = _intent(
        order_type="LIMIT",
        quantity=Decimal("0.1"),
        quote_quantity=None,
        price=Decimal("99"),
    )
    result = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", order_expiry_sec=10),
    ).submit(
        intent,
        _risk(intent),
        market=MarketSnapshot(last_price=Decimal("100"), ask_price=Decimal("100")),
    )

    assert result.status == "ACKNOWLEDGED"
    assert store.events[-1][1] == "ORDER_ACKNOWLEDGED"
    order = store.orders[result.order_id]
    order["expires_at"] = datetime.now(timezone.utc).replace(year=2020)

    recovered = PaperExecutor(store=store).get_open_orders()

    assert recovered == []
    assert store.orders[result.order_id]["status"] == "EXPIRED"
    assert store.events[-1][1] == "ORDER_EXPIRED"


def test_paper_partial_order_can_fill_on_a_later_market_cycle() -> None:
    store = MemoryStore()
    intent = _intent(order_type="LIMIT", price=Decimal("100"))
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", partial_fill_ratio=Decimal("0.5")),
    )

    first = executor.submit(
        intent,
        _risk(intent),
        market=MarketSnapshot(last_price=Decimal("100"), available_liquidity=Decimal("0.1")),
    )
    second = executor.process_market(
        first.order_id,
        MarketSnapshot(last_price=Decimal("100"), available_liquidity=Decimal("0.1")),
    )

    assert first.status == "PARTIALLY_FILLED"
    assert second.status == "FILLED"
    assert second.executed_quantity == Decimal("0.1")
    assert len(store.trades) == 2


def test_paper_duplicate_submit_is_idempotent() -> None:
    store = MemoryStore()
    intent = _intent()
    executor = PaperExecutor(store=store)
    first = executor.submit(intent, _risk(intent), market=MarketSnapshot(last_price=Decimal("100")))
    second = executor.submit(intent, _risk(intent), market=MarketSnapshot(last_price=Decimal("100")))

    assert second.order_id == first.order_id
    assert second.executed_quantity == first.executed_quantity
    assert len(store.trades) == 1
    assert [event[1] for event in store.events].count("ORDER_FILLED") == 1


def test_paper_restart_recovers_partial_order_without_duplicate_trade() -> None:
    store = MemoryStore()
    intent = _intent(order_type="LIMIT", price=Decimal("100"))
    first_executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", partial_fill_ratio=Decimal("0.5")),
    )
    first = first_executor.submit(
        intent,
        _risk(intent),
        market=MarketSnapshot(last_price=Decimal("100"), available_liquidity=Decimal("0.1")),
    )
    restarted_executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", partial_fill_ratio=Decimal("0.5")),
    )

    recovered = restarted_executor.recover()
    final = restarted_executor.process_market(
        first.order_id,
        MarketSnapshot(last_price=Decimal("100"), available_liquidity=Decimal("0.1")),
    )

    assert len(recovered) == 1
    assert recovered[0].status == "PARTIALLY_FILLED"
    assert final.status == "FILLED"
    assert len(store.trades) == 2
    assert sum(trade["fee"] for trade in store.trades) == Decimal("0.010")


def test_paper_mark_to_market_updates_unrealized_pnl() -> None:
    store = MemoryStore()
    intent = _intent()
    executor = PaperExecutor(store=store)
    executor.submit(intent, _risk(intent), market=MarketSnapshot(last_price=Decimal("100")))

    executor.mark_to_market("BTCUSDT", Decimal("110"))

    position = store.get_position("BTCUSDT")
    assert position is not None
    assert position["unrealized_pnl"] == Decimal("0.99500")


def test_paper_buy_sell_accounting_preserves_equity_components() -> None:
    store = MemoryStore()
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(
            mode="paper",
            fee_rate=Decimal("0.001"),
            slippage_bps=Decimal("0"),
        ),
    )
    buy = _intent(quantity=Decimal("0.1"))
    buy_result = executor.submit(
        buy,
        _risk(buy),
        market=MarketSnapshot(last_price=Decimal("100")),
    )
    position = store.get_position("BTCUSDT")
    assert position is not None
    assert position["quantity"] == Decimal("0.1")
    assert position["realized_pnl"] == -buy_result.fee

    sell = _intent(
        side="SELL",
        quantity=Decimal("0.1"),
        reason="test exit",
    )
    sell_risk = RiskGate().evaluate(
        sell,
        RiskContext(
            available_quote_usdt=store.get_balance("USDT")["free"],
            available_base_quantity=Decimal("0.1"),
            position_usdt=Decimal("10"),
            market_price=Decimal("110"),
        ),
    )
    sell_result = executor.submit(
        sell,
        sell_risk,
        market=MarketSnapshot(last_price=Decimal("110")),
    )

    position = store.get_position("BTCUSDT")
    assert position is not None
    assert position["quantity"] == Decimal("0")
    assert position["unrealized_pnl"] == Decimal("0")
    assert sell_result.executed_quantity == Decimal("0.1")
    assert len(store.trades) == 2
    assert store.get_balance("USDT")["free"] > Decimal("1000")


def test_paper_limit_cancel_has_no_trade_side_effect() -> None:
    store = MemoryStore()
    intent = _intent(order_type="LIMIT", price=Decimal("99"))
    executor = PaperExecutor(store=store)
    result = executor.submit(
        intent,
        _risk(intent),
        market=MarketSnapshot(last_price=Decimal("100"), ask_price=Decimal("100")),
    )

    cancelled = executor.cancel(result.order_id)

    assert cancelled.status == "CANCELLED"
    assert store.trades == []


def test_paper_rejects_when_cash_is_insufficient() -> None:
    store = MemoryStore()
    intent = _intent(quantity=Decimal("0.1"))
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(mode="paper", initial_usdt=Decimal("1")),
    )

    with pytest.raises(ExecutionRejected, match="quote balance"):
        executor.submit(
            intent,
            _risk(intent),
            market=MarketSnapshot(last_price=Decimal("100")),
        )

    assert store.orders == {}
