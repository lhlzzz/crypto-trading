from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from risk import ExchangeRules, RiskContext, RiskGate, RiskLimits
from trade_intent import TradeIntent


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


def _context(**updates: object) -> RiskContext:
    values: dict[str, object] = {
        "available_quote_usdt": Decimal("1000"),
        "market_price": Decimal("100"),
        "exchange_rules": ExchangeRules(
            symbol="BTCUSDT",
            min_qty=Decimal("0.001"),
            step_size=Decimal("0.001"),
            tick_size=Decimal("0.01"),
            min_notional=Decimal("5"),
        ),
    }
    values.update(updates)
    return RiskContext(**values)


def test_risk_allows_valid_intent() -> None:
    decision = RiskGate().evaluate(_intent(), _context())

    assert decision.decision == "ALLOW"
    assert decision.executable_intent == decision.intent


def test_halt_blocks_new_orders() -> None:
    decision = RiskGate().evaluate(_intent(), _context(halted=True))

    assert decision.decision == "HALT"
    assert decision.executable_intent is None


def test_risk_limits_load_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("MAX_ORDER_USDT", "12.5")
    monkeypatch.setenv("MAX_OPEN_ORDERS", "2")

    limits = RiskLimits.from_env()

    assert limits.max_order_usdt == Decimal("12.5")
    assert limits.max_open_orders == 2


def test_daily_loss_triggers_halt() -> None:
    limits = RiskLimits(max_daily_loss_usdt=Decimal("50"))
    decision = RiskGate(limits).evaluate(
        _intent(),
        _context(daily_pnl_usdt=Decimal("-50")),
    )

    assert decision.decision == "HALT"


def test_duplicate_order_is_denied() -> None:
    intent = _intent()
    decision = RiskGate().evaluate(
        intent,
        _context(existing_client_order_ids=frozenset({intent.client_order_id})),
    )

    assert decision.decision == "DENY"
    assert "duplicate" in decision.reason


def test_large_order_is_reduced_to_max_order_limit() -> None:
    intent = _intent(quantity=Decimal("2"))
    decision = RiskGate(
        RiskLimits(max_order_usdt=Decimal("100"), max_position_usdt=Decimal("500"))
    ).evaluate(intent, _context())

    assert decision.decision == "REDUCE"
    assert decision.adjusted_intent is not None
    assert decision.adjusted_intent.quantity == Decimal("1")


def test_exchange_rules_normalize_quantity_and_price() -> None:
    intent = _intent(
        quantity=Decimal("0.1099"),
        order_type="LIMIT",
        price=Decimal("100.019"),
    )
    decision = RiskGate().evaluate(intent, _context())

    assert decision.decision == "REDUCE"
    assert decision.adjusted_intent is not None
    assert decision.adjusted_intent.quantity == Decimal("0.109")
    assert decision.adjusted_intent.price == Decimal("100.01")


def test_cooldown_denies_recent_intent() -> None:
    now = datetime.now(timezone.utc)
    intent = _intent(created_at=now)
    decision = RiskGate().evaluate(
        intent,
        _context(last_intent_at=now - timedelta(seconds=1)),
    )

    assert decision.decision == "DENY"
    assert "cooldown" in decision.reason


def test_each_configured_risk_limit_fails_closed() -> None:
    intent = _intent()
    cases = (
        (RiskLimits(max_daily_loss_usdt=Decimal("50")), {"daily_pnl_usdt": Decimal("-50")}, "HALT"),
        (RiskLimits(max_drawdown_percent=Decimal("10")), {"drawdown_percent": Decimal("10")}, "HALT"),
        (RiskLimits(max_open_orders=1), {"open_orders": 1}, "DENY"),
        (RiskLimits(max_concurrent_symbols=1), {"active_symbols": frozenset({"ETHUSDT"})}, "DENY"),
        (RiskLimits(cooldown_seconds=60), {"last_intent_at": intent.created_at}, "DENY"),
        (RiskLimits(max_order_usdt=5), {}, "REDUCE"),
        (RiskLimits(max_position_usdt=5), {"position_usdt": Decimal("5")}, "DENY"),
    )
    for limits, updates, expected in cases:
        decision = RiskGate(limits).evaluate(intent, _context(**updates))
        assert decision.decision == expected, (limits, updates, decision)
