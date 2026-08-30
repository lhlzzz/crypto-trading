from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from risk import ExchangeRules, RiskContext, RiskGate, RiskLimits, classify_meme_risk_tier
from trade_intent import TradeIntent


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


def _context(**updates: object) -> RiskContext:
    values: dict[str, object] = {
        "wallet_balance": Decimal("1000"),
        "available_balance": Decimal("1000"),
        "equity": Decimal("1000"),
        "mark_price": Decimal("100"),
        "leverage": Decimal("2"),
        "margin_type": "ISOLATED",
        "position_mode": "ONE_WAY",
        "liquidation_price": Decimal("60"),
        "liquidation_distance_percent": Decimal("40"),
        "data_quality_score": Decimal("1"),
        "liquidity_score": Decimal("1"),
        "positioning_confidence": Decimal("1"),
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
    monkeypatch.setenv("MAX_LEVERAGE", "3")
    monkeypatch.setenv("MIN_DATA_QUALITY", "0.7")

    limits = RiskLimits.from_env()

    assert limits.max_order_usdt == Decimal("12.5")
    assert limits.max_open_orders == 2
    assert limits.max_leverage == Decimal("3")
    assert limits.min_data_quality_score == Decimal("0.7")


def test_daily_loss_triggers_halt() -> None:
    limits = RiskLimits(max_daily_loss_usdt=Decimal("50"))
    decision = RiskGate(limits).evaluate(
        _intent(),
        _context(daily_pnl_usdt=Decimal("-50")),
    )

    assert decision.decision == "HALT"


def test_drawdown_triggers_halt() -> None:
    decision = RiskGate(RiskLimits(max_drawdown_percent=Decimal("10"))).evaluate(
        _intent(),
        _context(drawdown_percent=Decimal("10")),
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


def test_insufficient_margin_is_denied() -> None:
    decision = RiskGate().evaluate(
        _intent(quantity=Decimal("1"), leverage=Decimal("1")),
        _context(available_balance=Decimal("10"), mark_price=Decimal("100")),
    )
    assert decision.decision == "DENY"
    assert "insufficient margin" in decision.reason


def test_max_leverage_is_denied() -> None:
    decision = RiskGate(RiskLimits(max_leverage=Decimal("2"))).evaluate(
        _intent(leverage=Decimal("5")),
        _context(leverage=Decimal("5")),
    )
    assert decision.decision == "DENY"
    assert "leverage" in decision.reason


def test_max_position_is_denied() -> None:
    decision = RiskGate(RiskLimits(max_position_usdt=Decimal("5"))).evaluate(
        _intent(),
        _context(),
    )
    assert decision.decision == "DENY"
    assert "maximum position" in decision.reason


def test_max_margin_is_denied() -> None:
    decision = RiskGate(RiskLimits(max_margin_usdt=Decimal("1"))).evaluate(
        _intent(),
        _context(used_margin=Decimal("0"), available_balance=Decimal("1000")),
    )
    assert decision.decision == "DENY"
    assert "maximum margin" in decision.reason


def test_liquidation_buffer_is_denied() -> None:
    decision = RiskGate(RiskLimits(min_liquidation_buffer_percent=Decimal("20"))).evaluate(
        _intent(),
        _context(liquidation_distance_percent=Decimal("5")),
    )
    assert decision.decision == "DENY"
    assert "liquidation" in decision.reason


def test_close_larger_than_position_is_denied() -> None:
    intent = _intent(action="CLOSE", reduce_only=True, quantity=Decimal("1"))
    decision = RiskGate().evaluate(
        intent,
        _context(
            position_direction="LONG",
            position_quantity=Decimal("0.1"),
            position_notional=Decimal("10"),
        ),
    )
    assert decision.decision == "DENY"
    assert "close larger than position" in decision.reason


def test_reverse_position_is_denied() -> None:
    intent = _intent(direction="SHORT", action="OPEN", reduce_only=False)
    decision = RiskGate().evaluate(
        intent,
        _context(position_direction="LONG", position_quantity=Decimal("0.1")),
    )
    assert decision.decision == "DENY"
    assert "reverse" in decision.reason


def test_duplicate_open_is_denied() -> None:
    decision = RiskGate().evaluate(
        _intent(),
        _context(position_direction="LONG", position_quantity=Decimal("0.1")),
    )
    assert decision.decision == "DENY"
    assert "duplicate open" in decision.reason


def test_reduce_only_close_is_allowed() -> None:
    intent = _intent(action="CLOSE", reduce_only=True, quantity=Decimal("0.1"))
    decision = RiskGate().evaluate(
        intent,
        _context(
            position_direction="LONG",
            position_quantity=Decimal("0.1"),
            position_notional=Decimal("10"),
        ),
    )
    assert decision.decision == "ALLOW"


def test_crowding_reduces_order() -> None:
    decision = RiskGate().evaluate(
        _intent(quantity=Decimal("0.2")),
        _context(crowding_score=Decimal("0.95")),
    )
    assert decision.decision == "REDUCE"
    assert decision.adjusted_intent is not None
    assert decision.adjusted_intent.quantity == Decimal("0.1")


def test_stale_data_quality_is_denied() -> None:
    decision = RiskGate().evaluate(
        _intent(),
        _context(data_quality_score=Decimal("0.2")),
    )
    assert decision.decision == "DENY"
    assert "data quality" in decision.reason


def test_low_liquidity_is_denied() -> None:
    decision = RiskGate().evaluate(
        _intent(),
        _context(liquidity_score=Decimal("0.1")),
    )
    assert decision.decision == "DENY"
    assert "liquidity" in decision.reason


def test_evidence_conflict_is_denied() -> None:
    decision = RiskGate().evaluate(_intent(), _context(evidence_conflict=True))
    assert decision.decision == "DENY"
    assert "conflicted" in decision.reason


def test_margin_type_mismatch_halts() -> None:
    decision = RiskGate().evaluate(_intent(), _context(margin_type="CROSSED"))
    assert decision.decision == "HALT"


def test_blocked_meme_tier_is_denied() -> None:
    decision = RiskGate().evaluate(_intent(), _context(meme_risk_tier="BLOCK"))
    assert decision.decision == "DENY"
    assert "blocked" in decision.reason


def test_meme_notional_cap() -> None:
    decision = RiskGate(
        RiskLimits(max_meme_notional_usdt=Decimal("15"))
    ).evaluate(
        _intent(quantity=Decimal("0.1")),
        _context(symbol_meme_notional=Decimal("6")),
    )
    assert decision.decision == "DENY"
    assert "meme symbol" in decision.reason


def test_directional_meme_cap() -> None:
    decision = RiskGate(
        RiskLimits(max_directional_meme_exposure_usdt=Decimal("15"))
    ).evaluate(
        _intent(quantity=Decimal("0.1")),
        _context(directional_meme_exposure=Decimal("6")),
    )
    assert decision.decision == "DENY"
    assert "directional meme" in decision.reason


def test_classify_meme_risk_tier_uses_canonical_thresholds() -> None:
    assert classify_meme_risk_tier(trading=False) == "BLOCK"
    assert classify_meme_risk_tier(data_quality_score=Decimal("0.1")) == "OBSERVE"
    assert classify_meme_risk_tier(
        crowding_score=Decimal("0.95"),
        liquidity_score=Decimal("1"),
        data_quality_score=Decimal("1"),
        spread_bps=Decimal("2"),
        open_interest=Decimal("10"),
    ) == "REDUCED"
    assert classify_meme_risk_tier(
        crowding_score=Decimal("0.1"),
        liquidity_score=Decimal("1"),
        data_quality_score=Decimal("1"),
        spread_bps=Decimal("2"),
        open_interest=Decimal("10"),
    ) == "TRADEABLE"


def test_each_configured_risk_limit_fails_closed() -> None:
    intent = _intent()
    cases = (
        (RiskLimits(max_daily_loss_usdt=Decimal("50")), {"daily_pnl_usdt": Decimal("-50")}, "HALT"),
        (RiskLimits(max_drawdown_percent=Decimal("10")), {"drawdown_percent": Decimal("10")}, "HALT"),
        (RiskLimits(max_open_orders=1), {"open_orders": 1}, "DENY"),
        (RiskLimits(max_concurrent_symbols=1), {"active_symbols": frozenset({"ETHUSDT"})}, "DENY"),
        (RiskLimits(cooldown_seconds=60), {"last_intent_at": intent.created_at}, "DENY"),
        (RiskLimits(max_order_usdt=5), {}, "REDUCE"),
        (RiskLimits(max_position_usdt=5), {}, "DENY"),
    )
    for limits, updates, expected in cases:
        decision = RiskGate(limits).evaluate(intent, _context(**updates))
        assert decision.decision == expected, (limits, updates, decision)


def test_kill_switch_blocks_open_but_allows_close(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_KILL_SWITCH", "true")
    open_decision = RiskGate().evaluate(_intent(), _context())
    close_decision = RiskGate().evaluate(
        _intent(action="CLOSE", reduce_only=True),
        _context(position_direction="LONG", position_quantity=Decimal("0.1")),
    )
    assert open_decision.decision == "DENY"
    assert "kill switch" in open_decision.reason
    assert close_decision.decision == "ALLOW"
