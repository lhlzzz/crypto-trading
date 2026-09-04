from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from risk import (
    ExchangeRules,
    FuturesAccountSnapshot,
    RiskContext,
    RiskGate,
    RiskLimits,
    classify_futures_risk_tier,
    reject_legacy_meme_risk_env,
)
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
        "mode": "paper",
        "mark_price": Decimal("100"),
        "leverage": Decimal("2"),
        "margin_type": "ISOLATED",
        "position_mode": "ONE_WAY",
        "liquidation_price": Decimal("60"),
        "liquidation_distance_percent": Decimal("40"),
        "data_quality_score": Decimal("1"),
        "liquidity_score": Decimal("1"),
        "positioning_confidence": Decimal("1"),
        "is_meme": False,
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
        "exchange_rules": ExchangeRules(
            symbol="BTCUSDT",
            min_qty=Decimal("0.001"),
            step_size=Decimal("0.001"),
            tick_size=Decimal("0.01"),
            min_notional=Decimal("5"),
        ),
    }
    values.update(updates)
    snapshot = values.get("account_snapshot")
    if isinstance(snapshot, FuturesAccountSnapshot) and snapshot.mode != values["mode"]:
        values["account_snapshot"] = FuturesAccountSnapshot(
            mode=values["mode"],  # type: ignore[arg-type]
            wallet_balance=snapshot.wallet_balance,
            available_balance=snapshot.available_balance,
            total_margin=snapshot.total_margin,
            used_margin=snapshot.used_margin,
            unrealized_pnl=snapshot.unrealized_pnl,
            realized_pnl=snapshot.realized_pnl,
            positions=snapshot.positions,
            open_orders=snapshot.open_orders,
            leverage=snapshot.leverage,
            margin_mode=snapshot.margin_mode,
            position_mode=snapshot.position_mode,
            captured_at=snapshot.captured_at,
            source=snapshot.source,
        )
    return RiskContext(**values)


def test_risk_allows_valid_intent() -> None:
    decision = RiskGate().evaluate(_intent(), _context())

    assert decision.decision == "ALLOW"
    assert decision.executable_intent == decision.intent


def test_testnet_without_exchange_rules_halts() -> None:
    decision = RiskGate().evaluate(
        _intent(),
        _context(mode="testnet", exchange_rules=None),
    )

    assert decision.decision == "HALT"
    assert "exchange rules" in decision.reason


def test_halt_blocks_new_orders() -> None:
    decision = RiskGate().evaluate(_intent(), _context(halted=True))

    assert decision.decision == "HALT"
    assert decision.executable_intent is None


def test_risk_limits_load_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("MAX_ORDER_USDT", "12.5")
    monkeypatch.setenv("MAX_OPEN_ORDERS", "2")
    monkeypatch.setenv("MAX_LEVERAGE", "3")
    monkeypatch.setenv("MIN_DATA_QUALITY", "0.7")
    monkeypatch.setenv("MAX_FUNDING_ABS", "0.02")
    monkeypatch.setenv("MAX_MARGIN_RATIO", "0.5")

    limits = RiskLimits.from_env()

    assert limits.max_order_usdt == Decimal("12.5")
    assert limits.max_open_orders == 2
    assert limits.max_leverage == Decimal("3")
    assert limits.min_data_quality_score == Decimal("0.7")
    assert limits.max_funding_abs == Decimal("0.02")
    assert limits.max_margin_ratio == Decimal("0.5")
    assert limits.max_symbol_notional_usdt == Decimal("500")


def test_risk_limits_do_not_fallback_to_legacy_meme_env(monkeypatch) -> None:
    monkeypatch.setenv("MAX_MEME_SYMBOL_NOTIONAL_USDT", "12")
    monkeypatch.setenv("MAX_SYMBOL_NOTIONAL_USDT", "80")
    limits = RiskLimits.from_env()
    assert limits.max_symbol_notional_usdt == Decimal("80")


def test_legacy_meme_risk_env_fails_closed_on_testnet(monkeypatch) -> None:
    import pytest

    monkeypatch.setenv("MAX_MEME_PORTFOLIO_NOTIONAL_USDT", "99")
    with pytest.raises(ValueError, match="legacy meme risk"):
        reject_legacy_meme_risk_env(mode="testnet")


def test_risk_gate_halts_unauthorized_symbol() -> None:
    intent = _intent()
    object.__setattr__(intent, "symbol", "DOGEUSDT")
    decision = RiskGate().evaluate(intent, _context())
    assert decision.decision == "HALT"
    assert "UNAUTHORIZED_SYMBOL" in decision.reason


def test_risk_limits_reject_out_of_range_margin_ratio() -> None:
    import pytest

    with pytest.raises(ValueError, match="max_margin_ratio"):
        RiskLimits(max_margin_ratio=Decimal("1.5"))


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
        quantity=Decimal("0.109"),
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


def test_mixed_margin_mode_halts_new_opens() -> None:
    decision = RiskGate().evaluate(_intent(), _context(margin_type="MIXED"))

    assert decision.decision == "HALT"
    assert "mixed margin" in decision.reason


def test_account_snapshot_mode_mismatch_halts() -> None:
    snapshot = _context().account_snapshot
    assert snapshot is not None
    mismatched_snapshot = replace(snapshot, mode="testnet")
    decision = RiskGate().evaluate(
        _intent(),
        replace(_context(), account_snapshot=mismatched_snapshot),
    )

    assert decision.decision == "HALT"
    assert "INVALID_ACCOUNT_STATE" in decision.reason


def test_stale_account_snapshot_blocks_open_and_reduce_only_exit() -> None:
    stale = FuturesAccountSnapshot(
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
        captured_at=datetime.now(timezone.utc) - timedelta(minutes=2),
        source="test",
    )
    open_decision = RiskGate().evaluate(_intent(), _context(account_snapshot=stale))
    close = _intent(action="CLOSE", reduce_only=True)
    close_decision = RiskGate().evaluate(
        close,
        _context(
            account_snapshot=stale,
            position_direction="LONG",
            position_quantity=Decimal("0.1"),
            position_notional=Decimal("10"),
        ),
    )

    assert open_decision.decision == "HALT"
    assert "ACCOUNT_SNAPSHOT_STALE" in open_decision.reason
    assert close_decision.decision == "HALT"
    assert "ACCOUNT_SNAPSHOT_STALE" in close_decision.reason


def test_unknown_position_mark_halts_new_open() -> None:
    decision = RiskGate().evaluate(
        _intent(),
        _context(
            position_direction="LONG",
            position_quantity=Decimal("1"),
            position_notional=None,
            mark_price=None,
        ),
    )

    assert decision.decision == "HALT"
    assert "position mark price" in decision.reason


def test_legacy_meme_fields_are_not_runtime_policy() -> None:
    blocked = RiskGate().evaluate(_intent(), _context(meme_risk_tier="BLOCK"))
    observed = RiskGate().evaluate(_intent(), _context(is_meme=True))
    assert blocked.decision != "DENY" or "blocked" not in blocked.reason
    assert observed.decision != "DENY" or "canonical universe" not in observed.reason
    assert blocked.decision in {"ALLOW", "REDUCE"}
    assert observed.decision in {"ALLOW", "REDUCE"}


def test_symbol_notional_cap() -> None:
    decision = RiskGate(
        RiskLimits(max_symbol_notional_usdt=Decimal("15"))
    ).evaluate(
        _intent(quantity=Decimal("0.1")),
        _context(symbol_notional=Decimal("6")),
    )
    assert decision.decision == "DENY"
    assert "symbol exposure" in decision.reason


def test_portfolio_cap_is_separate_from_symbol_cap() -> None:
    decision = RiskGate(
        RiskLimits(
            max_symbol_notional_usdt=Decimal("100"),
            max_portfolio_notional_usdt=Decimal("15"),
        )
    ).evaluate(
        _intent(quantity=Decimal("0.1")),
        _context(total_notional=Decimal("6")),
    )
    assert decision.decision == "DENY"
    assert "total exposure" in decision.reason


def test_directional_exposure_cap() -> None:
    decision = RiskGate(
        RiskLimits(max_directional_exposure_usdt=Decimal("15"))
    ).evaluate(
        _intent(quantity=Decimal("0.1")),
        _context(directional_exposure=Decimal("6")),
    )
    assert decision.decision == "DENY"
    assert "directional exposure" in decision.reason


def test_classify_futures_risk_tier_uses_canonical_thresholds() -> None:
    assert classify_futures_risk_tier(trading=False) == "BLOCK"
    assert classify_futures_risk_tier(data_quality_score=Decimal("0.1")) == "OBSERVE"
    assert classify_futures_risk_tier(
        crowding_score=Decimal("0.95"),
        liquidity_score=Decimal("1"),
        data_quality_score=Decimal("1"),
        spread_bps=Decimal("2"),
        open_interest=Decimal("10"),
    ) == "REDUCED"
    assert classify_futures_risk_tier(
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


def test_kill_switch_blocks_open_and_close(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_KILL_SWITCH", "true")
    open_decision = RiskGate().evaluate(_intent(), _context())
    close_decision = RiskGate().evaluate(
        _intent(action="CLOSE", reduce_only=True),
        _context(position_direction="LONG", position_quantity=Decimal("0.1")),
    )
    assert open_decision.decision == "DENY"
    assert "kill switch" in open_decision.reason
    assert close_decision.decision == "DENY"
    assert "kill switch" in close_decision.reason


def test_halt_and_untrusted_evidence_block_strategy_close() -> None:
    decision = RiskGate().evaluate(
        _intent(
            action="CLOSE",
            reduce_only=True,
            price=None,
            positioning_state="UNKNOWN",
            meme_risk_tier="BLOCK",
        ),
        _context(
            halted=True,
            evidence_conflict=True,
            meme_risk_tier="BLOCK",
            data_quality_score=Decimal("0"),
            liquidity_score=Decimal("0"),
            positioning_confidence=Decimal("0"),
            mark_price=None,
            position_direction="LONG",
            position_quantity=Decimal("0.1"),
        ),
    )

    assert decision.decision == "HALT"
    assert "halted" in decision.reason


def test_stale_evidence_denies_open_reduce_and_close() -> None:
    open_decision = RiskGate().evaluate(
        _intent(action="OPEN"),
        _context(evidence_freshness="STALE"),
    )
    close_decision = RiskGate().evaluate(
        _intent(action="CLOSE", reduce_only=True),
        _context(
            evidence_freshness="STALE",
            position_direction="LONG",
            position_quantity=Decimal("0.1"),
        ),
    )
    reduce_decision = RiskGate().evaluate(
        _intent(action="REDUCE", reduce_only=True, quantity=Decimal("0.05")),
        _context(
            evidence_freshness="STALE",
            position_direction="LONG",
            position_quantity=Decimal("0.1"),
        ),
    )
    assert open_decision.decision == "DENY"
    assert "EVIDENCE_STALE" in open_decision.reason
    assert close_decision.decision == "DENY"
    assert "EVIDENCE_STALE" in close_decision.reason
    assert reduce_decision.decision == "DENY"
    assert "EVIDENCE_STALE" in reduce_decision.reason


def test_symbol_isolation_allows_healthy_symbol_when_another_is_stale() -> None:
    btc_rules = ExchangeRules(
        symbol="BTCUSDT",
        min_qty=Decimal("0.001"),
        step_size=Decimal("0.001"),
        tick_size=Decimal("0.01"),
        min_notional=Decimal("5"),
    )
    eth_rules = ExchangeRules(
        symbol="ETHUSDT",
        min_qty=Decimal("0.001"),
        step_size=Decimal("0.001"),
        tick_size=Decimal("0.01"),
        min_notional=Decimal("5"),
    )
    btc = RiskGate().evaluate(
        _intent(symbol="BTCUSDT", action="OPEN"),
        _context(evidence_freshness="STALE", exchange_rules=btc_rules),
    )
    eth = RiskGate().evaluate(
        _intent(symbol="ETHUSDT", action="OPEN"),
        _context(evidence_freshness="FRESH", exchange_rules=eth_rules),
    )
    assert btc.decision == "DENY"
    assert eth.decision == "ALLOW"


def test_below_min_qty_is_denied() -> None:
    decision = RiskGate().evaluate(
        _intent(quantity=Decimal("0.0001")),
        _context(),
    )
    assert decision.decision == "DENY"
    assert "minimum quantity" in decision.reason


def test_above_max_qty_is_denied() -> None:
    rules = ExchangeRules(
        symbol="BTCUSDT",
        min_qty=Decimal("0.001"),
        step_size=Decimal("0.001"),
        tick_size=Decimal("0.01"),
        min_notional=Decimal("5"),
        max_qty=Decimal("0.05"),
    )
    decision = RiskGate().evaluate(_intent(quantity=Decimal("0.1")), _context(exchange_rules=rules))
    assert decision.decision == "DENY"
    assert "maximum quantity" in decision.reason


def test_unaligned_step_size_is_denied() -> None:
    decision = RiskGate().evaluate(
        _intent(quantity=Decimal("0.0015")),
        _context(),
    )
    assert decision.decision == "DENY"
    assert "step size" in decision.reason


def test_below_min_notional_is_denied() -> None:
    rules = ExchangeRules(
        symbol="BTCUSDT",
        min_qty=Decimal("0.001"),
        step_size=Decimal("0.001"),
        tick_size=Decimal("0.01"),
        min_notional=Decimal("50"),
    )
    decision = RiskGate().evaluate(_intent(quantity=Decimal("0.001")), _context(exchange_rules=rules))
    assert decision.decision == "DENY"
    assert "minimum notional" in decision.reason


def test_valid_quantity_is_allowed() -> None:
    decision = RiskGate().evaluate(_intent(quantity=Decimal("0.1")), _context())
    assert decision.decision == "ALLOW"


def test_missing_live_liquidation_evidence_denies_open() -> None:
    decision = RiskGate().evaluate(
        _intent(),
        _context(mode="live", liquidation_price=None, liquidation_distance_percent=None),
    )
    assert decision.decision in {"DENY", "HALT"}
    assert "liquidation" in decision.reason


def test_global_market_data_failure_halts() -> None:
    decision = RiskGate().evaluate(
        _intent(),
        _context(market_data_health="HALT"),
    )
    assert decision.decision == "HALT"
    assert decision.reason == "GLOBAL_HALT"
