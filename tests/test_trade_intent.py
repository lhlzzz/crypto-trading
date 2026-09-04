from __future__ import annotations

from decimal import Decimal

import pytest

from trade_intent import TradeIntent, exchange_side


def _intent(**updates: object) -> TradeIntent:
    values: dict[str, object] = {
        "symbol": "btcusdt",
        "direction": "LONG",
        "action": "OPEN",
        "reduce_only": False,
        "leverage": Decimal("2"),
        "order_type": "MARKET",
        "quantity": Decimal("0.01"),
        "confidence": Decimal("0.8"),
        "reason": "trend confirmation",
        "strategy_version": "test-1",
    }
    values.update(updates)
    return TradeIntent(**values)


def test_trade_intent_generates_idempotent_client_order_id() -> None:
    intent = _intent()

    assert intent.symbol == "BTCUSDT"
    assert intent.client_order_id.startswith("BIAN-")
    assert len(intent.client_order_id) <= 36
    assert intent.direction == "LONG"
    assert intent.action == "OPEN"
    assert intent.reduce_only is False
    assert "quote_quantity" not in TradeIntent.model_fields


def test_limit_intent_requires_quantity_and_price() -> None:
    with pytest.raises(ValueError, match="quantity and price"):
        _intent(order_type="LIMIT")


def test_open_long_maps_to_buy_without_reduce_only() -> None:
    intent = _intent(direction="LONG", action="OPEN", reduce_only=False)

    assert intent.exchange_side() == "BUY"
    assert exchange_side("LONG", "OPEN") == "BUY"


def test_open_short_maps_to_sell_without_reduce_only() -> None:
    intent = _intent(direction="SHORT", action="OPEN", reduce_only=False)

    assert intent.exchange_side() == "SELL"


def test_reduce_long_requires_reduce_only_and_maps_to_sell() -> None:
    intent = _intent(direction="LONG", action="REDUCE", reduce_only=True)

    assert intent.exchange_side() == "SELL"


def test_reduce_short_requires_reduce_only_and_maps_to_buy() -> None:
    intent = _intent(direction="SHORT", action="REDUCE", reduce_only=True)

    assert intent.exchange_side() == "BUY"


def test_close_long_requires_reduce_only_and_maps_to_sell() -> None:
    intent = _intent(direction="LONG", action="CLOSE", reduce_only=True)

    assert intent.exchange_side() == "SELL"


def test_close_short_requires_reduce_only_and_maps_to_buy() -> None:
    intent = _intent(direction="SHORT", action="CLOSE", reduce_only=True)

    assert intent.exchange_side() == "BUY"


def test_flat_open_is_rejected() -> None:
    with pytest.raises(ValueError, match="FLAT"):
        _intent(direction="FLAT", action="OPEN", reduce_only=False)


def test_open_without_quantity_is_rejected() -> None:
    with pytest.raises(Exception):
        _intent(quantity=None)


def test_close_without_reduce_only_is_rejected() -> None:
    with pytest.raises(ValueError, match="reduce_only"):
        _intent(direction="LONG", action="CLOSE", reduce_only=False)


def test_reduce_without_reduce_only_is_rejected() -> None:
    with pytest.raises(ValueError, match="reduce_only"):
        _intent(direction="SHORT", action="REDUCE", reduce_only=False)


def test_open_with_reduce_only_is_rejected() -> None:
    with pytest.raises(ValueError, match="reduce_only"):
        _intent(action="OPEN", reduce_only=True)


def test_leverage_must_be_positive() -> None:
    with pytest.raises(Exception):
        _intent(leverage=Decimal("0"))


def test_margin_type_must_be_isolated() -> None:
    with pytest.raises(Exception):
        _intent(margin_type="CROSSED")


def test_position_mode_must_be_one_way() -> None:
    with pytest.raises(Exception):
        _intent(position_mode="HEDGE")


def test_quote_quantity_is_not_a_contract_field() -> None:
    with pytest.raises(Exception):
        _intent(quote_quantity=Decimal("10"))


def test_directional_and_transition_strength_are_separate() -> None:
    intent = _intent(
        directional_strength=Decimal("0.8"),
        transition_strength=Decimal("0.2"),
    )

    assert intent.directional_strength == Decimal("0.8")
    assert intent.transition_strength == Decimal("0.2")
    assert intent.directional_strength != intent.transition_strength


def test_previous_state_is_positioning_metadata() -> None:
    intent = _intent(previous_state="NEUTRAL", positioning_state="LONG_BUILDING")
    assert intent.previous_state == "NEUTRAL"
    assert intent.positioning_state == "LONG_BUILDING"


def test_trade_intent_rejects_non_canonical_futures_symbol() -> None:
    with pytest.raises(ValueError, match="unauthorized symbol"):
        _intent(symbol="DOGEUSDT")
