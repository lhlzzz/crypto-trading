from __future__ import annotations

from decimal import Decimal

import pytest

from trade_intent import TradeIntent


def test_trade_intent_generates_idempotent_client_order_id() -> None:
    intent = TradeIntent(
        symbol="btcusdt",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("0.01"),
        confidence=Decimal("0.8"),
        reason="trend confirmation",
        strategy_version="test-1",
    )

    assert intent.symbol == "BTCUSDT"
    assert intent.client_order_id.startswith("BIAN-")
    assert len(intent.client_order_id) <= 36


def test_limit_intent_requires_quantity_and_price() -> None:
    with pytest.raises(ValueError, match="quantity and price"):
        TradeIntent(
            symbol="BTCUSDT",
            side="BUY",
            order_type="LIMIT",
            quantity=Decimal("0.01"),
            confidence=Decimal("0.8"),
            reason="limit test",
            strategy_version="test-1",
        )


def test_market_intent_rejects_two_quantity_forms() -> None:
    with pytest.raises(ValueError, match="not both"):
        TradeIntent(
            symbol="BTCUSDT",
            side="BUY",
            order_type="MARKET",
            quantity=Decimal("0.01"),
            quote_quantity=Decimal("10"),
            confidence=Decimal("0.8"),
            reason="market test",
            strategy_version="test-1",
        )

