"""Pure risk gate for TradeIntent objects.

The risk layer evaluates supplied state. It does not call Binance, persist
events, calculate strategy signals, or submit orders.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Literal
import os

from trade_intent import TradeIntent

RiskDecisionType = Literal["ALLOW", "DENY", "REDUCE", "HALT"]


def _zero(value: Decimal | None) -> Decimal:
    return value if value is not None else Decimal("0")


def _floor_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    units = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return units * step


def _decimal_env(name: str, default: str) -> Decimal:
    try:
        value = Decimal(os.environ.get(name, default))
    except Exception:
        value = Decimal(default)
    return value


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


@dataclass(frozen=True)
class RiskLimits:
    max_order_usdt: Decimal = Decimal("100")
    max_position_usdt: Decimal = Decimal("500")
    max_daily_loss_usdt: Decimal = Decimal("50")
    max_drawdown_percent: Decimal = Decimal("10")
    max_open_orders: int = 5
    max_concurrent_symbols: int = 3
    cooldown_seconds: int = 60
    max_price_deviation_percent: Decimal = Decimal("2")
    min_liquidity_usdt: Decimal = Decimal("0")

    @classmethod
    def from_env(cls) -> "RiskLimits":
        return cls(
            max_order_usdt=_decimal_env("MAX_ORDER_USDT", "100"),
            max_position_usdt=_decimal_env("MAX_POSITION_USDT", "500"),
            max_daily_loss_usdt=_decimal_env("MAX_DAILY_LOSS_USDT", "50"),
            max_drawdown_percent=_decimal_env("MAX_DRAWDOWN_PERCENT", "10"),
            max_open_orders=_int_env("MAX_OPEN_ORDERS", 5),
            max_concurrent_symbols=_int_env("MAX_CONCURRENT_SYMBOLS", 3),
            cooldown_seconds=_int_env("COOLDOWN_SECONDS", 60),
            max_price_deviation_percent=_decimal_env(
                "MAX_PRICE_DEVIATION_PERCENT", "2"
            ),
            min_liquidity_usdt=_decimal_env("MIN_LIQUIDITY_USDT", "0"),
        )

    def __post_init__(self) -> None:
        if any(
            value < 0
            for value in (
                self.max_order_usdt,
                self.max_position_usdt,
                self.max_daily_loss_usdt,
                self.max_drawdown_percent,
                self.max_price_deviation_percent,
                self.min_liquidity_usdt,
            )
        ):
            raise ValueError("risk limits cannot be negative")
        if any(
            value < 0
            for value in (
                self.max_open_orders,
                self.max_concurrent_symbols,
                self.cooldown_seconds,
            )
        ):
            raise ValueError("risk count limits cannot be negative")


@dataclass(frozen=True)
class ExchangeRules:
    symbol: str
    status: str = "TRADING"
    min_qty: Decimal = Decimal("0")
    max_qty: Decimal | None = None
    step_size: Decimal = Decimal("0")
    tick_size: Decimal = Decimal("0")
    min_notional: Decimal = Decimal("0")


@dataclass(frozen=True)
class RiskContext:
    halted: bool = False
    open_orders: int = 0
    active_symbols: frozenset[str] = frozenset()
    existing_client_order_ids: frozenset[str] = frozenset()
    daily_pnl_usdt: Decimal = Decimal("0")
    drawdown_percent: Decimal = Decimal("0")
    available_quote_usdt: Decimal = Decimal("0")
    available_base_quantity: Decimal = Decimal("0")
    position_usdt: Decimal = Decimal("0")
    market_price: Decimal | None = None
    liquidity_usdt: Decimal | None = None
    last_intent_at: datetime | None = None
    exchange_rules: ExchangeRules | None = None
    positioning_confidence: Decimal | None = None
    crowding_score: Decimal | None = None
    liquidity_score: Decimal | None = None
    data_quality_score: Decimal | None = None
    regime_risk: Decimal | None = None
    evidence_conflict: bool = False


@dataclass(frozen=True)
class RiskDecision:
    decision: RiskDecisionType
    reason: str
    intent: TradeIntent
    adjusted_intent: TradeIntent | None = None
    violations: tuple[str, ...] = ()

    @property
    def executable_intent(self) -> TradeIntent | None:
        if self.decision == "ALLOW":
            return self.intent
        if self.decision == "REDUCE":
            return self.adjusted_intent
        return None


class RiskGate:
    """Evaluate all supplied pre-trade constraints without side effects."""

    def __init__(self, limits: RiskLimits | None = None) -> None:
        self.limits = limits or RiskLimits()

    def evaluate(self, intent: TradeIntent, context: RiskContext) -> RiskDecision:
        if context.halted:
            return self._halt(intent, "trading is halted")
        if context.evidence_conflict:
            return self._deny(intent, "positioning evidence is conflicted")
        if context.data_quality_score is not None and context.data_quality_score < Decimal("0.8"):
            return self._deny(intent, "positioning data quality is below threshold")
        if context.liquidity_score is not None and context.liquidity_score < Decimal("0.4"):
            return self._deny(intent, "positioning liquidity is below threshold")
        if context.positioning_confidence is not None and context.positioning_confidence < Decimal("0.6"):
            return self._deny(intent, "positioning confidence is below threshold")
        if (
            self.limits.max_daily_loss_usdt > 0
            and context.daily_pnl_usdt <= -self.limits.max_daily_loss_usdt
        ):
            return self._halt(intent, "maximum daily loss reached")
        if (
            self.limits.max_drawdown_percent > 0
            and context.drawdown_percent >= self.limits.max_drawdown_percent
        ):
            return self._halt(intent, "maximum drawdown reached")
        if intent.client_order_id in context.existing_client_order_ids:
            return self._deny(intent, "duplicate client_order_id")
        if context.open_orders >= self.limits.max_open_orders:
            return self._deny(intent, "maximum open orders reached")
        if (
            intent.symbol not in context.active_symbols
            and len(context.active_symbols) >= self.limits.max_concurrent_symbols
        ):
            return self._deny(intent, "maximum concurrent symbols reached")
        if self._in_cooldown(intent, context):
            return self._deny(intent, "symbol cooldown is active")

        rules = context.exchange_rules
        if rules is not None:
            if rules.symbol.upper() != intent.symbol:
                return self._deny(intent, "exchange rules symbol mismatch")
            if rules.status != "TRADING":
                return self._deny(intent, "symbol is not trading")

        notional = self._notional(intent, context.market_price)
        if notional is None:
            return self._deny(intent, "market price is required to value the order")
        if notional <= 0:
            return self._deny(intent, "order notional must be positive")
        if (
            self.limits.min_liquidity_usdt > 0
            and _zero(context.liquidity_usdt) < self.limits.min_liquidity_usdt
        ):
            return self._deny(intent, "minimum liquidity requirement failed")
        if intent.price is not None and context.market_price is not None:
            deviation = abs(intent.price - context.market_price) / context.market_price * 100
            if deviation > self.limits.max_price_deviation_percent:
                return self._deny(intent, "price deviation limit exceeded")

        normalized = self._normalize(intent, rules)
        if normalized is None:
            return self._deny(intent, "exchange quantity or price rules failed")
        normalized_notional = self._notional(normalized, context.market_price)
        if normalized_notional is None or normalized_notional <= 0:
            return self._deny(intent, "normalized order notional is invalid")

        if rules is not None and normalized.quantity is not None:
            if normalized.quantity < rules.min_qty:
                return self._deny(intent, "minimum quantity requirement failed")
            if rules.max_qty is not None and normalized.quantity > rules.max_qty:
                return self._deny(intent, "maximum quantity requirement failed")
            if normalized_notional < rules.min_notional:
                return self._deny(intent, "minimum notional requirement failed")

        positioning_reduced = False
        if (
            context.crowding_score is not None
            and context.crowding_score >= Decimal("0.8")
        ) or (
            context.regime_risk is not None
            and context.regime_risk >= Decimal("0.8")
        ):
            updates: dict[str, Decimal] = {}
            if normalized.quote_quantity is not None:
                updates["quote_quantity"] = normalized.quote_quantity / Decimal("2")
            elif normalized.quantity is not None:
                reduced_quantity = normalized.quantity / Decimal("2")
                if rules is not None:
                    reduced_quantity = _floor_step(reduced_quantity, rules.step_size)
                updates["quantity"] = reduced_quantity
            if updates and all(value > 0 for value in updates.values()):
                normalized = normalized.model_copy(update=updates)
                normalized_notional = self._notional(normalized, context.market_price)
                positioning_reduced = True
                if (
                    normalized_notional is None
                    or normalized_notional < self._minimum_notional(rules)
                ):
                    return self._deny(
                        intent, "positioning reduction falls below exchange minimum"
                    )

        if normalized_notional > self.limits.max_order_usdt:
            reduced = self._reduce_to_order_limit(normalized, context.market_price, rules)
            if reduced is None:
                return self._deny(intent, "maximum order notional exceeded")
            reduced_notional = self._notional(reduced, context.market_price)
            if reduced_notional is None or reduced_notional < self._minimum_notional(rules):
                return self._deny(intent, "order limit reduction falls below exchange minimum")
            normalized = reduced
            normalized_notional = reduced_notional

        if intent.side == "BUY":
            if normalized_notional > context.available_quote_usdt:
                return self._deny(intent, "insufficient quote balance")
            projected_position = context.position_usdt + normalized_notional
        else:
            quantity = normalized.quantity
            if quantity is not None and quantity > context.available_base_quantity:
                return self._deny(intent, "insufficient base balance")
            projected_position = max(Decimal("0"), context.position_usdt - normalized_notional)
        if projected_position > self.limits.max_position_usdt:
            return self._deny(intent, "maximum position notional exceeded")

        if normalized != intent:
            return RiskDecision(
                decision="REDUCE",
                reason=(
                    "order reduced for positioning risk"
                    if positioning_reduced
                    else "order normalized to risk and exchange limits"
                ),
                intent=intent,
                adjusted_intent=normalized,
                violations=(("positioning_risk",) if positioning_reduced else ("normalized_order",)),
            )
        return RiskDecision(decision="ALLOW", reason="risk checks passed", intent=intent)

    def _normalize(self, intent: TradeIntent, rules: ExchangeRules | None) -> TradeIntent | None:
        if rules is None:
            return intent
        updates: dict[str, Decimal] = {}
        if intent.quantity is not None and rules.step_size > 0:
            updates["quantity"] = _floor_step(intent.quantity, rules.step_size)
        if intent.price is not None and rules.tick_size > 0:
            updates["price"] = _floor_step(intent.price, rules.tick_size)
        if updates.get("quantity") == Decimal("0") or updates.get("price") == Decimal("0"):
            return None
        return intent.model_copy(update=updates) if updates else intent

    def _reduce_to_order_limit(
        self,
        intent: TradeIntent,
        market_price: Decimal | None,
        rules: ExchangeRules | None,
    ) -> TradeIntent | None:
        if intent.quantity is None or market_price is None:
            return None
        quantity = self.limits.max_order_usdt / market_price
        if rules is not None:
            quantity = _floor_step(quantity, rules.step_size)
        if quantity <= 0:
            return None
        return intent.model_copy(update={"quantity": quantity, "quote_quantity": None})

    def _notional(self, intent: TradeIntent, market_price: Decimal | None) -> Decimal | None:
        if intent.quote_quantity is not None:
            return intent.quote_quantity
        if intent.quantity is None:
            return None
        price = intent.price or market_price
        return intent.quantity * price if price is not None else None

    def _minimum_notional(self, rules: ExchangeRules | None) -> Decimal:
        return rules.min_notional if rules is not None else Decimal("0")

    def _in_cooldown(self, intent: TradeIntent, context: RiskContext) -> bool:
        if self.limits.cooldown_seconds <= 0 or context.last_intent_at is None:
            return False
        last = context.last_intent_at
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        age = (intent.created_at - last).total_seconds()
        return age < self.limits.cooldown_seconds

    @staticmethod
    def _deny(intent: TradeIntent, reason: str) -> RiskDecision:
        return RiskDecision(decision="DENY", reason=reason, intent=intent, violations=(reason,))

    @staticmethod
    def _halt(intent: TradeIntent, reason: str) -> RiskDecision:
        return RiskDecision(decision="HALT", reason=reason, intent=intent, violations=(reason,))
