"""Pure futures risk gate for TradeIntent objects.

The risk layer evaluates supplied state. It does not call Binance, persist
events, calculate strategy signals, or submit orders.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Literal
import os

from trade_intent import TradeIntent

RiskDecisionType = Literal["ALLOW", "DENY", "REDUCE", "HALT"]
PositionDirection = Literal["LONG", "SHORT", "FLAT"]
MemeRiskTier = Literal["TRADEABLE", "REDUCED", "OBSERVE", "BLOCK"]


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
    max_leverage: Decimal = Decimal("5")
    max_margin_usdt: Decimal = Decimal("500")
    min_liquidation_buffer_percent: Decimal = Decimal("15")
    min_data_quality_score: Decimal = Decimal("0.8")
    min_liquidity_score: Decimal = Decimal("0.4")
    min_positioning_confidence: Decimal = Decimal("0.6")
    max_crowding_score: Decimal = Decimal("0.9")

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
            max_leverage=_decimal_env("MAX_LEVERAGE", "5"),
            max_margin_usdt=_decimal_env("MAX_MARGIN_USDT", "500"),
            min_liquidation_buffer_percent=_decimal_env(
                "MIN_LIQUIDATION_BUFFER_PERCENT", "15"
            ),
            min_data_quality_score=_decimal_env(
                "MIN_DATA_QUALITY", "0.8",
            ),
            min_liquidity_score=_decimal_env(
                "MIN_LIQUIDITY_SCORE", "0.4",
            ),
            min_positioning_confidence=_decimal_env(
                "MIN_POSITIONING_CONFIDENCE",
                "0.6",
            ),
            max_crowding_score=_decimal_env(
                "MAX_CROWDING", "0.9",
            ),
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
                self.max_leverage,
                self.max_margin_usdt,
                self.min_liquidation_buffer_percent,
                self.min_data_quality_score,
                self.min_liquidity_score,
                self.min_positioning_confidence,
                self.max_crowding_score,
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
    wallet_balance: Decimal = Decimal("0")
    available_balance: Decimal = Decimal("0")
    equity: Decimal = Decimal("0")
    used_margin: Decimal = Decimal("0")
    initial_margin: Decimal = Decimal("0")
    maintenance_margin: Decimal = Decimal("0")
    position_direction: PositionDirection = "FLAT"
    position_quantity: Decimal = Decimal("0")
    position_notional: Decimal = Decimal("0")
    entry_price: Decimal | None = None
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    unrealized_pnl: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    funding_pnl: Decimal = Decimal("0")
    leverage: Decimal = Decimal("1")
    account_leverage: Decimal | None = None
    margin_type: str = "ISOLATED"
    position_mode: str = "ONE_WAY"
    liquidation_price: Decimal | None = None
    liquidation_distance_percent: Decimal | None = None
    open_orders: int = 0
    active_symbols: frozenset[str] = frozenset()
    existing_client_order_ids: frozenset[str] = frozenset()
    daily_pnl_usdt: Decimal = Decimal("0")
    drawdown_percent: Decimal = Decimal("0")
    liquidity_usdt: Decimal | None = None
    last_intent_at: datetime | None = None
    exchange_rules: ExchangeRules | None = None
    positioning_confidence: Decimal | None = None
    crowding_score: Decimal | None = None
    liquidity_score: Decimal | None = None
    data_quality_score: Decimal | None = None
    regime_risk: Decimal | None = None
    evidence_conflict: bool = False
    meme_risk_tier: MemeRiskTier = "TRADEABLE"


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


def classify_meme_risk_tier(
    *,
    crowding_score: Decimal | None = None,
    liquidity_score: Decimal | None = None,
    data_quality_score: Decimal | None = None,
    spread_bps: Decimal | None = None,
    open_interest: Decimal | None = None,
    trading: bool = True,
    limits: RiskLimits | None = None,
) -> MemeRiskTier:
    resolved = limits or RiskLimits()
    if not trading:
        return "BLOCK"
    if spread_bps is not None and spread_bps >= Decimal("50"):
        return "BLOCK"
    if open_interest is not None and open_interest <= 0:
        return "BLOCK"
    if data_quality_score is not None and data_quality_score < resolved.min_data_quality_score:
        return "OBSERVE"
    if liquidity_score is not None and liquidity_score < resolved.min_liquidity_score:
        return "OBSERVE"
    if crowding_score is not None and crowding_score >= resolved.max_crowding_score:
        return "REDUCED"
    return "TRADEABLE"


class RiskGate:
    """Evaluate all supplied pre-trade constraints without side effects."""

    def __init__(self, limits: RiskLimits | None = None) -> None:
        self.limits = limits or RiskLimits()

    def evaluate(self, intent: TradeIntent, context: RiskContext) -> RiskDecision:
        if context.halted:
            return self._halt(intent, "trading is halted")
        if os.environ.get("BIAN_KILL_SWITCH", "false").strip().lower() in {"1", "true", "yes", "on"}:
            if intent.action == "OPEN":
                return self._deny(intent, "kill switch blocks new opens")
        if context.evidence_conflict:
            return self._deny(intent, "positioning evidence is conflicted")
        if intent.action in {"REDUCE", "CLOSE"} and intent.positioning_state in {
            "UNKNOWN", "CONFLICTED",
        }:
            return self._deny(intent, "positioning state does not permit position reduction")
        if context.meme_risk_tier == "BLOCK" or intent.meme_risk_tier == "BLOCK":
            return self._deny(intent, "meme symbol is blocked")
        if context.meme_risk_tier == "OBSERVE" or intent.meme_risk_tier == "OBSERVE":
            return self._deny(intent, "meme symbol is observe-only")
        if (
            context.data_quality_score is not None
            and context.data_quality_score < self.limits.min_data_quality_score
        ):
            return self._deny(intent, "positioning data quality is below threshold")
        if (
            context.liquidity_score is not None
            and context.liquidity_score < self.limits.min_liquidity_score
        ):
            return self._deny(intent, "positioning liquidity is below threshold")
        if (
            context.positioning_confidence is not None
            and context.positioning_confidence < self.limits.min_positioning_confidence
        ):
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

        if context.margin_type.upper() != "ISOLATED" or intent.margin_type != "ISOLATED":
            return self._halt(intent, "margin type mismatch")
        if context.position_mode.upper() != "ONE_WAY" or intent.position_mode != "ONE_WAY":
            return self._halt(intent, "position mode mismatch")
        if intent.leverage > self.limits.max_leverage or context.leverage > self.limits.max_leverage:
            return self._deny(intent, "maximum leverage exceeded")
        if context.account_leverage is not None and intent.leverage != context.account_leverage:
            return self._halt(intent, "intent leverage does not match validated account leverage")

        rules = context.exchange_rules
        if rules is not None:
            if rules.symbol.upper() != intent.symbol:
                return self._deny(intent, "exchange rules symbol mismatch")
            if rules.status != "TRADING":
                return self._deny(intent, "symbol is not trading")

        mark_price = context.mark_price
        notional = self._notional(intent, mark_price)
        if notional is None:
            return self._deny(intent, "mark price is required to value the order")
        if notional <= 0:
            return self._deny(intent, "order notional must be positive")
        if (
            self.limits.min_liquidity_usdt > 0
            and _zero(context.liquidity_usdt) < self.limits.min_liquidity_usdt
        ):
            return self._deny(intent, "minimum liquidity requirement failed")
        if intent.price is not None and mark_price is not None:
            deviation = abs(intent.price - mark_price) / mark_price * 100
            if deviation > self.limits.max_price_deviation_percent:
                return self._deny(intent, "price deviation limit exceeded")

        action_decision = self._position_action(intent, context)
        if action_decision is not None:
            return action_decision

        normalized = self._normalize(intent, rules)
        if normalized is None:
            return self._deny(intent, "exchange quantity or price rules failed")
        normalized_notional = self._notional(normalized, mark_price)
        if normalized_notional is None or normalized_notional <= 0:
            return self._deny(intent, "normalized order notional is invalid")

        if rules is not None:
            if normalized.quantity < rules.min_qty:
                return self._deny(intent, "minimum quantity requirement failed")
            if rules.max_qty is not None and normalized.quantity > rules.max_qty:
                return self._deny(intent, "maximum quantity requirement failed")
            if normalized_notional < rules.min_notional:
                return self._deny(intent, "minimum notional requirement failed")

        max_order = self.limits.max_order_usdt
        max_position = self.limits.max_position_usdt
        if context.meme_risk_tier == "REDUCED" or intent.meme_risk_tier == "REDUCED":
            max_order = max_order / Decimal("2")
            max_position = max_position / Decimal("2")

        positioning_reduced = False
        if (
            context.crowding_score is not None
            and context.crowding_score >= self.limits.max_crowding_score
        ) or (
            context.regime_risk is not None
            and context.regime_risk >= Decimal("0.8")
        ):
            reduced_quantity = _floor_step(
                normalized.quantity / Decimal("2"),
                rules.step_size if rules is not None else Decimal("0"),
            )
            if reduced_quantity <= 0:
                return self._deny(intent, "positioning reduction falls below exchange minimum")
            normalized = normalized.model_copy(update={"quantity": reduced_quantity})
            normalized_notional = self._notional(normalized, mark_price)
            positioning_reduced = True
            if (
                normalized_notional is None
                or normalized_notional < self._minimum_notional(rules)
            ):
                return self._deny(
                    intent, "positioning reduction falls below exchange minimum"
                )

        if normalized_notional > max_order:
            reduced = self._reduce_to_order_limit(normalized, mark_price, rules, max_order)
            if reduced is None:
                return self._deny(intent, "maximum order notional exceeded")
            reduced_notional = self._notional(reduced, mark_price)
            if reduced_notional is None or reduced_notional < self._minimum_notional(rules):
                return self._deny(intent, "order limit reduction falls below exchange minimum")
            normalized = reduced
            normalized_notional = reduced_notional

        if intent.action == "OPEN":
            required_margin = (
                normalized_notional / intent.leverage
                if intent.leverage > 0
                else normalized_notional
            )
            if required_margin > context.available_balance:
                return self._deny(intent, "insufficient margin")
            if context.used_margin + required_margin > self.limits.max_margin_usdt:
                return self._deny(intent, "maximum margin exceeded")
            projected_position = normalized_notional
            if projected_position > max_position:
                return self._deny(intent, "maximum position notional exceeded")
            if context.liquidation_distance_percent is None:
                return self._deny(intent, "liquidation price is not verified")
            if context.liquidation_distance_percent < self.limits.min_liquidation_buffer_percent:
                return self._deny(intent, "liquidation buffer is too small")
        else:
            if not intent.reduce_only:
                return self._deny(intent, "reduce only")
            if normalized.quantity > context.position_quantity:
                return self._deny(intent, "close larger than position")

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

    def _position_action(
        self, intent: TradeIntent, context: RiskContext
    ) -> RiskDecision | None:
        current = context.position_direction
        if current not in {"LONG", "SHORT", "FLAT"}:
            return self._halt(intent, "position state cannot be confirmed")
        if intent.action == "OPEN":
            if current != "FLAT":
                if current != intent.direction:
                    return self._deny(intent, "reverse position requires close first")
                return self._deny(intent, "duplicate open is not allowed")
            return None
        if current == "FLAT":
            return self._deny(intent, "no position to reduce or close")
        if current != intent.direction:
            return self._deny(intent, "close direction does not match position")
        return None

    def _normalize(self, intent: TradeIntent, rules: ExchangeRules | None) -> TradeIntent | None:
        if rules is None:
            return intent
        updates: dict[str, Decimal] = {}
        if rules.step_size > 0:
            updates["quantity"] = _floor_step(intent.quantity, rules.step_size)
        if intent.price is not None and rules.tick_size > 0:
            updates["price"] = _floor_step(intent.price, rules.tick_size)
        if updates.get("quantity") == Decimal("0") or updates.get("price") == Decimal("0"):
            return None
        return intent.model_copy(update=updates) if updates else intent

    def _reduce_to_order_limit(
        self,
        intent: TradeIntent,
        mark_price: Decimal | None,
        rules: ExchangeRules | None,
        max_order: Decimal,
    ) -> TradeIntent | None:
        if mark_price is None:
            return None
        quantity = max_order / mark_price
        if rules is not None:
            quantity = _floor_step(quantity, rules.step_size)
        if quantity <= 0:
            return None
        return intent.model_copy(update={"quantity": quantity})

    def _notional(self, intent: TradeIntent, mark_price: Decimal | None) -> Decimal | None:
        price = intent.price or mark_price
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
