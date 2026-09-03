"""Unified execution boundary for paper, testnet, and live modes."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import os
import time
from typing import Any, Literal
from uuid import UUID

from binance_client import (
    BinanceConnectionError,
    BinanceRateLimitError,
    ClientConfig,
    FuturesPrivateClient,
)
from risk import FuturesAccountSnapshot, FuturesRiskRules, RiskDecision
from trade_intent import TradeIntent
from trading_store import TradingStore

OrderStatus = Literal[
    "CREATED",
    "RISK_APPROVED",
    "SUBMITTED",
    "ACKNOWLEDGED",
    "PARTIALLY_FILLED",
    "FILLED",
    "REJECTED",
    "CANCELLED",
    "EXPIRED",
    "FAILED",
    "UNKNOWN",
]
PositionDirection = Literal["LONG", "SHORT", "FLAT"]
PENDING_ORDER_STATES: frozenset[str] = frozenset(
    {
        "CREATED",
        "RISK_APPROVED",
        "SUBMITTED",
        "ACKNOWLEDGED",
        "PARTIALLY_FILLED",
        "UNKNOWN",
    }
)
TERMINAL_ORDER_STATES: frozenset[str] = frozenset(
    {
        "FILLED",
        "REJECTED",
        "CANCELLED",
        "EXPIRED",
        "FAILED",
    }
)
PAPER_RISK_RULES = FuturesRiskRules(symbol="PAPER")


@dataclass(frozen=True)
class MarketSnapshot:
    """Market inputs used by paper execution."""

    last_price: Decimal
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    bid_price: Decimal | None = None
    ask_price: Decimal | None = None
    # Canonical liquidity unit is USDT notional.
    available_liquidity_notional_usdt: Decimal | None = None
    funding_rate: Decimal | None = None
    funding_timestamp: datetime | None = None
    settlement_timestamp: datetime | None = None
    current_timestamp: datetime | None = None

    def __post_init__(self) -> None:
        if self.last_price <= 0:
            raise ValueError("market last_price must be positive")


@dataclass(frozen=True)
class ExecutionConfig:
    mode: str = "paper"
    fee_rate: Decimal = Decimal("0.001")
    slippage_bps: Decimal = Decimal("5")
    latency_ms: int = 0
    partial_fill_ratio: Decimal = Decimal("1")
    initial_usdt: Decimal = Decimal("1000")
    order_expiry_sec: int = 0
    default_leverage: Decimal = Decimal("1")
    risk_rules: FuturesRiskRules | None = PAPER_RISK_RULES

    @classmethod
    def from_env(cls, mode: str | None = None) -> "ExecutionConfig":
        resolved_mode = mode or os.environ.get("BIAN_MODE", "paper").strip().lower()
        return cls(
            mode=resolved_mode,
            fee_rate=Decimal(os.environ.get("PAPER_FEE_RATE", "0.001")),
            slippage_bps=Decimal(os.environ.get("PAPER_SLIPPAGE_BPS", "5")),
            latency_ms=max(0, int(os.environ.get("PAPER_LATENCY_MS", "0"))),
            partial_fill_ratio=Decimal(
                os.environ.get("PAPER_PARTIAL_FILL_RATIO", "1")
            ),
            initial_usdt=Decimal(os.environ.get("PAPER_INITIAL_USDT", "1000")),
            order_expiry_sec=max(0, int(os.environ.get("PAPER_ORDER_EXPIRY_SEC", "0"))),
            default_leverage=Decimal(os.environ.get("DEFAULT_LEVERAGE", "1")),
            risk_rules=(
                FuturesRiskRules(
                    symbol="PAPER",
                    maintenance_margin_rate=Decimal(
                        os.environ.get(
                            "PAPER_MAINTENANCE_MARGIN_RATE",
                            str(PAPER_RISK_RULES.maintenance_margin_rate),
                        )
                    ),
                )
                if resolved_mode == "paper" else None
            ),
        )

    def __post_init__(self) -> None:
        if self.mode not in {"paper", "testnet", "live"}:
            raise ValueError("execution mode must be paper, testnet, or live")
        if self.fee_rate < 0 or self.slippage_bps < 0:
            raise ValueError("fee and slippage cannot be negative")
        if not Decimal("0") < self.partial_fill_ratio <= Decimal("1"):
            raise ValueError("partial_fill_ratio must be in (0, 1]")
        if self.latency_ms < 0 or self.initial_usdt < 0 or self.order_expiry_sec < 0:
            raise ValueError("latency and initial balance cannot be negative")
        if self.default_leverage <= 0:
            raise ValueError("default_leverage must be positive")
        if self.mode == "paper" and self.risk_rules is None:
            raise ValueError("paper execution requires explicit FuturesRiskRules")


@dataclass(frozen=True)
class ExecutionResult:
    order_id: UUID
    intent_id: UUID
    status: OrderStatus
    client_order_id: str
    executed_quantity: Decimal = Decimal("0")
    executed_price: Decimal | None = None
    fee: Decimal = Decimal("0")
    exchange_order_id: str | None = None
    reason: str | None = None


class Executor(ABC):
    """Common interface shared by every broker implementation."""

    @abstractmethod
    def account_snapshot(self) -> FuturesAccountSnapshot:
        raise NotImplementedError

    @abstractmethod
    def submit(
        self,
        intent: TradeIntent,
        risk_decision: RiskDecision,
        *,
        market: MarketSnapshot | None = None,
    ) -> ExecutionResult:
        raise NotImplementedError

    @abstractmethod
    def cancel(self, order_id: UUID) -> ExecutionResult:
        raise NotImplementedError

    @abstractmethod
    def get_order(self, order_id: UUID) -> ExecutionResult:
        raise NotImplementedError

    @abstractmethod
    def get_open_orders(self) -> list[ExecutionResult]:
        raise NotImplementedError

    @abstractmethod
    def recover(self) -> list[ExecutionResult]:
        raise NotImplementedError


class _BaseExecutor(Executor):
    def __init__(self, store: TradingStore, config: ExecutionConfig) -> None:
        self.store = store
        self.config = config
        self.store.initialize()

    def _approve(self, intent: TradeIntent, risk_decision: RiskDecision) -> TradeIntent:
        if risk_decision.intent.id != intent.id:
            raise ValueError("risk decision does not match trade intent")
        if risk_decision.decision not in {"ALLOW", "REDUCE"}:
            self.store.record_risk_event(
                intent_id=intent.id,
                decision=risk_decision.decision,
                reason=risk_decision.reason,
                payload=self._risk_audit_payload(risk_decision),
            )
            raise ExecutionRejected(risk_decision.reason)
        approved = risk_decision.executable_intent
        if approved is None:
            raise ExecutionRejected("risk decision has no executable intent")
        self.store.record_risk_event(
            intent_id=intent.id,
            decision=risk_decision.decision,
            reason=risk_decision.reason,
            payload=self._risk_audit_payload(risk_decision),
        )
        self.store.record_intent(approved, status="RISK_APPROVED")
        return approved

    def _risk_audit_payload(self, decision: RiskDecision) -> dict[str, Any]:
        """Persist strategy, risk, adjustment, and executable outcomes together."""
        return {
            "mode": self.config.mode,
            "market": "FUTURES",
            "strategy_action": decision.strategy_action,
            "risk_decision": decision.risk_decision,
            "risk_adjustment": decision.risk_adjustment,
            "final_action": decision.final_action,
            "final_quantity": (
                str(decision.final_quantity)
                if decision.final_quantity is not None else None
            ),
        }

    def _create_order(
        self,
        intent: TradeIntent,
        *,
        status: OrderStatus,
        expires_at: datetime | None = None,
    ) -> UUID:
        return self.store.create_order(
            intent,
            mode=self.config.mode,
            status=status,
            expires_at=expires_at,
        )

    def _event(self, order_id: UUID, event_type: str, status: OrderStatus, **payload: Any) -> None:
        self.store.append_order_event(
            order_id,
            event_type=event_type,
            status=status,
            payload=payload,
        )


class ExecutionRejected(RuntimeError):
    """The risk decision did not permit order submission."""


def _decimal_field(row: dict[str, Any] | None, *names: str, default: str = "0") -> Decimal:
    if not row:
        return Decimal(default)
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    for name in names:
        if row.get(name) is not None:
            return Decimal(str(row[name]))
        if payload.get(name) is not None:
            return Decimal(str(payload[name]))
    return Decimal(default)


def _mark_price(market: MarketSnapshot) -> Decimal:
    if market.mark_price is not None and market.mark_price > 0:
        return market.mark_price
    raise ValueError("paper mark-to-market requires a positive mark_price")


class PaperExecutor(_BaseExecutor):
    """Deterministic futures paper broker with margin, funding, and liquidation."""

    def __init__(
        self,
        store: TradingStore | None = None,
        config: ExecutionConfig | None = None,
    ) -> None:
        config = config or ExecutionConfig(mode="paper")
        if config.mode != "paper":
            raise ValueError("PaperExecutor requires mode=paper")
        super().__init__(store or TradingStore(), config)
        self._ensure_initial_balance()

    def submit(
        self,
        intent: TradeIntent,
        risk_decision: RiskDecision,
        *,
        market: MarketSnapshot | None = None,
    ) -> ExecutionResult:
        if market is None or market.last_price <= 0:
            raise ValueError("paper execution requires a positive market snapshot")
        if market.mark_price is None or market.mark_price <= 0:
            raise ValueError("paper execution requires a positive mark_price")
        if self.store.is_halted() and intent.action == "OPEN":
            raise ExecutionRejected("trading is halted")
        approved = self._approve(intent, risk_decision)
        requested_quantity = approved.quantity
        existing_getter = getattr(self.store, "get_order_by_client_order_id", None)
        existing = (
            existing_getter(approved.client_order_id)
            if existing_getter is not None
            else None
        )
        if existing is not None:
            return self.get_order(UUID(str(existing["order_id"])))
        estimated_price = self._fill_price(approved, market)
        estimated_notional = requested_quantity * estimated_price
        required_margin = (
            estimated_notional / approved.leverage
            if approved.action == "OPEN"
            else Decimal("0")
        )
        account = self.account_state()
        fee_estimate = estimated_notional * self.config.fee_rate
        if approved.action == "OPEN" and account["available_balance"] < required_margin + fee_estimate:
            self.store.record_system_event(
                event_type="ORDER_REJECTED",
                severity="WARNING",
                message="paper margin is insufficient",
                payload={
                    "intent_id": str(approved.id),
                    "required": str(required_margin + fee_estimate),
                    "available": str(account["available_balance"]),
                },
            )
            raise ExecutionRejected("paper margin is insufficient")
        expiry_base = market.current_timestamp or datetime.now(timezone.utc)
        if expiry_base.tzinfo is None:
            expiry_base = expiry_base.replace(tzinfo=timezone.utc)
        expires_at = (
            expiry_base + timedelta(seconds=self.config.order_expiry_sec)
            if approved.order_type == "LIMIT" and self.config.order_expiry_sec > 0
            else None
        )
        if self.config.latency_ms:
            time.sleep(self.config.latency_ms / 1000)
        order_id = self._create_order(
            approved,
            status="CREATED",
            expires_at=expires_at,
        )
        self._event(order_id, "ORDER_CREATED", "CREATED")
        self.store.update_order(order_id, status="RISK_APPROVED")
        self._event(order_id, "ORDER_RISK_APPROVED", "RISK_APPROVED")
        self.store.update_order(order_id, status="SUBMITTED")
        self._event(order_id, "ORDER_SUBMITTED", "SUBMITTED")
        self.store.update_order(order_id, status="ACKNOWLEDGED")
        self._event(order_id, "ORDER_ACKNOWLEDGED", "ACKNOWLEDGED")

        if not self._is_marketable(approved, market):
            return ExecutionResult(
                order_id=order_id,
                intent_id=approved.id,
                status="ACKNOWLEDGED",
                client_order_id=approved.client_order_id,
                reason="limit order is resting",
            )

        return self._apply_fill(
            order_id,
            approved,
            market,
            requested_quantity=requested_quantity,
            current_executed=Decimal("0"),
        )

    def cancel(self, order_id: UUID) -> ExecutionResult:
        local = self._local_order(order_id)
        if local is not None and str(local.get("status")) in {
            "FILLED",
            "REJECTED",
            "CANCELLED",
            "EXPIRED",
            "FAILED",
        }:
            return _result_from_local(local)
        self.store.update_order(order_id, status="CANCELLED")
        self._event(order_id, "ORDER_CANCELLED", "CANCELLED")
        return _result_from_local(
            {
                **(local or {}),
                "order_id": order_id,
                "status": "CANCELLED",
            }
        )

    def get_order(self, order_id: UUID) -> ExecutionResult:
        local = self._local_order(order_id)
        if local is None:
            raise KeyError(f"unknown paper order: {order_id}")
        self._expire_if_needed(local)
        local = self._local_order(order_id) or local
        return _result_from_local(local)

    def get_open_orders(self) -> list[ExecutionResult]:
        rows = getattr(self.store, "list_open_local_orders", lambda: [])()
        results: list[ExecutionResult] = []
        for row in rows:
            self._expire_if_needed(row)
            refreshed = self._local_order(UUID(str(row["order_id"]))) or row
            if str(refreshed.get("status")) not in {
                "FILLED",
                "REJECTED",
                "CANCELLED",
                "EXPIRED",
                "FAILED",
            }:
                results.append(_result_from_local(refreshed))
        return results

    def recover(self) -> list[ExecutionResult]:
        return self.get_open_orders()

    def process_market(
        self,
        order_id: UUID,
        market: MarketSnapshot,
    ) -> ExecutionResult:
        """Advance one persisted paper order using the next market snapshot."""
        local = self._local_order(order_id)
        if local is None:
            raise KeyError(f"unknown paper order: {order_id}")
        self._expire_if_needed(local)
        local = self._local_order(order_id) or local
        if str(local.get("status")) in {
            "FILLED",
            "REJECTED",
            "CANCELLED",
            "EXPIRED",
            "FAILED",
        }:
            return _result_from_local(local)
        intent = self._intent_from_order(local)
        requested_quantity = intent.quantity
        result = self._apply_fill(
            order_id,
            intent,
            market,
            requested_quantity=requested_quantity,
            current_executed=Decimal(str(local.get("executed_quantity", "0"))),
        )
        return result

    def account_state(self) -> dict[str, Decimal]:
        row = self.store.get_balance("USDT") or {}
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        wallet = _decimal_field(row, "wallet_balance", default=str(row.get("free", self.config.initial_usdt)))
        used = _decimal_field(row, "used_margin", default=str(row.get("locked", "0")))
        unrealized = _decimal_field(row, "unrealized_pnl", default="0")
        funding = _decimal_field(row, "funding_pnl", default="0")
        realized = _decimal_field(row, "realized_pnl", default="0")
        available = wallet - used
        equity = wallet + unrealized
        return {
            "wallet_balance": wallet,
            "available_balance": available,
            "used_margin": used,
            "unrealized_pnl": unrealized,
            "realized_pnl": realized,
            "funding_pnl": funding,
            "equity": equity,
            "margin_balance": wallet + unrealized,
        }

    def account_snapshot(self) -> FuturesAccountSnapshot:
        account = self.account_state()
        positions = getattr(self.store, "list_positions", lambda: [])()
        configured = {
            str(row.get("symbol")).upper(): self.config.default_leverage
            for row in positions
            if row.get("symbol")
        }
        return FuturesAccountSnapshot.from_paper(
            account=account,
            positions=positions,
            open_orders=getattr(self.store, "list_open_local_orders", lambda: [])(),
            captured_at=datetime.now(timezone.utc),
            symbol_leverage=configured,
        )

    def liquidation_price_for(
        self,
        symbol: str,
        direction: PositionDirection,
        entry_price: Decimal,
        leverage: Decimal,
    ) -> Decimal:
        """Return the paper model price used for pre-trade liquidation checks.

        PAPER_ONLY / SIMPLIFIED / NOT_BINANCE_PARITY: this is not Binance
        liquidation engine parity.
        """
        if direction == "FLAT":
            raise ValueError("FLAT has no liquidation price")
        return self._liquidation_price(
            direction,
            entry_price,
            leverage,
            self._risk_rules(symbol),
        )

    def mark_to_market(self, symbol: str, mark_price: Decimal) -> None:
        if mark_price <= 0:
            raise ValueError("mark price must be positive")
        position = self.store.get_position(symbol)
        if position is None:
            return
        quantity = _decimal_field(position, "quantity")
        direction = str(position.get("position_side") or position.get("direction") or "FLAT")
        if quantity == 0 or direction == "FLAT":
            self._persist_position(symbol, direction="FLAT", quantity=Decimal("0"), entry_price=Decimal("0"),
                                   mark_price=mark_price, realized_pnl=_decimal_field(position, "realized_pnl"),
                                   unrealized_pnl=Decimal("0"), funding_pnl=_decimal_field(position, "funding_pnl"),
                                   index_price=_decimal_field(position, "index_price", default="0") or None,
                                   leverage=_decimal_field(position, "leverage", default=str(self.config.default_leverage)),
                                   initial_margin=Decimal("0"), maintenance_margin=Decimal("0"),
                                   liquidation_price=None, fee_pnl=_decimal_field(position, "fee_pnl"))
            self._sync_account_unrealized(Decimal("0"))
            return
        entry = _decimal_field(position, "entry_price", "average_price")
        unrealized = (
            (mark_price - entry) * quantity
            if direction == "LONG"
            else (entry - mark_price) * quantity
        )
        leverage = _decimal_field(position, "leverage", default=str(self.config.default_leverage))
        notional = quantity * mark_price
        initial_margin = _decimal_field(position, "initial_margin", default=str(notional / leverage if leverage else notional))
        rules = self._risk_rules(symbol)
        maintenance_margin = notional * rules.maintenance_margin_rate
        liquidation_price = self._liquidation_price(direction, entry, leverage, rules)
        remaining = initial_margin + unrealized
        self._persist_position(
            symbol,
            direction=direction,  # type: ignore[arg-type]
            quantity=quantity,
            entry_price=entry,
            mark_price=mark_price,
            index_price=_decimal_field(position, "index_price", default="0") or None,
            realized_pnl=_decimal_field(position, "realized_pnl"),
            unrealized_pnl=unrealized,
            funding_pnl=_decimal_field(position, "funding_pnl"),
            leverage=leverage,
            initial_margin=initial_margin,
            maintenance_margin=maintenance_margin,
            liquidation_price=liquidation_price,
            fee_pnl=_decimal_field(position, "fee_pnl"),
        )
        self._sync_account_unrealized(unrealized)
        if remaining <= maintenance_margin:
            self._liquidate(symbol, mark_price, direction=direction, quantity=quantity, entry=entry)

    def apply_funding(self, symbol: str, market: MarketSnapshot) -> Decimal:
        if market.funding_rate is None:
            return Decimal("0")
        settlement = market.settlement_timestamp
        if settlement is None:
            return Decimal("0")
        current_timestamp = market.current_timestamp or settlement
        if settlement.tzinfo is None:
            settlement = settlement.replace(tzinfo=timezone.utc)
        if current_timestamp.tzinfo is None:
            current_timestamp = current_timestamp.replace(tzinfo=timezone.utc)
        if current_timestamp < settlement:
            return Decimal("0")
        position = self.store.get_position(symbol)
        if position is None:
            return Decimal("0")
        quantity = _decimal_field(position, "quantity")
        direction = str(position.get("position_side") or "FLAT")
        if quantity <= 0 or direction == "FLAT":
            return Decimal("0")
        payload = position.get("payload") if isinstance(position.get("payload"), dict) else {}
        last = payload.get("last_funding_settlement_timestamp")
        stamp = settlement.isoformat()
        if last is not None:
            try:
                last_timestamp = datetime.fromisoformat(str(last))
                if last_timestamp.tzinfo is None:
                    last_timestamp = last_timestamp.replace(tzinfo=timezone.utc)
                if settlement <= last_timestamp:
                    return Decimal("0")
            except ValueError:
                pass
        mark = _mark_price(market)
        notional = quantity * mark
        payment = notional * market.funding_rate
        signed = payment if direction == "LONG" else -payment
        recorder = getattr(self.store, "record_funding_settlement", None)
        if recorder is not None:
            inserted = recorder(
                mode=self.config.mode,
                symbol=symbol,
                settlement_timestamp=settlement,
                rate=market.funding_rate,
                notional=notional,
                payment=-signed,
                position_side=direction,
            )
            if not inserted:
                return Decimal("0")
        elif getattr(self.store, "funding_settlement_exists", None) is not None:
            if self.store.funding_settlement_exists(
                mode=self.config.mode, symbol=symbol, settlement_timestamp=settlement
            ):
                return Decimal("0")
        account = self.account_state()
        wallet = account["wallet_balance"] - signed
        funding_pnl = _decimal_field(position, "funding_pnl") - signed
        self._write_account(
            wallet_balance=wallet,
            used_margin=account["used_margin"],
            unrealized_pnl=account["unrealized_pnl"],
            realized_pnl=account["realized_pnl"],
            funding_pnl=account["funding_pnl"] - signed,
        )
        extra = dict(payload)
        extra["last_funding_settlement_timestamp"] = stamp
        extra["funding_timestamp"] = stamp
        extra["funding_pnl"] = str(funding_pnl)
        self.store.upsert_position(
            symbol,
            quantity=quantity,
            average_price=_decimal_field(position, "entry_price", "average_price"),
            realized_pnl=_decimal_field(position, "realized_pnl"),
            unrealized_pnl=_decimal_field(position, "unrealized_pnl"),
            position_side=direction,
            entry_price=_decimal_field(position, "entry_price", "average_price"),
            mark_price=mark,
            index_price=market.index_price,
            funding_pnl=funding_pnl,
            leverage=_decimal_field(position, "leverage", default="1"),
            payload=extra,
        )
        self.store.record_system_event(
            event_type="FUNDING_SETTLED",
            severity="INFO",
            message=f"funding settled for {symbol}",
            payload={
                "mode": self.config.mode,
                "symbol": symbol,
                "funding_timestamp": stamp,
                "funding_rate": str(market.funding_rate),
                "position_notional": str(notional),
                "funding_pnl": str(-signed),
            },
        )
        return -signed

    def _liquidate(
        self,
        symbol: str,
        mark_price: Decimal,
        *,
        direction: str,
        quantity: Decimal,
        entry: Decimal,
    ) -> None:
        realized = (
            (mark_price - entry) * quantity
            if direction == "LONG"
            else (entry - mark_price) * quantity
        )
        account = self.account_state()
        wallet = account["wallet_balance"] + realized
        self._write_account(
            wallet_balance=wallet,
            used_margin=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            realized_pnl=account["realized_pnl"] + realized,
            funding_pnl=account["funding_pnl"],
        )
        self._persist_position(
            symbol,
            direction="FLAT",
            quantity=Decimal("0"),
            entry_price=Decimal("0"),
            mark_price=mark_price,
            index_price=None,
            realized_pnl=_decimal_field(self.store.get_position(symbol), "realized_pnl") + realized,
            unrealized_pnl=Decimal("0"),
            funding_pnl=_decimal_field(self.store.get_position(symbol), "funding_pnl"),
            leverage=Decimal("1"),
            initial_margin=Decimal("0"),
            maintenance_margin=Decimal("0"),
            liquidation_price=None,
            fee_pnl=_decimal_field(self.store.get_position(symbol), "fee_pnl"),
            extra={"liquidated": True},
        )
        self.store.record_system_event(
            event_type="LIQUIDATED",
            severity="CRITICAL",
            message=f"{symbol} paper position liquidated at mark {mark_price}",
            payload={
                "mode": self.config.mode,
                "symbol": symbol,
                "mark_price": str(mark_price),
                "scope": "PAPER_ONLY",
                "model": "SIMPLIFIED",
                "binance_parity": "NOT_BINANCE_PARITY",
            },
        )
        setter = getattr(self.store, "set_halt", None)
        if setter is not None:
            setter(True, reason="LIQUIDATED", source="paper")
        elif hasattr(self.store, "halted"):
            self.store.halted = True

    def _risk_rules(self, symbol: str) -> FuturesRiskRules:
        configured = self.config.risk_rules
        if configured is None:
            raise ExecutionRejected("Futures risk rules are unavailable")
        if configured.symbol not in {"PAPER", symbol.upper()}:
            raise ExecutionRejected(
                f"risk rules for {configured.symbol} cannot price {symbol.upper()}"
            )
        return configured

    def _liquidation_price(
        self,
        direction: str,
        entry: Decimal,
        leverage: Decimal,
        rules: FuturesRiskRules,
    ) -> Decimal:
        if leverage <= 0:
            raise ValueError("leverage must be positive")
        denominator = Decimal("1") - rules.maintenance_margin_rate
        if denominator <= 0:
            raise ValueError("maintenance margin rate leaves no liquidation denominator")
        maintenance_adjusted = (Decimal("1") - (Decimal("1") / leverage)) / denominator
        if direction == "LONG":
            return max(Decimal("0"), entry * maintenance_adjusted)
        return entry * (Decimal("2") - maintenance_adjusted)

    def _local_order(self, order_id: UUID) -> dict[str, Any] | None:
        getter = getattr(self.store, "get_order", None)
        return getter(order_id) if getter is not None else None

    def _expire_if_needed(self, order: dict[str, Any]) -> None:
        expires_at = order.get("expires_at")
        if not expires_at or str(order.get("status")) in {
            "FILLED",
            "REJECTED",
            "CANCELLED",
            "EXPIRED",
            "FAILED",
        }:
            return
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) >= expires_at:
            order_id = UUID(str(order["order_id"]))
            self.store.update_order(order_id, status="EXPIRED")
            self._event(order_id, "ORDER_EXPIRED", "EXPIRED")

    def _is_marketable(self, intent: TradeIntent, market: MarketSnapshot) -> bool:
        if intent.order_type != "LIMIT" or intent.price is None:
            return True
        side = intent.exchange_side()
        reference = market.ask_price or market.last_price if side == "BUY" else market.bid_price or market.last_price
        return intent.price >= reference if side == "BUY" else intent.price <= reference

    def _fill_price(self, intent: TradeIntent, market: MarketSnapshot) -> Decimal:
        side = intent.exchange_side()
        if intent.order_type == "LIMIT" and intent.price is not None:
            return intent.price
        if side == "BUY":
            base = market.ask_price or market.last_price
        else:
            base = market.bid_price or market.last_price
        slippage = self.config.slippage_bps / Decimal("10000")
        price = base * (Decimal("1") + slippage if side == "BUY" else Decimal("1") - slippage)
        liquidity_quantity = self._liquidity_quantity(intent, market, base)
        if liquidity_quantity is not None and liquidity_quantity > 0:
            impact = min(Decimal("1"), intent.quantity / liquidity_quantity) * slippage
            price = price * (Decimal("1") + impact if side == "BUY" else Decimal("1") - impact)
        return price

    def _liquidity_quantity(
        self,
        intent: TradeIntent,
        market: MarketSnapshot,
        reference_price: Decimal,
    ) -> Decimal | None:
        del intent
        notional = market.available_liquidity_notional_usdt
        if notional is None or notional <= 0 or reference_price <= 0:
            return None
        return notional / reference_price

    def _apply_fill(
        self,
        order_id: UUID,
        intent: TradeIntent,
        market: MarketSnapshot,
        *,
        requested_quantity: Decimal,
        current_executed: Decimal,
    ) -> ExecutionResult:
        remaining = max(Decimal("0"), requested_quantity - current_executed)
        if remaining <= 0:
            return _result_from_local(self._local_order(order_id) or {})
        reference_price = (
            market.ask_price or market.last_price
            if intent.exchange_side() == "BUY"
            else market.bid_price or market.last_price
        )
        liquidity_quantity = self._liquidity_quantity(intent, market, reference_price)
        available = min(remaining, liquidity_quantity) if liquidity_quantity is not None else remaining
        fill_ratio = (
            self.config.partial_fill_ratio
            if current_executed == 0
            else Decimal("1")
        )
        fill_quantity = available * fill_ratio
        if fill_quantity <= 0:
            status: OrderStatus = "REJECTED" if current_executed == 0 else "PARTIALLY_FILLED"
            self.store.update_order(order_id, status=status)
            self._event(order_id, f"ORDER_{status}", status, reason="no executable quantity")
            return _result_from_local(
                {
                    **(self._local_order(order_id) or {}),
                    "order_id": order_id,
                    "status": status,
                }
            )

        fill_price = self._fill_price(intent, market)
        cumulative = current_executed + fill_quantity
        status = "FILLED" if cumulative >= requested_quantity else "PARTIALLY_FILLED"
        self.store.update_order(
            order_id,
            status=status,
            executed_quantity=cumulative,
        )
        self._event(
            order_id,
            f"ORDER_{status}",
            status,
            quantity=str(fill_quantity),
            cumulative_quantity=str(cumulative),
            price=str(fill_price),
        )
        fee = fill_quantity * fill_price * self.config.fee_rate
        mid = ( (market.bid_price or market.last_price) + (market.ask_price or market.last_price) ) / Decimal("2")
        slippage = (fill_price - mid) * fill_quantity if intent.exchange_side() == "BUY" else (mid - fill_price) * fill_quantity
        realized_delta = self._update_account(
            intent, fill_quantity, fill_price, fee, slippage,
            index_price=market.index_price,
        )
        trade_id = self.store.record_trade(
            order_id,
            symbol=intent.symbol,
            side=intent.exchange_side(),
            quantity=fill_quantity,
            price=fill_price,
            fee=fee,
            fee_asset="USDT",
            realized_pnl=realized_delta,
            position_side=intent.direction,
            funding=Decimal("0"),
            payload={
                "mode": "paper",
                "action": intent.action,
                "fee_pnl": str(-fee),
                "slippage": str(slippage),
            },
        )
        self.mark_to_market(intent.symbol, _mark_price(market))
        return ExecutionResult(
            order_id=order_id,
            intent_id=intent.id,
            status=status,
            client_order_id=intent.client_order_id,
            executed_quantity=cumulative,
            executed_price=fill_price,
            fee=fee,
            reason=str(trade_id),
        )

    def _intent_from_order(self, order: dict[str, Any]) -> TradeIntent:
        stored = order.get("intent")
        if isinstance(stored, TradeIntent):
            return stored
        payload = order.get("payload") if isinstance(order.get("payload"), dict) else {}
        intent_payload = payload.get("intent") if isinstance(payload.get("intent"), dict) else payload
        direction = str(order.get("position_side") or intent_payload.get("direction") or "LONG")
        action = str(order.get("position_action") or intent_payload.get("action") or "OPEN")
        reduce_only = bool(order.get("reduce_only") if order.get("reduce_only") is not None else action != "OPEN")
        return TradeIntent(
            id=UUID(str(order["intent_id"])),
            symbol=str(order["symbol"]),
            direction=direction,  # type: ignore[arg-type]
            action=action,  # type: ignore[arg-type]
            reduce_only=reduce_only,
            leverage=Decimal(str(order.get("leverage") or intent_payload.get("leverage") or "1")),
            order_type=str(order["order_type"]),  # type: ignore[arg-type]
            quantity=Decimal(str(order["quantity"])),
            price=Decimal(str(order["price"])) if order.get("price") is not None else None,
            confidence=Decimal("0"),
            reason="recovered paper order",
            strategy_version="recovered",
            client_order_id=str(order["client_order_id"]),
        )

    def _ensure_initial_balance(self) -> None:
        if self.store.get_balance("USDT") is None:
            self._write_account(
                wallet_balance=self.config.initial_usdt,
                used_margin=Decimal("0"),
                unrealized_pnl=Decimal("0"),
                realized_pnl=Decimal("0"),
                funding_pnl=Decimal("0"),
                initial=True,
            )

    def _write_account(
        self,
        *,
        wallet_balance: Decimal,
        used_margin: Decimal,
        unrealized_pnl: Decimal,
        realized_pnl: Decimal,
        funding_pnl: Decimal,
        initial: bool = False,
    ) -> None:
        available = wallet_balance - used_margin
        equity = wallet_balance + unrealized_pnl
        if wallet_balance < 0 or used_margin < 0 or available < 0:
            raise RuntimeError("paper accounting produced negative balance or margin")
        if equity != wallet_balance + unrealized_pnl:
            raise RuntimeError("paper equity invariant failed")
        if available != wallet_balance - used_margin:
            raise RuntimeError("paper available balance invariant failed")
        self.store.upsert_balance(
            "USDT",
            free=available,
            locked=used_margin,
            wallet_balance=wallet_balance,
            available_balance=available,
            margin_balance=wallet_balance + unrealized_pnl,
            used_margin=used_margin,
            unrealized_pnl=unrealized_pnl,
            mode="paper",
            payload={
                "updated_by": "paper_execution",
                "initial": initial,
                "wallet_balance": str(wallet_balance),
                "available_balance": str(available),
                "margin_balance": str(wallet_balance + unrealized_pnl),
                "used_margin": str(used_margin),
                "unrealized_pnl": str(unrealized_pnl),
                "realized_pnl": str(realized_pnl),
                "funding_pnl": str(funding_pnl),
            },
        )

    def _sync_account_unrealized(self, unrealized: Decimal) -> None:
        account = self.account_state()
        self._write_account(
            wallet_balance=account["wallet_balance"],
            used_margin=account["used_margin"],
            unrealized_pnl=unrealized,
            realized_pnl=account["realized_pnl"],
            funding_pnl=account["funding_pnl"],
        )

    def _persist_position(
        self,
        symbol: str,
        *,
        direction: PositionDirection,
        quantity: Decimal,
        entry_price: Decimal,
        mark_price: Decimal,
        realized_pnl: Decimal,
        unrealized_pnl: Decimal,
        funding_pnl: Decimal,
        leverage: Decimal,
        initial_margin: Decimal,
        maintenance_margin: Decimal,
        liquidation_price: Decimal | None,
        fee_pnl: Decimal,
        index_price: Decimal | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        notional = quantity * mark_price
        current = self.store.get_position(symbol)
        current_payload = (
            current.get("payload")
            if isinstance(current, dict) and isinstance(current.get("payload"), dict)
            else {}
        )
        payload = {
            **current_payload,
            "mode": "paper",
            "market": "FUTURES",
            "position_side": direction,
            "entry_price": str(entry_price),
            "mark_price": str(mark_price),
            "index_price": str(index_price) if index_price is not None else None,
            "notional": str(notional),
            "leverage": str(leverage),
            "margin_type": "ISOLATED",
            "initial_margin": str(initial_margin),
            "maintenance_margin": str(maintenance_margin),
            "liquidation_price": str(liquidation_price) if liquidation_price is not None else None,
            "funding_pnl": str(funding_pnl),
            "fee_pnl": str(fee_pnl),
            "trading_pnl": str(realized_pnl + unrealized_pnl),
            "net_pnl": str(realized_pnl + unrealized_pnl + funding_pnl + fee_pnl),
            **(extra or {}),
        }
        self.store.upsert_position(
            symbol,
            quantity=quantity,
            average_price=entry_price,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            market="FUTURES",
            position_side=direction,
            entry_price=entry_price,
            mark_price=mark_price,
            index_price=index_price,
            notional=notional,
            leverage=leverage,
            margin_type="ISOLATED",
            initial_margin=initial_margin,
            maintenance_margin=maintenance_margin,
            liquidation_price=liquidation_price,
            funding_pnl=funding_pnl,
            payload=payload,
        )

    def _update_account(
        self,
        intent: TradeIntent,
        quantity: Decimal,
        price: Decimal,
        fee: Decimal,
        slippage: Decimal,
        *,
        index_price: Decimal | None = None,
    ) -> Decimal:
        position = self.store.get_position(intent.symbol)
        current_direction = str((position or {}).get("position_side") or "FLAT")
        current_quantity = _decimal_field(position, "quantity")
        current_entry = _decimal_field(position, "entry_price", "average_price")
        current_realized = _decimal_field(position, "realized_pnl")
        current_funding = _decimal_field(position, "funding_pnl")
        current_fee = _decimal_field(position, "fee_pnl")
        current_im = _decimal_field(position, "initial_margin")
        account = self.account_state()
        wallet = account["wallet_balance"] - fee
        used = account["used_margin"]
        fee_pnl = current_fee - fee
        realized_delta = -fee
        direction: PositionDirection = current_direction if current_direction in {"LONG", "SHORT", "FLAT"} else "FLAT"
        next_quantity = current_quantity
        next_entry = current_entry
        next_im = current_im
        leverage = intent.leverage

        if intent.action == "OPEN":
            if current_quantity > 0 and current_direction not in {"FLAT", "", intent.direction}:
                raise ExecutionRejected("cannot open over existing opposite position")
            direction = intent.direction
            next_quantity = current_quantity + quantity
            next_entry = (
                ((current_quantity * current_entry) + (quantity * price)) / next_quantity
                if current_quantity > 0
                else price
            )
            added_im = (quantity * price) / leverage
            next_im = current_im + added_im
            used = used + added_im
        else:
            if current_direction != intent.direction or current_quantity <= 0:
                raise ExecutionRejected("no matching position to reduce or close")
            close_qty = min(quantity, current_quantity)
            trading_pnl = (
                (price - current_entry) * close_qty
                if current_direction == "LONG"
                else (current_entry - price) * close_qty
            )
            realized_delta = trading_pnl - fee
            current_realized = current_realized + trading_pnl
            wallet = account["wallet_balance"] + realized_delta
            released = current_im * (close_qty / current_quantity) if current_quantity else current_im
            used = max(Decimal("0"), used - released)
            next_quantity = current_quantity - close_qty
            next_im = max(Decimal("0"), current_im - released)
            if next_quantity <= 0:
                direction = "FLAT"
                next_quantity = Decimal("0")
                next_entry = Decimal("0")
                next_im = Decimal("0")
                used = Decimal("0")
            else:
                direction = current_direction  # type: ignore[assignment]
                next_entry = current_entry

        self._write_account(
            wallet_balance=wallet,
            used_margin=used,
            unrealized_pnl=Decimal("0"),
            realized_pnl=account["realized_pnl"] + (realized_delta + fee if intent.action != "OPEN" else Decimal("0")),
            funding_pnl=account["funding_pnl"],
        )
        self._persist_position(
            intent.symbol,
            direction=direction,
            quantity=next_quantity,
            entry_price=next_entry,
            mark_price=price,
            realized_pnl=current_realized,
            unrealized_pnl=Decimal("0"),
            funding_pnl=current_funding,
            leverage=leverage,
            initial_margin=next_im,
            maintenance_margin=next_im * self._risk_rules(intent.symbol).maintenance_margin_rate,
            liquidation_price=(
                self._liquidation_price(
                    direction,
                    next_entry,
                    leverage,
                    self._risk_rules(intent.symbol),
                )
                if direction != "FLAT"
                else None
            ),
            fee_pnl=fee_pnl,
            index_price=index_price,
            extra={"slippage": str(slippage)},
        )
        return realized_delta


class BinanceExecutor(_BaseExecutor):
    """Testnet/live executor using the same interface as PaperExecutor."""

    def __init__(
        self,
        store: TradingStore | None = None,
        config: ExecutionConfig | None = None,
        client_config: ClientConfig | None = None,
    ) -> None:
        config = config or ExecutionConfig(mode="testnet")
        if config.mode not in {"testnet", "live"}:
            raise ValueError("BinanceExecutor requires testnet or live mode")
        super().__init__(store or TradingStore(), config)
        resolved_client_config = client_config or ClientConfig.from_env()
        if resolved_client_config.mode != config.mode:
            raise ValueError("execution and Binance client modes must match")
        self.client = FuturesPrivateClient(resolved_client_config)

    def account_snapshot(self) -> FuturesAccountSnapshot:
        return self.client.account_snapshot()

    def submit(
        self,
        intent: TradeIntent,
        risk_decision: RiskDecision,
        *,
        market: MarketSnapshot | None = None,
    ) -> ExecutionResult:
        if self.store.is_halted() and intent.action == "OPEN":
            raise ExecutionRejected("trading is halted")
        approved = self._approve(intent, risk_decision)
        order_id = self._create_order(approved, status="CREATED")
        self._event(order_id, "ORDER_CREATED", "CREATED")
        self.store.update_order(order_id, status="RISK_APPROVED")
        self._event(order_id, "ORDER_RISK_APPROVED", "RISK_APPROVED")
        try:
            response = self.client.create_order(
                symbol=approved.symbol,
                side=approved.exchange_side(),
                order_type=approved.order_type,
                quantity=approved.quantity,
                price=approved.price,
                client_order_id=approved.client_order_id,
                reduce_only=approved.reduce_only,
                position_side=(
                    "BOTH" if approved.position_mode == "ONE_WAY" else approved.direction
                ),
            )
        except Exception as exc:
            if not self._is_uncertain_transport_error(exc):
                self.store.update_order(
                    order_id, status="REJECTED", payload={"error": str(exc)}
                )
                self._event(order_id, "ORDER_REJECTED", "REJECTED", error=str(exc))
                raise
            return self._resolve_uncertain_order(order_id, approved, exc)
        exchange_order_id = str(response.get("orderId")) if response.get("orderId") is not None else None
        self.store.update_order(
            order_id,
            status="ACKNOWLEDGED",
            exchange_order_id=exchange_order_id,
            payload=response,
        )
        self._event(order_id, "ORDER_ACKNOWLEDGED", "ACKNOWLEDGED", response=response)
        status = _normal_status(str(response.get("status", "ACKNOWLEDGED")))
        executed_quantity = Decimal(str(response.get("executedQty", "0")))
        self.store.update_order(
            order_id,
            status=status,
            executed_quantity=executed_quantity,
            exchange_order_id=exchange_order_id,
            payload=response,
        )
        if status != "ACKNOWLEDGED":
            self._event(order_id, f"ORDER_{status}", status, response=response)
        self._record_fills(order_id, approved, response)
        return ExecutionResult(
            order_id=order_id,
            intent_id=approved.id,
            status=status,
            client_order_id=approved.client_order_id,
            executed_quantity=executed_quantity,
            exchange_order_id=exchange_order_id,
        )

    @staticmethod
    def _is_uncertain_transport_error(exc: Exception) -> bool:
        return isinstance(
            exc,
            (
                BinanceConnectionError,
                BinanceRateLimitError,
                TimeoutError,
                ConnectionError,
                OSError,
            ),
        )

    def _resolve_uncertain_order(
        self,
        order_id: UUID,
        intent: TradeIntent,
        error: Exception,
    ) -> ExecutionResult:
        """Resolve a possibly accepted mutation without ever resubmitting it."""
        local = self.store.get_order(order_id) or {}
        exchange_order_id = local.get("exchange_order_id")
        lookups: list[dict[str, Any]] = []
        if exchange_order_id is not None:
            lookups.append({"order_id": exchange_order_id})
        lookups.append({"client_order_id": intent.client_order_id})
        response: dict[str, Any] | None = None
        last_error: Exception | None = None
        for lookup in lookups:
            try:
                candidate = self.client.get_order(
                    symbol=intent.symbol,
                    order_id=lookup.get("order_id"),
                    client_order_id=lookup.get("client_order_id"),
                )
                if isinstance(candidate, dict) and candidate:
                    response = candidate
                    break
            except Exception as exc:
                last_error = exc
        if response is None:
            reason = f"order outcome unresolved after transport failure: {error}"
            if last_error is not None:
                reason = f"{reason}; lookup failed: {last_error}"
            self.store.update_order(
                order_id,
                status="UNKNOWN",
                payload={"error": str(error), "resolution_error": reason},
            )
            self._event(order_id, "ORDER_UNKNOWN", "UNKNOWN", error=reason)
            self.store.set_halt(True, reason=reason, source="execution")
            return ExecutionResult(
                order_id=order_id,
                intent_id=intent.id,
                status="UNKNOWN",
                client_order_id=intent.client_order_id,
                reason=reason,
            )

        status = _normal_status(str(response.get("status", "UNKNOWN")))
        if status == "UNKNOWN":
            reason = "order lookup returned UNKNOWN status"
            self.store.update_order(order_id, status="UNKNOWN", payload=response)
            self._event(order_id, "ORDER_UNKNOWN", "UNKNOWN", response=response)
            self.store.set_halt(True, reason=reason, source="execution")
            return ExecutionResult(
                order_id=order_id,
                intent_id=intent.id,
                status="UNKNOWN",
                client_order_id=intent.client_order_id,
                exchange_order_id=(
                    str(response["orderId"])
                    if response.get("orderId") is not None
                    else None
                ),
                reason=reason,
            )
        resolved_exchange_id = (
            str(response["orderId"])
            if response.get("orderId") is not None
            else exchange_order_id
        )
        executed_quantity = Decimal(str(response.get("executedQty", "0")))
        self.store.update_order(
            order_id,
            status=status,
            executed_quantity=executed_quantity,
            exchange_order_id=resolved_exchange_id,
            payload={"uncertain_create_error": str(error), "resolution": response},
        )
        self._event(order_id, "ORDER_RECONCILED_AFTER_UNKNOWN", status, response=response)
        self._record_fills(order_id, intent, response)
        return ExecutionResult(
            order_id=order_id,
            intent_id=intent.id,
            status=status,
            client_order_id=intent.client_order_id,
            executed_quantity=executed_quantity,
            exchange_order_id=resolved_exchange_id,
        )

    def cancel(self, order_id: UUID) -> ExecutionResult:
        local = self.store.get_order(order_id)
        if local is None:
            raise KeyError(f"unknown local order: {order_id}")
        try:
            response = self.client.cancel_order(
                symbol=str(local["symbol"]),
                order_id=local.get("exchange_order_id"),
                client_order_id=(
                    None
                    if local.get("exchange_order_id") is not None
                    else str(local["client_order_id"])
                ),
            )
        except Exception as exc:
            if not self._is_uncertain_transport_error(exc):
                raise
            return self._resolve_uncertain_existing_order(order_id, local, exc)
        status = _normal_status(str(response.get("status", "CANCELED")))
        self.store.update_order(
            order_id,
            status=status,
            exchange_order_id=(
                str(response["orderId"])
                if response.get("orderId") is not None
                else None
            ),
            payload=response,
        )
        self._event(order_id, "ORDER_CANCELLED", status, response=response)
        return ExecutionResult(
            order_id=order_id,
            intent_id=UUID(str(local["intent_id"])),
            status=status,
            client_order_id=str(local["client_order_id"]),
            exchange_order_id=(
                str(response["orderId"])
                if response.get("orderId") is not None
                else local.get("exchange_order_id")
            ),
        )

    def _resolve_uncertain_existing_order(
        self,
        order_id: UUID,
        local: dict[str, Any],
        error: Exception,
    ) -> ExecutionResult:
        exchange_id = local.get("exchange_order_id")
        response: dict[str, Any] | None = None
        last_error: Exception | None = None
        lookups = [
            {"order_id": exchange_id, "client_order_id": None},
            {"order_id": None, "client_order_id": str(local["client_order_id"])},
        ] if exchange_id is not None else [
            {"order_id": None, "client_order_id": str(local["client_order_id"])}
        ]
        for lookup in lookups:
            try:
                candidate = self.client.get_order(
                    symbol=str(local["symbol"]), **lookup
                )
                if isinstance(candidate, dict) and candidate:
                    response = candidate
                    break
            except Exception as exc:
                last_error = exc
        if response is None:
            reason = f"cancel outcome unresolved: {error}"
            if last_error is not None:
                reason = f"{reason}; lookup failed: {last_error}"
            self.store.update_order(
                order_id, status="UNKNOWN", payload={"error": reason}
            )
            self._event(order_id, "CANCEL_UNKNOWN", "UNKNOWN", error=reason)
            self.store.set_halt(True, reason=reason, source="execution")
            return ExecutionResult(
                order_id=order_id,
                intent_id=UUID(str(local["intent_id"])),
                status="UNKNOWN",
                client_order_id=str(local["client_order_id"]),
                exchange_order_id=str(exchange_id) if exchange_id is not None else None,
                reason=reason,
            )
        status = _normal_status(str(response.get("status", "UNKNOWN")))
        if status == "UNKNOWN":
            reason = "cancel lookup returned UNKNOWN status"
            self.store.update_order(order_id, status="UNKNOWN", payload=response)
            self._event(order_id, "CANCEL_UNKNOWN", "UNKNOWN", response=response)
            self.store.set_halt(True, reason=reason, source="execution")
        else:
            self.store.update_order(
                order_id,
                status=status,
                executed_quantity=Decimal(str(response.get("executedQty", "0"))),
                exchange_order_id=(
                    str(response["orderId"])
                    if response.get("orderId") is not None else exchange_id
                ),
                payload=response,
            )
            self._event(order_id, "CANCEL_RECONCILED", status, response=response)
        return ExecutionResult(
            order_id=order_id,
            intent_id=UUID(str(local["intent_id"])),
            status=status,
            client_order_id=str(local["client_order_id"]),
            executed_quantity=Decimal(str(response.get("executedQty", "0"))),
            exchange_order_id=(
                str(response["orderId"])
                if response.get("orderId") is not None else exchange_id
            ),
            reason="cancel outcome reconciled",
        )

    def get_order(self, order_id: UUID) -> ExecutionResult:
        local = self.store.get_order(order_id)
        if local is None:
            raise KeyError(f"unknown local order: {order_id}")
        response = self.client.get_order(
            symbol=str(local["symbol"]),
            order_id=local.get("exchange_order_id"),
            client_order_id=(
                None
                if local.get("exchange_order_id") is not None
                else str(local["client_order_id"])
            ),
        )
        status = _normal_status(str(response.get("status", "UNKNOWN")))
        executed_quantity = Decimal(str(response.get("executedQty", "0")))
        self.store.update_order(
            order_id,
            status=status,
            executed_quantity=executed_quantity,
            exchange_order_id=(
                str(response["orderId"])
                if response.get("orderId") is not None
                else None
            ),
            payload=response,
        )
        self._event(order_id, "ORDER_STATUS_RECONCILED", status, response=response)
        return ExecutionResult(
            order_id=order_id,
            intent_id=UUID(str(local["intent_id"])),
            status=status,
            client_order_id=str(local["client_order_id"]),
            executed_quantity=executed_quantity,
            exchange_order_id=(
                str(response["orderId"])
                if response.get("orderId") is not None
                else local.get("exchange_order_id")
            ),
        )

    def get_open_orders(self) -> list[ExecutionResult]:
        results: list[ExecutionResult] = []
        for local in self.store.list_open_local_orders():
            results.append(self.get_order(UUID(str(local["order_id"]))))
        return results

    def recover(self) -> list[ExecutionResult]:
        return self.get_open_orders()

    def _record_fills(
        self,
        order_id: UUID,
        intent: TradeIntent,
        response: dict[str, Any],
    ) -> None:
        for fill in response.get("fills") or []:
            quantity = Decimal(str(fill.get("qty", "0")))
            price = Decimal(str(fill.get("price", "0")))
            if quantity <= 0 or price <= 0:
                continue
            fee = Decimal(str(fill.get("commission", "0")))
            self.store.record_trade(
                order_id,
                symbol=intent.symbol,
                side=intent.exchange_side(),
                quantity=quantity,
                price=price,
                fee=fee,
                fee_asset=str(fill.get("commissionAsset") or "USDT"),
                position_side=intent.direction,
                payload={"mode": self.config.mode, "source": "order_response"},
            )


def _normal_status(status: str) -> OrderStatus:
    if status in {
        "CREATED",
        "RISK_APPROVED",
        "SUBMITTED",
        "ACKNOWLEDGED",
        "PARTIALLY_FILLED",
        "FILLED",
        "REJECTED",
        "CANCELLED",
        "EXPIRED",
        "FAILED",
        "UNKNOWN",
    }:
        return status  # type: ignore[return-value]
    if status in {"NEW", "PENDING_NEW"}:
        return "ACKNOWLEDGED"
    if status == "PARTIALLY_FILLED":
        return "PARTIALLY_FILLED"
    if status == "FILLED":
        return "FILLED"
    if status == "CANCELED":
        return "CANCELLED"
    if status == "EXPIRED":
        return "EXPIRED"
    if status == "REJECTED":
        return "REJECTED"
    return "UNKNOWN"


def _result_from_local(order: dict[str, Any]) -> ExecutionResult:
    intent_id = order.get("intent_id") or UUID(int=0)
    return ExecutionResult(
        order_id=UUID(str(order.get("order_id") or UUID(int=0))),
        intent_id=UUID(str(intent_id)),
        status=_normal_status(str(order.get("status", "UNKNOWN"))),
        client_order_id=str(order.get("client_order_id", "")),
        executed_quantity=Decimal(str(order.get("executed_quantity", "0"))),
        exchange_order_id=(
            str(order["exchange_order_id"])
            if order.get("exchange_order_id") is not None
            else None
        ),
    )


def executor_from_env(
    *,
    store: TradingStore | None = None,
    config: ExecutionConfig | None = None,
    client_config: ClientConfig | None = None,
) -> Executor:
    config = config or ExecutionConfig(mode=ClientConfig.from_env().mode)
    if config.mode == "paper":
        return PaperExecutor(store=store, config=config)
    return BinanceExecutor(store=store, config=config, client_config=client_config)
