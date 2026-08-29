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

from binance_client import ClientConfig, FuturesPrivateClient
from risk import RiskDecision
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


@dataclass(frozen=True)
class MarketSnapshot:
    """Market inputs used by paper execution."""

    last_price: Decimal
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    bid_price: Decimal | None = None
    ask_price: Decimal | None = None
    available_liquidity: Decimal | None = None
    funding_rate: Decimal | None = None
    funding_timestamp: datetime | None = None


@dataclass(frozen=True)
class ExecutionConfig:
    mode: str = "paper"
    fee_rate: Decimal = Decimal("0.001")
    slippage_bps: Decimal = Decimal("5")
    latency_ms: int = 0
    partial_fill_ratio: Decimal = Decimal("1")
    initial_usdt: Decimal = Decimal("1000")
    order_expiry_sec: int = 0
    maintenance_margin_ratio: Decimal = Decimal("0.5")
    default_leverage: Decimal = Decimal("1")

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
            maintenance_margin_ratio=Decimal(
                os.environ.get("PAPER_MAINTENANCE_MARGIN_RATIO", "0.5")
            ),
            default_leverage=Decimal(os.environ.get("PAPER_DEFAULT_LEVERAGE", "1")),
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
        if not Decimal("0") < self.maintenance_margin_ratio <= Decimal("1"):
            raise ValueError("maintenance_margin_ratio must be in (0, 1]")
        if self.default_leverage <= 0:
            raise ValueError("default_leverage must be positive")


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
            )
            raise ExecutionRejected(risk_decision.reason)
        approved = risk_decision.executable_intent
        if approved is None:
            raise ExecutionRejected("risk decision has no executable intent")
        self.store.record_risk_event(
            intent_id=intent.id,
            decision=risk_decision.decision,
            reason=risk_decision.reason,
        )
        self.store.record_intent(approved, status="RISK_APPROVED")
        return approved

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
        if self.store.is_halted():
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
        expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=self.config.order_expiry_sec)
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
        self.apply_funding(intent.symbol, market)
        self.mark_to_market(intent.symbol, _mark_price(market))
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
        maintenance_margin = initial_margin * self.config.maintenance_margin_ratio
        liquidation_price = self._liquidation_price(direction, entry, leverage)
        remaining = initial_margin + unrealized
        self._persist_position(
            symbol,
            direction=direction,  # type: ignore[arg-type]
            quantity=quantity,
            entry_price=entry,
            mark_price=mark_price,
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
        position = self.store.get_position(symbol)
        if position is None:
            return Decimal("0")
        quantity = _decimal_field(position, "quantity")
        direction = str(position.get("position_side") or "FLAT")
        if quantity <= 0 or direction == "FLAT":
            return Decimal("0")
        payload = position.get("payload") if isinstance(position.get("payload"), dict) else {}
        last = payload.get("funding_timestamp")
        stamp = market.funding_timestamp.isoformat() if market.funding_timestamp is not None else None
        if stamp is not None and last == stamp:
            return Decimal("0")
        mark = _mark_price(market)
        notional = quantity * mark
        payment = notional * market.funding_rate
        signed = payment if direction == "LONG" else -payment
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
            funding_pnl=funding_pnl,
            leverage=_decimal_field(position, "leverage", default="1"),
            payload=extra,
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
            payload={"symbol": symbol, "mark_price": str(mark_price)},
        )
        setter = getattr(self.store, "set_halt", None)
        if setter is not None:
            setter(True, reason="LIQUIDATED", source="paper")
        elif hasattr(self.store, "halted"):
            self.store.halted = True

    def _liquidation_price(self, direction: str, entry: Decimal, leverage: Decimal) -> Decimal:
        buffer = (Decimal("1") / leverage) * (Decimal("1") - self.config.maintenance_margin_ratio)
        if direction == "LONG":
            return max(Decimal("0"), entry * (Decimal("1") - buffer))
        return entry * (Decimal("1") + buffer)

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
        if market.available_liquidity is not None and market.available_liquidity > 0:
            impact = min(Decimal("1"), intent.quantity / market.available_liquidity) * slippage
            price = price * (Decimal("1") + impact if side == "BUY" else Decimal("1") - impact)
        return price

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
        available = (
            min(remaining, market.available_liquidity)
            if market.available_liquidity is not None
            else remaining
        )
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
        realized_delta = self._update_account(intent, fill_quantity, fill_price, fee, slippage)
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
        self.apply_funding(intent.symbol, market)
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
        extra: dict[str, Any] | None = None,
    ) -> None:
        notional = quantity * mark_price
        payload = {
            "mode": "paper",
            "market": "FUTURES",
            "position_side": direction,
            "entry_price": str(entry_price),
            "mark_price": str(mark_price),
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
            maintenance_margin=next_im * self.config.maintenance_margin_ratio,
            liquidation_price=(
                self._liquidation_price(direction, next_entry, leverage)
                if direction != "FLAT"
                else None
            ),
            fee_pnl=fee_pnl,
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

    def submit(
        self,
        intent: TradeIntent,
        risk_decision: RiskDecision,
        *,
        market: MarketSnapshot | None = None,
    ) -> ExecutionResult:
        if self.store.is_halted():
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
            )
        except Exception as exc:
            self.store.update_order(order_id, status="UNKNOWN", payload={"error": str(exc)})
            self._event(order_id, "ORDER_UNKNOWN", "UNKNOWN", error=str(exc))
            raise
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

    def cancel(self, order_id: UUID) -> ExecutionResult:
        local = self.store.get_order(order_id)
        if local is None:
            raise KeyError(f"unknown local order: {order_id}")
        response = self.client.cancel_order(
            symbol=str(local["symbol"]),
            order_id=local.get("exchange_order_id"),
            client_order_id=(
                None
                if local.get("exchange_order_id") is not None
                else str(local["client_order_id"])
            ),
        )
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
