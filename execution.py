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


@dataclass(frozen=True)
class MarketSnapshot:
    """Market inputs used by paper execution."""

    last_price: Decimal
    bid_price: Decimal | None = None
    ask_price: Decimal | None = None
    available_liquidity: Decimal | None = None


@dataclass(frozen=True)
class ExecutionConfig:
    mode: str = "paper"
    fee_rate: Decimal = Decimal("0.001")
    slippage_bps: Decimal = Decimal("5")
    latency_ms: int = 0
    partial_fill_ratio: Decimal = Decimal("1")
    initial_usdt: Decimal = Decimal("1000")
    order_expiry_sec: int = 0

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


class PaperExecutor(_BaseExecutor):
    """Deterministic paper broker with an order lifecycle and accounting."""

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
        if self.store.is_halted():
            raise ExecutionRejected("trading is halted")
        approved = self._approve(intent, risk_decision)
        requested_quantity = self._requested_quantity(approved, market.last_price)
        if approved.quote_quantity is not None:
            approved = approved.model_copy(
                update={
                    "quantity": requested_quantity,
                    "quote_quantity": None,
                }
            )
            self.store.record_intent(approved, status="RISK_APPROVED")
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
        expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=self.config.order_expiry_sec)
            if approved.order_type == "LIMIT" and self.config.order_expiry_sec > 0
            else None
        )
        quote_balance = self.store.get_balance("USDT")
        if approved.side == "BUY" and (
            quote_balance is None
            or quote_balance["free"] < estimated_notional
        ):
            self.store.record_system_event(
                event_type="ORDER_REJECTED",
                severity="WARNING",
                message="paper quote balance is insufficient",
                payload={
                    "intent_id": str(approved.id),
                    "required": str(estimated_notional),
                    "available": str(quote_balance["free"] if quote_balance else 0),
                },
            )
            raise ExecutionRejected("paper quote balance is insufficient")
        if approved.side == "SELL":
            base_asset = approved.symbol.removesuffix("USDT")
            base_balance = self.store.get_balance(base_asset)
            if base_balance is None or base_balance["free"] < requested_quantity:
                self.store.record_system_event(
                    event_type="ORDER_REJECTED",
                    severity="WARNING",
                    message="paper base balance is insufficient",
                    payload={
                        "intent_id": str(approved.id),
                        "required": str(requested_quantity),
                        "available": str(base_balance["free"] if base_balance else 0),
                    },
                )
                raise ExecutionRejected("paper base balance is insufficient")
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
        requested_quantity = self._requested_quantity(intent, market.last_price)
        return self._apply_fill(
            order_id,
            intent,
            market,
            requested_quantity=requested_quantity,
            current_executed=Decimal(str(local.get("executed_quantity", "0"))),
        )

    def mark_to_market(self, symbol: str, market_price: Decimal) -> None:
        if market_price <= 0:
            raise ValueError("mark price must be positive")
        position = self.store.get_position(symbol)
        if position is None:
            return
        quantity = Decimal(str(position["quantity"]))
        average_price = Decimal(str(position["average_price"]))
        unrealized = (
            (market_price - average_price) * quantity if quantity > 0 else Decimal("0")
        )
        self.store.upsert_position(
            symbol,
            quantity=quantity,
            average_price=average_price,
            realized_pnl=Decimal(str(position["realized_pnl"])),
            unrealized_pnl=unrealized,
            payload={"mode": "paper", "mark_price": str(market_price)},
        )

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
        reference = market.ask_price or market.last_price if intent.side == "BUY" else market.bid_price or market.last_price
        return intent.price >= reference if intent.side == "BUY" else intent.price <= reference

    def _fill_price(self, intent: TradeIntent, market: MarketSnapshot) -> Decimal:
        if intent.order_type == "LIMIT" and intent.price is not None:
            return intent.price
        if intent.side == "BUY":
            base = market.ask_price or market.last_price
        else:
            base = market.bid_price or market.last_price
        slippage = self.config.slippage_bps / Decimal("10000")
        return base * (Decimal("1") + slippage if intent.side == "BUY" else Decimal("1") - slippage)

    def _requested_quantity(
        self,
        intent: TradeIntent,
        market_price: Decimal | None = None,
    ) -> Decimal:
        if intent.quantity is not None:
            return intent.quantity
        if intent.quote_quantity is not None and market_price is not None:
            return intent.quote_quantity / market_price
        return Decimal("0")

    def _fill_quantity(self, intent: TradeIntent, market: MarketSnapshot) -> Decimal:
        requested = (
            intent.quantity
            if intent.quantity is not None
            else intent.quote_quantity / market.last_price
            if intent.quote_quantity is not None
            else Decimal("0")
        )
        if market.available_liquidity is not None:
            requested = min(requested, market.available_liquidity)
        return requested * self.config.partial_fill_ratio

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
        status: OrderStatus = (
            "FILLED" if cumulative >= requested_quantity else "PARTIALLY_FILLED"
        )
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
        realized_delta = self._update_account(intent, fill_quantity, fill_price, fee)
        trade_id = self.store.record_trade(
            order_id,
            symbol=intent.symbol,
            side=intent.side,
            quantity=fill_quantity,
            price=fill_price,
            fee=fee,
            fee_asset="USDT",
            realized_pnl=realized_delta,
            payload={"mode": "paper"},
        )
        self.mark_to_market(intent.symbol, market.last_price)
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
        quantity = order.get("quantity")
        quote_quantity = order.get("quote_quantity")
        return TradeIntent(
            id=UUID(str(order["intent_id"])),
            symbol=str(order["symbol"]),
            side=str(order["side"]),  # type: ignore[arg-type]
            order_type=str(order["order_type"]),  # type: ignore[arg-type]
            quantity=Decimal(str(quantity)) if quantity is not None else None,
            quote_quantity=(
                Decimal(str(quote_quantity)) if quote_quantity is not None else None
            ),
            price=Decimal(str(order["price"])) if order.get("price") is not None else None,
            confidence=Decimal("0"),
            reason="recovered paper order",
            strategy_version="recovered",
            client_order_id=str(order["client_order_id"]),
        )

    def _ensure_initial_balance(self) -> None:
        if self.store.get_balance("USDT") is None:
            self.store.upsert_balance(
                "USDT",
                free=self.config.initial_usdt,
                mode="paper",
                payload={"initial": True},
            )

    def _update_account(
        self,
        intent: TradeIntent,
        quantity: Decimal,
        price: Decimal,
        fee: Decimal,
    ) -> Decimal:
        quote_asset = "USDT"
        base_asset = intent.symbol.removesuffix(quote_asset)
        quote_balance = self.store.get_balance(quote_asset)
        current_quote = quote_balance["free"] if quote_balance else Decimal("0")
        position = self.store.get_position(intent.symbol)
        current_quantity = position["quantity"] if position else Decimal("0")
        current_average = position["average_price"] if position else Decimal("0")
        current_realized = position["realized_pnl"] if position else Decimal("0")
        gross = quantity * price
        if intent.side == "BUY":
            signed_quantity = current_quantity + quantity
            average_price = (
                (current_quantity * current_average + gross) / signed_quantity
                if signed_quantity > 0
                else price
            )
            realized_pnl = current_realized - fee
            realized_delta = -fee
            next_quote = current_quote - gross - fee
        else:
            signed_quantity = current_quantity - quantity
            realized_delta = (price - current_average) * quantity - fee
            realized_pnl = current_realized + realized_delta
            average_price = current_average if signed_quantity > 0 else Decimal("0")
            next_quote = current_quote + gross - fee
        self.store.upsert_balance(
            quote_asset,
            free=next_quote,
            mode="paper",
            payload={"updated_by": "paper_execution"},
        )
        base_balance = self.store.get_balance(base_asset)
        current_base = base_balance["free"] if base_balance else Decimal("0")
        self.store.upsert_balance(
            base_asset,
            free=current_base + quantity if intent.side == "BUY" else current_base - quantity,
            mode="paper",
            payload={"updated_by": "paper_execution"},
        )
        self.store.upsert_position(
            intent.symbol,
            quantity=signed_quantity,
            average_price=average_price,
            realized_pnl=realized_pnl,
            unrealized_pnl=Decimal("0"),
            payload={"mode": "paper"},
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
        if approved.quantity is None:
            raise ExecutionRejected("Futures orders require quantity")
        order_id = self._create_order(approved, status="CREATED")
        self._event(order_id, "ORDER_CREATED", "CREATED")
        self.store.update_order(order_id, status="RISK_APPROVED")
        self._event(order_id, "ORDER_RISK_APPROVED", "RISK_APPROVED")
        try:
            response = self.client.create_order(
                symbol=approved.symbol,
                side=approved.side,
                order_type=approved.order_type,
                quantity=approved.quantity,
                price=approved.price,
                client_order_id=approved.client_order_id,
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
                side=intent.side,
                quantity=quantity,
                price=price,
                fee=fee,
                fee_asset=str(fill.get("commissionAsset") or "USDT"),
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
