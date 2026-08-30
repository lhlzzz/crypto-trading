"""Startup and recovery reconciliation for local futures trading state."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import os
from typing import Any, Protocol

from trading_store import TradingStore

_TERMINAL_STATES = {"FILLED", "REJECTED", "CANCELLED", "EXPIRED", "FAILED"}


class ReconciliationClient(Protocol):
    def get_account(self) -> dict[str, Any]: ...
    def get_positions(self, symbol: str | None = None) -> list[dict[str, Any]]: ...
    def get_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]: ...
    def get_order(
        self,
        symbol: str,
        order_id: int | str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]: ...
    def get_user_trades(self, symbol: str, limit: int | None = None) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class ReconciliationResult:
    status: str
    safe_to_trade: bool
    differences: tuple[str, ...] = ()
    recovered_orders: int = 0
    recovered_balances: int = 0
    recovered_positions: int = 0


class Reconciler:
    """Compare Binance futures truth with local state and fail closed."""

    def __init__(
        self,
        store: TradingStore,
        *,
        client: ReconciliationClient | None = None,
        mode: str = "paper",
    ) -> None:
        self.store = store
        self.client = client
        self.mode = mode

    def recover(self) -> ReconciliationResult:
        if self.store.is_halted():
            return ReconciliationResult("HALTED", False, ("trading is halted",))
        try:
            if self.mode == "paper":
                return self._recover_paper()
            if self.client is None:
                return self._fail("private client is required")
            return self._recover_exchange()
        except Exception as exc:
            return self._fail(f"reconciliation failed: {exc}")

    def _recover_paper(self) -> ReconciliationResult:
        for order in self.store.list_open_local_orders():
            if order["status"] == "UNKNOWN":
                return self._fail(
                    f"paper order {order['client_order_id']} remains UNKNOWN"
                )
        return ReconciliationResult("SAFE", True)

    def _recover_exchange(self) -> ReconciliationResult:
        assert self.client is not None
        differences: list[str] = []
        account = self.client.get_account()
        recovered_balances = self._reconcile_balances(account, differences)
        recovered_positions = self._reconcile_positions(differences)
        recovered_orders = self._reconcile_orders(differences)
        if differences:
            self._record_differences(differences)
            return self._fail("; ".join(differences))
        return ReconciliationResult(
            "SAFE",
            True,
            recovered_orders=recovered_orders,
            recovered_balances=recovered_balances,
            recovered_positions=recovered_positions,
        )

    def _reconcile_balances(self, account: dict[str, Any], differences: list[str]) -> int:
        broker_balances = {}
        for row in account.get("assets") or account.get("balances") or []:
            asset = str(row.get("asset") or "")
            if not asset:
                continue
            broker_balances[asset] = {
                "wallet_balance": Decimal(str(row.get("walletBalance") or row.get("free") or "0")),
                "available_balance": Decimal(str(row.get("availableBalance") or row.get("free") or "0")),
                "margin_balance": Decimal(str(row.get("marginBalance") or row.get("walletBalance") or "0")),
                "unrealized_pnl": Decimal(str(row.get("unrealizedProfit") or "0")),
            }
        local_balances = {
            str(row["asset"]): row
            for row in self.store.list_balances()
            if row.get("mode") == self.mode
        }
        if local_balances:
            for asset in set(broker_balances) | set(local_balances):
                local = local_balances.get(asset, {})
                broker = broker_balances.get(asset)
                if broker is None:
                    differences.append(f"balance mismatch: {asset}")
                    continue
                local_wallet = Decimal(str(local.get("wallet_balance") or local.get("free") or "0"))
                if local_wallet != broker["wallet_balance"]:
                    differences.append(f"balance mismatch: {asset}")
        recovered = 0
        upsert = getattr(self.store, "upsert_balance", None)
        if upsert is not None:
            for asset, balance in broker_balances.items():
                upsert(
                    asset,
                    free=balance["available_balance"],
                    locked=balance["wallet_balance"] - balance["available_balance"],
                    wallet_balance=balance["wallet_balance"],
                    available_balance=balance["available_balance"],
                    margin_balance=balance["margin_balance"],
                    unrealized_pnl=balance["unrealized_pnl"],
                    mode=self.mode,
                    payload={"source": "binance_rest", "reconciled": True},
                )
                recovered += 1
        return recovered

    def _reconcile_positions(self, differences: list[str]) -> int:
        assert self.client is not None
        getter = getattr(self.client, "get_positions", None)
        broker_rows = getter() if getter is not None else []
        broker = {
            str(row.get("symbol")): row
            for row in broker_rows
            if row.get("symbol")
        }
        local_rows = []
        lister = getattr(self.store, "list_positions", None)
        if lister is not None:
            local_rows = lister()
        recovered = 0
        local_by_symbol = {str(row["symbol"]): row for row in local_rows}
        for symbol in set(broker) | set(local_by_symbol):
            exchange = broker.get(symbol)
            local = local_by_symbol.get(symbol)
            exchange_dir, exchange_qty = _exchange_position(exchange)
            local_qty = abs(Decimal(str((local or {}).get("quantity") or "0")))
            local_dir = str((local or {}).get("position_side") or "FLAT")
            if local_qty == 0:
                local_dir = "FLAT"
            elif local_dir not in {"LONG", "SHORT"}:
                local_dir = "LONG"
            if exchange is None and local_qty == 0:
                continue
            if local is None and exchange_qty == 0:
                continue
            if exchange is None or local is None:
                differences.append(f"position mismatch: {symbol}")
                continue
            if local_dir != exchange_dir or local_qty != exchange_qty:
                differences.append(f"position mismatch: {symbol}")
                continue
            entry = Decimal(str(exchange.get("entryPrice") or "0"))
            local_entry = Decimal(str(local.get("entry_price") or local.get("average_price") or "0"))
            if entry != local_entry:
                differences.append(f"position mismatch: {symbol}")
                continue
            recovered += 1
        return recovered

    def _reconcile_orders(self, differences: list[str]) -> int:
        assert self.client is not None
        broker_orders = self.client.get_open_orders()
        broker_by_client_id = {
            str(row.get("clientOrderId") or row.get("origClientOrderId")): row
            for row in broker_orders
            if row.get("clientOrderId") or row.get("origClientOrderId")
        }
        local_orders = self.store.list_open_local_orders()
        local_client_ids = {str(row["client_order_id"]) for row in local_orders}
        recovered_orders = 0
        for order in local_orders:
            local_id = str(order["client_order_id"])
            exchange_order = broker_by_client_id.get(local_id)
            if exchange_order is None:
                try:
                    exchange_order = self.client.get_order(
                        str(order["symbol"]), client_order_id=local_id
                    )
                except Exception as exc:
                    differences.append(f"order lookup failed: {local_id}: {exc}")
                    continue
            status = _normal_status(str(exchange_order.get("status", "UNKNOWN")))
            if status == "UNKNOWN":
                differences.append(f"order status is UNKNOWN: {local_id}")
                continue
            self._restore_order(order, exchange_order, status)
            recovered_orders += 1
        unexpected = sorted(set(broker_by_client_id) - local_client_ids)
        differences.extend(
            f"exchange order has no local intent: {value}" for value in unexpected
        )
        return recovered_orders

    def _restore_order(
        self,
        local_order: dict[str, Any],
        exchange_order: dict[str, Any],
        status: str,
    ) -> None:
        exchange_id = exchange_order.get("orderId")
        executed = exchange_order.get("executedQty")
        update = getattr(self.store, "update_order", None)
        if update is not None:
            update(
                local_order["order_id"],
                status=status,
                executed_quantity=(
                    Decimal(str(executed)) if executed is not None else None
                ),
                exchange_order_id=(
                    str(exchange_id) if exchange_id is not None else None
                ),
                payload=exchange_order,
            )
        append = getattr(self.store, "append_order_event", None)
        if append is not None:
            append(
                local_order["order_id"],
                event_type="ORDER_RECONCILED",
                status=status,
                payload={"source": "binance_rest", "response": exchange_order},
            )

    def _record_differences(self, differences: list[str]) -> None:
        message = "; ".join(differences)
        record_system = getattr(self.store, "record_system_event", None)
        if record_system is not None:
            record_system(
                event_type="RECONCILIATION_MISMATCH",
                severity="CRITICAL",
                message=message,
                payload={"mode": self.mode, "differences": differences},
            )
        record_risk = getattr(self.store, "record_risk_event", None)
        if record_risk is not None:
            record_risk(
                intent_id=None,
                decision="HALT",
                reason="reconciliation mismatch",
                payload={"mode": self.mode, "differences": differences},
            )

    def _fail(self, reason: str) -> ReconciliationResult:
        self.store.set_halt(True, reason=reason, source="reconciliation")
        return ReconciliationResult("HALT", False, (reason,))


def apply_user_stream_event(store: TradingStore, event: Any) -> None:
    """Apply a private futures event as a local observation, never as final truth."""
    event_type = getattr(event, "event_type", None)
    if event_type == "ACCOUNT_UPDATE":
        mode = os.environ.get("BIAN_MODE", "testnet")
        for balance in getattr(event, "balance_updates", ()):
            asset = str(balance.get("asset") or "").upper()
            if not asset:
                continue
            current = getattr(store, "get_balance", lambda _asset: None)(asset) or {}
            payload = dict(current.get("payload") or {})
            payload.update({"source": "user_stream", "balance_change": balance.get("balance_change", "0")})
            wallet = Decimal(str(balance.get("wallet_balance") or current.get("wallet_balance") or "0"))
            available = Decimal(str(balance.get("available_balance") or current.get("available_balance") or wallet))
            store.upsert_balance(
                asset,
                free=available,
                locked=max(Decimal("0"), wallet - available),
                wallet_balance=wallet,
                available_balance=available,
                margin_balance=Decimal(str(current.get("margin_balance") or wallet)),
                used_margin=Decimal(str(current.get("used_margin") or max(Decimal("0"), wallet - available))),
                unrealized_pnl=Decimal(str(current.get("unrealized_pnl") or "0")),
                mode=mode,
                payload=payload,
            )
        for position in getattr(event, "position_updates", ()):
            symbol = str(position.get("symbol") or "").upper()
            if not symbol:
                continue
            amount = Decimal(str(position.get("quantity") or "0"))
            current = getattr(store, "get_position", lambda _symbol: None)(symbol) or {}
            direction, quantity = _exchange_position({
                "positionAmt": str(amount),
                "positionSide": position.get("position_side"),
            })
            entry = Decimal(str(position.get("entry_price") or current.get("entry_price") or "0"))
            mark = current.get("mark_price")
            index = current.get("index_price")
            store.upsert_position(
                symbol,
                quantity=quantity,
                average_price=entry,
                realized_pnl=Decimal(str(position.get("realized_pnl") or current.get("realized_pnl") or "0")),
                unrealized_pnl=Decimal(str(position.get("unrealized_pnl") or current.get("unrealized_pnl") or "0")),
                market="FUTURES",
                position_side=direction,
                entry_price=entry,
                mark_price=Decimal(str(mark)) if mark is not None else None,
                index_price=Decimal(str(index)) if index is not None else None,
                notional=(quantity * Decimal(str(mark))) if mark is not None else current.get("notional"),
                leverage=current.get("leverage"),
                margin_type=str(position.get("margin_type") or current.get("margin_type") or "ISOLATED").upper(),
                initial_margin=current.get("initial_margin"),
                maintenance_margin=current.get("maintenance_margin"),
                liquidation_price=current.get("liquidation_price"),
                funding_pnl=Decimal(str(current.get("funding_pnl") or "0")),
                payload={**(current.get("payload") or {}), "source": "user_stream"},
            )
        return
    if event_type in {"malformed", "unknown"}:
        return
    client_order_id = getattr(event, "client_order_id", None)
    if not client_order_id:
        return
    local = store.get_order_by_client_order_id(str(client_order_id))
    if local is None:
        store.set_halt(
            True,
            reason=f"user stream order has no local record: {client_order_id}",
            source="user_stream",
        )
        return
    status = _normal_status(str(getattr(event, "order_status", "UNKNOWN")))
    if status == "UNKNOWN":
        store.set_halt(
            True,
            reason=f"user stream order status unknown: {client_order_id}",
            source="user_stream",
        )
        return
    store.update_order(
        local["order_id"],
        status=status,
        executed_quantity=Decimal(str(getattr(event, "executed_quantity", "0"))),
        exchange_order_id=getattr(event, "exchange_order_id", None),
        payload=getattr(event, "raw", {}),
    )
    store.append_order_event(
        local["order_id"],
        event_type="USER_STREAM_ORDER_UPDATE",
        status=status,
        payload=getattr(event, "raw", {}),
    )


def _exchange_position(row: dict[str, Any] | None) -> tuple[str, Decimal]:
    """Map exchange positionAmt/positionSide onto abs quantity + direction."""
    if not row:
        return "FLAT", Decimal("0")
    amount = Decimal(str(row.get("positionAmt") or "0"))
    side = str(row.get("positionSide") or "").upper()
    if side in {"LONG", "SHORT"}:
        return side, abs(amount)
    if amount > 0:
        return "LONG", amount
    if amount < 0:
        return "SHORT", abs(amount)
    return "FLAT", Decimal("0")


def _normal_status(status: str) -> str:
    return {
        "NEW": "ACKNOWLEDGED",
        "PENDING_NEW": "ACKNOWLEDGED",
        "PARTIALLY_FILLED": "PARTIALLY_FILLED",
        "FILLED": "FILLED",
        "CANCELED": "CANCELLED",
        "CANCELLED": "CANCELLED",
        "EXPIRED": "EXPIRED",
        "REJECTED": "REJECTED",
    }.get(status, status if status in _TERMINAL_STATES else "UNKNOWN")
