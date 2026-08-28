"""Startup and recovery reconciliation for local trading state."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import os
from typing import Any, Protocol

from trading_store import TradingStore


class ReconciliationClient(Protocol):
    def get_account(self) -> dict[str, Any]: ...
    def get_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]: ...
    def get_order(
        self,
        symbol: str,
        order_id: int | str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class ReconciliationResult:
    status: str
    safe_to_trade: bool
    differences: tuple[str, ...] = ()
    recovered_orders: int = 0
    recovered_balances: int = 0


_TERMINAL_STATES = {"FILLED", "REJECTED", "CANCELLED", "EXPIRED", "FAILED"}


class Reconciler:
    """Compare broker state with local state and fail closed on uncertainty."""

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
        account = self.client.get_account()
        broker_balances = {
            str(row.get("asset")): {
                "free": Decimal(str(row.get("free", "0"))),
                "locked": Decimal(str(row.get("locked", "0"))),
            }
            for row in account.get("balances", [])
            if row.get("asset")
        }
        local_balances = {
            str(row["asset"]): {
                "free": Decimal(str(row.get("free", "0"))),
                "locked": Decimal(str(row.get("locked", "0"))),
            }
            for row in self.store.list_balances()
            if row.get("mode") == self.mode
        }
        differences: list[str] = []
        # An empty local balance set is a first-run bootstrap, not a broker
        # mismatch. Once local state exists, every balance is compared.
        if local_balances:
            for asset in set(broker_balances) | set(local_balances):
                if broker_balances.get(asset, _empty_balance()) != local_balances.get(
                    asset, _empty_balance()
                ):
                    differences.append(f"balance mismatch: {asset}")

        recovered_balances = 0
        upsert = getattr(self.store, "upsert_balance", None)
        if upsert is not None:
            for asset, balance in broker_balances.items():
                upsert(
                    asset,
                    free=balance["free"],
                    locked=balance["locked"],
                    mode=self.mode,
                    payload={"source": "binance_rest", "reconciled": True},
                )
                recovered_balances += 1

        broker_orders = self.client.get_open_orders()
        broker_by_client_id = {
            str(row.get("clientOrderId")): row
            for row in broker_orders
            if row.get("clientOrderId")
        }
        local_orders = self.store.list_open_local_orders()
        local_client_ids = {str(row["client_order_id"]) for row in local_orders}
        recovered_orders = 0

        for order in local_orders:
            local_id = str(order["client_order_id"])
            exchange_order = broker_by_client_id.get(local_id)
            if exchange_order is None:
                # A missing open order may already be FILLED/CANCELED. Query
                # by clientOrderId; never submit a replacement here.
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
        if differences:
            self._record_differences(differences)
            return self._fail("; ".join(differences))
        return ReconciliationResult(
            "SAFE",
            True,
            recovered_orders=recovered_orders,
            recovered_balances=recovered_balances,
        )

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
    """Apply a private event as a local observation, never as final truth."""

    client_order_id = getattr(event, "client_order_id", None)
    if not client_order_id:
        if getattr(event, "event_type", None) == "outboundAccountPosition":
            for balance in getattr(event, "balances", ()):
                store.upsert_balance(
                    balance["asset"],
                    free=Decimal(str(balance["free"])),
                    locked=Decimal(str(balance["locked"])),
                    mode=os.environ.get("BIAN_MODE", "testnet"),
                    payload={"source": "user_stream"},
                )
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


def _empty_balance() -> dict[str, Decimal]:
    return {"free": Decimal("0"), "locked": Decimal("0")}


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
