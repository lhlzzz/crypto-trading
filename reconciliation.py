"""Startup and recovery reconciliation for local futures trading state."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import os
from typing import Any, Protocol

from risk import FuturesAccountSnapshot
from trading_store import TradingStore

_TERMINAL_STATES = {"FILLED", "REJECTED", "CANCELLED", "EXPIRED", "FAILED"}


class ReconciliationClient(Protocol):
    def account_snapshot(self) -> FuturesAccountSnapshot: ...
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
        try:
            halted = self.store.is_halted(mode=self.mode)
        except TypeError:
            halted = self.store.is_halted()
        if halted:
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
        for order in self.store.list_open_local_orders(mode=self.mode):
            if order["status"] == "UNKNOWN":
                return self._fail(
                    f"paper order {order['client_order_id']} remains UNKNOWN"
                )
        return ReconciliationResult("SAFE", True)

    def _recover_exchange(self) -> ReconciliationResult:
        assert self.client is not None
        differences: list[str] = []
        snapshot_getter = getattr(self.client, "account_snapshot", None)
        if snapshot_getter is None:
            return self._fail("FuturesAccountSnapshot is unavailable")
        snapshot = snapshot_getter()
        if not isinstance(snapshot, FuturesAccountSnapshot) or not snapshot.fresh:
            return self._fail("FuturesAccountSnapshot is unavailable or stale")
        recovered_balances = self._reconcile_balances(snapshot, differences)
        recovered_positions = self._reconcile_positions(differences)
        recovered_orders = self._reconcile_orders(differences)
        recovered_trades = self._reconcile_user_trades(differences)
        if differences:
            self._record_differences(differences)
            return self._fail("; ".join(differences))
        record_system = getattr(self.store, "record_system_event", None)
        if callable(record_system):
            exchange_positions = []
            getter = getattr(self.client, "get_positions", None)
            if callable(getter):
                for row in getter() or []:
                    if not row.get("symbol"):
                        continue
                    direction, quantity = _exchange_position(row)
                    exchange_positions.append(
                        {
                            "symbol": str(row.get("symbol")),
                            "position_side": direction,
                            "quantity": str(quantity),
                        }
                    )
            exchange_flat = all(
                Decimal(str(item.get("quantity") or 0)) == 0
                or str(item.get("position_side") or "FLAT").upper() == "FLAT"
                for item in exchange_positions
            )
            record_system(
                event_type="RECONCILIATION_OK",
                severity="INFO",
                message="local and exchange state match",
                payload={
                    "mode": self.mode,
                    "ok": True,
                    "open_matches": True,
                    "recovered_orders": recovered_orders,
                    "recovered_balances": recovered_balances,
                    "recovered_positions": recovered_positions,
                    "recovered_trades": recovered_trades,
                    "exchange_positions": exchange_positions,
                    "exchange_open_orders": [
                        {
                            "symbol": str(row.get("symbol") or ""),
                            "exchange_order_id": str(row.get("orderId") or row.get("order_id") or ""),
                            "client_order_id": str(row.get("clientOrderId") or row.get("origClientOrderId") or ""),
                            "status": str(row.get("status") or ""),
                        }
                        for row in (getattr(self.client, "get_open_orders", lambda: [])() or [])
                    ],
                    "exchange_trades": recovered_trades,
                    "exchange_flat": exchange_flat,
                    "local_flat": all(
                        Decimal(str(row.get("quantity") or 0)) == 0
                        or str(row.get("position_side") or "FLAT").upper() == "FLAT"
                        for row in (
                            getattr(self.store, "list_positions", lambda **_: [])(mode=self.mode)
                            if callable(getattr(self.store, "list_positions", None))
                            else []
                        )
                    ),
                },
            )
        return ReconciliationResult(
            "SAFE",
            True,
            recovered_orders=recovered_orders,
            recovered_positions=recovered_positions,
            recovered_balances=recovered_balances,
        )

    def _reconcile_balances(
        self,
        snapshot: FuturesAccountSnapshot,
        differences: list[str],
    ) -> int:
        broker_balances = {}
        broker_balances["USDT"] = {
            "wallet_balance": snapshot.wallet_balance,
            "available_balance": snapshot.available_balance,
            "margin_balance": snapshot.total_margin,
            "unrealized_pnl": snapshot.unrealized_pnl,
        }
        local_balances = {
            str(row["asset"]): row
            for row in self.store.list_balances(mode=self.mode)
            if row.get("mode") == self.mode
        }
        if snapshot.mode not in {"testnet", "live"}:
            differences.append("USDT futures balance is unavailable")
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
                    continue
                for field in ("available_balance", "margin_balance", "unrealized_pnl"):
                    if field not in local and not isinstance(local.get("payload"), dict):
                        continue
                    if field not in local and field not in (local.get("payload") or {}):
                        continue
                    local_value = Decimal(str(local.get(field) or (local.get("payload") or {}).get(field) or "0"))
                    if local_value != broker[field]:
                        differences.append(f"balance mismatch: {asset}:{field}")
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
            try:
                local_rows = lister(mode=self.mode)
            except TypeError:
                local_rows = lister()
        recovered = 0
        local_by_symbol = {str(row["symbol"]): row for row in local_rows}
        for symbol in set(broker) | set(local_by_symbol):
            exchange = broker.get(symbol)
            local = local_by_symbol.get(symbol)
            exchange_dir, exchange_qty = _exchange_position(exchange)
            local_qty = abs(Decimal(str((local or {}).get("quantity") or "0")))
            raw_local_side = (local or {}).get("position_side")
            local_dir = str(raw_local_side or "").strip().upper()
            if local_qty == 0:
                local_dir = "FLAT"
            elif local_dir not in {"LONG", "SHORT"}:
                differences.append(
                    f"invalid local position_side: {symbol}={raw_local_side!r}"
                )
                continue
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
            if local_qty > 0:
                for field, exchange_key, required in (
                    ("leverage", "leverage", True),
                    ("margin_type", "marginType", True),
                    ("liquidation_price", "liquidationPrice", True),
                    ("mark_price", "markPrice", False),
                    ("unrealized_pnl", "unRealizedProfit", False),
                ):
                    exchange_value = exchange.get(exchange_key)
                    local_value = local.get(field)
                    if required:
                        if _missing_position_field(exchange_value):
                            differences.append(
                                f"position mismatch: {symbol}:{field}: exchange missing"
                            )
                            continue
                        if _missing_position_field(local_value):
                            differences.append(
                                f"position mismatch: {symbol}:{field}: local missing"
                            )
                            continue
                    elif _missing_position_field(exchange_value) or _missing_position_field(local_value):
                        if _missing_position_field(exchange_value) != _missing_position_field(local_value):
                            differences.append(
                                f"position mismatch: {symbol}:{field}: unavailable"
                            )
                        continue
                    if field == "margin_type":
                        matches = str(local_value).upper() == str(exchange_value).upper()
                    else:
                        matches = Decimal(str(local_value)) == Decimal(str(exchange_value))
                    if not matches:
                        differences.append(f"position mismatch: {symbol}:{field}")
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
        local_orders = self.store.list_open_local_orders(mode=self.mode)
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

    def _reconciliation_orders(self) -> list[dict[str, Any]]:
        """Load canonical Futures orders for this reconciler mode, including FILLED."""
        for name in ("list_reconciliation_orders", "list_orders"):
            lister = getattr(self.store, name, None)
            if not callable(lister):
                continue
            try:
                rows = lister(mode=self.mode, market="FUTURES")
            except TypeError:
                try:
                    rows = lister(mode=self.mode)
                except TypeError:
                    continue
            return [row for row in list(rows or []) if isinstance(row, dict)]
        lister = getattr(self.store, "list_open_local_orders", None)
        if not callable(lister):
            raise RuntimeError("canonical order loader is unavailable")
        try:
            rows = lister(mode=self.mode)
        except TypeError:
            rows = lister()
        return [row for row in list(rows or []) if isinstance(row, dict)]

    def _trade_reconciliation_symbols(self, local_orders: list[dict[str, Any]]) -> list[str]:
        symbols: set[str] = set()
        for order in local_orders:
            symbol = str(order.get("symbol") or "").strip().upper()
            if symbol:
                symbols.add(symbol)
        lister = getattr(self.store, "list_positions", None)
        if callable(lister):
            try:
                rows = lister(mode=self.mode)
            except TypeError:
                rows = lister()
            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                symbol = str(row.get("symbol") or "").strip().upper()
                if symbol:
                    symbols.add(symbol)
        try:
            from runtime_gate import trading_symbols_for_mode

            symbols.update(str(item).strip().upper() for item in trading_symbols_for_mode(self.mode) if str(item).strip())
        except Exception:
            pass
        return sorted(symbols)

    def _existing_exchange_trade_ids(self) -> set[str]:
        lister = getattr(self.store, "list_trades", None)
        rows: list[Any]
        if callable(lister):
            try:
                rows = list(lister(mode=self.mode) or [])
            except TypeError:
                rows = list(lister() or [])
        else:
            rows = list(getattr(self.store, "trades", None) or [])
        found: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            trade_id = row.get("exchange_trade_id")
            if trade_id is None or str(trade_id).strip() == "":
                continue
            found.add(str(trade_id))
        return found

    def _reconcile_user_trades(self, differences: list[str]) -> int:
        getter = getattr(self.client, "get_user_trades", None)
        recorder = getattr(self.store, "record_trade", None)
        if not callable(getter):
            return 0
        local_orders = self._reconciliation_orders()
        symbols = self._trade_reconciliation_symbols(local_orders)
        if not symbols:
            return 0
        if not callable(recorder):
            differences.append("trade persistence is unavailable")
            return 0
        recovered = 0
        seen: set[str] = set()
        existing = self._existing_exchange_trade_ids()
        by_exchange_id = {
            str(order.get("exchange_order_id")): order
            for order in local_orders
            if order.get("exchange_order_id") is not None
            and str(order.get("exchange_order_id")).strip() != ""
            and str(order.get("mode") or self.mode) == self.mode
        }
        for symbol in symbols:
            try:
                rows = getter(symbol) or []
            except Exception as exc:
                differences.append(f"user trade lookup failed: {symbol}: {exc}")
                continue
            if not rows:
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                exchange_order_id = str(row.get("orderId") or row.get("order_id") or "").strip()
                exchange_trade_id = row.get("id")
                if exchange_trade_id is None or str(exchange_trade_id).strip() == "":
                    differences.append(
                        "unknown exchange trade: missing exchange_trade_id, "
                        f"exchange_order_id={exchange_order_id or '<missing>'}, symbol={symbol}"
                    )
                    continue
                trade_key = str(exchange_trade_id)
                if trade_key in seen:
                    continue
                seen.add(trade_key)
                if not exchange_order_id:
                    differences.append(
                        "unknown exchange trade: "
                        f"exchange_trade_id={trade_key}, exchange_order_id=<missing>, symbol={symbol}"
                    )
                    continue
                local = by_exchange_id.get(exchange_order_id)
                if local is None:
                    differences.append(
                        "unknown exchange trade: "
                        f"exchange_trade_id={trade_key}, exchange_order_id={exchange_order_id}, symbol={symbol}"
                    )
                    continue
                if trade_key in existing:
                    continue
                recorder(
                    local["order_id"],
                    symbol=str(row.get("symbol") or local.get("symbol") or symbol).upper(),
                    side=str(row.get("side") or local.get("side") or ""),
                    quantity=Decimal(str(row.get("qty") or row.get("quantity") or "0")),
                    price=Decimal(str(row.get("price") or "0")),
                    fee=Decimal(str(row.get("commission") or row.get("fee") or "0")),
                    fee_asset=str(row.get("commissionAsset") or row.get("fee_asset") or "USDT"),
                    realized_pnl=Decimal(str(row.get("realizedPnl") or "0")),
                    market="FUTURES",
                    position_side=str(row.get("positionSide") or local.get("position_side") or "").upper() or None,
                    exchange_trade_id=trade_key,
                    mode=self.mode,
                    payload={
                        "source": "binance_rest",
                        "exchange_trade_id": trade_key,
                        "mode": self.mode,
                    },
                )
                existing.add(trade_key)
                recovered += 1
        return recovered

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
        try:
            self.store.set_halt(True, reason=reason, source="reconciliation", mode=self.mode)
        except TypeError:
            self.store.set_halt(True, reason=reason, source="reconciliation")
        return ReconciliationResult("HALT", False, (reason,))


def apply_user_stream_event(store: TradingStore, event: Any) -> None:
    """Apply a private futures event as a local observation, never as final truth."""
    event_id = getattr(event, "event_id", None)
    if event_id:
        seen = getattr(store, "_user_stream_event_ids", None)
        if seen is None:
            seen = set()
            setattr(store, "_user_stream_event_ids", seen)
        if event_id in seen:
            return
        seen.add(event_id)
    event_type = getattr(event, "event_type", None)
    if event_type == "ACCOUNT_UPDATE":
        payload_mode = None
        raw_payload = getattr(event, "raw", None)
        if isinstance(raw_payload, dict):
            payload_mode = raw_payload.get("mode")
        mode = str(payload_mode or os.environ.get("BIAN_MODE", "testnet")).strip().lower()
        for balance in getattr(event, "balance_updates", ()):
            asset = str(balance.get("asset") or "").upper()
            if not asset:
                continue
            getter = getattr(store, "get_balance", None)
            try:
                current = getter(asset, mode=mode) if callable(getter) else None
            except TypeError:
                current = getter(asset) if callable(getter) else None
            current = current or {}
            payload = dict(current.get("payload") or {})
            payload.update({
                "source": getattr(event, "source", "USER_STREAM"),
                "completeness": getattr(event, "completeness", "PARTIAL"),
                "balance_change": balance.get("balance_change", "0"),
            })
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
            getter = getattr(store, "get_position", None)
            current = getter(symbol, mode=mode) if callable(getter) else {}
            current = current or {}
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
                mode=mode,
                payload={
                    **(current.get("payload") or {}),
                    "source": getattr(event, "source", "USER_STREAM"),
                    "completeness": getattr(event, "completeness", "PARTIAL"),
                },
            )
        record_system = getattr(store, "record_system_event", None)
        if callable(record_system):
            symbol = next(
                (
                    str(position.get("symbol") or "")
                    for position in getattr(event, "position_updates", ())
                    if position.get("symbol")
                ),
                "USDT",
            )
            record_system(
                event_type="USER_STREAM_ACCOUNT_UPDATE",
                severity="INFO",
                message="user stream ACCOUNT_UPDATE observed",
                payload={
                    "event_id": event_id,
                    "event_type": event_type,
                    "event_time": getattr(event, "event_time_ms", None),
                    "execution_type": getattr(event, "execution_type", None) or "ACCOUNT_UPDATE",
                    "symbol": symbol,
                    "exchange_order_id": getattr(event, "exchange_order_id", None),
                    "position_updates": [
                        {
                            "symbol": str(position.get("symbol") or ""),
                            "position_side": position.get("position_side"),
                            "quantity": str(position.get("quantity") or "0"),
                        }
                        for position in getattr(event, "position_updates", ())
                        if position.get("symbol")
                    ],
                },
            )
        return
    if event_type in {"malformed", "unknown"}:
        return
    client_order_id = getattr(event, "client_order_id", None)
    if not client_order_id:
        return
    order_mode = str(os.environ.get("BIAN_MODE", "")).strip().lower() or None
    getter = getattr(store, "get_order_by_client_order_id", None)
    local = None
    if callable(getter):
        try:
            local = getter(str(client_order_id), mode=order_mode) if order_mode else getter(str(client_order_id))
        except TypeError:
            local = getter(str(client_order_id))
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
    position_side = str(getattr(event, "position_side", "") or "").upper()
    if position_side == "BOTH":
        position_side = str(local.get("position_side") or "").upper()
    store.update_order(
        local["order_id"],
        status=status,
        executed_quantity=Decimal(str(getattr(event, "executed_quantity", "0"))),
        exchange_order_id=getattr(event, "exchange_order_id", None),
        payload={
            **(getattr(event, "raw", {}) or {}),
            "client_order_id": getattr(event, "client_order_id", None),
            "exchange_order_id": getattr(event, "exchange_order_id", None),
            "status": status,
            "execution_type": getattr(event, "execution_type", None),
            "executed_quantity": str(getattr(event, "executed_quantity", "0")),
            "last_quantity": str(getattr(event, "last_quantity", "0")),
            "last_price": (
                str(getattr(event, "last_price"))
                if getattr(event, "last_price", None) is not None else None
            ),
            "fee": str(getattr(event, "fee", "0")),
            "fee_asset": getattr(event, "fee_asset", None),
            "realized_pnl": (
                str(getattr(event, "realized_pnl"))
                if getattr(event, "realized_pnl", None) is not None else None
            ),
            "position_side": position_side or None,
            "reduce_only": getattr(event, "reduce_only", None),
        },
    )
    store.append_order_event(
        local["order_id"],
        event_type="USER_STREAM_ORDER_UPDATE",
        status=status,
        event_id=event_id,
        payload=getattr(event, "raw", {}),
    )
    if (
        getattr(event, "execution_type", None) == "TRADE"
        and Decimal(str(getattr(event, "last_quantity", "0"))) > 0
        and getattr(store, "record_trade", None) is not None
    ):
        last_price = getattr(event, "last_price", None)
        if last_price is not None and Decimal(str(last_price)) > 0:
            store.record_trade(
                local["order_id"],
                symbol=str(local.get("symbol") or getattr(event, "symbol", "")).upper(),
                side=str(local.get("side") or ""),
                quantity=Decimal(str(getattr(event, "last_quantity", "0"))),
                price=Decimal(str(last_price)),
                fee=Decimal(str(getattr(event, "fee", "0"))),
                fee_asset=str(getattr(event, "fee_asset", None) or "USDT"),
                realized_pnl=Decimal(str(getattr(event, "realized_pnl", "0") or "0")),
                market="FUTURES",
                position_side=position_side or None,
                source_event_id=event_id,
                exchange_trade_id=getattr(event, "trade_id", None),
                mode=str(local.get("mode") or order_mode or os.environ.get("BIAN_MODE", "testnet")),
                payload={
                    "source": "user_stream",
                    "event_id": event_id,
                    "exchange_trade_id": getattr(event, "trade_id", None),
                    "execution_type": getattr(event, "execution_type", None),
                    "reduce_only": getattr(event, "reduce_only", None),
                },
            )


def _missing_position_field(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    return text == "" or text.upper() in {"NONE", "NULL", "UNKNOWN"}


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
