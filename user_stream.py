"""Private Binance USD-M Futures User Data Stream."""
from __future__ import annotations

import asyncio
import inspect
import json
import os
import random
from collections import OrderedDict
from dataclasses import dataclass, field
import hashlib
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable

from binance_client import BinanceAuthError, ClientConfig, FuturesPrivateClient

ReconcileCallback = Callable[[], Awaitable[None] | None]
EventCallback = Callable[["UserStreamEvent"], Awaitable[None] | None]
HaltCallback = Callable[[str], Awaitable[None] | None]

FUTURES_LIVE_WS = "wss://fstream.binance.com"
FUTURES_TESTNET_WS = "wss://stream.binancefuture.com"


@dataclass(frozen=True)
class UserStreamEvent:
    """Futures-native event contract consumed by execution/reconciliation."""

    event_type: str
    event_id: str | None = None
    event_time_ms: int | None = None
    symbol: str | None = None
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    order_status: str | None = None
    execution_type: str | None = None
    trade_id: str | None = None
    executed_quantity: Decimal = Decimal("0")
    last_quantity: Decimal = Decimal("0")
    last_price: Decimal | None = None
    fee: Decimal = Decimal("0")
    fee_asset: str | None = None
    position_side: str | None = None
    realized_pnl: Decimal | None = None
    realized_pnl_asset: str | None = None
    reduce_only: bool | None = None
    balance_updates: tuple[dict[str, str], ...] = ()
    position_updates: tuple[dict[str, str], ...] = ()
    completeness: str = "FULL"
    source: str = "USER_STREAM"
    raw: dict[str, Any] = field(default_factory=dict)


def _decimal(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value if value is not None else default))
    except Exception:
        return Decimal(default)


def _payload(event: Any) -> dict[str, Any]:
    if isinstance(event, dict):
        return dict(event)
    if isinstance(event, (bytes, str)):
        try:
            parsed = json.loads(event)
        except Exception:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    actual = getattr(event, "actual_instance", None)
    if actual is not None:
        event = actual
    if hasattr(event, "model_dump"):
        return event.model_dump(mode="json", by_alias=True, exclude_none=True)
    if hasattr(event, "to_dict"):
        value = event.to_dict()
        return dict(value) if isinstance(value, dict) else {}
    return {}


def _canonical_event_id(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def normalize_user_event(event: Any) -> UserStreamEvent:
    """Normalize Binance USD-M user-data JSON into one event contract."""
    raw = _payload(event)
    if not raw:
        return UserStreamEvent(event_type="malformed", raw={"raw": str(event)})
    event_type = str(raw.get("e") or raw.get("eventType") or "unknown")
    if event_type == "listenKeyExpired":
        return UserStreamEvent(
            event_type=event_type,
            event_id=f"listenKeyExpired:{_int_or_none(raw.get('E'))}",
            event_time_ms=_int_or_none(raw.get("E")),
            raw=raw,
        )
    if event_type == "ORDER_TRADE_UPDATE":
        order = raw.get("o") if isinstance(raw.get("o"), dict) else {}
        realized = order.get("rp")
        trade_id = order.get("t")
        try:
            has_trade_id = trade_id is not None and int(trade_id) >= 0
        except (TypeError, ValueError):
            has_trade_id = False
        identity = {
            "event_type": event_type,
            "symbol": _upper_or_none(order.get("s")) or "",
            "exchange_order_id": str(order.get("i") or ""),
            "trade_id": str(trade_id) if has_trade_id else str(order.get("t") or ""),
            "execution_type": str(order.get("x") or ""),
            "event_time": str(raw.get("E") or ""),
        }
        event_id = _canonical_event_id(identity)
        return UserStreamEvent(
            event_type=event_type,
            event_id=event_id,
            event_time_ms=_int_or_none(raw.get("E")),
            symbol=_upper_or_none(order.get("s")),
            client_order_id=_str_or_none(order.get("c")),
            exchange_order_id=_str_or_none(order.get("i")),
            order_status=_str_or_none(order.get("X")),
            execution_type=_str_or_none(order.get("x")),
            executed_quantity=_decimal(order.get("z")),
            trade_id=str(trade_id) if has_trade_id else _str_or_none(order.get("t")),
            last_quantity=_decimal(order.get("l")),
            last_price=_decimal_or_none(order.get("L")),
            fee=_decimal(order.get("n")),
            fee_asset=_upper_or_none(order.get("N")),
            position_side=_upper_or_none(order.get("ps")),
            realized_pnl=_decimal_or_none(realized),
            realized_pnl_asset="USDT" if realized is not None else None,
            reduce_only=bool(order.get("R")) if order.get("R") is not None else None,
            raw=raw,
        )
    if event_type == "ACCOUNT_UPDATE":
        account = raw.get("a") if isinstance(raw.get("a"), dict) else {}
        balances = tuple(
            {
                "asset": _upper_or_none(row.get("a")) or "",
                "wallet_balance": str(row.get("wb", "0")),
                "available_balance": str(row.get("cw", row.get("wb", "0"))),
                "balance_change": str(row.get("bc", "0")),
            }
            for row in account.get("B", [])
            if isinstance(row, dict)
        )
        positions = tuple(
            {
                "symbol": _upper_or_none(row.get("s")) or "",
                "quantity": str(row.get("pa", "0")),
                "entry_price": str(row.get("ep", "0")),
                "unrealized_pnl": str(row.get("up", "0")),
                "realized_pnl": str(row.get("cr", "0")),
                "margin_type": str(row.get("mt", "")).upper(),
                "position_side": _upper_or_none(row.get("ps")) or "",
            }
            for row in account.get("P", [])
            if isinstance(row, dict)
        )
        return UserStreamEvent(
            event_type=event_type,
            event_id=_canonical_event_id(
                {
                    "event_type": event_type,
                    "event_time": str(raw.get("E") or ""),
                    "transaction_time": str(raw.get("T") or ""),
                    "balances": list(balances),
                    "positions": list(positions),
                }
            ),
            event_time_ms=_int_or_none(raw.get("E")),
            balance_updates=balances,
            position_updates=positions,
            completeness="PARTIAL",
            source="USER_STREAM",
            raw=raw,
        )
    return UserStreamEvent(event_type="malformed", raw=raw)


class UserStreamClient:
    """USD-M user data stream with reconnect and fail-closed halt."""

    def __init__(
        self,
        config: ClientConfig,
        *,
        on_event: EventCallback | None = None,
        on_reconcile: ReconcileCallback | None = None,
        on_halt: HaltCallback | None = None,
        on_listen_key: Callable[[str], Awaitable[None] | None] | None = None,
        rest_client: Any | None = None,
        websocket_connect: Callable[[str], Any] | None = None,
        reconnect_delay_sec: float = 2,
        max_failures: int = 3,
        keepalive_sec: float = 1800,
        max_backoff_sec: float | None = None,
    ) -> None:
        self.config = config
        if self.config.mode == "paper":
            raise BinanceAuthError("UserStreamClient cannot run in paper mode")
        self.on_event = on_event
        self.on_reconcile = on_reconcile
        self.on_halt = on_halt
        self.on_listen_key = on_listen_key
        self._rest = rest_client
        self._websocket_connect = websocket_connect or _default_websocket_connect
        self._stopped = False
        self._listen_key: str | None = None
        self._reconnects = 0
        self._reconnect_delay_sec = reconnect_delay_sec
        self._max_failures = max_failures
        self._max_backoff_sec = max_backoff_sec if max_backoff_sec is not None else float(
            os.environ.get("BIAN_WS_MAX_BACKOFF_SEC", "16")
        )
        self._keepalive_sec = max(0.01, float(keepalive_sec))
        self._seen_event_ids: OrderedDict[str, None] = OrderedDict()
        self._seen_event_limit = max(
            1,
            int(os.environ.get("BIAN_WS_EVENT_DEDUPE_MAX", "10000")),
        )
        self.stream_failure_reason: str | None = None
        self.state = "DISCONNECTED"
        self.connection_attempts = 0
        self.connected_at: datetime | None = None
        self.last_message_at: datetime | None = None
        self.last_error_at: datetime | None = None
        self.last_disconnect_at: datetime | None = None
        self.last_error: str | None = None
        self.transport_latency_ms: int | None = None
        self.proxy_mode = (
            "CONFIGURED"
            if os.environ.get("BIAN_HTTP_PROXY")
            or os.environ.get("HTTPS_PROXY")
            or os.environ.get("https_proxy")
            or os.environ.get("HTTP_PROXY")
            or os.environ.get("http_proxy")
            else "DIRECT"
        )
        self.reconnect_count = 0
        self.consecutive_failures = 0

    def _client(self) -> Any:
        if self._rest is None:
            self._rest = FuturesPrivateClient(self.config)
        return self._rest

    def _ws_url(self, listen_key: str) -> str:
        host = FUTURES_TESTNET_WS if self.config.mode == "testnet" else FUTURES_LIVE_WS
        return f"{host}/ws/{listen_key}"

    async def connect_once(self) -> Any:
        self.state = "CONNECTING"
        self.connection_attempts += 1
        listen_key = self._client().create_listen_key()
        self._listen_key = listen_key
        _dispatch(self.on_listen_key, listen_key)
        websocket = await _maybe_await(self._websocket_connect(self._ws_url(listen_key)))
        self._websocket = websocket
        self.state = "LIVE"
        self.connected_at = datetime.now(timezone.utc)
        self.last_error = None
        return websocket

    async def run_forever(self) -> None:
        self._stopped = False
        failures = 0
        keepalive_task = asyncio.create_task(self._keepalive_loop())
        try:
            while not self._stopped:
                websocket = None
                try:
                    websocket = await self.connect_once()
                    self.stream_failure_reason = None
                    await self._consume(websocket)
                    if self.stream_failure_reason is not None:
                        raise RuntimeError(self.stream_failure_reason)
                    failures = 0
                    self.consecutive_failures = 0
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    failures += 1
                    self.consecutive_failures = failures
                    now = datetime.now(timezone.utc)
                    self.last_error_at = now
                    self.last_disconnect_at = now
                    self.last_error = f"{type(exc).__name__}: {exc}"
                    self.state = "DISCONNECTED"
                    _dispatch(self.on_reconcile)
                    if failures >= self._max_failures:
                        self.state = "FAILED"
                        reason = f"futures user stream unrecoverable: {exc}"
                        _dispatch(self.on_halt, reason)
                        _dispatch(self.on_reconcile)
                        raise
                    self.state = "RECONNECTING"
                    self._reconnects += 1
                    self.reconnect_count += 1
                    base = min(
                        max(0.0, self._reconnect_delay_sec) * (2 ** (failures - 1)),
                        max(0.0, self._max_backoff_sec),
                    )
                    delay = min(
                        max(0.0, base + (random.uniform(0.0, 0.1) if base else 0.0)),
                        max(0.0, self._max_backoff_sec),
                    )
                    await asyncio.sleep(delay)
                finally:
                    await _close_ws(websocket)
        finally:
            keepalive_task.cancel()
            await asyncio.gather(keepalive_task, return_exceptions=True)
            await self.close()

    async def _consume(self, websocket: Any) -> None:
        if hasattr(websocket, "on_message"):
            websocket.on_message(self._message_received)
            if hasattr(websocket, "wait_closed"):
                await websocket.wait_closed()
            return
        async for message in websocket:
            received_at = datetime.now(timezone.utc)
            self.last_message_at = received_at
            raw = _payload(message)
            event_time = _int_or_none(raw.get("E"))
            if event_time is not None:
                self.transport_latency_ms = max(
                    0,
                    int(received_at.timestamp() * 1000) - event_time,
                )
            self._message_received(message)
            if self.stream_failure_reason is not None:
                return

    def _message_received(self, event: Any) -> None:
        try:
            normalized = normalize_user_event(event)
        except Exception:
            normalized = UserStreamEvent(event_type="malformed", raw={"raw": str(event)})
        if normalized.event_type == "listenKeyExpired":
            self.stream_failure_reason = "LISTEN_KEY_EXPIRED"
            _dispatch(self.on_reconcile)
            return
        if normalized.event_id is not None:
            if normalized.event_id in self._seen_event_ids:
                return
            self._seen_event_ids[normalized.event_id] = None
            while len(self._seen_event_ids) > self._seen_event_limit:
                self._seen_event_ids.popitem(last=False)
        _dispatch(self.on_event, normalized)

    async def _keepalive_loop(self) -> None:
        while not self._stopped:
            await asyncio.sleep(self._keepalive_sec)
            if self._stopped:
                return
            try:
                keeper = getattr(self._client(), "keepalive_listen_key", None)
                if keeper is not None:
                    keeper()
            except Exception:
                self.state = "DEGRADED"
                self.stream_failure_reason = "LISTEN_KEY_KEEPALIVE_FAILED"
                self.last_error = "LISTEN_KEY_KEEPALIVE_FAILED"
                self.last_error_at = datetime.now(timezone.utc)
                _dispatch(self.on_reconcile)
                await _close_ws(getattr(self, "_websocket", None))

    async def close(self) -> None:
        self._stopped = True
        if self.state != "FAILED":
            self.state = "DISCONNECTED"
        if self._listen_key is not None:
            closer = getattr(self._client(), "close_listen_key", None)
            if closer is not None:
                try:
                    closer()
                except Exception:
                    pass


def _dispatch(callback: Callable[..., Any] | None, *args: Any) -> None:
    if callback is None:
        return
    result = callback(*args)
    if inspect.isawaitable(result):
        try:
            asyncio.get_running_loop().create_task(result)
        except RuntimeError:
            result.close() if inspect.iscoroutine(result) else None


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _close_ws(websocket: Any) -> None:
    if websocket is None:
        return
    close = getattr(websocket, "close", None)
    if close is None:
        return
    result = close()
    if inspect.isawaitable(result):
        try:
            await result
        except Exception:
            pass


async def _default_websocket_connect(url: str) -> Any:
    import websockets

    proxy = (
        os.environ.get("BIAN_HTTP_PROXY")
        or os.environ.get("HTTPS_PROXY")
        or os.environ.get("https_proxy")
        or os.environ.get("HTTP_PROXY")
        or os.environ.get("http_proxy")
    )
    return await websockets.connect(url, proxy=proxy or True)


def _str_or_none(value: Any) -> str | None:
    return None if value is None else str(value)


def _upper_or_none(value: Any) -> str | None:
    return None if value is None else str(value).upper()


def _int_or_none(value: Any) -> int | None:
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


def _decimal_or_none(value: Any) -> Decimal | None:
    return None if value is None else _decimal(value)
