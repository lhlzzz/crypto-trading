"""Private Binance USD-M Futures User Data Stream."""
from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import dataclass, field
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
    event_time_ms: int | None = None
    symbol: str | None = None
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    order_status: str | None = None
    execution_type: str | None = None
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


def normalize_user_event(event: Any) -> UserStreamEvent:
    """Normalize Binance USD-M user-data JSON into one event contract."""
    raw = _payload(event)
    if not raw:
        return UserStreamEvent(event_type="malformed", raw={"raw": str(event)})
    event_type = str(raw.get("e") or raw.get("eventType") or "unknown")
    if event_type in {"executionReport", "outboundAccountPosition"}:
        return UserStreamEvent(event_type="malformed", raw=raw)
    if event_type == "ORDER_TRADE_UPDATE":
        order = raw.get("o") if isinstance(raw.get("o"), dict) else {}
        realized = order.get("rp")
        return UserStreamEvent(
            event_type=event_type,
            event_time_ms=_int_or_none(raw.get("E")),
            symbol=_upper_or_none(order.get("s")),
            client_order_id=_str_or_none(order.get("c")),
            exchange_order_id=_str_or_none(order.get("i")),
            order_status=_str_or_none(order.get("X")),
            execution_type=_str_or_none(order.get("x")),
            executed_quantity=_decimal(order.get("z")),
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
            event_time_ms=_int_or_none(raw.get("E")),
            balance_updates=balances,
            position_updates=positions,
            raw=raw,
        )
    return UserStreamEvent(
        event_type=event_type,
        event_time_ms=_int_or_none(raw.get("E")),
        symbol=_upper_or_none(raw.get("s")),
        raw=raw,
    )


class UserStreamClient:
    """USD-M user data stream with reconnect and fail-closed halt."""

    def __init__(
        self,
        config: ClientConfig | None = None,
        *,
        on_event: EventCallback | None = None,
        on_reconcile: ReconcileCallback | None = None,
        on_halt: HaltCallback | None = None,
        rest_client: Any | None = None,
        websocket_connect: Callable[[str], Any] | None = None,
        reconnect_delay_sec: float = 2,
        max_failures: int = 3,
        keepalive_sec: float = 1800,
    ) -> None:
        self.config = config or ClientConfig.from_env()
        if self.config.mode == "paper":
            raise BinanceAuthError("UserStreamClient cannot run in paper mode")
        self.on_event = on_event
        self.on_reconcile = on_reconcile
        self.on_halt = on_halt
        self._rest = rest_client
        self._websocket_connect = websocket_connect or _default_websocket_connect
        self._stopped = False
        self._listen_key: str | None = None
        self._reconnects = 0
        self._reconnect_delay_sec = reconnect_delay_sec
        self._max_failures = max_failures
        self._keepalive_sec = max(0.01, float(keepalive_sec))

    def _client(self) -> Any:
        if self._rest is None:
            self._rest = FuturesPrivateClient(self.config)
        return self._rest

    def _ws_url(self, listen_key: str) -> str:
        host = FUTURES_TESTNET_WS if self.config.mode == "testnet" else FUTURES_LIVE_WS
        return f"{host}/ws/{listen_key}"

    async def connect_once(self) -> Any:
        listen_key = self._client().create_listen_key()
        self._listen_key = listen_key
        websocket = await _maybe_await(self._websocket_connect(self._ws_url(listen_key)))
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
                    failures = 0
                    await self._consume(websocket)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    failures += 1
                    _dispatch(self.on_reconcile)
                    if failures >= self._max_failures:
                        reason = f"futures user stream unrecoverable: {exc}"
                        _dispatch(self.on_halt, reason)
                        _dispatch(self.on_reconcile)
                        raise
                    self._reconnects += 1
                    await asyncio.sleep(self._reconnect_delay_sec * failures)
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
            self._message_received(message)

    def _message_received(self, event: Any) -> None:
        try:
            normalized = normalize_user_event(event)
        except Exception:
            normalized = UserStreamEvent(event_type="malformed", raw={"raw": str(event)})
        if normalized.event_type == "listenKeyExpired":
            _dispatch(self.on_reconcile)
            raise RuntimeError("listenKey expired")
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
                _dispatch(self.on_reconcile)

    async def close(self) -> None:
        self._stopped = True
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

    return await websockets.connect(url)


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
