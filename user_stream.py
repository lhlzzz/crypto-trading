"""Private Binance User Data Stream, isolated from public market streams."""
from __future__ import annotations

import asyncio
import inspect
import os
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Awaitable, Callable

from binance_client import BinanceAuthError, ClientConfig


ReconcileCallback = Callable[[], Awaitable[None] | None]
EventCallback = Callable[["UserStreamEvent"], Awaitable[None] | None]


@dataclass(frozen=True)
class UserStreamEvent:
    """Stable event contract consumed by execution/reconciliation layers."""

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
    balances: tuple[dict[str, str], ...] = ()
    raw: dict[str, Any] = field(default_factory=dict)


def _decimal(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value if value is not None else default))
    except Exception:
        return Decimal(default)


def _payload(event: Any) -> dict[str, Any]:
    if isinstance(event, dict):
        return dict(event)
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
    """Normalize raw SDK models and Binance JSON into one event contract."""

    raw = _payload(event)
    event_type = str(raw.get("e") or raw.get("eventType") or raw.get("filterType") or "unknown")
    if event_type == "executionReport":
        return UserStreamEvent(
            event_type=event_type,
            event_time_ms=_int_or_none(raw.get("E")),
            symbol=_upper_or_none(raw.get("s")),
            client_order_id=_str_or_none(raw.get("c")),
            exchange_order_id=_str_or_none(raw.get("i")),
            order_status=_str_or_none(raw.get("X")),
            execution_type=_str_or_none(raw.get("x")),
            executed_quantity=_decimal(raw.get("z")),
            last_quantity=_decimal(raw.get("l")),
            last_price=_decimal_or_none(raw.get("L")),
            fee=_decimal(raw.get("n")),
            fee_asset=_upper_or_none(raw.get("N")),
            raw=raw,
        )
    if event_type == "outboundAccountPosition":
        balances = tuple(
            {
                "asset": _upper_or_none(row.get("a")) or "",
                "free": str(row.get("f", "0")),
                "locked": str(row.get("l", "0")),
            }
            for row in raw.get("B", [])
            if isinstance(row, dict)
        )
        return UserStreamEvent(
            event_type=event_type,
            event_time_ms=_int_or_none(raw.get("E")),
            balances=balances,
            raw=raw,
        )
    return UserStreamEvent(
        event_type=event_type,
        event_time_ms=_int_or_none(raw.get("E")),
        symbol=_upper_or_none(raw.get("s")),
        raw=raw,
    )


class UserStreamClient:
    """Run the official SDK's private user stream with safe reconnect hooks."""

    def __init__(
        self,
        config: ClientConfig | None = None,
        *,
        on_event: EventCallback | None = None,
        on_reconcile: ReconcileCallback | None = None,
        sdk_factory: Callable[[ClientConfig], Any] | None = None,
    ) -> None:
        self.config = config or ClientConfig.from_env()
        if self.config.mode == "paper":
            raise BinanceAuthError("UserStreamClient cannot run in paper mode")
        self.on_event = on_event
        self.on_reconcile = on_reconcile
        self._sdk_factory = sdk_factory or _build_sdk
        self._sdk: Any = None
        self._stream: Any = None
        self._stopped = False

    async def connect_once(self) -> None:
        self._sdk = self._sdk_factory(self.config)
        websocket_api = self._sdk.websocket_api
        await websocket_api.create_connection()
        websocket_api.on_connection("reconnect", self._connection_recovered)
        websocket_api.on_connection("error", self._connection_lost)
        websocket_api.on_connection("close", self._connection_lost)
        response = await websocket_api.user_data_stream_subscribe_signature()
        self._stream = response.stream
        self._stream.on("message", self._message_received)

    async def run_forever(self) -> None:
        """Keep the stream alive; the SDK handles socket replacement."""

        self._stopped = False
        try:
            await self.connect_once()
            while not self._stopped:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            raise
        finally:
            await self.close()

    async def close(self) -> None:
        self._stopped = True
        if self._stream is not None:
            try:
                await self._stream.unsubscribe()
            except Exception:
                pass
        if self._sdk is not None:
            websocket_api = self._sdk.websocket_api
            try:
                await websocket_api.close_connection(close_session=True)
            except Exception:
                pass

    def _message_received(self, event: Any) -> None:
        normalized = normalize_user_event(event)
        _dispatch(self.on_event, normalized)

    def _connection_lost(self, *_: Any) -> None:
        # Reconciliation is intentionally fail-closed and remains the source
        # of truth; the callback may set HALT when REST is unavailable.
        return None

    def _connection_recovered(self, *_: Any) -> None:
        _dispatch(self.on_reconcile)


def _dispatch(callback: Callable[..., Any] | None, *args: Any) -> None:
    if callback is None:
        return
    result = callback(*args)
    if inspect.isawaitable(result):
        try:
            asyncio.get_running_loop().create_task(result)
        except RuntimeError:
            result.close() if inspect.iscoroutine(result) else None


def _build_sdk(config: ClientConfig) -> Any:
    from binance_common.configuration import ConfigurationRestAPI, ConfigurationWebSocketAPI
    from binance_sdk_spot import Spot

    if not config.api_key or not config.api_secret:
        raise BinanceAuthError(
            f"credentials are required for BIAN_MODE={config.mode}"
        )
    stream_url = (
        "wss://ws-api.testnet.binance.vision/ws-api/v3"
        if config.mode == "testnet"
        else "wss://ws-api.binance.com:443/ws-api/v3"
    )
    rest_url = (
        "https://testnet.binance.vision"
        if config.mode == "testnet"
        else "https://api.binance.com"
    )
    return Spot(
        config_rest_api=ConfigurationRestAPI(
            api_key=config.api_key,
            api_secret=config.api_secret,
            base_path=rest_url,
            timeout=config.timeout_ms,
            retries=config.retries,
            backoff=config.backoff_ms,
        ),
        config_ws_api=ConfigurationWebSocketAPI(
            api_key=config.api_key,
            api_secret=config.api_secret,
            stream_url=stream_url,
            timeout=config.timeout_ms,
            reconnect_attempts=max(1, min(10, config.retries or 1)),
            reconnect_delay=max(100, config.backoff_ms),
        ),
    )


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
