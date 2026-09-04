"""Binance REST adapter with separate public and private clients."""
from __future__ import annotations

import os
import time
import json
import hmac
import hashlib
import http.client
import logging
import random
import ssl
import urllib.parse
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable

from scripts.bian_market import _get_http_opener

from risk import FuturesAccountSnapshot, FuturesRiskRules

LOGGER = logging.getLogger("bian.futures.rest")


class BinanceError(Exception):
    """Base class for adapter errors."""


class BinanceAuthError(BinanceError):
    """Credentials or permissions are invalid."""


class BinanceConnectionError(BinanceError):
    """The exchange could not be reached or returned a server failure."""


class BinanceRateLimitError(BinanceError):
    """The exchange rate limit was reached."""


class BinanceOrderError(BinanceError):
    """The exchange rejected an order operation."""


class BinanceAPIError(BinanceError):
    """A non-order Binance API operation failed."""


@dataclass
class TransportMetadata:
    """Runtime evidence shared by Futures REST operations."""

    connected_at: datetime | None = None
    last_message_at: datetime | None = None
    reconnect_count: int = 0
    last_disconnect_at: datetime | None = None
    last_error: str | None = None
    transport_latency_ms: int | None = None
    proxy_mode: str = "DIRECT"

    def as_dict(self) -> dict[str, Any]:
        return {
            "connected_at": (
                self.connected_at.isoformat() if self.connected_at else None
            ),
            "last_message_at": (
                self.last_message_at.isoformat() if self.last_message_at else None
            ),
            "reconnect_count": self.reconnect_count,
            "last_disconnect_at": (
                self.last_disconnect_at.isoformat()
                if self.last_disconnect_at
                else None
            ),
            "last_error": self.last_error,
            "transport_latency_ms": self.transport_latency_ms,
            "proxy_mode": self.proxy_mode,
        }


@dataclass(frozen=True)
class ClientConfig:
    """Configuration loaded from environment or an explicit test mapping."""

    mode: str = "paper"
    api_key: str | None = None
    api_secret: str | None = None
    live_trading_enabled: bool = False
    live_confirmation_token: str | None = None
    timeout_ms: int = 10_000
    retries: int = 3
    backoff_ms: int = 250
    exchange_info_ttl_sec: int = 300
    transport_metadata: TransportMetadata = field(default_factory=TransportMetadata)

    @classmethod
    def from_env(cls, mode: str) -> "ClientConfig":
        resolved_mode = str(mode or "").strip().lower()
        credential_prefix = {
            "testnet": "BIAN_TESTNET",
            "live": "BIAN_LIVE",
        }.get(resolved_mode, "BIAN")
        return cls(
            mode=resolved_mode,
            api_key=os.environ.get(f"{credential_prefix}_API_KEY"),
            api_secret=os.environ.get(f"{credential_prefix}_API_SECRET"),
            live_trading_enabled=_env_bool("LIVE_TRADING_ENABLED"),
            live_confirmation_token=os.environ.get("LIVE_CONFIRMATION_TOKEN"),
            timeout_ms=_env_int("BIAN_BINANCE_TIMEOUT_MS", 10_000, minimum=100),
            retries=_env_int("BIAN_BINANCE_RETRIES", 3, minimum=0),
            backoff_ms=_env_int("BIAN_BINANCE_BACKOFF_MS", 250, minimum=0),
            exchange_info_ttl_sec=_env_int(
                "BIAN_EXCHANGE_INFO_TTL_SEC", 300, minimum=1
            ),
        )

    def __post_init__(self) -> None:
        if self.mode not in {"paper", "testnet", "live"}:
            raise ValueError("BIAN_MODE must be paper, testnet, or live")
        if (
            self.timeout_ms < 100
            or self.retries < 0
            or self.backoff_ms < 0
            or self.exchange_info_ttl_sec < 1
        ):
            raise ValueError("invalid Binance transport configuration")
        self.transport_metadata.proxy_mode = _proxy_mode()


def _env_bool(name: str) -> bool:
    return os.environ.get(name, "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _env_int(name: str, default: int, *, minimum: int) -> int:
    try:
        return max(minimum, int(os.environ.get(name, default)))
    except ValueError:
        return default


def _proxy_mode() -> str:
    return "CONFIGURED" if (
        os.environ.get("BIAN_HTTP_PROXY")
        or os.environ.get("HTTPS_PROXY")
        or os.environ.get("https_proxy")
        or os.environ.get("HTTP_PROXY")
        or os.environ.get("http_proxy")
    ) else "DIRECT"


def _translate_error(exc: Exception, *, operation: str) -> BinanceError:
    if isinstance(exc, urllib.error.HTTPError):
        code = int(exc.code)
        try:
            detail = exc.read().decode("utf-8")
        except Exception:
            detail = str(exc)
        if code in {401, 403}:
            return BinanceAuthError(f"{operation} authentication failed")
        if code == 429:
            return BinanceRateLimitError(f"{operation} rate limited")
        if code >= 500:
            return BinanceConnectionError(f"{operation} connection failure")
        if operation in {"create_order", "cancel_order", "cancel_all_orders"}:
            return BinanceOrderError(f"{operation} rejected: {detail}")
        return BinanceAPIError(f"{operation} failed: {detail}")
    if isinstance(
        exc,
        (
            OSError,
            TimeoutError,
            ConnectionError,
            ssl.SSLError,
            http.client.RemoteDisconnected,
            http.client.IncompleteRead,
            http.client.BadStatusLine,
        ),
    ):
        return BinanceConnectionError(f"{operation} connection failure")
    return BinanceAPIError(f"{operation} failed: {exc}")


FUTURES_LIVE_REST = "https://fapi.binance.com"
FUTURES_TESTNET_REST = "https://testnet.binancefuture.com"


def _futures_rest_host(config: ClientConfig) -> str:
    return FUTURES_TESTNET_REST if config.mode == "testnet" else FUTURES_LIVE_REST


def _signed_query(secret: str, params: dict[str, Any]) -> str:
    payload = {key: value for key, value in params.items() if value is not None}
    payload["timestamp"] = int(time.time() * 1000)
    payload["recvWindow"] = 5000
    query = urllib.parse.urlencode(payload)
    signature = hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{query}&signature={signature}"


def _reset_shared_opener() -> Any:
    try:
        from scripts.bian_market import reset_http_opener
    except Exception:
        return None
    reset_http_opener()
    return _get_http_opener()


def _safe_diagnostics(
    *,
    endpoint: str,
    method: str,
    symbol: str | None,
    attempt: int,
    elapsed_ms: int,
    exception_type: str | None,
    status_code: int | None,
    retryable: bool,
    request_id: str | None,
) -> dict[str, Any]:
    return {
        "endpoint": endpoint,
        "method": method,
        "symbol": symbol,
        "attempt": attempt,
        "elapsed_ms": elapsed_ms,
        "exception_type": exception_type,
        "status_code": status_code,
        "retryable": retryable,
        "request_id": request_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def execute_rest_request(
    opener: Any,
    request: urllib.request.Request,
    *,
    operation: str,
    attempts: int,
    timeout_sec: float,
    backoff_sec: float,
    max_backoff_sec: float = 8.0,
    metadata: TransportMetadata | None = None,
    logger: logging.Logger | None = None,
    on_connection_error: Callable[[], Any] | None = None,
    symbol: str | None = None,
) -> Any:
    """Single Futures REST retry loop for public and private calls."""
    logger = logger or LOGGER
    last_error: Exception | None = None
    parsed = urllib.parse.urlparse(request.full_url)
    endpoint = parsed.path or request.full_url
    host = parsed.netloc or "unknown"
    method = request.get_method() or "GET"
    tries = max(1, attempts)
    current_opener = opener
    for attempt in range(tries):
        started = time.monotonic()
        status_code: int | None = None
        request_id: str | None = None
        retryable = False
        error_class = None
        try:
            with current_opener.open(request, timeout=timeout_sec) as response:
                request_id = response.headers.get("X-MbX-Used-Weight") or response.headers.get(
                    "x-mbx-used-weight"
                )
                payload = json.loads(response.read().decode("utf-8") or "null")
            elapsed_ms = int((time.monotonic() - started) * 1000)
            if metadata is not None:
                now = datetime.now(timezone.utc)
                metadata.connected_at = metadata.connected_at or now
                metadata.last_message_at = now
                metadata.transport_latency_ms = elapsed_ms
                metadata.last_error = None
            return payload
        except urllib.error.HTTPError as exc:
            status_code = int(exc.code)
            request_id = exc.headers.get("x-mbx-used-weight") if exc.headers else None
            retry_after = exc.headers.get("Retry-After") if exc.headers else None
            translated = _translate_error(exc, operation=operation)
            last_error = translated
            error_class = type(exc).__name__
            retryable = isinstance(translated, (BinanceConnectionError, BinanceRateLimitError))
            if retry_after and str(retry_after).replace(".", "", 1).isdigit():
                delay = min(float(retry_after), max_backoff_sec)
            else:
                delay = min(backoff_sec * (2 ** attempt), max_backoff_sec)
        except Exception as exc:
            translated = _translate_error(exc, operation=operation)
            last_error = translated
            error_class = type(exc).__name__
            retryable = isinstance(translated, (BinanceConnectionError, BinanceRateLimitError))
            delay = min(backoff_sec * (2 ** attempt), max_backoff_sec)
            if retryable and on_connection_error is not None:
                replacement = on_connection_error()
                if replacement is not None:
                    current_opener = replacement
        elapsed_ms = int((time.monotonic() - started) * 1000)
        if metadata is not None:
            metadata.last_error = str(last_error)
            metadata.last_disconnect_at = datetime.now(timezone.utc)
        logger.warning(
            "REST request failed",
            extra=_safe_diagnostics(
                endpoint=endpoint,
                method=method,
                symbol=symbol,
                attempt=attempt + 1,
                elapsed_ms=elapsed_ms,
                exception_type=error_class,
                status_code=status_code,
                retryable=retryable,
                request_id=request_id,
            )
            | {"operation": operation, "host": host, "error_class": error_class},
        )
        if attempt + 1 < tries and retryable and delay:
            if metadata is not None:
                metadata.reconnect_count += 1
            jitter = random.uniform(0.0, min(0.1, max_backoff_sec))
            time.sleep(min(delay + jitter, max_backoff_sec))
        elif not retryable:
            break
    if isinstance(last_error, BinanceError):
        raise last_error
    raise BinanceConnectionError(f"{operation} connection failure")


def _futures_transport(
    config: ClientConfig,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    auth: str = "public",
    retries: int = 0,
    operation: str,
) -> Any:
    query_params = dict(params or {})
    headers = {"Accept": "application/json", "User-Agent": "bian-futures-adapter/1.0"}
    if auth in {"signed", "api_key"}:
        if not config.api_key:
            raise BinanceAuthError(f"credentials are required for BIAN_MODE={config.mode}")
        headers["X-MBX-APIKEY"] = config.api_key
    if auth == "signed":
        if not config.api_key or not config.api_secret:
            raise BinanceAuthError(f"credentials are required for BIAN_MODE={config.mode}")
        query = _signed_query(config.api_secret, query_params)
    else:
        query = urllib.parse.urlencode(
            {key: value for key, value in query_params.items() if value is not None}
        )
    if path.startswith("http://") or path.startswith("https://"):
        url = path
    else:
        url = f"{_futures_rest_host(config)}{path}"
    if query:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}{query}"
    request = urllib.request.Request(url, method=method.upper(), headers=headers)
    opener = _get_http_opener()
    metadata = config.transport_metadata
    metadata.proxy_mode = _proxy_mode()
    symbol = None
    if query_params.get("symbol"):
        symbol = str(query_params["symbol"])
    return execute_rest_request(
        opener,
        request,
        operation=operation,
        attempts=max(0, retries) + 1,
        timeout_sec=config.timeout_ms / 1000,
        backoff_sec=config.backoff_ms / 1000,
        max_backoff_sec=8.0,
        metadata=metadata,
        on_connection_error=_reset_shared_opener,
        symbol=symbol,
    )


def _futures_signed_request(
    config: ClientConfig,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    operation: str,
) -> Any:
    """Retryable signed transport for read-only authenticated endpoints."""
    return _futures_transport(
        config,
        method,
        path,
        params=params,
        auth="signed",
        retries=config.retries,
        operation=operation,
    )


def _futures_order_request(
    config: ClientConfig,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    operation: str,
) -> Any:
    """Single-attempt signed transport for order mutations.

    A transport failure after submission is intentionally surfaced to the
    caller as UNKNOWN. Re-submission belongs to exact-order reconciliation,
    never to this transport helper.
    """
    return _futures_transport(
        config,
        method,
        path,
        params=params,
        auth="signed",
        retries=0,
        operation=operation,
    )


def _futures_api_key_request(
    config: ClientConfig,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    operation: str,
) -> Any:
    """USD-M user-stream transport: API key header, no signature."""
    return _futures_transport(
        config,
        method,
        path,
        params=params,
        auth="api_key",
        retries=config.retries,
        operation=operation,
    )


class FuturesPublicClient:
    """Public USD-M Futures observation adapter; it has no order methods."""

    def __init__(self, config: ClientConfig) -> None:
        self.config = config
        self.base_url = (
            "https://testnet.binancefuture.com/fapi/v1"
            if self.config.mode == "testnet"
            else "https://fapi.binance.com/fapi/v1"
        )
        self.data_url = (
            "https://testnet.binancefuture.com/futures/data"
            if self.config.mode == "testnet"
            else "https://fapi.binance.com/futures/data"
        )

    def _get(self, path: str, **params: Any) -> Any:
        query = urllib.parse.urlencode({key: value for key, value in params.items() if value is not None})
        url = f"{path}?{query}" if query else path
        parsed = urllib.parse.urlparse(path)
        operation = parsed.path.rsplit("/", 1)[-1] or "futures_observation"
        return _futures_transport(
            self.config,
            "GET",
            url,
            auth="public",
            retries=self.config.retries,
            operation=operation,
        )

    def get_mark_price(self, symbol: str) -> dict[str, Any]:
        return self._get(f"{self.base_url}/premiumIndex", symbol=symbol.upper())

    def get_klines(
        self, symbol: str, *, interval: str = "1m", limit: int = 500
    ) -> list[Any]:
        payload = self._get(
            f"{self.base_url}/klines",
            symbol=symbol.upper(),
            interval=interval,
            limit=max(1, min(limit, 1500)),
        )
        return payload if isinstance(payload, list) else []

    def get_ticker_price(self, symbol: str) -> dict[str, Any]:
        return self._get(f"{self.base_url}/ticker/price", symbol=symbol.upper())

    def get_aggregate_trades(
        self, symbol: str, *, limit: int = 1000
    ) -> list[dict[str, Any]]:
        payload = self._get(
            f"{self.base_url}/aggTrades",
            symbol=symbol.upper(),
            limit=max(1, min(limit, 1000)),
        )
        return payload if isinstance(payload, list) else []

    def get_open_interest(self, symbol: str) -> dict[str, Any]:
        return self._get(f"{self.base_url}/openInterest", symbol=symbol.upper())

    def get_funding_rate(self, symbol: str, *, limit: int = 10) -> list[dict[str, Any]]:
        return self._get(
            f"{self.base_url}/fundingRate", symbol=symbol.upper(), limit=max(1, min(limit, 1000))
        )

    def get_taker_buy_sell(self, symbol: str, *, period: str = "5m", limit: int = 30) -> list[dict[str, Any]]:
        return self._get(
            f"{self.data_url}/takerlongshortRatio", symbol=symbol.upper(),
            period=period, limit=max(1, min(limit, 500))
        )

    def get_global_long_short_ratio(self, symbol: str, *, period: str = "5m", limit: int = 30) -> list[dict[str, Any]]:
        return self._get(
            f"{self.data_url}/globalLongShortAccountRatio", symbol=symbol.upper(),
            period=period, limit=max(1, min(limit, 500))
        )

    def get_top_trader_long_short_ratio(self, symbol: str, *, period: str = "5m", limit: int = 30) -> list[dict[str, Any]]:
        return self._get(
            f"{self.data_url}/topLongShortAccountRatio", symbol=symbol.upper(),
            period=period, limit=max(1, min(limit, 500))
        )

    def get_exchange_info(self, symbol: str | None = None) -> dict[str, Any]:
        return self._get(
            f"{self.base_url}/exchangeInfo",
            symbol=symbol.upper() if symbol else None,
        )

    def get_symbol_rules(self, symbol: str) -> dict[str, str]:
        normalized = symbol.upper().strip()
        payload = self.get_exchange_info(normalized)
        rows = payload.get("symbols") or []
        row = next(
            (item for item in rows if str(item.get("symbol", "")).upper() == normalized),
            None,
        )
        if row is None:
            raise BinanceAPIError(f"exchange info has no symbol: {normalized}")
        filters = {str(item.get("filterType")): item for item in row.get("filters", [])}
        lot = filters.get("LOT_SIZE") or filters.get("MARKET_LOT_SIZE") or {}
        price = filters.get("PRICE_FILTER") or {}
        notional = filters.get("MIN_NOTIONAL") or filters.get("NOTIONAL") or {}
        normalized_rules = {
            "symbol": normalized,
            "status": str(row.get("status", "")),
            "min_qty": str(lot.get("minQty", "0")),
            "max_qty": str(lot.get("maxQty", "0")),
            "step_size": str(lot.get("stepSize", "0")),
            "tick_size": str(price.get("tickSize", "0")),
            "min_notional": str(notional.get("notional", notional.get("minNotional", "0"))),
            "quantity_precision": str(row.get("quantityPrecision", "")),
            "price_precision": str(row.get("pricePrecision", "")),
        }
        if normalized_rules["status"] != "TRADING":
            raise BinanceAPIError(f"Futures symbol is not trading: {normalized}")
        if any(
            Decimal(normalized_rules[name]) <= 0
            for name in ("min_qty", "step_size", "tick_size")
        ):
            raise BinanceAPIError(
                f"Futures exchange rules are incomplete for {normalized}"
            )
        if not normalized_rules["quantity_precision"].strip():
            raise BinanceAPIError(
                f"Futures quantity precision is unavailable for {normalized}"
            )
        return normalized_rules


class FuturesPrivateClient:
    """Authenticated USD-M Futures adapter. Paper cannot construct it."""

    def __init__(self, config: ClientConfig) -> None:
        self.config = config
        if self.config.mode == "paper":
            raise BinanceAuthError(
                "FuturesPrivateClient cannot be created in paper mode; use execution.py"
            )
        if self.config.mode == "live" and not self.config.live_trading_enabled:
            raise BinanceAuthError(
                "live mode is hard-blocked unless LIVE_TRADING_ENABLED=true"
            )
        if self.config.mode == "live" and not self.config.live_confirmation_token:
            raise BinanceAuthError(
                "live mode requires LIVE_CONFIRMATION_TOKEN"
            )
        if not self.config.api_key or not self.config.api_secret:
            raise BinanceAuthError(
                f"credentials are required for BIAN_MODE={self.config.mode}"
            )

    def _signed(
        self,
        method: str,
        path: str,
        *,
        operation: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return _futures_signed_request(
            self.config,
            method,
            path,
            params=params,
            operation=operation,
        )

    def _order(
        self,
        method: str,
        path: str,
        *,
        operation: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        if self.config.mode == "live" and operation in {
            "create_order",
            "cancel_order",
            "cancel_all_orders",
        }:
            from runtime_gate import LiveAuthorizationError, authorize_live_order_mutation

            try:
                authorize_live_order_mutation(mode="live")
            except LiveAuthorizationError as exc:
                raise BinanceAuthError(str(exc)) from exc
        return _futures_order_request(
            self.config,
            method,
            path,
            params=params,
            operation=operation,
        )

    def _api_key(
        self,
        method: str,
        path: str,
        *,
        operation: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return _futures_api_key_request(
            self.config,
            method,
            path,
            params=params,
            operation=operation,
        )

    def get_account(self) -> dict[str, Any]:
        return self._signed("GET", "/fapi/v2/account", operation="get_account")

    def account_snapshot(self) -> FuturesAccountSnapshot:
        captured_at = datetime.now(timezone.utc)
        account = self.get_account()
        position_risk = self.get_position_risk()
        positions = [
            row for row in position_risk
            if Decimal(str(row.get("positionAmt") or "0")) != 0
        ]
        open_orders = self.get_open_orders()
        position_mode = self.get_position_mode()
        return FuturesAccountSnapshot.from_binance(
            mode=self.config.mode,  # type: ignore[arg-type]
            account=account,
            positions=positions,
            open_orders=open_orders,
            position_mode=position_mode,
            captured_at=captured_at,
            symbol_leverage={
                str(row["symbol"]).upper(): Decimal(str(row["leverage"]))
                for row in position_risk
                if row.get("symbol") and row.get("leverage") is not None
            },
        )

    def get_server_time(self) -> dict[str, Any]:
        return _futures_transport(
            self.config,
            "GET",
            "/fapi/v1/time",
            auth="public",
            retries=self.config.retries,
            operation="get_server_time",
        )

    def get_balance(self, asset: str | None = None) -> dict[str, Any] | list[dict[str, Any]]:
        account = self.get_account()
        balances = account.get("assets") or account.get("balances") or []
        if asset is None:
            return balances
        wanted = asset.upper()
        return next(
            (row for row in balances if str(row.get("asset", "")).upper() == wanted),
            {"asset": wanted, "walletBalance": "0", "availableBalance": "0"},
        )

    def get_position_risk(self, symbol: str | None = None) -> list[dict[str, Any]]:
        payload = self._signed(
            "GET",
            "/fapi/v2/positionRisk",
            operation="get_position_risk",
            params={"symbol": symbol.upper() if symbol else None},
        )
        return payload if isinstance(payload, list) else [payload]

    def get_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        return [
            row for row in self.get_position_risk(symbol)
            if Decimal(str(row.get("positionAmt") or "0")) != 0
        ]

    def get_position_mode(self) -> dict[str, Any]:
        return self._signed("GET", "/fapi/v1/positionSide/dual", operation="get_position_mode")

    def get_leverage(self, symbol: str) -> dict[str, Any]:
        rows = self.get_position_risk(symbol)
        if not rows:
            raise BinanceAPIError(f"no position risk for {symbol}")
        return {"symbol": symbol.upper(), "leverage": rows[0].get("leverage")}

    def get_margin_type(self, symbol: str) -> dict[str, Any]:
        rows = self.get_position_risk(symbol)
        if not rows:
            raise BinanceAPIError(f"no position risk for {symbol}")
        return {"symbol": symbol.upper(), "marginType": rows[0].get("marginType")}

    def create_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal | str | float | int | None = None,
        price: Decimal | str | float | int | None = None,
        client_order_id: str | None = None,
        *,
        reduce_only: bool = False,
        position_side: str | None = None,
        time_in_force: str | None = None,
    ) -> dict[str, Any]:
        from trade_intent import is_canonical_futures_symbol

        normalized_symbol = str(symbol or "").upper().strip()
        if self.config.mode in {"testnet", "live"} and not is_canonical_futures_symbol(
            normalized_symbol
        ):
            raise BinanceOrderError(
                "canonical futures universe is BTCUSDT, ETHUSDT, BNBUSDT; "
                f"unauthorized symbol: {normalized_symbol or symbol}"
            )
        normalized_side = side.upper().strip()
        normalized_type = order_type.upper().strip()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError("unsupported Binance order side")
        if normalized_type not in {"MARKET", "LIMIT"}:
            raise ValueError("unsupported Binance order type")
        if quantity is None:
            raise ValueError("Futures orders require quantity")
        params: dict[str, Any] = {
            "symbol": normalized_symbol,
            "side": normalized_side,
            "type": normalized_type,
            "quantity": str(quantity),
            "newClientOrderId": client_order_id,
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        if position_side:
            params["positionSide"] = position_side.upper()
        if normalized_type == "LIMIT":
            if price is None:
                raise ValueError("LIMIT intents require price")
            params["price"] = str(price)
            params["timeInForce"] = (time_in_force or "GTC").upper()
        return self._order("POST", "/fapi/v1/order", operation="create_order", params=params)

    def cancel_order(
        self,
        symbol: str,
        order_id: int | str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        from trade_intent import is_canonical_futures_symbol

        normalized_symbol = str(symbol or "").upper().strip()
        if self.config.mode in {"testnet", "live"} and not is_canonical_futures_symbol(
            normalized_symbol
        ):
            raise BinanceOrderError(
                "canonical futures universe is BTCUSDT, ETHUSDT, BNBUSDT; "
                f"unauthorized symbol: {normalized_symbol or symbol}"
            )
        return self._order(
            "DELETE",
            "/fapi/v1/order",
            operation="cancel_order",
            params={
                "symbol": normalized_symbol,
                "orderId": int(order_id) if order_id is not None else None,
                "origClientOrderId": client_order_id,
            },
        )

    def cancel_all_orders(self, symbol: str) -> dict[str, Any]:
        from trade_intent import is_canonical_futures_symbol

        normalized_symbol = str(symbol or "").upper().strip()
        if self.config.mode in {"testnet", "live"} and not is_canonical_futures_symbol(
            normalized_symbol
        ):
            raise BinanceOrderError(
                "canonical futures universe is BTCUSDT, ETHUSDT, BNBUSDT; "
                f"unauthorized symbol: {normalized_symbol or symbol}"
            )
        return self._order(
            "DELETE",
            "/fapi/v1/allOpenOrders",
            operation="cancel_all_orders",
            params={"symbol": normalized_symbol},
        )

    def get_order(
        self,
        symbol: str,
        order_id: int | str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        return self._signed(
            "GET",
            "/fapi/v1/order",
            operation="get_order",
            params={
                "symbol": symbol.upper(),
                "orderId": int(order_id) if order_id is not None else None,
                "origClientOrderId": client_order_id,
            },
        )

    def get_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        payload = self._signed(
            "GET",
            "/fapi/v1/openOrders",
            operation="get_open_orders",
            params={"symbol": symbol.upper() if symbol else None},
        )
        return payload if isinstance(payload, list) else [payload]

    def get_all_orders(
        self,
        symbol: str,
        order_id: int | str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        payload = self._signed(
            "GET",
            "/fapi/v1/allOrders",
            operation="get_all_orders",
            params={
                "symbol": symbol.upper(),
                "orderId": int(order_id) if order_id is not None else None,
                "limit": limit,
            },
        )
        return payload if isinstance(payload, list) else [payload]

    def get_user_trades(self, symbol: str, limit: int | None = None) -> list[dict[str, Any]]:
        payload = self._signed(
            "GET",
            "/fapi/v1/userTrades",
            operation="get_user_trades",
            params={"symbol": symbol.upper(), "limit": limit},
        )
        return payload if isinstance(payload, list) else [payload]

    def get_income_history(
        self,
        symbol: str | None = None,
        income_type: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        payload = self._signed(
            "GET",
            "/fapi/v1/income",
            operation="get_income_history",
            params={
                "symbol": symbol.upper() if symbol else None,
                "incomeType": income_type,
                "limit": limit,
            },
        )
        return payload if isinstance(payload, list) else [payload]



    def create_listen_key(self) -> str:
        payload = self._api_key("POST", "/fapi/v1/listenKey", operation="create_listen_key")
        return str(payload["listenKey"])

    def keepalive_listen_key(self) -> None:
        self._api_key("PUT", "/fapi/v1/listenKey", operation="keepalive_listen_key")

    def close_listen_key(self) -> None:
        self._api_key("DELETE", "/fapi/v1/listenKey", operation="close_listen_key")
