"""Binance REST adapter with separate public and private clients."""
from __future__ import annotations

import os
import time
import json
import hmac
import hashlib
import urllib.parse
import urllib.request
import urllib.error
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

from scripts.bian_market import _get_http_opener

from binance_common.configuration import ConfigurationRestAPI
from binance_sdk_spot import (
    BadRequestError,
    ClientError,
    ForbiddenError,
    NetworkError,
    RateLimitBanError,
    ServerError,
    Spot,
    TooManyRequestsError,
    UnauthorizedError,
)
from binance_sdk_spot.rest_api.models import (
    KlinesIntervalEnum,
)
from risk import FuturesRiskRules


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

    @classmethod
    def from_env(cls, mode: str | None = None) -> "ClientConfig":
        resolved_mode = (mode or os.environ.get("BIAN_MODE", "paper")).strip().lower()
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


def _translate_error(exc: Exception, *, operation: str) -> BinanceError:
    if isinstance(exc, (UnauthorizedError, ForbiddenError)):
        return BinanceAuthError(f"{operation} authentication failed")
    if isinstance(exc, (TooManyRequestsError, RateLimitBanError)):
        return BinanceRateLimitError(f"{operation} rate limited")
    if isinstance(exc, (NetworkError, ServerError)):
        return BinanceConnectionError(f"{operation} connection failure")
    if isinstance(exc, (BadRequestError, ClientError)):
        message = str(exc)
        if operation in {"create_order", "cancel_order"}:
            return BinanceOrderError(f"{operation} rejected: {message}")
        return BinanceAPIError(f"{operation} failed: {message}")
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
    if isinstance(exc, (OSError, TimeoutError, ConnectionError)):
        return BinanceConnectionError(f"{operation} connection failure")
    return BinanceAPIError(f"{operation} failed: {exc}")


def _model_dict(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, list):
        return [_model_dict(item) for item in value]
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, dict):
        return {key: _model_dict(item) for key, item in value.items()}
    return value


def _call(operation: str, function: Callable[[], Any]) -> Any:
    try:
        response = function()
        return _model_dict(response.data())
    except Exception as exc:
        raise _translate_error(exc, operation=operation) from exc


def _configuration(config: ClientConfig, *, private: bool) -> ConfigurationRestAPI:
    if private and (not config.api_key or not config.api_secret):
        raise BinanceAuthError(
            f"credentials are required for BIAN_MODE={config.mode}"
        )
    base_path = (
        "https://api.binance.com"
    )
    return ConfigurationRestAPI(
        api_key=config.api_key if private else None,
        api_secret=config.api_secret if private else None,
        base_path=base_path,
        timeout=config.timeout_ms,
        retries=config.retries,
        backoff=config.backoff_ms,
    )


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
    url = f"{_futures_rest_host(config)}{path}"
    if query:
        url = f"{url}?{query}"
    request = urllib.request.Request(url, method=method.upper(), headers=headers)
    for attempt in range(max(0, retries) + 1):
        try:
            with urllib.request.urlopen(request, timeout=config.timeout_ms / 1000) as response:
                return json.loads(response.read().decode("utf-8") or "null")
        except Exception as exc:
            translated = _translate_error(exc, operation=operation)
            retryable = isinstance(translated, (BinanceConnectionError, BinanceRateLimitError))
            if not retryable or attempt >= retries:
                raise translated from exc
            delay_sec = config.backoff_ms / 1000 * (2 ** attempt)
            if delay_sec:
                time.sleep(delay_sec)
    raise AssertionError("Futures transport loop must return or raise")


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


class SpotPublicClient:
    """Read-only Spot public market-data adapter. It has no order methods."""

    def __init__(self, config: ClientConfig | None = None) -> None:
        self.config = config or ClientConfig.from_env()
        self._client = Spot(config_rest_api=_configuration(self.config, private=False))
        self._exchange_info_cache: tuple[float, dict[str, Any]] | None = None

    def get_klines(
        self,
        symbol: str,
        interval: str = "1m",
        limit: int = 500,
    ) -> list[Any]:
        try:
            interval_enum = KlinesIntervalEnum(interval)
        except ValueError as exc:
            raise ValueError(f"unsupported Binance kline interval: {interval}") from exc
        return _call(
            "get_klines",
            lambda: self._client.rest_api.klines(symbol, interval_enum, limit=limit),
        )

    def get_depth(self, symbol: str, limit: int = 100) -> dict[str, Any]:
        return _call(
            "get_depth",
            lambda: self._client.rest_api.depth(symbol, limit=limit),
        )

    def get_ticker(self, symbol: str) -> dict[str, Any] | list[dict[str, Any]]:
        return _call(
            "get_ticker",
            lambda: self._client.rest_api.ticker_price(symbol=symbol),
        )

    def get_exchange_info(self, symbol: str | None = None) -> dict[str, Any]:
        if symbol is None and self._exchange_info_cache is not None:
            cached_at, cached = self._exchange_info_cache
            if time.monotonic() - cached_at < self.config.exchange_info_ttl_sec:
                return cached
        payload = _call(
            "get_exchange_info",
            lambda: self._client.rest_api.exchange_info(symbol=symbol),
        )
        if symbol is None:
            self._exchange_info_cache = (time.monotonic(), payload)
        return payload

    def get_symbol_rules(
        self,
        symbol: str,
        *,
        force_refresh: bool = False,
    ) -> dict[str, str]:
        """Return normalized exchange filters for one symbol.

        This is exchange metadata only. Risk owns how these values are
        applied to an intent; the adapter only fetches, caches, and parses the
        Binance response.
        """
        symbol = symbol.upper().strip()
        if not symbol:
            raise ValueError("symbol is required")
        if force_refresh:
            self._exchange_info_cache = None
        payload = self.get_exchange_info()
        symbols = payload.get("symbols") or []
        row = next(
            (item for item in symbols if str(item.get("symbol", "")).upper() == symbol),
            None,
        )
        if row is None:
            raise BinanceAPIError(f"exchange info has no symbol: {symbol}")
        filters = {
            str(item.get("filterType")): item
            for item in row.get("filters", [])
        }
        lot = filters.get("LOT_SIZE") or filters.get("MARKET_LOT_SIZE") or {}
        price = filters.get("PRICE_FILTER") or {}
        notional = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}
        return {
            "symbol": symbol,
            "status": str(row.get("status", "")),
            "min_qty": str(lot.get("minQty", "0")),
            "max_qty": str(lot.get("maxQty", "0")),
            "step_size": str(lot.get("stepSize", "0")),
            "tick_size": str(price.get("tickSize", "0")),
            "min_notional": str(
                notional.get("minNotional", notional.get("notional", "0"))
            ),
        }


PublicClient = SpotPublicClient


class FuturesPublicClient:
    """Public USD-M Futures observation adapter; it has no order methods."""

    def __init__(self, config: ClientConfig | None = None) -> None:
        self.config = config or ClientConfig.from_env()
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
        request = urllib.request.Request(
            url, headers={"Accept": "application/json", "User-Agent": "bian-futures-observer/1.0"}
        )
        opener = _get_http_opener()
        for attempt in range(self.config.retries + 1):
            try:
                with opener.open(request, timeout=self.config.timeout_ms / 1000) as response:
                    return json.loads(response.read().decode("utf-8"))
            except Exception as exc:
                translated = _translate_error(exc, operation="futures_observation")
                retryable = isinstance(
                    translated, (BinanceConnectionError, BinanceRateLimitError)
                )
                if not retryable or attempt >= self.config.retries:
                    raise translated from exc
                delay_sec = self.config.backoff_ms / 1000 * (2**attempt)
                if delay_sec:
                    time.sleep(delay_sec)
        raise AssertionError("Futures public retry loop must return or raise")

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
        return {
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


class FuturesPrivateClient:
    """Authenticated USD-M Futures adapter. Paper cannot construct it."""

    def __init__(self, config: ClientConfig | None = None) -> None:
        self.config = config or ClientConfig.from_env()
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
        normalized_side = side.upper().strip()
        normalized_type = order_type.upper().strip()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError("unsupported Binance order side")
        if normalized_type not in {"MARKET", "LIMIT"}:
            raise ValueError("unsupported Binance order type")
        if quantity is None:
            raise ValueError("Futures orders require quantity")
        params: dict[str, Any] = {
            "symbol": symbol.upper(),
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
        return self._order(
            "DELETE",
            "/fapi/v1/order",
            operation="cancel_order",
            params={
                "symbol": symbol.upper(),
                "orderId": int(order_id) if order_id is not None else None,
                "origClientOrderId": client_order_id,
            },
        )

    def cancel_all_orders(self, symbol: str) -> dict[str, Any]:
        return self._order(
            "DELETE",
            "/fapi/v1/allOpenOrders",
            operation="cancel_all_orders",
            params={"symbol": symbol.upper()},
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
