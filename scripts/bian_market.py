#!/usr/bin/env python3
"""Public Binance market collector for the independent bian bot."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
import random
import sys
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse
import base64
import socket

# Running this owner as ``python scripts/bian_market.py`` does not place the
# repository root on sys.path. Keep root-owned contracts importable in both
# module and script execution modes.
PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


try:
    from database import configured_dsn, ensure_schema, record_collection_failure
except ModuleNotFoundError:
    from scripts.database import configured_dsn, ensure_schema, record_collection_failure

# Historical/research Spot REST hosts only. Futures runtime observation
# uses FUTURES_HOSTS. Do not call SPOT_HOSTS from execution, risk, or gate.
SPOT_HOSTS = (
    "https://api.binance.com/api/v3",
    "https://data-api.binance.vision/api/v3",
)
FUTURES_HOSTS = ("https://fapi.binance.com/fapi/v1",)
PRODUCT_ENDPOINTS = {
    "spot": ("https://api.binance.com/api/v3", "/exchangeInfo", "symbols"),
    "perpetual": ("https://fapi.binance.com/fapi/v1", "/exchangeInfo", "symbols"),
    "delivery": ("https://dapi.binance.com/dapi/v1", "/exchangeInfo", "symbols"),
    "options": ("https://eapi.binance.com/eapi/v1", "/exchangeInfo", "optionSymbols"),
}
USER_AGENT = "bian-market/1.1 (+public-read-only)"
LOGGER = logging.getLogger(__name__)
STREAM_STATES = frozenset({"CONNECTING", "LIVE", "DISCONNECTED", "RECONNECTING", "FAILED"})
CHANNEL_STATES = frozenset(
    {"STARTING", "LIVE", "STALE", "RECONNECTING", "FAILED", "STOPPED"}
)
FUTURES_LIVE_PUBLIC_WS = "wss://fstream.binance.com/stream"
FUTURES_TESTNET_PUBLIC_WS = "wss://stream.binancefuture.com/stream"
RECONNECT_BACKOFF_SEC = (1, 2, 4, 8, 16, 30, 60)
FUTURES_CHANNEL_STREAMS = {
    "TRADE": "trade",
    "BOOK_TICKER": "bookTicker",
    "DEPTH": "depth",
    "MARK_PRICE": "markPrice",
    "LIQUIDATION": "forceOrder",
}
FUTURES_CHANNEL_SOURCES = {
    "TRADE": "futures_trade_flow",
    "BOOK_TICKER": "futures_book_ticker",
    "DEPTH": "futures_orderbook",
    "MARK_PRICE": "futures_mark_price",
    "LIQUIDATION": "futures_liquidation",
}
_HTTP_OPENER: Any | None = None
_HTTP_OPENER_PROXY: str | None = None


class _DefaultHTTPTransport:
    """Keep the stdlib urlopen seam while using the process proxy policy."""

    @staticmethod
    def open(request: urllib.request.Request, *, timeout: float) -> Any:
        return urllib.request.urlopen(request, timeout=timeout)


@dataclass
class StreamHealth:
    """Operational counters for one public websocket stream."""

    state: str = "CONNECTING"
    connection_attempts: int = 0
    connected_at: datetime | None = None
    last_message_at: datetime | None = None
    last_error_at: datetime | None = None
    reconnect_count: int = 0
    consecutive_failures: int = 0
    last_disconnect_at: datetime | None = None
    last_error: str | None = None
    transport_latency_ms: int | None = None
    proxy_mode: str = "DIRECT"

    def __post_init__(self) -> None:
        if self.state not in STREAM_STATES:
            raise ValueError(f"invalid stream state: {self.state}")
        self.proxy_mode = "CONFIGURED" if _http_proxy() else "DIRECT"

    def mark_connected(self, now: datetime) -> None:
        self.connected_at = self.connected_at or now
        self.state = "LIVE"
        self.consecutive_failures = 0

    def mark_message(self, now: datetime, latency_ms: int | None = None) -> None:
        self.mark_connected(now)
        self.last_message_at = now
        if latency_ms is not None:
            self.transport_latency_ms = max(0, int(latency_ms))

    def mark_disconnected(self, error: Exception | str) -> None:
        now = datetime.now(timezone.utc)
        self.state = "DISCONNECTED"
        self.last_disconnect_at = now
        self.last_error_at = now
        self.last_error = str(error)
        self.consecutive_failures += 1

    def mark_reconnecting(self) -> None:
        self.state = "RECONNECTING"
        self.reconnect_count += 1


@dataclass
class ChannelSession:
    """Independent Futures public websocket session for one channel."""

    channel: str
    symbols: tuple[str, ...]
    state: str = "STARTING"
    connected_at: datetime | None = None
    last_message_at: datetime | None = None
    last_error_at: datetime | None = None
    last_disconnect_at: datetime | None = None
    reconnect_count: int = 0
    consecutive_failures: int = 0
    message_count: int = 0
    last_event_timestamp: datetime | None = None
    last_latency_ms: int | None = None
    stale_count: int = 0
    last_error: str | None = None
    last_error_class: str | None = None
    time_to_recover_ms: int | None = None
    proxy_mode: str = "DIRECT"
    subscribed: bool = False
    connection_attempts: int = 0
    connection_id: str | None = None

    def __post_init__(self) -> None:
        self.channel = str(self.channel).upper()
        if self.channel not in FUTURES_CHANNEL_STREAMS:
            raise ValueError(f"invalid futures channel: {self.channel}")
        if self.state not in CHANNEL_STATES:
            raise ValueError(f"invalid channel state: {self.state}")
        self.symbols = tuple(dict.fromkeys(self.symbols))
        self.proxy_mode = "CONFIGURED" if _http_proxy() else "DIRECT"

    def as_health(self) -> dict[str, Any]:
        return {
            "channel": self.channel,
            "state": self.state,
            "connected": self.state == "LIVE" and self.subscribed,
            "last_message_at": (
                self.last_message_at.isoformat() if self.last_message_at else None
            ),
            "message_count": self.message_count,
            "reconnect_count": self.reconnect_count,
            "stale_count": self.stale_count,
            "error": self.last_error,
            "last_error_class": self.last_error_class,
            "time_to_recover_ms": self.time_to_recover_ms,
            "proxy_mode": self.proxy_mode,
            "consecutive_failures": self.consecutive_failures,
            "connection_id": self.connection_id,
        }


def _get_http_opener() -> Any:
    """Return the one public REST opener used by every collector request."""
    global _HTTP_OPENER, _HTTP_OPENER_PROXY
    # urllib.request.urlopen already honors standard proxy environment
    # variables. Build an explicit opener only for BIAN_HTTP_PROXY so the
    # existing transport seam remains observable and both paths share policy.
    proxy = os.environ.get("BIAN_HTTP_PROXY")
    if _HTTP_OPENER is None or _HTTP_OPENER_PROXY != proxy:
        _HTTP_OPENER = (
            urllib.request.build_opener(
                urllib.request.ProxyHandler({"http": proxy, "https": proxy})
            )
            if proxy
            else _DefaultHTTPTransport()
        )
        _HTTP_OPENER_PROXY = proxy
    return _HTTP_OPENER


def reset_http_opener() -> Any:
    """Drop cached REST opener after SSL/connection reset."""
    global _HTTP_OPENER, _HTTP_OPENER_PROXY
    _HTTP_OPENER = None
    _HTTP_OPENER_PROXY = None
    return _get_http_opener()


def _int_env(name: str, default: int, *, minimum: int = 1) -> int:
    try:
        value = int(os.environ.get(name, default))
    except ValueError:
        return default
    return max(minimum, value)


def _float_env(name: str, default: float, *, minimum: float) -> float:
    try:
        value = float(os.environ.get(name, default))
    except ValueError:
        return default
    return max(minimum, value)


def _http_proxy() -> str | None:
    """Use the configured egress proxy for public REST snapshots when needed."""
    return (
        os.environ.get("BIAN_HTTP_PROXY")
        or os.environ.get("HTTPS_PROXY")
        or os.environ.get("https_proxy")
        or os.environ.get("HTTP_PROXY")
        or os.environ.get("http_proxy")
    )


def _proxy_tunnel_socket(
    host: str,
    port: int,
    proxy_url: str,
    timeout: float = 10.0,
) -> socket.socket:
    """Open a TCP tunnel through an HTTP CONNECT proxy.

    websockets' native HTTP proxy handshake times out here even when CONNECT
    plus TLS succeeds, so the tunnel is created explicitly and handed over.
    """
    parsed = urlparse(proxy_url)
    if not parsed.hostname:
        raise OSError("invalid_proxy")
    proxy_port = parsed.port or (443 if parsed.scheme == "https" else 80)
    sock = socket.create_connection((parsed.hostname, proxy_port), timeout=timeout)
    try:
        target = f"{host}:{port}"
        headers = [
            f"CONNECT {target} HTTP/1.1",
            f"Host: {target}",
        ]
        if parsed.username is not None:
            token = base64.b64encode(
                f"{parsed.username}:{parsed.password or ''}".encode()
            ).decode()
            headers.append(f"Proxy-Authorization: Basic {token}")
        sock.sendall(("\r\n".join(headers) + "\r\n\r\n").encode())
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
        status = data.split(b"\r\n", 1)[0]
        if b" 200 " not in status:
            raise OSError("proxy_connect_failed")
        sock.settimeout(None)
        return sock
    except Exception:
        sock.close()
        raise


def _get_json(
    url: str,
    timeout_sec: float = 8.0,
    attempts: int | None = None,
) -> Any:
    """Fetch JSON with bounded retry for transient public API failures."""
    attempts = max(1, attempts or _int_env("BIAN_HTTP_ATTEMPTS", 3))
    backoff_sec = _float_env("BIAN_HTTP_BACKOFF_SEC", 0.25, minimum=0.0)
    max_backoff_sec = _float_env(
        "BIAN_HTTP_MAX_BACKOFF_SEC", 8.0, minimum=0.0
    )
    parsed = urlparse(url)
    operation = parsed.path.rsplit("/", 1)[-1] or "request"
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    from binance_client import BinanceError, execute_rest_request

    try:
        return execute_rest_request(
            _get_http_opener(),
            request,
            operation=operation,
            attempts=attempts,
            timeout_sec=timeout_sec,
            backoff_sec=backoff_sec,
            max_backoff_sec=max_backoff_sec,
            logger=LOGGER,
            on_connection_error=reset_http_opener,
        )
    except BinanceError as exc:
        raise RuntimeError(f"request failed for {url}: {exc}") from exc


def _decimal(value: Any) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _sign(value: Decimal | None) -> int:
    if value is None:
        return 0
    return 1 if value > 0 else -1 if value < 0 else 0


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _event_datetime(value: Any) -> datetime:
    """Normalize seconds, milliseconds, and ISO timestamps to UTC."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float, Decimal)):
        number = float(value)
        if number > 10_000_000_000:
            number /= 1000
        return datetime.fromtimestamp(number, tz=timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)



def _native_event_id(*parts: object) -> str:
    """Deterministic UUID identity derived from a native exchange key."""
    identity = ":".join("" if part is None else str(part) for part in parts)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"bian:{identity}"))


def _latency_ms(source_timestamp: Any, received_timestamp: Any) -> int:
    delta = _event_datetime(received_timestamp) - _event_datetime(source_timestamp)
    latency = int(delta.total_seconds() * 1000)
    if latency < 0:
        raise ValueError("received timestamp precedes source timestamp")
    return latency


def _persistence_error_code(error: Exception) -> str:
    """Keep collection failure codes queryable without persisting exception text."""
    return f"persist_{type(error).__name__.lower()}"[:80]


def _collection_error_code(error: Exception) -> str:
    """Record the failure class without putting exception text into PostgreSQL."""
    return f"collection_{type(error).__name__.lower()}"[:80]


def _market_data_envelope_type() -> type:
    """Import the root-level decision contract when this file runs as a script."""
    try:
        from engine import MarketDataEnvelope
    except ModuleNotFoundError as exc:
        if exc.name != "engine":
            raise
        project_root = str(Path(__file__).resolve().parents[1])
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        from engine import MarketDataEnvelope
    return MarketDataEnvelope


def _normalize_market_data_event(
    event: dict[str, Any], *, default_source: str
) -> dict[str, Any]:
    """Validate one persisted observation against the shared data envelope."""
    envelope = _market_data_envelope_type().create(
        source=str(event.get("source") or default_source),
        market=str(event.get("market") or "UNKNOWN").upper(),
        symbol=str(event["symbol"]),
        event_type=str(event["event_type"]),
        source_timestamp=_event_datetime(event["source_timestamp"]),
        received_timestamp=_event_datetime(event["received_timestamp"]),
        payload=event,
    )
    normalized = dict(event)
    normalized.update(
        {
            "source": envelope.source,
            "market": envelope.market,
            "symbol": envelope.symbol,
            "event_type": envelope.event_type,
            "source_timestamp": envelope.source_timestamp.isoformat(),
            "received_timestamp": envelope.received_timestamp.isoformat(),
            "latency_ms": envelope.latency_ms,
        }
    )
    return normalized


@dataclass(frozen=True)
class TradeFlowAggregate:
    buy_volume: Decimal = Decimal("0")
    sell_volume: Decimal = Decimal("0")
    cvd: Decimal = Decimal("0")
    buy_notional: Decimal = Decimal("0")
    sell_notional: Decimal = Decimal("0")
    delta_notional: Decimal = Decimal("0")
    trade_count: int = 0
    bucket: str = "all"

    @property
    def net_flow(self) -> Decimal:
        return self.buy_volume - self.sell_volume

    @property
    def buy_sell_ratio(self) -> Decimal:
        return self.buy_volume / self.sell_volume if self.sell_volume else Decimal("0")


def aggregate_trade_flow(
    events: list[dict[str, Any]], *, as_of: datetime | None = None
) -> TradeFlowAggregate:
    """Aggregate only received trade events at or before the decision time."""
    cutoff = _event_datetime(as_of) if as_of is not None else None
    buy = Decimal("0")
    sell = Decimal("0")
    buy_notional = Decimal("0")
    sell_notional = Decimal("0")
    trade_count = 0
    for event in sorted(events, key=lambda item: _event_datetime(item["event_timestamp"])):
        event_time = _event_datetime(event["event_timestamp"])
        received_time = _event_datetime(event.get("received_timestamp", event_time))
        if cutoff is not None and (event_time > cutoff or received_time > cutoff):
            continue
        trade_count += 1
        quantity = _decimal(event.get("quantity")) or Decimal("0")
        notional = _event_notional(event)
        metadata = event.get("metadata") or {}
        embedded = metadata.get("metadata") if isinstance(metadata, dict) else None
        buyer_maker = event.get(
            "buyer_maker",
            metadata.get("buyer_maker", embedded.get("buyer_maker") if isinstance(embedded, dict) else None),
        )
        if bool(buyer_maker):
            sell += quantity
            sell_notional += notional
        else:
            buy += quantity
            buy_notional += notional
    return TradeFlowAggregate(
        buy_volume=buy,
        sell_volume=sell,
        cvd=buy - sell,
        buy_notional=buy_notional,
        sell_notional=sell_notional,
        delta_notional=buy_notional - sell_notional,
        trade_count=trade_count,
    )


FLOW_WINDOWS: tuple[tuple[str, int], ...] = (
    ("1s", 1), ("5s", 5), ("15s", 15),
    ("1m", 60), ("3m", 180), ("5m", 300), ("15m", 900), ("30m", 1800), ("1h", 3600),
)
NATIVE_FUTURES_PERIODS: tuple[str, ...] = ("5m", "15m", "30m", "1h")
FUNDING_STATS_MIN_SAMPLES = 8


def _window_events(
    events: list[dict[str, Any]], *, end: datetime, seconds: int
) -> list[dict[str, Any]]:
    start = end.timestamp() - max(1, seconds)
    return [
        event for event in events
        if start < _event_datetime(event["event_timestamp"]).timestamp() <= end.timestamp()
    ]


def _event_notional(event: dict[str, Any]) -> Decimal:
    notional = _decimal(event.get("notional"))
    if notional is not None:
        return abs(notional)
    price = _decimal(event.get("price")) or Decimal("0")
    quantity = _decimal(event.get("quantity")) or Decimal("0")
    return abs(price * quantity)


def _trade_volume(events: list[dict[str, Any]]) -> Decimal:
    return sum((_event_notional(event) for event in events), Decimal("0"))


def _trade_price_change(events: list[dict[str, Any]]) -> Decimal | None:
    if len(events) < 2:
        return None
    first = _decimal(events[0].get("price"))
    last = _decimal(events[-1].get("price"))
    if first is None or last is None or first <= 0:
        return None
    return (last - first) / first


def _window_volume_ratio(
    events: list[dict[str, Any]], *, end: datetime, seconds: int, baseline_windows: int
) -> Decimal | None:
    """Compare the current window with immediately preceding equal windows.

    A missing or zero baseline is intentionally not treated as exceptional
    volume. The decision layer must distinguish absent history from evidence.
    """
    current = _trade_volume(_window_events(events, end=end, seconds=seconds))
    baseline: list[Decimal] = []
    for index in range(1, baseline_windows + 1):
        previous_end = datetime.fromtimestamp(
            end.timestamp() - seconds * index,
            tz=timezone.utc,
        )
        baseline.append(
            _trade_volume(_window_events(events, end=previous_end, seconds=seconds))
        )
    nonzero = [value for value in baseline if value > 0]
    if not nonzero:
        return None
    mean = sum(nonzero, Decimal("0")) / Decimal(len(nonzero))
    return current / mean if mean > 0 else None


def _window_volume_zscore(
    events: list[dict[str, Any]], *, end: datetime, seconds: int, baseline_windows: int
) -> Decimal | None:
    """Return a population z-score for the current completed rolling window."""
    current = _trade_volume(_window_events(events, end=end, seconds=seconds))
    baseline: list[Decimal] = []
    for index in range(1, baseline_windows + 1):
        previous_end = datetime.fromtimestamp(
            end.timestamp() - seconds * index,
            tz=timezone.utc,
        )
        baseline.append(
            _trade_volume(_window_events(events, end=previous_end, seconds=seconds))
        )
    if len(baseline) < 2:
        return None
    mean = sum(baseline, Decimal("0")) / Decimal(len(baseline))
    variance = sum(
        ((value - mean) ** 2 for value in baseline), Decimal("0")
    ) / Decimal(len(baseline))
    if variance <= 0:
        return None
    return (current - mean) / variance.sqrt()


def _window_relative_change(
    events: list[dict[str, Any]],
    *,
    value: Callable[[dict[str, Any]], Decimal | None],
    seconds: int,
) -> Decimal | None:
    """Return a change only when historical sampling can support its window."""
    if len(events) < 2:
        return None
    current = events[-1]
    current_time = _event_datetime(current["event_timestamp"])
    target = current_time.timestamp() - max(1, seconds)
    previous = next(
        (
            event for event in reversed(events[:-1])
            if _event_datetime(event["event_timestamp"]).timestamp() <= target
        ),
        None,
    )
    if previous is None:
        return None
    elapsed = current_time.timestamp() - _event_datetime(previous["event_timestamp"]).timestamp()
    if elapsed > max(2 * seconds, seconds + 60):
        return None
    before, after = value(previous), value(current)
    if before in {None, Decimal("0")} or after is None:
        return None
    return (after - before) / before


def _first_not_none(*values: Decimal | None) -> Decimal | None:
    return next((value for value in values if value is not None), None)


def _rolling_funding_stats(rates: list[Decimal]) -> tuple[Decimal | None, Decimal | None]:
    """Percentile and z-score from persisted funding only; small samples stay absent."""
    if len(rates) < FUNDING_STATS_MIN_SAMPLES:
        return None, None
    current = rates[-1]
    percentile = Decimal(sum(rate <= current for rate in rates)) / Decimal(len(rates))
    mean = sum(rates, Decimal("0")) / Decimal(len(rates))
    variance = sum((rate - mean) ** 2 for rate in rates) / Decimal(len(rates))
    if variance == 0:
        return percentile, Decimal("0")
    return percentile, (current - mean) / variance.sqrt()


def positioning_feature_values(
    events: list[dict[str, Any]],
    *,
    as_of: datetime,
    trade_window_seconds: int = 300,
) -> dict[str, Any]:
    """Build normalized, no-lookahead positioning features from persisted events.

    The result contains only aggregates and timestamp provenance. Missing
    sources intentionally remain ``None`` so the engine can fail closed.
    """
    cutoff = _event_datetime(as_of)
    trade_start = cutoff.timestamp() - max(1, trade_window_seconds)
    visible = [
        event for event in events
        if _event_datetime(event["event_timestamp"]) <= cutoff
        and _event_datetime(event.get("received_timestamp", event["event_timestamp"])) <= cutoff
    ]
    visible.sort(key=lambda item: (
        _event_datetime(item["event_timestamp"]),
        _event_datetime(item.get("received_timestamp", item["event_timestamp"])),
        str(item.get("event_id", "")),
    ))
    deduplicated: list[dict[str, Any]] = []
    seen_event_ids: set[str] = set()
    for event in visible:
        event_id = str(event.get("event_id") or "")
        if event_id and event_id in seen_event_ids:
            continue
        if event_id:
            seen_event_ids.add(event_id)
        deduplicated.append(event)
    visible = deduplicated
    by_type: dict[str, list[dict[str, Any]]] = {}
    for event in visible:
        by_type.setdefault(str(event.get("event_type", "")), []).append(event)

    all_trades = by_type.get("FUTURES_TRADE", []) or by_type.get("TRADE", [])
    trades = [
        event for event in all_trades
        if _event_datetime(event["event_timestamp"]).timestamp() >= trade_start
    ]
    flow = aggregate_trade_flow(trades, as_of=cutoff)
    window_flows = {
        label: aggregate_trade_flow(
            _window_events(all_trades, end=cutoff, seconds=seconds), as_of=cutoff
        )
        for label, seconds in FLOW_WINDOWS
    }
    cvd_windows = {
        label: aggregate.cvd if _window_events(all_trades, end=cutoff, seconds=seconds) else None
        for (label, seconds), aggregate in zip(FLOW_WINDOWS, window_flows.values())
    }
    current_one_minute = cvd_windows["1m"]
    previous_one_minute_events = _window_events(
        all_trades,
        end=datetime.fromtimestamp(cutoff.timestamp() - 60, tz=timezone.utc),
        seconds=60,
    )
    previous_one_minute = aggregate_trade_flow(
        previous_one_minute_events, as_of=cutoff
    ).cvd
    cvd_acceleration = (
        current_one_minute - previous_one_minute
        if current_one_minute is not None and previous_one_minute_events
        else None
    )
    price_change = _trade_price_change(trades)
    cvd_5m = cvd_windows["5m"]
    price_cvd_divergence = (
        _sign(price_change) != _sign(cvd_5m)
        if price_change is not None and cvd_5m is not None and _sign(price_change) and _sign(cvd_5m)
        else None
    )
    volume_ratio_1m = _window_volume_ratio(
        all_trades, end=cutoff, seconds=60, baseline_windows=5
    )
    volume_ratio_5m = _window_volume_ratio(
        all_trades, end=cutoff, seconds=300, baseline_windows=3
    )
    volume_ratio_15m = _window_volume_ratio(
        all_trades, end=cutoff, seconds=900, baseline_windows=3
    )
    volume_zscore = _window_volume_zscore(
        all_trades, end=cutoff, seconds=60, baseline_windows=5
    )

    def latest(event_type: str) -> dict[str, Any] | None:
        rows = by_type.get(event_type, [])
        return rows[-1] if rows else None

    def metadata(event: dict[str, Any] | None) -> dict[str, Any]:
        stored = dict(event.get("metadata") or {}) if event else {}
        nested = stored.get("metadata")
        return dict(nested) if isinstance(nested, dict) else stored

    def decimal_metadata(event: dict[str, Any] | None, key: str) -> Decimal | None:
        return _decimal(metadata(event).get(key)) if event else None

    def latest_change(event_type: str, value: Callable[[dict[str, Any]], Decimal | None]) -> Decimal | None:
        rows = by_type.get(event_type, [])
        if len(rows) < 2:
            return None
        previous, current = value(rows[-2]), value(rows[-1])
        if previous in {None, Decimal("0")} or current is None:
            return None
        return (current - previous) / previous

    def latest_period(event_type: str, period: str) -> dict[str, Any] | None:
        return next(
            (
                event for event in reversed(by_type.get(event_type, []))
                if str(metadata(event).get("observationPeriod", "")) == period
            ),
            None,
        )

    def oi_change(seconds: int) -> Decimal | None:
        return _window_relative_change(
            by_type.get("OPEN_INTEREST", []),
            value=lambda event: _decimal(event.get("quantity")),
            seconds=seconds,
        )

    def counter_delta(key: str) -> Decimal | None:
        rows = by_type.get("ORDERBOOK", [])
        if not rows:
            return None
        current = _decimal(metadata(rows[-1]).get(key))
        if current is None:
            return None
        if len(rows) == 1:
            return current
        previous = _decimal(metadata(rows[-2]).get(key))
        if previous is None or current < previous:
            return current
        return current - previous

    oi_event = latest("OPEN_INTEREST")
    oi_value = _decimal(oi_event.get("quantity")) if oi_event else None
    mark_event = latest("MARK_INDEX_FUNDING")
    taker_event = latest("TAKER_RATIO")
    taker_5m_event = latest_period("TAKER_RATIO", "5m") or taker_event
    funding_event = latest("FUNDING")
    last_event = latest("LAST_PRICE")
    taker_30m_event = latest_period("TAKER_RATIO", "30m")
    mark_funding_rates = [
        rate for event in by_type.get("MARK_INDEX_FUNDING", [])
        if (rate := decimal_metadata(event, "lastFundingRate")) is not None
    ]
    settlement_funding_rates = [
        rate for event in by_type.get("FUNDING", [])
        if (rate := decimal_metadata(event, "fundingRate")) is not None
    ]
    funding_percentile, funding_zscore = _rolling_funding_stats(
        mark_funding_rates or settlement_funding_rates
    )
    global_long_short_event = latest_period("GLOBAL_LONG_SHORT", "5m") or latest("GLOBAL_LONG_SHORT")
    top_trader_long_short_event = latest_period("TOP_TRADER_LONG_SHORT", "5m") or latest("TOP_TRADER_LONG_SHORT")
    book_event = latest("BOOK_TICKER")
    orderbook_event = latest("ORDERBOOK")
    book_meta = metadata(book_event)
    orderbook_meta = metadata(orderbook_event)
    liquidation_events = [
        event for event in by_type.get("FORCE_ORDER", [])
        if _event_datetime(event["event_timestamp"]).timestamp() >= trade_start
    ]
    long_liquidations = sum(
        (_decimal(event.get("notional")) or Decimal("0"))
        for event in liquidation_events
        if str(event.get("direction", "")).upper() == "SELL"
    )
    short_liquidations = sum(
        (_decimal(event.get("notional")) or Decimal("0"))
        for event in liquidation_events
        if str(event.get("direction", "")).upper() == "BUY"
    )
    previous_liquidations = [
        event for event in by_type.get("FORCE_ORDER", [])
        if trade_start - trade_window_seconds
        <= _event_datetime(event["event_timestamp"]).timestamp() < trade_start
    ]
    liquidation_acceleration = None
    if liquidation_events:
        previous_notional = sum(
            (_decimal(event.get("notional")) or Decimal("0"))
            for event in previous_liquidations
        )
        current_notional = long_liquidations + short_liquidations
        liquidation_acceleration = (
            current_notional / previous_notional
            if previous_notional > 0 else Decimal("1")
        )

    timestamp_sources: dict[str, dict[str, Any]] = {}
    source_events = {
        "futures_trade": latest("FUTURES_TRADE"),
        "futures_trade_flow": latest("FUTURES_TRADE"),
        "spot_trade": latest("TRADE"),
        "futures_open_interest": oi_event,
        "futures_funding": mark_event if decimal_metadata(mark_event, "lastFundingRate") is not None else funding_event,
        "futures_taker_ratio": taker_event,
        "futures_global_long_short": global_long_short_event,
        "futures_top_trader_long_short": top_trader_long_short_event,
        "futures_book_ticker": book_event,
        "futures_orderbook": orderbook_event,
        "futures_mark_price": mark_event,
        "futures_index_price": mark_event,
        "futures_last_price": latest("LAST_PRICE"),
        # Liquidations are sparse. A historical force order outside the
        # current aggregation window is not proof that the source is stale;
        # it contributes no current liquidation evidence and must not poison
        # freshness for every later decision.
        "futures_liquidation": liquidation_events[-1] if liquidation_events else None,
    }
    for source, event in source_events.items():
        if event is None:
            continue
        timestamp_sources[source] = {
            "source_timestamp": _event_datetime(event["event_timestamp"]).isoformat(),
            "received_timestamp": _event_datetime(
                event.get("received_timestamp", event["event_timestamp"])
            ).isoformat(),
            "latency_ms": int(event.get("latency_ms", 0)),
        }

    return {
        "futures_buy_volume": flow.buy_volume if trades else None,
        "futures_sell_volume": flow.sell_volume if trades else None,
        "futures_trade_flow": flow.net_flow if trades else None,
        "futures_buy_notional": flow.buy_notional if trades else None,
        "futures_sell_notional": flow.sell_notional if trades else None,
        "futures_delta_notional": flow.delta_notional if trades else None,
        "notional_cvd": flow.delta_notional if trades else None,
        "spot_buy_volume": (
            aggregate_trade_flow(by_type.get("TRADE", []), as_of=cutoff).buy_volume
            if by_type.get("TRADE") else None
        ),
        "spot_sell_volume": (
            aggregate_trade_flow(by_type.get("TRADE", []), as_of=cutoff).sell_volume
            if by_type.get("TRADE") else None
        ),
        "net_spot_flow": (
            aggregate_trade_flow(by_type.get("TRADE", []), as_of=cutoff).net_flow
            if by_type.get("TRADE") else None
        ),
        "cvd": flow.cvd if trades else None,
        "cvd_change": cvd_5m,
        "cvd_1m": cvd_windows["1m"],
        "cvd_3m": cvd_windows["3m"],
        "cvd_5m": cvd_5m,
        "cvd_15m": cvd_windows["15m"],
        "cvd_30m": cvd_windows["30m"],
        "cvd_1h": cvd_windows["1h"],
        "cvd_acceleration": cvd_acceleration,
        "price_cvd_divergence": price_cvd_divergence,
        "volume_ratio_1m": volume_ratio_1m,
        "volume_ratio_5m": volume_ratio_5m,
        "volume_ratio_15m": volume_ratio_15m,
        "volume_zscore": volume_zscore,
        "last_price": _first_not_none(
            _decimal(last_event.get("price")) if last_event else None,
            decimal_metadata(last_event, "price"),
        ),
        "mark_price": decimal_metadata(mark_event, "markPrice"),
        "index_price": decimal_metadata(mark_event, "indexPrice"),
        "oi": oi_value,
        "oi_change": _first_not_none(oi_change(300), latest_change(
            "OPEN_INTEREST", lambda event: _decimal(event.get("quantity"))
        )),
        "oi_change_1m": oi_change(60),
        "oi_change_3m": oi_change(180),
        "oi_change_5m": oi_change(300),
        "oi_change_15m": oi_change(900),
        "oi_change_1h": oi_change(3600),
        "oi_change_30m": oi_change(1800),
        "funding_rate": _first_not_none(
            decimal_metadata(mark_event, "lastFundingRate"),
            decimal_metadata(funding_event, "fundingRate"),
        ),
        "funding_timestamp": (
            _event_datetime(funding_event["event_timestamp"])
            if funding_event is not None
            else (
                _event_datetime(mark_event["event_timestamp"])
                if mark_event is not None
                else None
            )
        ),
        "funding_settlement_timestamp": (
            _event_datetime(funding_event["event_timestamp"])
            if funding_event is not None
            else None
        ),
        "funding_change": _first_not_none(
            latest_change(
                "MARK_INDEX_FUNDING",
                lambda event: decimal_metadata(event, "lastFundingRate"),
            ),
            latest_change(
                "FUNDING", lambda event: decimal_metadata(event, "fundingRate")
            ),
        ),
        "funding_percentile": funding_percentile,
        "funding_zscore": funding_zscore,
        "taker_ratio": (
            decimal_metadata(taker_5m_event, "buyVol")
            / decimal_metadata(taker_5m_event, "sellVol")
            if decimal_metadata(taker_5m_event, "buyVol") is not None
            and decimal_metadata(taker_5m_event, "sellVol") not in {None, Decimal("0")}
            else None
        ),
        "basis_bps": decimal_metadata(mark_event, "basisBps"),
        "taker_buy_volume": decimal_metadata(taker_5m_event, "buyVol"),
        "taker_sell_volume": decimal_metadata(taker_5m_event, "sellVol"),
        "taker_buy_volume_30m": decimal_metadata(taker_30m_event, "buyVol"),
        "taker_sell_volume_30m": decimal_metadata(taker_30m_event, "sellVol"),
        "global_long_short_ratio": decimal_metadata(global_long_short_event, "longShortRatio"),
        "top_trader_long_short_ratio": decimal_metadata(top_trader_long_short_event, "longShortRatio"),
        "bid_price": _decimal(book_meta.get("bidPrice")),
        "ask_price": _decimal(book_meta.get("askPrice")),
        "spread_bps": decimal_metadata(book_event, "spreadBps"),
        "depth_25bps": _decimal(orderbook_meta.get("depth_25bps")),
        "bid_depth_5": _decimal(orderbook_meta.get("bid_depth_5")),
        "ask_depth_5": _decimal(orderbook_meta.get("ask_depth_5")),
        "bid_depth_10": _decimal(orderbook_meta.get("bid_depth_10")),
        "ask_depth_10": _decimal(orderbook_meta.get("ask_depth_10")),
        "bid_depth_20": _decimal(orderbook_meta.get("bid_depth_20")),
        "ask_depth_20": _decimal(orderbook_meta.get("ask_depth_20")),
        "depth_10bps": _decimal(orderbook_meta.get("depth_10bps")),
        "depth_50bps": _decimal(orderbook_meta.get("depth_50bps")),
        "price_impact_buy": (
            max(Decimal("0"), price_change)
            if price_change is not None and flow.net_flow > 0 else None
        ),
        "price_impact_sell": (
            max(Decimal("0"), -price_change)
            if price_change is not None and flow.net_flow < 0 else None
        ),
        "liquidity_added": counter_delta("liquidity_added"),
        "liquidity_removed": counter_delta("liquidity_removed"),
        "observed_short_liquidation_notional": short_liquidations if liquidation_events else None,
        "observed_long_liquidation_notional": long_liquidations if liquidation_events else None,
        "observed_liquidation_notional": (
            long_liquidations + short_liquidations if liquidation_events else None
        ),
        "liquidation_observed": bool(liquidation_events),
        "liquidation_acceleration": liquidation_acceleration,
        "source_timestamps": timestamp_sources,
    }


class OrderBookGap(RuntimeError):
    """Raised when a Binance diff-depth update is not contiguous."""


@dataclass
class LocalOrderBook:
    """Single local book state machine with fail-closed sequence handling."""

    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    last_update_id: int | None = None
    liquidity_added: Decimal = Decimal("0")
    liquidity_removed: Decimal = Decimal("0")
    snapshot_update_id: int | None = None
    snapshot_sync_origin: str = "UNKNOWN"
    snapshot_received_timestamp: datetime | None = None
    state: str = "UNINITIALIZED"
    received_U: int | None = None
    received_u: int | None = None
    expected_next: int | None = None
    error_class: str | None = None
    recovery_attempt: int = 0

    @classmethod
    def from_snapshot(
        cls,
        payload: dict[str, Any],
        *,
        sync_origin: str = "STREAM_SNAPSHOT",
        received_at: datetime | None = None,
    ) -> "LocalOrderBook":
        update_id = int(payload["lastUpdateId"])
        return cls(
            bids={Decimal(str(price)): Decimal(str(quantity)) for price, quantity in payload.get("bids", [])},
            asks={Decimal(str(price)): Decimal(str(quantity)) for price, quantity in payload.get("asks", [])},
            last_update_id=update_id,
            snapshot_update_id=update_id,
            snapshot_sync_origin=sync_origin,
            snapshot_received_timestamp=received_at,
            state="SYNCING",
            expected_next=update_id + 1,
        )

    @classmethod
    def synchronize(
        cls,
        snapshot: dict[str, Any],
        buffered_events: list[dict[str, Any]],
    ) -> "LocalOrderBook":
        """Apply buffered Binance diffs after a REST snapshot.

        Events ending before the snapshot are discarded. The first applicable
        event must bridge ``lastUpdateId + 1``; a gap raises and callers must
        discard this instance and obtain a new snapshot.
        """
        book = cls.from_snapshot(snapshot, sync_origin="REST_SNAPSHOT")
        applicable = [
            event for event in buffered_events
            if int(event["u"]) > (book.last_update_id or 0)
        ]
        if not applicable:
            return book
        first = applicable[0]
        if not (
            int(first["U"]) <= (book.last_update_id or 0) + 1
            <= int(first["u"])
        ):
            book._invalidate(first, "OrderBookGap")
            raise OrderBookGap("buffered depth events do not bridge snapshot")
        for event in applicable:
            book.apply_diff(event)
        return book

    def apply_diff(self, event: dict[str, Any]) -> bool:
        first = int(event["U"])
        last = int(event["u"])
        if self.last_update_id is None:
            self._invalidate(event, "OrderBookGap", state="UNSAFE")
            raise OrderBookGap("book must be initialized from a REST snapshot")
        if last <= self.last_update_id:
            return False
        previous_final = event.get("pu")
        if previous_final is not None:
            if self.state == "VALID" and int(previous_final) != self.last_update_id:
                self._invalidate(event, "OrderBookGap")
                raise OrderBookGap(
                    f"depth bridge gap: expected pu={self.last_update_id}, got {previous_final}"
                )
        elif first > self.last_update_id + 1:
            self._invalidate(event, "OrderBookGap")
            raise OrderBookGap(
                f"depth gap: expected {self.last_update_id + 1}, got {first}"
            )
        for side, target in (("b", self.bids), ("a", self.asks)):
            for price_text, quantity_text in event.get(side, []):
                price = Decimal(str(price_text))
                quantity = Decimal(str(quantity_text))
                previous = target.get(price, Decimal("0"))
                if quantity == 0:
                    target.pop(price, None)
                else:
                    target[price] = quantity
                delta = quantity - previous
                if delta > 0:
                    self.liquidity_added += delta
                elif delta < 0:
                    self.liquidity_removed += abs(delta)
        self.last_update_id = last
        self.received_U = first
        self.received_u = last
        self.expected_next = last + 1
        self.state = "VALID"
        self.error_class = None
        return True

    def _invalidate(
        self,
        event: dict[str, Any],
        error_class: str,
        *,
        state: str = "GAP",
    ) -> None:
        """Discard stale depth immediately; no old feature can be reused."""
        self.bids.clear()
        self.asks.clear()
        self.state = state
        self.received_U = int(event.get("U", 0))
        self.received_u = int(event.get("u", 0))
        self.expected_next = (
            self.last_update_id + 1 if self.last_update_id is not None else None
        )
        self.error_class = error_class

    def features(self, mid_price: Decimal | None = None) -> dict[str, Decimal]:
        if self.state != "VALID" or not self.bids or not self.asks:
            return {}
        bids = sorted(self.bids.items(), reverse=True)
        asks = sorted(self.asks.items())
        best_bid, best_ask = bids[0][0], asks[0][0]
        mid = mid_price or ((best_bid + best_ask) / Decimal("2"))

        def depth(rows: list[tuple[Decimal, Decimal]], count: int) -> Decimal:
            return sum((quantity for _, quantity in rows[:count]), Decimal("0"))

        def within(rows: list[tuple[Decimal, Decimal]], bps: Decimal) -> Decimal:
            return sum(
                (quantity for price, quantity in rows
                 if abs(price - mid) / mid * Decimal("10000") <= bps),
                Decimal("0"),
            )

        spread_bps = (best_ask - best_bid) / mid * Decimal("10000") if mid else Decimal("0")
        bid10, ask10 = depth(bids, 10), depth(asks, 10)
        bid5, ask5 = depth(bids, 5), depth(asks, 5)
        bid20, ask20 = depth(bids, 20), depth(asks, 20)
        return {
            "bid_depth_5": bid5, "ask_depth_5": ask5,
            "bid_depth_10": bid10, "ask_depth_10": ask10,
            "bid_depth_20": bid20, "ask_depth_20": ask20,
            "imbalance_5": (bid5 - ask5) / (bid5 + ask5) if bid5 + ask5 else Decimal("0"),
            "imbalance_10": (bid10 - ask10) / (bid10 + ask10) if bid10 + ask10 else Decimal("0"),
            "imbalance_20": (bid20 - ask20) / (bid20 + ask20) if bid20 + ask20 else Decimal("0"),
            "spread_bps": spread_bps,
            "depth_10bps": within(bids, Decimal("10")) + within(asks, Decimal("10")),
            "depth_25bps": within(bids, Decimal("25")) + within(asks, Decimal("25")),
            "depth_50bps": within(bids, Decimal("50")) + within(asks, Decimal("50")),
            "liquidity_added": self.liquidity_added,
            "liquidity_removed": self.liquidity_removed,
        }


def _depth_snapshot(
    symbol: str, *, market: str = "SPOT", limit: int = 1000
) -> dict[str, Any]:
    """Fetch the Binance REST depth snapshot used to initialize a local book."""
    normalized_symbol = (
        _storage_symbol(symbol)
        if market.upper() == "FUTURES"
        else symbol.replace("-", "").upper()
    )
    bounded_limit = max(5, min(int(limit), 5000))
    host = SPOT_HOSTS[0] if market.upper() == "SPOT" else FUTURES_HOSTS[0]
    payload = _get_json(f"{host}/depth?symbol={normalized_symbol}&limit={bounded_limit}")
    if not isinstance(payload, dict) or "lastUpdateId" not in payload:
        raise ValueError("Binance depth snapshot is missing lastUpdateId")
    return payload


def _spot_depth_snapshot(symbol: str, *, limit: int = 1000) -> dict[str, Any]:
    return _depth_snapshot(symbol, market="SPOT", limit=limit)


def _resynchronize_local_order_book(
    standard_symbol: str,
    buffered_events: list[dict[str, Any]],
    books: dict[str, LocalOrderBook],
    *,
    market: str = "SPOT",
) -> LocalOrderBook:
    """Discard any stale book and reconstruct it from REST snapshot plus diffs."""
    snapshot = _depth_snapshot(standard_symbol, market=market)
    local_book = LocalOrderBook.synchronize(snapshot, buffered_events)
    local_book.snapshot_received_timestamp = datetime.now(timezone.utc)
    books[standard_symbol] = local_book
    return local_book


def _universe_ticker_rows() -> tuple[list[dict[str, Any]], str]:
    """Return the USD-M Futures universe for scanner aggregation."""
    last_error: Exception | None = None
    for host in FUTURES_HOSTS:
        url = f"{host}/ticker/24hr"
        try:
            payload = _get_json(url)
            rows = [
                row
                for row in payload
                if isinstance(row, dict)
                and str(row.get("symbol") or "").endswith("USDT")
                and _decimal(row.get("lastPrice")) is not None
            ]
            rows.sort(
                key=lambda row: _decimal(row.get("quoteVolume")) or Decimal(),
                reverse=True,
            )
            return rows, url
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"all Binance Futures hosts failed: {last_error}")


def _ticker_rows(limit: int) -> tuple[list[dict[str, Any]], str]:
    rows, source_url = _universe_ticker_rows()
    return rows[:limit], source_url


def classify_market_regime(
    *,
    btc_return_pct: Decimal | None,
    eth_return_pct: Decimal | None,
    breadth_score: Decimal,
) -> str:
    """Classify the current 24h cross-sectional context without prediction."""
    btc = btc_return_pct or Decimal("0")
    eth = eth_return_pct or Decimal("0")
    if abs(btc) >= Decimal("7") or abs(eth) >= Decimal("9"):
        return "HIGH_VOL"
    if abs(btc) <= Decimal("0.35") and abs(eth) <= Decimal("0.5") and abs(breadth_score) <= Decimal("0.1"):
        return "LOW_VOL"
    if btc > Decimal("0.75") and eth > Decimal("0.75") and breadth_score >= Decimal("0.2"):
        return "RISK_ON"
    if btc < Decimal("-0.75") and eth < Decimal("-0.75") and breadth_score <= Decimal("-0.2"):
        return "RISK_OFF"
    if btc > Decimal("1") and breadth_score > Decimal("0"):
        return "TRENDING_UP"
    if btc < Decimal("-1") and breadth_score < Decimal("0"):
        return "TRENDING_DOWN"
    return "NEUTRAL"


def universe_features(rows: list[dict[str, Any]], *, candidate_limit: int) -> dict[str, Any]:
    """Derive bounded Futures universe tiers from public 24h ticker fields.

    These are cross-sectional observations, not a replacement for the
    timestamp-aligned intraday flow features used by positioning decisions.
    First-phase membership is BTCUSDT, ETHUSDT, and BNBUSDT only.
    """
    from risk import classify_futures_risk_tier
    from trade_intent import CANONICAL_FUTURES_SYMBOLS, is_canonical_futures_symbol

    raw_volume_values = [
        volume for row in rows
        if (volume := _decimal(row.get("quoteVolume"))) is not None and volume > 0
    ]
    maximum_quote_volume = max(raw_volume_values, default=Decimal("0"))
    normalized: list[tuple[dict[str, Any], Decimal, Decimal]] = []
    for row in rows:
        price_change = _decimal(row.get("priceChangePercent"))
        quote_volume = _decimal(row.get("quoteVolume"))
        if price_change is None or quote_volume is None:
            continue
        symbol = str(row.get("symbol", "")).upper()
        if symbol.endswith("USDT"):
            contract_type = str(row.get("contractType", "PERPETUAL")).upper()
            quote_asset = str(row.get("quoteAsset", "USDT")).upper()
            trading = str(row.get("status", "TRADING")).upper() == "TRADING"
            reason_codes: list[str] = []
            canonical = is_canonical_futures_symbol(symbol)
            classification_source = (
                "CANONICAL_FUTURES_UNIVERSE" if canonical else "NON_CANONICAL_SYMBOL"
            )
            if not canonical:
                reason_codes.append("NOT_CANONICAL")
            if not trading or contract_type != "PERPETUAL" or quote_asset != "USDT":
                tier = "BLOCK"
                if not trading:
                    reason_codes.append("NOT_TRADING")
                if contract_type != "PERPETUAL":
                    reason_codes.append("NOT_PERPETUAL")
                if quote_asset != "USDT":
                    reason_codes.append("NOT_USDT_QUOTE")
            elif not canonical:
                tier = "BLOCK"
            else:
                liquidity_score = (
                    quote_volume / maximum_quote_volume
                    if maximum_quote_volume > 0 else None
                )
                tier = classify_futures_risk_tier(
                    liquidity_score=liquidity_score,
                    data_quality_score=Decimal("1"),
                    spread_bps=_decimal(row.get("spreadBps")),
                    open_interest=_decimal(row.get("openInterest")),
                    trading=trading,
                )
                if spread_bps := _decimal(row.get("spreadBps")):
                    if spread_bps >= Decimal("50"):
                        reason_codes.append("SPREAD_TOO_WIDE")
                if quote_volume <= 0:
                    reason_codes.append("LIQUIDITY_MISSING")
                if (_decimal(row.get("openInterest")) or Decimal("0")) <= 0:
                    reason_codes.append("OI_NOT_POSITIVE")
                if tier == "TRADEABLE":
                    reason_codes.append("QUALIFIED")
                else:
                    reason_codes.append(tier)
            row["market"] = "FUTURES"
            row["contract_type"] = contract_type
            row["quote_asset"] = quote_asset
            row["base_asset"] = row.get("baseAsset") or symbol[:-4]
            row["volume"] = _decimal(row.get("volume"))
            row["quote_volume"] = quote_volume
            row["last_price"] = _decimal(row.get("lastPrice"))
            row["open_interest"] = _decimal(row.get("openInterest"))
            row["meme_risk_tier"] = tier
            classified_at = datetime.now(timezone.utc).isoformat()
            version = os.environ.get(
                "FUTURES_CLASSIFICATION_VERSION", "major-futures-v1"
            )
            row["is_meme"] = False
            row["meme_classification_source"] = classification_source
            row["meme_classification_version"] = version
            row["meme_classified_at"] = classified_at
            row["meme_reason_codes"] = reason_codes
            row["classification_source"] = classification_source
            row["classification_version"] = version
            row["classified_at"] = classified_at
            row["reason_codes"] = reason_codes
            normalized.append((row, price_change, quote_volume))
    advancers = sum(change > 0 for _, change, _ in normalized)
    decliners = sum(change < 0 for _, change, _ in normalized)
    unchanged = len(normalized) - advancers - decliners
    breadth_score = (
        Decimal(advancers - decliners) / Decimal(len(normalized))
        if normalized else Decimal("0")
    )
    advance_decline_ratio = (
        Decimal(advancers) / Decimal(decliners) if decliners else None
    )
    new_highs = sum(
        (_decimal(row.get("lastPrice")) or Decimal("0"))
        >= (_decimal(row.get("highPrice")) or Decimal("Infinity"))
        for row, _, _ in normalized
    )
    new_lows = sum(
        (_decimal(row.get("lastPrice")) or Decimal("Infinity"))
        <= (_decimal(row.get("lowPrice")) or Decimal("-Infinity"))
        for row, _, _ in normalized
    )
    volumes = sorted(volume for _, _, volume in normalized)
    volume_threshold = volumes[(len(volumes) * 3) // 4] if volumes else Decimal("0")
    volume_expansion_count = sum(volume >= volume_threshold for _, _, volume in normalized)
    by_symbol = {str(row.get("symbol", "")).upper(): change for row, change, _ in normalized}
    regime = classify_market_regime(
        btc_return_pct=by_symbol.get("BTCUSDT"),
        eth_return_pct=by_symbol.get("ETHUSDT"),
        breadth_score=breadth_score,
    )
    major_rows = [
        item for item in normalized
        if str(item[0].get("symbol", "")).upper() in CANONICAL_FUTURES_SYMBOLS
    ]
    candidates = sorted(
        major_rows,
        key=lambda item: (item[2], abs(item[1]), str(item[0].get("symbol", ""))),
        reverse=True,
    )[:max(1, min(candidate_limit, len(CANONICAL_FUTURES_SYMBOLS)))]
    if not candidates:
        candidates = major_rows[: len(CANONICAL_FUTURES_SYMBOLS)]
    tier_rows = {
        tier: [str(row["symbol"]).upper() for row, _, _ in normalized if row.get("meme_risk_tier") == tier]
        for tier in ("BLOCK", "OBSERVE", "REDUCED", "TRADEABLE")
    }
    tradeable_candidates = [
        row for row, _, _ in candidates
        if row.get("meme_risk_tier") in {"TRADEABLE", "REDUCED"}
    ]
    return {
        "market": "FUTURES",
        "observation_window": "24h",
        "universe_size": len(normalized),
        "advancers": advancers,
        "decliners": decliners,
        "unchanged": unchanged,
        "new_highs": new_highs,
        "new_lows": new_lows,
        "volume_expansion_count": volume_expansion_count,
        "volume_expansion_definition": "top_quartile_quote_volume_cross_section",
        "advance_decline_ratio": (
            str(advance_decline_ratio) if advance_decline_ratio is not None else None
        ),
        "breadth_score": str(breadth_score),
        "market_regime": regime,
        "candidate_symbols": [
            str(row["symbol"]).upper() for row, _, _ in candidates
        ],
        "meme_candidate_symbols": [],
        "major_candidate_symbols": [
            str(row["symbol"]).upper() for row in tradeable_candidates
        ],
        "tiers": tier_rows,
        "symbols": [row for row, _, _ in normalized],
    }


def get_klines(
    symbol: str,
    *,
    interval: str = "1m",
    limit: int = 100,
    market: str = "FUTURES",
) -> dict[str, Any]:
    """Fetch public klines through the market owner.

    Futures is the production default; Spot callers must opt in explicitly as
    auxiliary confirmation data.
    """
    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol.endswith("USDT"):
        raise ValueError("strategy symbols must be public USDT pairs")
    bounded_limit = max(1, min(int(limit), 1000))
    last_error: Exception | None = None
    hosts = FUTURES_HOSTS if market.upper() == "FUTURES" else SPOT_HOSTS
    for host in hosts:
        url = (
            f"{host}/klines?symbol={normalized_symbol}"
            f"&interval={interval}&limit={bounded_limit}"
        )
        try:
            payload = _get_json(url)
            if not isinstance(payload, list):
                raise RuntimeError("Binance klines response was not a list")
            received_at = datetime.now(timezone.utc)
            closed = [
                row for row in payload
                if len(row) > 6 and _event_datetime(row[6]) <= received_at
            ]
            if not closed:
                raise RuntimeError("Binance klines response had no closed candles")
            source_timestamp = (
                _event_datetime(closed[-1][6]).isoformat()
            )
            return {
                "symbol": normalized_symbol,
                "interval": interval,
                "limit": bounded_limit,
                "source_url": url,
                "market": market.upper(),
                "klines": closed,
                "captured_at": received_at.isoformat(),
                "source_timestamp": source_timestamp,
                "received_timestamp": received_at.isoformat(),
            }
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"all Binance kline hosts failed: {last_error}")


def _coverage(captured_at: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for product_type, (host, path, field) in PRODUCT_ENDPOINTS.items():
        url = f"{host}{path}"
        try:
            payload = _get_json(url)
            symbols = payload.get(field) if isinstance(payload, dict) else []
            rows.append(
                {
                    "product_type": product_type,
                    "status": "ok",
                    "symbol_count": len(symbols) if isinstance(symbols, list) else 0,
                    "source_url": url,
                    "captured_at": captured_at,
                    "detail": {"symbol_field": field},
                }
            )
        except Exception:
            rows.append(
                {
                    "product_type": product_type,
                    "status": "unavailable",
                    "symbol_count": None,
                    "source_url": url,
                    "captured_at": captured_at,
                    "detail": {"error": "request_failed"},
                }
            )
    return rows


def collect(limit: int = 20, *, run_id: str | None = None) -> dict[str, Any]:
    universe, source_url = _universe_ticker_rows()
    captured_at = _now()
    received_timestamp = _event_datetime(captured_at)
    scanner = universe_features(universe, candidate_limit=limit)
    candidate_symbols = set(scanner["candidate_symbols"])
    candidate_symbols.update({"BTCUSDT", "ETHUSDT", "BNBUSDT"})
    markets = [
        row for row in universe
        if str(row.get("symbol", "")).upper() in candidate_symbols
    ]
    markets = markets[: max(1, limit + 2)]
    breadth_event = {
        "event_id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"bian:universe-breadth:{received_timestamp.isoformat()}",
        )),
        "symbol": "__MARKET__",
        "market": "FUTURES",
        "event_type": "UNIVERSE_BREADTH",
        "source_timestamp": received_timestamp.isoformat(),
        "received_timestamp": received_timestamp.isoformat(),
        "latency_ms": 0,
        "price": None,
        "quantity": None,
        "notional": None,
        "direction": None,
        "metadata": scanner,
    }
    return {
        "run_id": run_id or str(uuid.uuid4()),
        "collection_kind": "rest_24h",
        "captured_at": captured_at,
        "source_url": source_url,
        "markets": [
            {
                "symbol": str(row["symbol"]),
                "market": "FUTURES",
                "last_price": _decimal(row.get("lastPrice")),
                "price_change_percent": _decimal(row.get("priceChangePercent")),
                "quote_volume": _decimal(row.get("quoteVolume")),
                "source_timestamp": _event_datetime(
                    row.get("closeTime") or received_timestamp
                ).isoformat(),
                "received_timestamp": received_timestamp.isoformat(),
                "latency_ms": _latency_ms(
                    row.get("closeTime") or received_timestamp,
                    received_timestamp,
                ),
                "payload": row,
            }
            for row in markets
        ],
        "events": [breadth_event],
        "universe": scanner,
        "product_coverage": _coverage(captured_at),
    }


def collect_futures_observations(
    symbols: list[str],
    *,
    run_id: str | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    """Collect public USD-M positioning context without enabling trading."""
    if client is None:
        try:
            from binance_client import ClientConfig, FuturesPublicClient
        except ModuleNotFoundError:
            import sys
            from pathlib import Path

            sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
            from binance_client import ClientConfig, FuturesPublicClient

        client = FuturesPublicClient(ClientConfig(mode="paper"))
    events: list[dict[str, Any]] = []

    def append(
        symbol: str,
        event_type: str,
        payload: dict[str, Any],
        timestamp: Any,
        *,
        observation_period: str | None = None,
    ) -> None:
        source = _event_datetime(timestamp or datetime.now(timezone.utc))
        received = datetime.now(timezone.utc)
        normalized_payload = dict(payload)
        if observation_period is not None:
            normalized_payload["observationPeriod"] = observation_period
        event_id = uuid.uuid5(
            uuid.NAMESPACE_URL,
            ":".join((
                "bian", "futures", symbol, event_type,
                observation_period or "instant", source.isoformat(),
            )),
        )
        events.append(
            {
                "event_id": str(event_id),
                "symbol": symbol,
                "market": "FUTURES",
                "event_type": event_type,
                "source_timestamp": source.isoformat(),
                "received_timestamp": received.isoformat(),
                "latency_ms": _latency_ms(source, received),
                "price": normalized_payload.get("markPrice") or normalized_payload.get("price"),
                "quantity": normalized_payload.get("openInterest"),
                "direction": None,
                "metadata": normalized_payload,
            }
        )

    for raw_symbol in symbols:
        symbol = raw_symbol.replace("-", "").upper()
        get_aggregate_trades = getattr(client, "get_aggregate_trades", None)
        if get_aggregate_trades is not None:
            for trade in get_aggregate_trades(symbol, limit=1000):
                price = _decimal(trade.get("p"))
                quantity = _decimal(trade.get("q"))
                timestamp = trade.get("T")
                if (
                    price is None
                    or quantity is None
                    or price <= 0
                    or quantity <= 0
                    or timestamp is None
                ):
                    continue
                source = _event_datetime(timestamp)
                received = datetime.now(timezone.utc)
                buyer_maker = bool(trade.get("m"))
                aggregate_id = str(trade.get("a"))
                events.append(
                    {
                        "event_id": str(uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"bian:futures-aggregate-trade:{symbol}:{aggregate_id}",
                        )),
                        "symbol": symbol,
                        "source": "binance_futures_agg_trades_rest",
                        "market": "FUTURES",
                        "event_type": "FUTURES_TRADE",
                        "source_timestamp": source.isoformat(),
                        "received_timestamp": received.isoformat(),
                        "latency_ms": _latency_ms(source, received),
                        "price": str(price),
                        "quantity": str(quantity),
                        "notional": str(price * quantity),
                        "buyer_maker": buyer_maker,
                        "direction": "SELL" if buyer_maker else "BUY",
                        "metadata": dict(trade),
                    }
                )
        get_klines = getattr(client, "get_klines", None)
        klines = get_klines(symbol, interval="1m", limit=2) if get_klines else []
        if klines:
            latest_kline = klines[-1]
            if isinstance(latest_kline, (list, tuple)) and len(latest_kline) >= 7:
                append(
                    symbol,
                    "FUTURES_KLINES",
                    {
                        "openTime": latest_kline[0],
                        "open": latest_kline[1],
                        "high": latest_kline[2],
                        "low": latest_kline[3],
                        "close": latest_kline[4],
                        "volume": latest_kline[5],
                        "closeTime": latest_kline[6],
                    },
                    datetime.fromtimestamp(int(latest_kline[0]) / 1000, tz=timezone.utc),
                )
        mark = client.get_mark_price(symbol)
        mark_price = _decimal(mark.get("markPrice"))
        index_price = _decimal(mark.get("indexPrice"))
        if mark_price is not None and index_price not in {None, Decimal("0")}:
            mark = {
                **mark,
                "absoluteBasis": str(mark_price - index_price),
                "basisBps": str((mark_price - index_price) / index_price * Decimal("10000")),
            }
        append(symbol, "MARK_INDEX_FUNDING", mark, mark.get("time"))
        ticker = client.get_ticker_price(symbol)
        append(symbol, "LAST_PRICE", ticker, ticker.get("time"))
        oi = client.get_open_interest(symbol)
        append(symbol, "OPEN_INTEREST", oi, oi.get("time"))
        funding_rows = client.get_funding_rate(symbol, limit=2)
        if funding_rows:
            latest = dict(funding_rows[-1])
            append(
                symbol,
                "FUNDING",
                latest,
                latest.get("fundingTime") or latest.get("timestamp"),
            )
        # Binance publishes these aggregate observations on fixed windows.
        # Preserve the native window instead of fabricating unavailable 1m/3m data.
        for period in NATIVE_FUTURES_PERIODS:
            for event_type, rows in (
                ("TAKER_RATIO", client.get_taker_buy_sell(symbol, period=period, limit=2)),
                ("GLOBAL_LONG_SHORT", client.get_global_long_short_ratio(symbol, period=period, limit=2)),
                ("TOP_TRADER_LONG_SHORT", client.get_top_trader_long_short_ratio(symbol, period=period, limit=2)),
            ):
                if rows:
                    latest = dict(rows[-1])
                    append(
                        symbol,
                        event_type,
                        latest,
                        latest.get("timestamp") or latest.get("fundingTime"),
                        observation_period=period,
                    )
    return {
        "run_id": run_id or str(uuid.uuid4()),
        "collection_kind": "futures_observation",
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "source_url": "https://fapi.binance.com",
        "markets": [],
        "events": events,
        "product_coverage": [],
    }


def persist(report: dict[str, Any], dsn: str | None = None) -> None:
    """Atomically persist one collection batch and make retries idempotent."""
    import psycopg2

    dsn = dsn or configured_dsn()
    run_id = str(report["run_id"])
    collection_kind = str(report["collection_kind"])
    markets = list(report["markets"])
    coverage = list(report.get("product_coverage", []))
    events = [
        _normalize_market_data_event(event, default_source=str(report["source_url"]))
        for event in report.get("events", [])
    ]
    ensure_schema(dsn)

    with psycopg2.connect(dsn, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bian_collection_runs(
                    run_id, collection_kind, status, started_at, source_url
                ) VALUES (%s, %s, 'running', %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    status = 'running',
                    source_url = EXCLUDED.source_url,
                    error_code = NULL
                """,
                (
                    run_id,
                    collection_kind,
                    report["captured_at"],
                    report["source_url"],
                ),
            )
            for market in markets:
                cursor.execute(
                    """
                    INSERT INTO bian_market_snapshots(
                        run_id, captured_at, symbol, last_price,
                        price_change_percent, quote_volume, source_url, market, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB))
                    ON CONFLICT (run_id, symbol, market) WHERE run_id IS NOT NULL
                    DO UPDATE SET
                        captured_at = EXCLUDED.captured_at,
                        last_price = EXCLUDED.last_price,
                        price_change_percent = EXCLUDED.price_change_percent,
                        quote_volume = EXCLUDED.quote_volume,
                        source_url = EXCLUDED.source_url,
                        payload = EXCLUDED.payload
                    """,
                    (
                        run_id,
                        report["captured_at"],
                        market["symbol"],
                        market["last_price"],
                        market["price_change_percent"],
                        market["quote_volume"],
                        report["source_url"],
                        str(market.get("market") or report.get("market") or "FUTURES").upper(),
                        json.dumps(market["payload"], default=str),
                    ),
                )
            for event in events:
                cursor.execute(
                    """
                    INSERT INTO market_flow_events(
                        event_id, symbol, market, event_type, event_timestamp,
                        received_timestamp, latency_ms, price, quantity, notional,
                        direction, metadata
                        , validation_session_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              CAST(%s AS JSONB), %s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (
                        event["event_id"], event["symbol"] if "symbol" in event else "",
                        event.get("market") or "UNKNOWN", event["event_type"],
                        event["source_timestamp"], event["received_timestamp"],
                        event["latency_ms"], event.get("price"), event.get("quantity"),
                        event.get("notional"), event.get("direction"),
                        json.dumps(event, default=str),
                        os.environ.get("BIAN_VALIDATION_SESSION_ID") or None,
                    ),
                )
                if event.get("event_type") == "FORCE_ORDER":
                    price = _decimal(event.get("price"))
                    quantity = _decimal(event.get("quantity"))
                    if price is None or quantity is None:
                        raise ValueError("force order requires price and quantity")
                    cursor.execute(
                        """
                        INSERT INTO liquidation_events(
                            event_id, symbol, market, side, price, quantity,
                            notional, event_timestamp, received_timestamp,
                            latency_ms, payload
                        ) VALUES (%s, %s, 'FUTURES', %s, %s, %s, %s, %s, %s, %s,
                                  CAST(%s AS JSONB))
                        ON CONFLICT (event_id) DO NOTHING
                        """,
                        (
                            event["event_id"], event["symbol"], event["direction"],
                            price, quantity, price * quantity,
                            event["source_timestamp"], event["received_timestamp"],
                            event["latency_ms"], json.dumps(event, default=str),
                        ),
                    )
            for item in coverage:
                cursor.execute(
                    """
                    INSERT INTO bian_product_coverage(
                        product_type, status, symbol_count, source_url,
                        captured_at, detail
                    ) VALUES (%s, %s, %s, %s, %s, CAST(%s AS JSONB))
                    ON CONFLICT (product_type) DO UPDATE SET
                        status = EXCLUDED.status,
                        symbol_count = EXCLUDED.symbol_count,
                        source_url = EXCLUDED.source_url,
                        captured_at = EXCLUDED.captured_at,
                        detail = EXCLUDED.detail
                    """,
                    (
                        item["product_type"],
                        item["status"],
                        item["symbol_count"],
                        item["source_url"],
                        item["captured_at"],
                        json.dumps(item["detail"], default=str),
                    ),
                )
            cursor.execute(
                """
                UPDATE bian_collection_runs
                SET status = 'ok',
                    completed_at = NOW(),
                    market_count = %s,
                    coverage_count = %s,
                    error_code = NULL
                WHERE run_id = %s
                """,
                (len(markets), len(coverage), run_id),
            )


def _stream_symbols(value: str) -> list[str]:
    symbols = [symbol.strip().upper() for symbol in value.split(",") if symbol.strip()]
    if not symbols:
        raise ValueError("at least one Binance stream symbol is required")
    if any(not symbol.endswith("-USDT") for symbol in symbols):
        raise ValueError("stream symbols must be public USDT pairs, for example BTC-USDT")
    return list(dict.fromkeys(symbols))


def _futures_feed_symbol(symbol: str) -> str:
    """Translate the operator-facing USD-M symbol to Cryptofeed perpetual form."""
    normalized = symbol.strip().upper()
    if normalized.endswith("-USDT-PERP"):
        return normalized
    if normalized.endswith("-USDT"):
        return f"{normalized}-PERP"
    raise ValueError("Futures stream symbols must be USDT perpetual contracts")


def _storage_symbol(symbol: str) -> str:
    """Keep persistent Futures symbols in Binance REST form, e.g. BTCUSDT."""
    return symbol.upper().replace("-PERP", "").removesuffix("PERP").replace("-", "")


def _build_stream_handler(
    symbols: list[str],
    callback: Callable[[Any, float], Awaitable[None]],
    *,
    ticker_callback: Callable[[Any, float], Awaitable[None]] | None = None,
    book_callback: Callable[..., Awaitable[None]] | None = None,
) -> Any:
    from cryptofeed import FeedHandler
    from cryptofeed.defines import L2_BOOK, TICKER, TRADES
    from cryptofeed.exchanges import Binance

    channels = [TRADES]
    callbacks: dict[str, Callable[..., Awaitable[None]]] = {TRADES: callback}
    if ticker_callback is not None:
        channels.append(TICKER)
        callbacks[TICKER] = ticker_callback
    if book_callback is not None:
        channels.append(L2_BOOK)
        callbacks[L2_BOOK] = book_callback
    handler = FeedHandler()
    handler.add_feed(
        Binance(
            symbols=symbols,
            channels=channels,
            callbacks=callbacks,
            retries=-1,
            timeout=60,
            http_proxy=_http_proxy(),
        )
    )
    return handler


def _stream_report(
    markets: list[dict[str, Any]], *, market: str = "SPOT"
) -> dict[str, Any]:
    captured_at = _now()
    return {
        "run_id": str(uuid.uuid4()),
        "collection_kind": "stream_trade",
        "captured_at": captured_at,
        "source_url": (
            "wss://fstream.binance.com"
            if market.upper() == "FUTURES"
            else "wss://stream.binance.com:9443"
        ),
        "markets": markets,
        "product_coverage": [],
    }


def _stream_market(
    trade: Any,
    receipt_timestamp: float,
    *,
    market: str = "SPOT",
) -> tuple[str, dict[str, Any]] | None:
    price = _decimal(trade.price)
    if price is None:
        return None
    standard_symbol = str(trade.symbol)
    source_timestamp = _event_datetime(trade.timestamp)
    received_at = _event_datetime(receipt_timestamp)
    quantity = _decimal(trade.amount) or Decimal("0")
    side = str(trade.side).lower()
    buyer_maker = side in {"sell", "none"}
    native_trade_id = getattr(trade, "id", None)
    raw_trade = getattr(trade, "raw", None)
    if native_trade_id in {None, ""} and isinstance(raw_trade, dict):
        native_trade_id = raw_trade.get("t")
    storage_symbol = _storage_symbol(standard_symbol)
    if market.upper() == "FUTURES" and native_trade_id not in {None, ""}:
        event_id = _native_event_id("futures-trade", storage_symbol, native_trade_id)
    else:
        event_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"bian:trade:{standard_symbol}:{source_timestamp.isoformat()}:{price}:{quantity}",
        ))
    event = {
        "symbol": storage_symbol,
        "event_id": event_id,
        "event_type": "FUTURES_TRADE" if market.upper() == "FUTURES" else "TRADE",
        "source": (
            "binance_futures_trade_stream"
            if market.upper() == "FUTURES"
            else "binance_spot_trade_stream"
        ),
        "market": market.upper(),
        "source_timestamp": source_timestamp.isoformat(),
        "received_timestamp": received_at.isoformat(),
        "latency_ms": _latency_ms(source_timestamp, received_at),
        "price": str(price),
        "quantity": str(quantity),
        "notional": str(price * quantity),
        "buyer_maker": buyer_maker,
        "direction": "SELL" if buyer_maker else "BUY",
    }
    return (
        standard_symbol,
        {
            "symbol": _storage_symbol(standard_symbol),
            "last_price": price,
            "price_change_percent": None,
            "quote_volume": None,
            "payload": {
                "exchange": str(trade.exchange),
                "symbol": standard_symbol,
                "price": str(price),
                "amount": str(trade.amount),
                "side": str(trade.side),
                "event_timestamp": trade.timestamp,
                "receipt_timestamp": receipt_timestamp,
                "market_data_event": event,
            },
            "market_data_event": event,
        },
    )


def _stream_book_ticker(
    ticker: Any,
    receipt_timestamp: float,
    *,
    market: str = "SPOT",
) -> tuple[str, dict[str, Any]] | None:
    """Normalize bookTicker as an observation, not an executable quote."""
    bid = _decimal(ticker.bid)
    ask = _decimal(ticker.ask)
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None
    standard_symbol = str(ticker.symbol)
    source_timestamp = _event_datetime(ticker.timestamp or receipt_timestamp)
    received_at = _event_datetime(receipt_timestamp)
    mid = (bid + ask) / Decimal("2")
    event = {
        "symbol": _storage_symbol(standard_symbol),
        "event_id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"bian:bookticker:{standard_symbol}:{source_timestamp.isoformat()}:{bid}:{ask}",
        )),
        "event_type": "BOOK_TICKER",
        "source": (
            "binance_futures_book_ticker"
            if market.upper() == "FUTURES"
            else "binance_spot_book_ticker"
        ),
        "market": market.upper(),
        "source_timestamp": source_timestamp.isoformat(),
        "received_timestamp": received_at.isoformat(),
        "latency_ms": _latency_ms(source_timestamp, received_at),
        "price": str(mid),
        "quantity": None,
        "direction": None,
        "metadata": {
            "bidPrice": str(bid),
            "askPrice": str(ask),
            "bid_price": str(bid),
            "ask_price": str(ask),
            "bid_qty": str(_decimal(getattr(ticker, "bid_size", None) or getattr(ticker, "bid_qty", None)) or "") or None,
            "ask_qty": str(_decimal(getattr(ticker, "ask_size", None) or getattr(ticker, "ask_qty", None)) or "") or None,
            "spread": str(ask - bid),
            "spreadBps": str((ask - bid) / mid * Decimal("10000")),
            "timestamp_semantics": "exchange_event" if getattr(ticker, "raw", {}).get("E") else "receipt_observed",
        },
    }
    return standard_symbol, event


def _stream_orderbook(
    book: Any,
    receipt_timestamp: float,
    *,
    raw: dict[str, Any] | None = None,
    books: dict[str, LocalOrderBook],
    market: str = "SPOT",
) -> tuple[str, dict[str, Any]] | None:
    """Apply a verified snapshot/diff update to the local order book.

    Cryptofeed obtains the exchange snapshot and buffers depth updates. This
    owner independently applies the raw Binance ``U/u`` sequence to preserve
    an auditable, fail-closed local book for feature aggregation.
    """
    raw = raw or getattr(book, "raw", None)
    if not isinstance(raw, dict):
        return None
    standard_symbol = str(book.symbol)
    received_at = _event_datetime(receipt_timestamp)
    if "lastUpdateId" in raw:
        local_book = LocalOrderBook.from_snapshot(raw, sync_origin="STREAM_SNAPSHOT")
        books[standard_symbol] = local_book
        source_timestamp = received_at
        timestamp_semantics = "receipt_observed_snapshot"
    else:
        local_book = books.get(standard_symbol)
        if local_book is None:
            return None
        try:
            local_book.apply_diff(raw)
        except OrderBookGap as exc:
            books.pop(standard_symbol, None)
            return _orderbook_health_event(
                standard_symbol,
                state="UNSAFE",
                error_class=type(exc).__name__,
                raw=raw,
                received_at=received_at,
                market=market,
            )
        source_timestamp = _event_datetime(raw.get("E", receipt_timestamp))
        timestamp_semantics = "exchange_event"
    return _orderbook_event(
        standard_symbol,
        local_book,
        source_timestamp=source_timestamp,
        received_at=received_at,
        timestamp_semantics=timestamp_semantics,
        market=market,
    )


def _orderbook_health_event(
    standard_symbol: str,
    *,
    state: str,
    error_class: str,
    raw: dict[str, Any],
    received_at: datetime,
    market: str,
) -> tuple[str, dict[str, Any]]:
    """Persist a gap/failure as evidence instead of silently dropping it."""
    source_timestamp = _event_datetime(raw.get("E", received_at.timestamp()))
    symbol = _storage_symbol(standard_symbol)
    event = {
        "symbol": symbol,
        "event_id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"bian:orderbook-health:{symbol}:{source_timestamp.isoformat()}:{error_class}",
        )),
        "event_type": "ORDERBOOK",
        "source": (
            "binance_futures_diff_depth"
            if market.upper() == "FUTURES"
            else "binance_spot_diff_depth"
        ),
        "market": market.upper(),
        "source_timestamp": source_timestamp.isoformat(),
        "received_timestamp": received_at.isoformat(),
        "latency_ms": _latency_ms(source_timestamp, received_at),
        "price": None,
        "quantity": None,
        "direction": None,
        "metadata": {
            "health_status": state,
            "state": state,
            "error_class": error_class,
            "received_U": raw.get("U"),
            "received_u": raw.get("u"),
        },
    }
    return standard_symbol, event


def _queue_orderbook_observation(
    pending_events: list[dict[str, Any]],
    pending_observations: dict[tuple[str, str], dict[str, Any]],
    event: dict[str, Any],
) -> None:
    """Keep order book faults as immutable evidence through a later resync."""
    health_status = str((event.get("metadata") or {}).get("health_status", "")).upper()
    if health_status in {"GAP", "UNSAFE", "ERROR"}:
        pending_events.append(event)
        return
    pending_observations[(event["symbol"], event["event_type"])] = event


def _orderbook_event(
    standard_symbol: str,
    local_book: LocalOrderBook,
    *,
    source_timestamp: datetime,
    received_at: datetime,
    timestamp_semantics: str,
    market: str = "SPOT",
) -> tuple[str, dict[str, Any]] | None:
    """Normalize one already-synchronized local book for feature persistence."""
    features = local_book.features()
    if not features and (local_book.state != "SYNCING" or not local_book.bids or not local_book.asks):
        return None
    symbol = _storage_symbol(standard_symbol)
    best_bid = max(local_book.bids)
    best_ask = min(local_book.asks)
    event = {
        "symbol": symbol,
        "event_id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"bian:orderbook:{symbol}:{local_book.last_update_id}:{source_timestamp.isoformat()}",
        )),
        "event_type": "ORDERBOOK",
        "source": (
            "binance_futures_diff_depth"
            if market.upper() == "FUTURES"
            else "binance_spot_diff_depth"
        ),
        "market": market.upper(),
        "source_timestamp": source_timestamp.isoformat(),
        "received_timestamp": received_at.isoformat(),
        "latency_ms": _latency_ms(source_timestamp, received_at),
        "price": str((best_bid + best_ask) / Decimal("2")),
        "quantity": None,
        "direction": None,
        "metadata": {
            **{key: str(value) for key, value in features.items()},
            "health_status": local_book.state if local_book.state != "VALID" else None,
            "state": local_book.state,
            "bestBid": str(best_bid),
            "bestAsk": str(best_ask),
            "lastUpdateId": local_book.last_update_id,
            "received_U": local_book.received_U,
            "received_u": local_book.received_u,
            "expected_next": local_book.expected_next,
            "error_class": local_book.error_class,
            "recovery_attempt": local_book.recovery_attempt,
            "timestamp_semantics": timestamp_semantics,
            "snapshotSyncOrigin": local_book.snapshot_sync_origin,
            "snapshotLastUpdateId": local_book.snapshot_update_id,
            "snapshotReceivedTimestamp": (
                local_book.snapshot_received_timestamp.isoformat()
                if local_book.snapshot_received_timestamp is not None
                else None
            ),
        },
    }
    return standard_symbol, event


def _stream_liquidation(
    liquidation: Any,
    receipt_timestamp: float,
) -> tuple[str, dict[str, Any]] | None:
    """Normalize a public Futures force-order event for observation only."""
    price = _decimal(liquidation.price)
    quantity = _decimal(liquidation.quantity)
    if price is None or quantity is None or price <= 0 or quantity <= 0:
        return None
    standard_symbol = str(liquidation.symbol)
    source_timestamp = _event_datetime(liquidation.timestamp)
    received_at = _event_datetime(receipt_timestamp)
    side = str(liquidation.side).upper()
    if side not in {"BUY", "SELL"}:
        return None
    event = {
        "symbol": _storage_symbol(standard_symbol),
        "event_id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"bian:forceorder:{standard_symbol}:{source_timestamp.isoformat()}:{side}:{price}:{quantity}",
        )),
        "event_type": "FORCE_ORDER",
        "source": "binance_futures_force_order",
        "market": "FUTURES",
        "source_timestamp": source_timestamp.isoformat(),
        "received_timestamp": received_at.isoformat(),
        "latency_ms": _latency_ms(source_timestamp, received_at),
        "price": str(price),
        "quantity": str(quantity),
        "notional": str(price * quantity),
        "direction": side,
        "metadata": {
            "side_semantics": (
                "short_liquidation" if side == "BUY" else "long_liquidation"
            ),
            "status": str(getattr(liquidation, "status", "")),
        },
    }
    return standard_symbol, event


def _stream_funding(
    funding: Any,
    receipt_timestamp: float,
) -> tuple[str, dict[str, Any]] | None:
    """Normalize Binance Futures mark/index/funding websocket updates."""
    mark_price = _decimal(getattr(funding, "price", None))
    if mark_price is None or mark_price <= 0:
        return None
    standard_symbol = str(funding.symbol)
    source_timestamp = _event_datetime(funding.timestamp or receipt_timestamp)
    received_at = _event_datetime(receipt_timestamp)
    raw = getattr(funding, "raw", {})
    index_price = _decimal(raw.get("i")) if isinstance(raw, dict) else None
    rate = _decimal(getattr(funding, "rate", None))
    event = {
        "symbol": _storage_symbol(standard_symbol),
        "event_id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"bian:mark-index-funding:{standard_symbol}:{source_timestamp.isoformat()}:{mark_price}",
        )),
        "event_type": "MARK_INDEX_FUNDING",
        "source": "binance_futures_mark_price_stream",
        "market": "FUTURES",
        "source_timestamp": source_timestamp.isoformat(),
        "received_timestamp": received_at.isoformat(),
        "latency_ms": _latency_ms(source_timestamp, received_at),
        "price": str(mark_price),
        "quantity": None,
        "direction": None,
        "metadata": {
            **(raw if isinstance(raw, dict) else {}),
            "markPrice": str(mark_price),
            "indexPrice": str(index_price) if index_price is not None else None,
            "lastFundingRate": str(rate) if rate is not None else None,
        },
    }
    return standard_symbol, event


def futures_public_ws_url() -> str:
    """Return the official USD-M public combined stream endpoint for the mode."""
    mode = os.environ.get("BIAN_MODE", "paper").strip().lower()
    if mode == "testnet":
        return FUTURES_TESTNET_PUBLIC_WS
    return FUTURES_LIVE_PUBLIC_WS


def channel_idle_sec(channel: str) -> int:
    """Per-channel stale budget from SOURCE_REGISTRY, never a local hardcoded map."""
    from engine import SOURCE_REGISTRY

    source = FUTURES_CHANNEL_SOURCES[str(channel).upper()]
    return int(SOURCE_REGISTRY[source].ttl_sec)


def futures_stream_params(
    channel: str, symbols: list[str] | tuple[str, ...]
) -> list[str]:
    suffix = FUTURES_CHANNEL_STREAMS[str(channel).upper()]
    params: list[str] = []
    for symbol in symbols:
        stream_symbol = _storage_symbol(symbol).lower()
        if stream_symbol and stream_symbol not in {
            item.split("@", 1)[0] for item in params
        }:
            params.append(f"{stream_symbol}@{suffix}")
    if not params:
        raise ValueError("at least one Futures public stream symbol is required")
    return params


def _connection_error(exc: Exception) -> Exception:
    from binance_client import BinanceConnectionError

    if isinstance(exc, BinanceConnectionError):
        return exc
    wrapped = BinanceConnectionError(type(exc).__name__)
    wrapped.__cause__ = exc
    return wrapped


async def _default_futures_websocket_connect(url: str) -> Any:
    import websockets

    proxy = _http_proxy()
    kwargs: dict[str, Any] = {
        "ping_interval": 20,
        "ping_timeout": 20,
        "close_timeout": 5,
        "max_size": 2**23,
        "max_queue": None,
        "open_timeout": 10,
        "compression": None,
    }
    sock = None
    try:
        if proxy and urlparse(proxy).scheme == "http":
            parsed_url = urlparse(url)
            if not parsed_url.hostname:
                raise OSError("invalid_ws_url")
            sock = await asyncio.to_thread(
                _proxy_tunnel_socket,
                parsed_url.hostname,
                parsed_url.port or 443,
                proxy,
            )
            kwargs["sock"] = sock
            kwargs["proxy"] = None
            sock = None
        else:
            kwargs["proxy"] = proxy
        return await websockets.connect(url, **kwargs)
    except Exception as exc:
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
        raise _connection_error(exc)


def _ws_payload(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _futures_trade_event(payload: dict[str, Any], received_at: datetime) -> dict[str, Any] | None:
    symbol = _storage_symbol(str(payload.get("s") or ""))
    trade_id = payload.get("t")
    price = _decimal(payload.get("p"))
    quantity = _decimal(payload.get("q"))
    if not symbol or trade_id in {None, ""} or price is None or quantity is None:
        return None
    if price <= 0 or quantity <= 0:
        return None
    source_timestamp = _event_datetime(payload.get("T") or payload.get("E") or received_at)
    buyer_maker = bool(payload.get("m"))
    return {
        "symbol": symbol,
        "event_id": _native_event_id("futures-trade", symbol, trade_id),
        "event_type": "FUTURES_TRADE",
        "source": "binance_futures_trade_stream",
        "market": "FUTURES",
        "source_timestamp": source_timestamp.isoformat(),
        "received_timestamp": received_at.isoformat(),
        "latency_ms": _latency_ms(source_timestamp, received_at),
        "price": str(price),
        "quantity": str(quantity),
        "notional": str(price * quantity),
        "buyer_maker": buyer_maker,
        "direction": "SELL" if buyer_maker else "BUY",
        "trade_id": str(trade_id),
        "metadata": {
            "health": "LIVE",
            "health_status": "LIVE",
            "native_trade_id": str(trade_id),
        },
    }


def _futures_book_ticker_event(
    payload: dict[str, Any], received_at: datetime
) -> dict[str, Any] | None:
    symbol = _storage_symbol(str(payload.get("s") or ""))
    bid = _decimal(payload.get("b"))
    ask = _decimal(payload.get("a"))
    bid_qty = _decimal(payload.get("B"))
    ask_qty = _decimal(payload.get("A"))
    if not symbol or bid is None or ask is None or bid <= 0 or ask <= 0:
        return None
    source_timestamp = _event_datetime(payload.get("E") or payload.get("T") or received_at)
    mid = (bid + ask) / Decimal("2")
    return {
        "symbol": symbol,
        "event_id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"bian:bookticker:{symbol}:{source_timestamp.isoformat()}:{bid}:{ask}",
        )),
        "event_type": "BOOK_TICKER",
        "source": "binance_futures_book_ticker",
        "market": "FUTURES",
        "source_timestamp": source_timestamp.isoformat(),
        "received_timestamp": received_at.isoformat(),
        "latency_ms": _latency_ms(source_timestamp, received_at),
        "price": str(mid),
        "quantity": None,
        "direction": None,
        "metadata": {
            "health": "LIVE",
            "health_status": "LIVE",
            "bid_price": str(bid),
            "bid_qty": str(bid_qty) if bid_qty is not None else None,
            "ask_price": str(ask),
            "ask_qty": str(ask_qty) if ask_qty is not None else None,
            "bidPrice": str(bid),
            "askPrice": str(ask),
            "spread": str(ask - bid),
            "spreadBps": str((ask - bid) / mid * Decimal("10000")),
            "timestamp_semantics": "exchange_event",
        },
    }


def _futures_mark_price_event(
    payload: dict[str, Any], received_at: datetime
) -> dict[str, Any] | None:
    symbol = _storage_symbol(str(payload.get("s") or ""))
    mark_price = _decimal(payload.get("p"))
    if not symbol or mark_price is None or mark_price <= 0:
        return None
    source_timestamp = _event_datetime(payload.get("E") or received_at)
    index_price = _decimal(payload.get("i"))
    rate = _decimal(payload.get("r"))
    return {
        "symbol": symbol,
        "event_id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"bian:mark-index-funding:{symbol}:{source_timestamp.isoformat()}:{mark_price}",
        )),
        "event_type": "MARK_INDEX_FUNDING",
        "source": "binance_futures_mark_price_stream",
        "market": "FUTURES",
        "source_timestamp": source_timestamp.isoformat(),
        "received_timestamp": received_at.isoformat(),
        "latency_ms": _latency_ms(source_timestamp, received_at),
        "price": str(mark_price),
        "quantity": None,
        "direction": None,
        "metadata": {
            "health": "LIVE",
            "health_status": "LIVE",
            "markPrice": str(mark_price),
            "indexPrice": str(index_price) if index_price is not None else None,
            "lastFundingRate": str(rate) if rate is not None else None,
            "nextFundingTime": payload.get("T"),
        },
    }


def _futures_force_order_event(
    payload: dict[str, Any], received_at: datetime
) -> dict[str, Any] | None:
    order = payload.get("o") if isinstance(payload.get("o"), dict) else payload
    symbol = _storage_symbol(str(order.get("s") or payload.get("s") or ""))
    price = _decimal(order.get("ap") or order.get("p"))
    quantity = _decimal(order.get("z") or order.get("q") or order.get("l"))
    side = str(order.get("S") or "").upper()
    if not symbol or price is None or quantity is None or price <= 0 or quantity <= 0:
        return None
    if side not in {"BUY", "SELL"}:
        return None
    source_timestamp = _event_datetime(order.get("T") or payload.get("E") or received_at)
    native_id = payload.get("E")
    event_id = _native_event_id(
        "forceorder",
        symbol,
        native_id if native_id not in {None, ""} else source_timestamp.isoformat(),
        side,
        quantity,
        price,
    )
    return {
        "symbol": symbol,
        "event_id": event_id,
        "event_type": "FORCE_ORDER",
        "source": "binance_futures_force_order",
        "market": "FUTURES",
        "source_timestamp": source_timestamp.isoformat(),
        "received_timestamp": received_at.isoformat(),
        "latency_ms": _latency_ms(source_timestamp, received_at),
        "price": str(price),
        "quantity": str(quantity),
        "notional": str(price * quantity),
        "direction": side,
        "metadata": {
            "health": "LIVE",
            "health_status": "LIVE",
            "side_semantics": (
                "short_liquidation" if side == "BUY" else "long_liquidation"
            ),
            "status": str(order.get("X") or ""),
        },
    }


def liquidation_heartbeat_event(
    symbol: str,
    received_at: datetime,
    *,
    health: str = "LIVE",
) -> dict[str, Any]:
    storage = _storage_symbol(symbol)
    return {
        "symbol": storage,
        "event_id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"bian:liquidation-heartbeat:{storage}:{received_at.isoformat()}",
        )),
        "event_type": "LIQUIDATION_HEARTBEAT",
        "source": "binance_futures_force_order_subscription",
        "market": "FUTURES",
        "source_timestamp": received_at.isoformat(),
        "received_timestamp": received_at.isoformat(),
        "latency_ms": 0,
        "health": health,
        "metadata": {
            "observed_liquidation": False,
            "health": health,
            "health_status": health,
            "liveness_source": "FUTURES_LIQUIDATION_LIVENESS",
        },
    }


def ws_lifecycle_event(
    lifecycle: dict[str, Any],
    received_at: datetime,
    *,
    symbol: str = "BTCUSDT",
) -> dict[str, Any]:
    """Canonical websocket disconnect/reconnect event. One owner, one event_type."""
    action = str(lifecycle.get("action") or "reconnect")
    identity = (
        f"bian:ws-lifecycle:{lifecycle.get('channel')}:{action}:"
        f"{lifecycle.get('old_connection_id')}:{lifecycle.get('new_connection_id')}:"
        f"{lifecycle.get('disconnect_at')}:{lifecycle.get('reconnect_at')}"
    )
    payload = dict(lifecycle)
    payload["action"] = action
    return {
        "symbol": _storage_symbol(symbol),
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, identity)),
        "event_type": "WS_LIFECYCLE",
        "source": "collector_lifecycle",
        "market": "FUTURES",
        "source_timestamp": received_at.isoformat(),
        "received_timestamp": received_at.isoformat(),
        "latency_ms": 0,
        "metadata": payload,
        "channel": payload.get("channel"),
        "old_connection_id": payload.get("old_connection_id"),
        "new_connection_id": payload.get("new_connection_id"),
        "disconnect_at": payload.get("disconnect_at"),
        "reconnect_at": payload.get("reconnect_at"),
        "recovery_ms": payload.get("recovery_ms"),
        "subscriptions_restored": payload.get("subscriptions_restored"),
        "reason": payload.get("reason"),
        "action": action,
    }


def _stale_source_event(
    channel: str, symbol: str, now: datetime
) -> dict[str, Any]:
    event_type = {
        "TRADE": "FUTURES_TRADE",
        "BOOK_TICKER": "BOOK_TICKER",
        "DEPTH": "ORDERBOOK",
        "MARK_PRICE": "MARK_INDEX_FUNDING",
        "LIQUIDATION": "LIQUIDATION_HEARTBEAT",
    }[channel]
    source = {
        "TRADE": "binance_futures_trade_stream",
        "BOOK_TICKER": "binance_futures_book_ticker",
        "DEPTH": "binance_futures_diff_depth",
        "MARK_PRICE": "binance_futures_mark_price_stream",
        "LIQUIDATION": "binance_futures_force_order_subscription",
    }[channel]
    storage = _storage_symbol(symbol)
    return {
        "symbol": storage,
        "event_id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"bian:stale:{channel}:{storage}:{now.isoformat()}",
        )),
        "event_type": event_type,
        "source": source,
        "market": "FUTURES",
        "source_timestamp": now.isoformat(),
        "received_timestamp": now.isoformat(),
        "latency_ms": 0,
        "price": None,
        "quantity": None,
        "direction": None,
        "health": "STALE",
        "metadata": {
            "health": "STALE",
            "health_status": "STALE",
            "state": "STALE" if channel != "DEPTH" else "UNSAFE",
        },
    }


class FuturesStreamSupervisor:
    """Single owner for USD-M public websocket sessions and observation persist."""

    def __init__(
        self,
        symbols: list[str],
        *,
        flush_sec: float,
        dsn: str | None = None,
        channels: tuple[str, ...] | None = None,
        websocket_connect: Callable[[str], Any] | None = None,
        persist_fn: Callable[..., Any] | None = None,
        depth_snapshot_fn: Callable[..., dict[str, Any]] | None = None,
        sleep: Callable[[float], Any] | None = None,
        now: Callable[[], datetime] | None = None,
        max_reconnects: int | None = None,
        controlled_reconnect_after: float | None = None,
    ) -> None:
        if not symbols:
            raise ValueError("at least one Futures public stream symbol is required")
        selected = tuple(channels or tuple(FUTURES_CHANNEL_STREAMS))
        for channel in selected:
            if channel not in FUTURES_CHANNEL_STREAMS:
                raise ValueError(f"invalid futures channel: {channel}")
        self.symbols = tuple(dict.fromkeys(symbols))
        self.storage_symbols = tuple(
            dict.fromkeys(_storage_symbol(symbol) for symbol in self.symbols)
        )
        self.flush_sec = max(1.0, float(flush_sec))
        self.dsn = dsn
        self.sessions = {
            channel: ChannelSession(channel=channel, symbols=self.symbols)
            for channel in selected
        }
        self._connect = websocket_connect or _default_futures_websocket_connect
        self._persist = persist_fn or persist
        self._depth_snapshot = depth_snapshot_fn or (
            lambda symbol: _depth_snapshot(symbol, market="FUTURES")
        )
        self._sleep = sleep or asyncio.sleep
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._max_reconnects = (
            max_reconnects
            if max_reconnects is not None
            else _int_env("BIAN_WS_MAX_RECONNECTS", 5, minimum=0)
        )
        self._stopped = False
        self._request_id = 0
        self.pending_events: list[dict[str, Any]] = []
        self.pending_observations: dict[tuple[str, str], dict[str, Any]] = {}
        self.pending_markets: dict[str, dict[str, Any]] = {}
        self.local_books: dict[str, LocalOrderBook] = {}
        self.depth_buffers: dict[str, list[dict[str, Any]]] = {}
        self._depth_snapshots: dict[str, dict[str, Any]] = {}
        self._seen_event_ids: set[str] = set()
        self.global_transport_health = "OK"
        self._recover_started: dict[str, datetime] = {}
        self.lifecycle_events: list[dict[str, Any]] = []
        self._sockets: dict[str, Any] = {}
        self._reconnect_reasons: dict[str, str] = {}
        self._controlled_reconnect_after = controlled_reconnect_after

    def start(self) -> None:
        self._stopped = False
        for session in self.sessions.values():
            if session.state == "STOPPED":
                session.state = "STARTING"

    async def stop(self) -> None:
        self._stopped = True
        for session in self.sessions.values():
            session.state = "STOPPED"

    def channel_health(self) -> dict[str, dict[str, Any]]:
        return {name: session.as_health() for name, session in self.sessions.items()}

    def reconnect_events(self) -> list[dict[str, Any]]:
        """Canonical websocket lifecycle events from ChannelSession reconnects."""
        return [dict(event) for event in self.lifecycle_events]

    async def request_reconnect(self, channel: str, reason: str = "controlled_reconnect") -> None:
        session = self.sessions.get(channel)
        if session is None:
            return
        self._reconnect_reasons[channel] = reason
        session.last_error = reason
        ws = self._sockets.get(channel)
        closer = getattr(ws, "close", None)
        if closer is None:
            return
        result = closer()
        if asyncio.iscoroutine(result):
            await result

    def _new_connection_id(self, session: ChannelSession) -> str:
        return f"{session.channel.lower()}-{uuid.uuid4().hex[:12]}"

    def _mark_subscribed(self, session: ChannelSession) -> None:
        now = self._now()
        new_id = self._new_connection_id(session)
        disconnected = session.last_disconnect_at is not None
        reconnecting = session.reconnect_count > 0 or session.state == "RECONNECTING"
        if disconnected or reconnecting:
            started = session.last_disconnect_at or self._recover_started.get(session.channel)
            recovery_ms = (
                max(0, int((now - started).total_seconds() * 1000))
                if started is not None
                else 0
            )
            self.lifecycle_events.append({
                "action": "reconnect",
                "old_connection_id": session.connection_id or f"{session.channel.lower()}-none",
                "new_connection_id": new_id,
                "channel": session.channel,
                "disconnect_at": (
                    session.last_disconnect_at.isoformat()
                    if session.last_disconnect_at is not None
                    else None
                ),
                "reconnect_at": now.isoformat(),
                "recovery_ms": recovery_ms,
                "subscriptions_restored": True,
                "reason": session.last_error or "disconnect",
                "source": "collector_lifecycle",
            })
            session.time_to_recover_ms = recovery_ms
        session.connection_id = new_id
        session.subscribed = True
        if session.channel == "LIQUIDATION":
            session.state = "LIVE"

    def transport_health(self) -> dict[str, Any]:
        sessions = self.channel_health()
        live = [name for name, row in sessions.items() if row["state"] == "LIVE"]
        failed = [name for name, row in sessions.items() if row["state"] == "FAILED"]
        if self.global_transport_health == "HALT":
            global_health = "HALT"
        elif failed and not live:
            global_health = "HALT"
        elif failed or any(row["state"] == "STALE" for row in sessions.values()):
            global_health = "DEGRADED"
        else:
            global_health = "OK"
        per_symbol = {
            symbol: (
                "FAILED"
                if failed and not live
                else "DEGRADED"
                if any(row["state"] in {"STALE", "RECONNECTING", "FAILED"} for row in sessions.values())
                else "OK"
            )
            for symbol in self.storage_symbols
        }
        return {
            "global_transport_health": global_health,
            "channel_health": sessions,
            "per_symbol_transport_health": per_symbol,
        }

    def build_flush_events(self, now: datetime | None = None) -> list[dict[str, Any]]:
        """Heartbeat is unconditional; empty market queues still persist liveness."""
        now = now or self._now()
        events = [*self.pending_events, *self.pending_observations.values()]
        liquidation = self.sessions.get("LIQUIDATION")
        health = (
            "LIVE"
            if liquidation is not None and liquidation.state == "LIVE"
            else (liquidation.state if liquidation is not None else "LIVE")
        )
        lifecycle = self.reconnect_events()
        reconnects = len(lifecycle)
        attempts = sum(session.reconnect_count for session in self.sessions.values())
        symbol = self.storage_symbols[0] if self.storage_symbols else "BTCUSDT"
        for row in lifecycle:
            events.append(ws_lifecycle_event(row, now, symbol=symbol))
        for symbol in self.storage_symbols:
            event = liquidation_heartbeat_event(symbol, now, health=health)
            event["metadata"]["reconnect_count"] = reconnects
            event["metadata"]["session_reconnect_attempts"] = attempts
            event["metadata"]["lifecycle_source"] = "WS_LIFECYCLE"
            event["metadata"]["channel_health"] = self.channel_health()
            events.append(event)
        return events

    def _queue(self, event: dict[str, Any] | None, *, snapshot: dict[str, Any] | None = None) -> None:
        if event is None:
            return
        event_id = str(event.get("event_id") or "")
        if event_id:
            if event_id in self._seen_event_ids:
                return
            self._seen_event_ids.add(event_id)
            if len(self._seen_event_ids) > 20000:
                self._seen_event_ids = set(list(self._seen_event_ids)[-10000:])
        health_status = str((event.get("metadata") or {}).get("health_status") or "").upper()
        if health_status in {"GAP", "UNSAFE", "ERROR", "STALE"}:
            self.pending_events.append(event)
        elif event["event_type"] in {"FUTURES_TRADE", "FORCE_ORDER"}:
            self.pending_events.append(event)
        else:
            self.pending_observations[(event["symbol"], event["event_type"])] = event
        if snapshot is not None:
            self.pending_markets[snapshot["symbol"]] = snapshot

    def handle_payload(
        self,
        channel: str,
        payload: dict[str, Any],
        received_at: datetime | None = None,
    ) -> dict[str, Any] | None:
        session = self.sessions[channel]
        now = received_at or self._now()
        try:
            return self._handle_payload_body(channel, session, payload, now)
        except (ValueError, KeyError, TypeError):
            return None

    def _handle_payload_body(
        self,
        channel: str,
        session: ChannelSession,
        payload: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any] | None:
        if channel == "DEPTH":
            return self._handle_depth(payload, now)
        if channel == "TRADE":
            event = _futures_trade_event(payload, now)
            if event is not None:
                self._queue(
                    event,
                    snapshot={
                        "symbol": event["symbol"],
                        "last_price": _decimal(event["price"]),
                        "price_change_percent": None,
                        "quote_volume": None,
                        "payload": event,
                    },
                )
                self._mark_message(session, now, event)
                return event
        elif channel == "BOOK_TICKER":
            event = _futures_book_ticker_event(payload, now)
        elif channel == "MARK_PRICE":
            event = _futures_mark_price_event(payload, now)
        elif channel == "LIQUIDATION":
            event = _futures_force_order_event(payload, now)
        else:
            event = None
        if event is not None and channel != "TRADE":
            self._queue(event)
            self._mark_message(session, now, event)
        elif event is None and channel == "DEPTH":
            pass
        return event

    def inspect_idle(self, now: datetime | None = None) -> list[str]:
        now = now or self._now()
        stale: list[str] = []
        for session in self.sessions.values():
            if session.channel == "LIQUIDATION":
                continue
            if session.state in {"FAILED", "STOPPED", "STALE", "RECONNECTING"}:
                continue
            idle = channel_idle_sec(session.channel)
            anchor = session.last_message_at or session.connected_at
            if anchor is None:
                continue
            if (now - anchor).total_seconds() > idle:
                self._mark_stale(session, now)
                stale.append(session.channel)
        return stale

    def _mark_message(
        self, session: ChannelSession, now: datetime, event: dict[str, Any]
    ) -> None:
        started = self._recover_started.pop(session.channel, None)
        if session.state in {"STALE", "RECONNECTING", "STARTING"} and started is not None:
            session.time_to_recover_ms = max(
                0, int((now - started).total_seconds() * 1000)
            )
        session.state = "LIVE"
        session.subscribed = True
        session.consecutive_failures = 0
        session.last_message_at = now
        session.message_count += 1
        session.last_event_timestamp = _event_datetime(event["source_timestamp"])
        session.last_latency_ms = int(event.get("latency_ms") or 0)
        session.connected_at = session.connected_at or now

    def _mark_stale(self, session: ChannelSession, now: datetime) -> None:
        session.state = "STALE"
        session.stale_count += 1
        session.last_error = "STALE"
        session.last_error_class = "STALE"
        session.last_error_at = now
        self._recover_started.setdefault(session.channel, now)
        for symbol in self.storage_symbols:
            self._queue(_stale_source_event(session.channel, symbol, now))

    def _mark_disconnected(self, session: ChannelSession, error: Exception | str) -> None:
        now = self._now()
        session.last_disconnect_at = now
        session.last_error_at = now
        if isinstance(error, Exception):
            wrapped = _connection_error(error)
            session.last_error_class = type(wrapped).__name__
            session.last_error = session.last_error_class
        else:
            session.last_error = str(error)
            session.last_error_class = str(error)
        session.consecutive_failures += 1
        session.subscribed = False
        self._recover_started.setdefault(session.channel, now)
        pending = self._reconnect_reasons.pop(session.channel, None)
        if pending:
            session.last_error = pending
        self.lifecycle_events.append({
            "action": "disconnect",
            "old_connection_id": session.connection_id or f"{session.channel.lower()}-none",
            "new_connection_id": None,
            "channel": session.channel,
            "disconnect_at": now.isoformat(),
            "reconnect_at": None,
            "recovery_ms": None,
            "subscriptions_restored": False,
            "reason": session.last_error or "disconnect",
            "source": "collector_lifecycle",
        })

    def _handle_depth(
        self, payload: dict[str, Any], received_at: datetime
    ) -> dict[str, Any] | None:
        symbol = _storage_symbol(str(payload.get("s") or ""))
        if not symbol:
            return None
        session = self.sessions["DEPTH"]
        book = self.local_books.get(symbol)
        buffer = self.depth_buffers.setdefault(symbol, [])
        if book is None or book.state in {"UNINITIALIZED", "GAP", "UNSAFE", "SYNCING"}:
            buffer.append(payload)
            try:
                snapshot = self._depth_snapshots.get(symbol)
                if snapshot is None:
                    snapshot = self._depth_snapshot(symbol)
                    self._depth_snapshots[symbol] = snapshot
                book = LocalOrderBook.synchronize(snapshot, list(buffer))
            except (OrderBookGap, ValueError, RuntimeError, OSError) as exc:
                self.local_books.pop(symbol, None)
                self._depth_snapshots.pop(symbol, None)
                self.depth_buffers[symbol] = buffer[-1000:]
                event = {
                    "symbol": symbol,
                    "event_id": str(uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"bian:orderbook-health:{symbol}:{received_at.isoformat()}:{type(exc).__name__}",
                    )),
                    "event_type": "ORDERBOOK",
                    "source": "binance_futures_diff_depth",
                    "market": "FUTURES",
                    "source_timestamp": received_at.isoformat(),
                    "received_timestamp": received_at.isoformat(),
                    "latency_ms": 0,
                    "price": None,
                    "quantity": None,
                    "direction": None,
                    "metadata": {
                        "health_status": "UNSAFE",
                        "state": "GAP" if isinstance(exc, OrderBookGap) else "UNSAFE",
                        "error_class": type(exc).__name__,
                        "recovery_attempt": 1,
                    },
                }
                self._queue(event)
                self._mark_message(session, received_at, event)
                return event
            if book.state != "VALID":
                self.depth_buffers[symbol] = buffer[-1000:]
                event = {
                    "symbol": symbol,
                    "event_id": str(uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"bian:orderbook-health:{symbol}:{received_at.isoformat()}:SYNCING",
                    )),
                    "event_type": "ORDERBOOK",
                    "source": "binance_futures_diff_depth",
                    "market": "FUTURES",
                    "source_timestamp": received_at.isoformat(),
                    "received_timestamp": received_at.isoformat(),
                    "latency_ms": 0,
                    "price": None,
                    "quantity": None,
                    "direction": None,
                    "metadata": {
                        "health_status": "SYNCING",
                        "state": "SYNCING",
                        "buffered_events": len(buffer),
                    },
                }
                self._queue(event)
                self._mark_message(session, received_at, event)
                return event
            self.local_books[symbol] = book
            self.depth_buffers.pop(symbol, None)
            self._depth_snapshots.pop(symbol, None)
            normalized = _orderbook_event(
                symbol,
                book,
                source_timestamp=_event_datetime(payload.get("E", received_at)),
                received_at=received_at,
                timestamp_semantics="rest_snapshot_plus_diff",
                market="FUTURES",
            )
            event = None if normalized is None else normalized[1]
            if event is not None:
                self._queue(event)
                self._mark_message(session, received_at, event)
            return event
        try:
            applied = book.apply_diff(payload)
        except OrderBookGap as exc:
            book._invalidate(payload, "OrderBookGap")
            self.local_books.pop(symbol, None)
            self._depth_snapshots.pop(symbol, None)
            self.depth_buffers[symbol] = []
            event = {
                "symbol": symbol,
                "event_id": str(uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"bian:orderbook-health:{symbol}:{received_at.isoformat()}:OrderBookGap",
                )),
                "event_type": "ORDERBOOK",
                "source": "binance_futures_diff_depth",
                "market": "FUTURES",
                "source_timestamp": received_at.isoformat(),
                "received_timestamp": received_at.isoformat(),
                "latency_ms": 0,
                "price": None,
                "quantity": None,
                "direction": None,
                "metadata": {
                    "health_status": "GAP",
                    "state": "GAP",
                    "error_class": type(exc).__name__,
                    "received_U": payload.get("U"),
                    "received_u": payload.get("u"),
                },
            }
            self._queue(event)
            self._mark_message(session, received_at, event)
            return event
        if not applied:
            return None
        normalized = _orderbook_event(
            symbol,
            book,
            source_timestamp=_event_datetime(payload.get("E", received_at)),
            received_at=received_at,
            timestamp_semantics="exchange_event",
            market="FUTURES",
        )
        event = None if normalized is None else normalized[1]
        if event is not None:
            self._queue(event)
            self._mark_message(session, received_at, event)
        return event

    async def _flush_loop(self) -> None:
        while not self._stopped:
            await self._sleep(self.flush_sec)
            await self.flush_once()

    async def flush_once(self) -> None:
        now = self._now()
        self.inspect_idle(now)
        events = self.build_flush_events(now)
        markets = list(self.pending_markets.values())
        self.pending_events.clear()
        self.pending_observations.clear()
        self.pending_markets.clear()
        report = _stream_report(markets, market="FUTURES")
        report["events"] = events
        report["channel_health"] = self.channel_health()
        report["transport_health"] = self.transport_health()
        try:
            await asyncio.to_thread(self._persist, report, self.dsn)
            if self.global_transport_health != "HALT":
                self.global_transport_health = self.transport_health()["global_transport_health"]
        except Exception as exc:
            self.global_transport_health = "HALT"
            LOGGER.exception(
                "futures stream persistence failed",
                extra={
                    "collection_kind": "stream_trade",
                    "run_id": report["run_id"],
                    "event_count": len(events),
                    "error_class": type(exc).__name__,
                },
            )
            try:
                await asyncio.to_thread(
                    record_collection_failure,
                    report["run_id"],
                    "stream_trade",
                    _persistence_error_code(exc),
                    dsn=self.dsn,
                )
            except Exception:
                pass
            try:
                from trading_store import TradingStore

                TradingStore(dsn=self.dsn).set_halt(
                    True,
                    reason=f"GLOBAL_HALT:{type(exc).__name__}",
                    source="futures_stream",
                    mode="paper",
                )
            except Exception:
                pass

    async def _run_channel(self, session: ChannelSession) -> None:
        while not self._stopped and session.state != "STOPPED":
            session.state = "STARTING" if session.reconnect_count == 0 else "RECONNECTING"
            session.connection_attempts += 1
            try:
                await self._connect_and_consume(session)
            except asyncio.CancelledError:
                session.state = "STOPPED"
                raise
            except Exception as exc:
                self._mark_disconnected(session, exc)
                if session.consecutive_failures > self._max_reconnects:
                    session.state = "FAILED"
                    return
                session.state = "RECONNECTING"
                session.reconnect_count += 1
                delay = RECONNECT_BACKOFF_SEC[
                    min(session.reconnect_count - 1, len(RECONNECT_BACKOFF_SEC) - 1)
                ]
                await self._sleep(float(delay))

    async def _connect_and_consume(self, session: ChannelSession) -> None:
        from binance_client import BinanceConnectionError

        ws = await self._connect(futures_public_ws_url())
        try:
            self._sockets[session.channel] = ws
            session.connected_at = session.connected_at or self._now()
            session.proxy_mode = "CONFIGURED" if _http_proxy() else "DIRECT"
            self._request_id += 1
            request_id = self._request_id
            params = futures_stream_params(session.channel, session.symbols)
            await ws.send(json.dumps({
                "method": "SUBSCRIBE",
                "params": params,
                "id": request_id,
            }))
            subscribed = False
            pre_ack: list[dict[str, Any]] = []
            deadline = time.monotonic() + 10.0
            while not subscribed:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BinanceConnectionError("subscribe_timeout")
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                payload = _ws_payload(raw)
                if payload.get("id") == request_id:
                    if payload.get("error"):
                        session.state = "FAILED"
                        raise BinanceConnectionError("subscribe_failed")
                    self._mark_subscribed(session)
                    break
                if payload.get("data") or payload.get("e"):
                    pre_ack.append(payload)
            for payload in pre_ack:
                data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                if isinstance(data, dict):
                    self.handle_payload(session.channel, data)
            if session.channel == "DEPTH":
                for symbol in self.storage_symbols:
                    self.depth_buffers.setdefault(symbol, [])
            while not self._stopped:
                idle_stale = self.inspect_idle()
                if session.channel in idle_stale or session.state == "STALE":
                    raise BinanceConnectionError("channel_stale")
                timeout = (
                    5.0
                    if session.channel == "LIQUIDATION"
                    else max(1.0, float(channel_idle_sec(session.channel)))
                )
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    if session.channel == "LIQUIDATION":
                        continue
                    if session.channel in self.inspect_idle():
                        raise BinanceConnectionError("channel_stale")
                    continue
                payload = _ws_payload(raw)
                data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                if not isinstance(data, dict):
                    continue
                if payload.get("id") is not None and "result" in payload:
                    continue
                self.handle_payload(session.channel, data)
        finally:
            self._sockets.pop(session.channel, None)
            closer = getattr(ws, "close", None)
            if closer is not None:
                try:
                    result = closer()
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    pass


    async def run(self) -> None:
        self.start()
        tasks = [
            asyncio.create_task(self._run_channel(session), name=f"futures-{channel}")
            for channel, session in self.sessions.items()
        ]
        flush_task = asyncio.create_task(self._flush_loop())
        reconnect_task = None
        if self._controlled_reconnect_after is not None:
            reconnect_task = asyncio.create_task(self._controlled_reconnect_once())
        try:
            await asyncio.gather(*tasks)
        finally:
            try:
                await self.flush_once()
            except Exception:
                LOGGER.exception("final futures stream flush failed")
            self._stopped = True
            if reconnect_task is not None:
                reconnect_task.cancel()
            flush_task.cancel()
            await asyncio.gather(flush_task, *tasks, *((reconnect_task,) if reconnect_task is not None else ()), return_exceptions=True)

    async def _controlled_reconnect_once(self) -> None:
        await self._sleep(float(self._controlled_reconnect_after or 0))
        if self._stopped:
            return
        channel = "TRADE" if "TRADE" in self.sessions else next(iter(self.sessions), None)
        if channel:
            await self.request_reconnect(channel, reason="controlled_reconnect")




async def _shutdown_feed_handler(handler: Any, loop: asyncio.AbstractEventLoop) -> None:
    """Close Cryptofeed resources without nesting the active event loop."""
    await handler.stop_async(loop=loop)


async def stream(
    symbols: list[str],
    *,
    flush_sec: float,
    dsn: str | None = None,
    market: str = "FUTURES",
    controlled_reconnect_after: float | None = None,
) -> None:
    """Maintain a public ticker stream and persist the latest batch per interval."""
    if market.upper() == "FUTURES":
        await FuturesStreamSupervisor(
            list(symbols),
            flush_sec=flush_sec,
            dsn=dsn,
            controlled_reconnect_after=controlled_reconnect_after,
        ).run()
        return
    pending: dict[str, dict[str, Any]] = {}
    pending_events: list[dict[str, Any]] = []
    pending_observations: dict[tuple[str, str], dict[str, Any]] = {}
    local_books: dict[str, LocalOrderBook] = {}
    depth_buffers: dict[str, list[dict[str, Any]]] = {}
    last_queued = time.monotonic()

    def mark_queued() -> None:
        nonlocal last_queued
        last_queued = time.monotonic()

    async def on_trade(trade: Any, receipt_timestamp: float) -> None:
        normalized = _stream_market(trade, receipt_timestamp, market=market)
        if normalized is None:
            return
        symbol, snapshot = normalized
        pending[symbol] = snapshot
        event = snapshot.get("market_data_event")
        if event:
            pending_events.append(event)
            mark_queued()

    async def on_ticker(ticker: Any, receipt_timestamp: float) -> None:
        normalized = _stream_book_ticker(
            ticker, receipt_timestamp, market=market
        )
        if normalized is not None:
            _, event = normalized
            _queue_orderbook_observation(
                pending_events, pending_observations, event
            )
            mark_queued()
            mark_queued()

    async def on_book(book: Any, receipt_timestamp: float) -> None:
        raw = getattr(book, "raw", None)
        if not isinstance(raw, dict):
            return
        standard_symbol = str(book.symbol)

        async def resynchronize() -> tuple[str, dict[str, Any]] | None:
            buffered = depth_buffers.setdefault(standard_symbol, [])
            if "U" in raw and "u" in raw:
                buffered.append(raw)
            try:
                local_book = await asyncio.to_thread(
                    _resynchronize_local_order_book,
                    standard_symbol,
                    list(buffered),
                    local_books,
                    market=market,
                )
            except (OrderBookGap, ValueError, RuntimeError, OSError) as exc:
                local_books.pop(standard_symbol, None)
                LOGGER.warning(
                    "%s order book resynchronization failed: %s",
                    market.lower(),
                    type(exc).__name__,
                )
                received_at = _event_datetime(receipt_timestamp)
                return standard_symbol, {
                    "symbol": _storage_symbol(standard_symbol),
                    "event_id": str(uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"bian:orderbook-health:{standard_symbol}:{received_at.isoformat()}:{type(exc).__name__}",
                    )),
                    "event_type": "ORDERBOOK",
                    "source": "binance_futures_diff_depth",
                    "market": "FUTURES",
                    "source_timestamp": received_at.isoformat(),
                    "received_timestamp": received_at.isoformat(),
                    "latency_ms": 0,
                        "metadata": {
                        "health_status": "UNSAFE",
                        "state": "UNSAFE",
                        "reason": type(exc).__name__,
                        "error_class": type(exc).__name__,
                        "recovery_attempt": 1,
                    },
                }
            depth_buffers.pop(standard_symbol, None)
            received_at = _event_datetime(receipt_timestamp)
            source_timestamp = _event_datetime(raw.get("E", receipt_timestamp))
            semantics = (
                "rest_snapshot_plus_diff"
                if "U" in raw and "u" in raw
                else "rest_snapshot"
            )
            return _orderbook_event(
                standard_symbol,
                local_book,
                source_timestamp=source_timestamp,
                received_at=received_at,
                timestamp_semantics=semantics,
                market=market,
            )

        if standard_symbol not in local_books or "lastUpdateId" in raw:
            normalized = await resynchronize()
        else:
            normalized = _stream_orderbook(
                book,
                receipt_timestamp,
                raw=raw,
                books=local_books,
                market=market,
            )
            if normalized is None and standard_symbol not in local_books:
                normalized = await resynchronize()
        if normalized is not None:
            _, event = normalized
            _queue_orderbook_observation(
                pending_events, pending_observations, event
            )

    async def flush() -> None:
        while True:
            await asyncio.sleep(flush_sec)
            if not pending_events and not pending_observations:
                continue
            markets = list(pending.values())
            events = [*pending_events, *pending_observations.values()]
            pending.clear()
            pending_events.clear()
            pending_observations.clear()
            report = _stream_report(markets, market=market)
            report["events"] = events
            try:
                await asyncio.to_thread(persist, report, dsn)
            except Exception as exc:
                LOGGER.exception(
                    "stream market persistence failed",
                    extra={
                        "collection_kind": "stream_trade",
                        "run_id": report["run_id"],
                        "event_count": len(events),
                    },
                )
                try:
                    await asyncio.to_thread(
                        record_collection_failure,
                        report["run_id"],
                        "stream_trade",
                        _persistence_error_code(exc),
                        dsn=dsn,
                    )
                except Exception:
                    pass

    handler = _build_stream_handler(
        symbols,
        on_trade,
        ticker_callback=on_ticker,
        book_callback=on_book,
    )
    loop = asyncio.get_running_loop()
    flush_task = asyncio.create_task(flush())
    handler.run(start_loop=False, install_signal_handlers=False)
    try:
        idle_sec = _int_env("BIAN_WS_IDLE_RESTART_SEC", 30, minimum=5)
        started_at = time.monotonic()
        while True:
            await asyncio.sleep(min(5.0, float(idle_sec)))
            health = getattr(handler, "bian_stream_health", None)
            if not health:
                if market.upper() != "FUTURES":
                    continue
                raise RuntimeError("websocket channels missing")
            now = datetime.now(timezone.utc)
            live_channels = [
                name
                for name in health
                if str(name).upper() in {"TRADES", "TICKER", "L2_BOOK"}
            ]
            if market.upper() == "FUTURES" and not live_channels:
                raise RuntimeError("websocket live channels missing")
            if time.monotonic() - last_queued > idle_sec:
                if time.monotonic() - started_at < idle_sec * 2:
                    continue
                raise RuntimeError("websocket idle:no_queued_events")
    finally:
        flush_task.cancel()
        try:
            await asyncio.wait_for(flush_task, timeout=5.0)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        try:
            await asyncio.wait_for(_shutdown_feed_handler(handler, loop), timeout=8.0)
        except Exception:
            LOGGER.warning(
                "websocket shutdown failed",
                extra={"market": market, "error_class": "shutdown_timeout"},
            )


async def stream_futures_liquidations(
    symbols: list[str],
    *,
    flush_sec: float,
    dsn: str | None = None,
) -> None:
    """Persist public Futures liquidations without starting Futures execution."""
    await FuturesStreamSupervisor(
        list(symbols),
        flush_sec=flush_sec,
        dsn=dsn,
        channels=("LIQUIDATION",),
    ).run()


def _candidate_stream_symbols(
    report: dict[str, Any], *, fallback_symbols: list[str]
) -> list[str]:
    """Select Tier 2 symbols from a persisted Tier 1 scanner report.

    First-phase observation is BTC, ETH, and BNB only. Scanner extras such as
    SOL or ADA cannot expand the stream set. The returned exchange symbols are
    stable and deduplicated so an observer only reconnects streams when the
    actual candidate set changes.
    """
    from trade_intent import is_canonical_futures_symbol

    scanner = report.get("universe") or {}
    scanned = scanner.get("candidate_symbols") if isinstance(scanner, dict) else []
    raw_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", *(scanned or [])]
    raw_symbols.extend(symbol.replace("-", "") for symbol in fallback_symbols)
    normalized: list[str] = []
    for raw_symbol in raw_symbols:
        symbol = str(raw_symbol).upper().replace("-", "").strip()
        if not is_canonical_futures_symbol(symbol):
            continue
        stream_symbol = f"{symbol[:-4]}-USDT"
        if stream_symbol not in normalized:
            normalized.append(stream_symbol)
    return normalized or ["BTC-USDT", "ETH-USDT", "BNB-USDT"]


def collect_positioning_observations(
    *,
    fallback_symbols: list[str],
    candidate_limit: int,
    dsn: str | None = None,
) -> list[str]:
    """Persist one public scanner/Futures observation cycle and return Tier 2.

    This is intentionally the only orchestration point for the public market
    observer. It has no access to private clients, engine decisions, Risk, or
    Execution.
    """
    normalized_limit = max(1, min(candidate_limit, 100))
    scanner_run_id = str(uuid.uuid4())
    try:
        scanner_report = collect(limit=normalized_limit, run_id=scanner_run_id)
        persist(scanner_report, dsn)
    except Exception as exc:
        LOGGER.exception(
            "futures scanner observation failed",
            extra={"collection_kind": "positioning_observe_scanner", "run_id": scanner_run_id},
        )
        try:
            record_collection_failure(
                scanner_run_id,
                "positioning_observe_scanner",
                _collection_error_code(exc),
                dsn=dsn,
            )
        except Exception:
            pass
        raise

    candidate_symbols = _candidate_stream_symbols(
        scanner_report, fallback_symbols=fallback_symbols
    )
    futures_run_id = str(uuid.uuid4())
    try:
        futures_report = collect_futures_observations(
            [symbol.replace("-", "") for symbol in candidate_symbols],
            run_id=futures_run_id,
        )
        persist(futures_report, dsn)
    except Exception as exc:
        LOGGER.exception(
            "futures positioning observation failed",
            extra={
                "collection_kind": "positioning_observe_futures",
                "run_id": futures_run_id,
            },
        )
        try:
            record_collection_failure(
                futures_run_id,
                "positioning_observe_futures",
                _collection_error_code(exc),
                dsn=dsn,
            )
        except Exception:
            pass
        raise
    return candidate_symbols


async def _stop_observation_streams(tasks: list[asyncio.Task[None]]) -> None:
    for task in tasks:
        task.cancel()
    if tasks:
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=15.0,
            )
        except Exception:
            pass


async def observe(
    fallback_symbols: list[str],
    *,
    candidate_limit: int,
    refresh_sec: float,
    flush_sec: float,
    dsn: str | None = None,
    duration_sec: float | None = None,
    controlled_reconnect_after: float | None = None,
) -> None:
    """Continuously observe candidates without enabling any trading decision.

    Tier 1 scanner/Futures observations are refreshed on a bounded cadence.
    Tier 2 Futures trade/book/mark/index/liquidation streams are restarted
    only when the selected candidate universe materially changes.
    """
    from trade_intent import is_canonical_futures_symbol

    fallback = [
        symbol for symbol in dict.fromkeys(fallback_symbols)
        if is_canonical_futures_symbol(symbol.replace("-", ""))
    ] or ["BTC-USDT", "ETH-USDT", "BNB-USDT"]
    active_symbols: tuple[str, ...] = tuple(fallback)

    def _spawn_stream(target: tuple[str, ...]) -> asyncio.Task[None]:
        return asyncio.create_task(
            stream(
                target,
                flush_sec=flush_sec,
                dsn=dsn,
                market="FUTURES",
                controlled_reconnect_after=controlled_reconnect_after,
            )
        )

    stream_tasks: list[asyncio.Task[None]] = [
        _spawn_stream(active_symbols)
    ]
    deadline = (
        None
        if duration_sec is None
        else time.monotonic() + max(1.0, float(duration_sec))
    )
    try:
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                break
            try:
                collect_task = asyncio.create_task(
                    asyncio.to_thread(
                        collect_positioning_observations,
                        fallback_symbols=fallback,
                        candidate_limit=candidate_limit,
                        dsn=dsn,
                    )
                )
                candidates: list[str] | None = None
                while True:
                    if deadline is not None and time.monotonic() >= deadline:
                        collect_task.cancel()
                        await asyncio.gather(collect_task, return_exceptions=True)
                        candidates = None
                        break
                    streams_failed = any(task.done() for task in stream_tasks)
                    if streams_failed:
                        await _stop_observation_streams(stream_tasks)
                        stream_tasks = [
                            _spawn_stream(active_symbols)
                        ]
                    done, _pending = await asyncio.wait(
                        {collect_task},
                        timeout=5.0,
                    )
                    if collect_task in done:
                        try:
                            candidates = list(collect_task.result())
                        except Exception:
                            candidates = list(active_symbols) or fallback
                        break
                if candidates is None:
                    break
            except Exception:
                # Failure has been persisted with a run ID. Keep existing
                # streams, or use the operator-provided baseline on startup.
                candidates = list(active_symbols) or fallback

            candidate_set = tuple(candidates)
            streams_failed = any(task.done() for task in stream_tasks)
            if candidate_set != active_symbols or streams_failed:
                await _stop_observation_streams(stream_tasks)
                stream_tasks = [
                    _spawn_stream(candidate_set),
                ]
                active_symbols = candidate_set
            sleep_for = max(1.0, refresh_sec)
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                sleep_for = min(sleep_for, remaining)
            slept = 0.0
            while slept < sleep_for:
                if any(task.done() for task in stream_tasks):
                    await _stop_observation_streams(stream_tasks)
                    stream_tasks = [
                        _spawn_stream(active_symbols)
                    ]
                slice_sec = min(5.0, sleep_for - slept)
                await asyncio.sleep(slice_sec)
                slept += slice_sec
    finally:
        await _stop_observation_streams(stream_tasks)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect public Binance market data.")
    parser.add_argument(
        "command", choices=(
            "collect", "stream", "observe", "observe-futures", "stream-futures",
            "measure-storage", "prune-raw-events",
        )
    )
    parser.add_argument("--limit", type=int, default=_int_env("BIAN_MARKET_LIMIT", 20))
    parser.add_argument(
        "--stream-symbols",
        default=os.environ.get("BIAN_STREAM_SYMBOLS", "BTC-USDT,ETH-USDT,BNB-USDT"),
    )
    parser.add_argument(
        "--stream-flush-sec",
        type=float,
        default=_float_env("BIAN_STREAM_FLUSH_SEC", 5.0, minimum=1.0),
    )
    parser.add_argument(
        "--observe-interval-sec",
        type=float,
        default=_float_env("POSITIONING_OBSERVER_INTERVAL_SEC", 60.0, minimum=1.0),
    )
    parser.add_argument(
        "--observe-candidate-limit",
        type=int,
        default=_int_env("POSITIONING_OBSERVER_CANDIDATE_LIMIT", 5),
        help="maximum scanner candidates upgraded to continuous positioning observation",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="stop observe after this many seconds; omit to run until interrupted",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=(
            int(os.environ["POSITIONING_RAW_RETENTION_DAYS"])
            if os.environ.get("POSITIONING_RAW_RETENTION_DAYS", "").isdigit()
            else None
        ),
        help="explicit raw-event retention age; unset means no deletion",
    )
    args = parser.parse_args(argv)

    if args.command in {"measure-storage", "prune-raw-events"}:
        try:
            from trading_store import TradingStore
        except ModuleNotFoundError:
            import sys
            from pathlib import Path

            sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
            from trading_store import TradingStore
        store = TradingStore()
        if args.command == "measure-storage":
            print(json.dumps(store.market_flow_storage_metrics(), default=str))
            return 0
        if args.retention_days is None:
            raise SystemExit(
                "prune-raw-events requires --retention-days after storage measurement"
            )
        deleted = store.prune_raw_market_flow_events(retention_days=args.retention_days)
        print(json.dumps({
            "status": "ok", "deleted_raw_events": deleted,
            "retention_days": args.retention_days,
        }))
        return 0

    if args.command == "stream":
        asyncio.run(
            stream(
                _stream_symbols(args.stream_symbols),
                flush_sec=max(1.0, args.stream_flush_sec),
                market="FUTURES",
            )
        )
        return 0

    if args.command == "stream-futures":
        asyncio.run(
            stream_futures_liquidations(
                _stream_symbols(args.stream_symbols),
                flush_sec=max(1.0, args.stream_flush_sec),
            )
        )
        return 0

    if args.command == "observe":
        asyncio.run(
            observe(
                _stream_symbols(args.stream_symbols),
                candidate_limit=max(1, min(args.observe_candidate_limit, 100)),
                refresh_sec=max(1.0, args.observe_interval_sec),
                flush_sec=max(1.0, args.stream_flush_sec),
                duration_sec=None if args.duration is None else max(1.0, args.duration),
            )
        )
        return 0

    if args.command == "observe-futures":
        run_id = str(uuid.uuid4())
        symbols = [value.replace("-", "") for value in _stream_symbols(args.stream_symbols)]
        try:
            report = collect_futures_observations(symbols, run_id=run_id)
            persist(report)
        except Exception:
            try:
                record_collection_failure(run_id, "futures_observation", "collection_failed")
            except Exception:
                pass
            raise
        print(json.dumps({"status": "ok", "run_id": run_id, "events": len(report["events"])}))
        return 0

    run_id = str(uuid.uuid4())
    try:
        report = collect(limit=max(1, min(args.limit, 100)), run_id=run_id)
        persist(report)
    except Exception:
        try:
            record_collection_failure(run_id, "rest_24h", "collection_failed")
        except Exception:
            pass
        raise
    print(
        json.dumps(
            {
                "status": "ok",
                "run_id": report["run_id"],
                "markets": len(report["markets"]),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
