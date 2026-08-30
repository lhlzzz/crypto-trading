#!/usr/bin/env python3
"""Public Binance market collector for the independent bian bot."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
import sys
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable


try:
    from database import configured_dsn, ensure_schema, record_collection_failure
except ModuleNotFoundError:
    from scripts.database import configured_dsn, ensure_schema, record_collection_failure

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


def _get_json(
    url: str,
    timeout_sec: float = 8.0,
    attempts: int | None = None,
) -> Any:
    """Fetch JSON with bounded retry for transient public API failures."""
    attempts = attempts or _int_env("BIAN_HTTP_ATTEMPTS", 3)
    backoff_sec = _float_env("BIAN_HTTP_BACKOFF_SEC", 0.25, minimum=0.0)
    last_error: Exception | None = None

    for attempt in range(attempts):
        request = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_sec) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_error = RuntimeError(f"HTTP {exc.code} from {url}")
            retry_after = exc.headers.get("Retry-After")
            retryable = exc.code == 429 or 500 <= exc.code < 600
            if retry_after and retry_after.isdecimal():
                delay = float(retry_after)
            else:
                delay = backoff_sec * (2**attempt)
            if not retryable:
                break
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            last_error = RuntimeError(f"request failed for {url}: {exc}")
            delay = backoff_sec * (2**attempt)

        if attempt + 1 < attempts and delay:
            time.sleep(delay)

    raise last_error or RuntimeError(f"request failed for {url}")


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
        market=str(event.get("market", "SPOT")).upper(),
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
    for event in sorted(events, key=lambda item: _event_datetime(item["event_timestamp"])):
        event_time = _event_datetime(event["event_timestamp"])
        received_time = _event_datetime(event.get("received_timestamp", event_time))
        if cutoff is not None and (event_time > cutoff or received_time > cutoff):
            continue
        quantity = _decimal(event.get("quantity")) or Decimal("0")
        metadata = event.get("metadata") or {}
        embedded = metadata.get("metadata") if isinstance(metadata, dict) else None
        buyer_maker = event.get(
            "buyer_maker",
            metadata.get("buyer_maker", embedded.get("buyer_maker") if isinstance(embedded, dict) else None),
        )
        if bool(buyer_maker):
            sell += quantity
        else:
            buy += quantity
    return TradeFlowAggregate(buy_volume=buy, sell_volume=sell, cvd=buy - sell)


FLOW_WINDOWS: tuple[tuple[str, int], ...] = (
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
    """Minimal Binance snapshot + diff-depth book with gap fail-closed behavior."""

    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    last_update_id: int | None = None
    liquidity_added: Decimal = Decimal("0")
    liquidity_removed: Decimal = Decimal("0")
    snapshot_update_id: int | None = None
    snapshot_sync_origin: str = "UNKNOWN"
    snapshot_received_timestamp: datetime | None = None

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
            raise OrderBookGap("buffered depth events do not bridge snapshot")
        for event in applicable:
            book.apply_diff(event)
        return book

    def apply_diff(self, event: dict[str, Any]) -> bool:
        first = int(event["U"])
        last = int(event["u"])
        if self.last_update_id is None:
            raise OrderBookGap("book must be initialized from a REST snapshot")
        if last <= self.last_update_id:
            return False
        if first > self.last_update_id + 1:
            raise OrderBookGap(
                f"depth gap: expected {self.last_update_id + 1}, got {first}"
            )
        previous_final = event.get("pu")
        if previous_final is not None and int(previous_final) != self.last_update_id:
            raise OrderBookGap(
                f"depth bridge gap: expected pu={self.last_update_id}, got {previous_final}"
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
        return True

    def features(self, mid_price: Decimal | None = None) -> dict[str, Decimal]:
        if not self.bids or not self.asks:
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
    normalized_symbol = symbol.replace("-", "").upper()
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
    """
    allowlist = {
        item.strip().upper()
        for item in os.environ.get("MEME_ALLOWLIST", "").split(",")
        if item.strip()
    }
    blocklist = {
        item.strip().upper()
        for item in os.environ.get("MEME_BLOCKLIST", "").split(",")
        if item.strip()
    }
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
            if symbol in blocklist or (allowlist and symbol not in allowlist):
                tier = "BLOCK"
            else:
                from risk import classify_meme_risk_tier
                liquidity_score = (
                    quote_volume / maximum_quote_volume
                    if maximum_quote_volume > 0 else None
                )
                tier = classify_meme_risk_tier(
                    liquidity_score=liquidity_score,
                    data_quality_score=Decimal("1"),
                    open_interest=_decimal(row.get("openInterest")),
                    trading=str(row.get("status", "TRADING")).upper() == "TRADING",
                )
            row["market"] = "FUTURES"
            row["contract_type"] = row.get("contractType", "PERPETUAL")
            row["quote_asset"] = row.get("quoteAsset", "USDT")
            row["base_asset"] = row.get("baseAsset") or symbol[:-4]
            row["volume"] = _decimal(row.get("volume"))
            row["quote_volume"] = quote_volume
            row["last_price"] = _decimal(row.get("lastPrice"))
            row["open_interest"] = _decimal(row.get("openInterest"))
            row["meme_risk_tier"] = tier
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
    candidates = sorted(
        normalized,
        key=lambda item: (item[2], abs(item[1]), str(item[0].get("symbol", ""))),
        reverse=True,
    )[:max(1, candidate_limit)]
    tier_rows = {
        tier: [str(row["symbol"]).upper() for row, _, _ in normalized if row.get("meme_risk_tier") == tier]
        for tier in ("BLOCK", "OBSERVE", "REDUCED", "TRADEABLE")
    }
    tradeable_candidates = [
        row for row, _, _ in candidates if row.get("meme_risk_tier") in {"TRADEABLE", "REDUCED"}
    ][: max(1, candidate_limit)]
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
            "candidate_symbols": [str(row["symbol"]).upper() for row in tradeable_candidates],
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
    candidate_symbols.update({"BTCUSDT", "ETHUSDT"})
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
        "market": "SPOT",
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
            from binance_client import FuturesPublicClient
        except ModuleNotFoundError:
            import sys
            from pathlib import Path

            sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
            from binance_client import FuturesPublicClient

        client = FuturesPublicClient()
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
                        price_change_percent, quote_volume, source_url, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB))
                    ON CONFLICT (run_id, symbol) WHERE run_id IS NOT NULL
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
                        json.dumps(market["payload"]),
                    ),
                )
            for event in events:
                cursor.execute(
                    """
                    INSERT INTO market_flow_events(
                        event_id, symbol, market, event_type, event_timestamp,
                        received_timestamp, latency_ms, price, quantity, notional,
                        direction, metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              CAST(%s AS JSONB))
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (
                        event["event_id"], event["symbol"] if "symbol" in event else "",
                        event.get("market", "SPOT"), event["event_type"],
                        event["source_timestamp"], event["received_timestamp"],
                        event["latency_ms"], event.get("price"), event.get("quantity"),
                        event.get("notional"), event.get("direction"),
                        json.dumps(event),
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
                            event["latency_ms"], json.dumps(event),
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
                        json.dumps(item["detail"]),
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


def _stream_report(markets: list[dict[str, Any]]) -> dict[str, Any]:
    captured_at = _now()
    return {
        "run_id": str(uuid.uuid4()),
        "collection_kind": "stream_trade",
        "captured_at": captured_at,
        "source_url": "wss://stream.binance.com:9443",
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
    event = {
        "symbol": standard_symbol.replace("-", ""),
        "event_id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"bian:trade:{standard_symbol}:{source_timestamp.isoformat()}:{price}:{quantity}",
        )),
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
            "symbol": standard_symbol.replace("-", ""),
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
        "symbol": standard_symbol.replace("-", ""),
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
    symbol = standard_symbol.replace("-", "")
    received_at = _event_datetime(receipt_timestamp)
    if "lastUpdateId" in raw:
        local_book = LocalOrderBook.from_snapshot(raw)
        books[standard_symbol] = local_book
        source_timestamp = received_at
        timestamp_semantics = "receipt_observed_snapshot"
    else:
        local_book = books.get(standard_symbol)
        if local_book is None:
            return None
        try:
            local_book.apply_diff(raw)
        except OrderBookGap:
            books.pop(standard_symbol, None)
            return None
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
    if not features:
        return None
    symbol = standard_symbol.replace("-", "")
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
            "bestBid": str(best_bid),
            "bestAsk": str(best_ask),
            "lastUpdateId": local_book.last_update_id,
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
        "symbol": standard_symbol.replace("-", ""),
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
        "symbol": standard_symbol.replace("-", ""),
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


def _build_futures_stream_handler(
    symbols: list[str],
    callback: Callable[[Any, float], Awaitable[None]] | None = None,
    *,
    ticker_callback: Callable[[Any, float], Awaitable[None]] | None = None,
    book_callback: Callable[[Any, float], Awaitable[None]] | None = None,
    funding_callback: Callable[[Any, float], Awaitable[None]] | None = None,
    liquidation_callback: Callable[[Any, float], Awaitable[None]] | None = None,
) -> Any:
    from cryptofeed import FeedHandler
    from cryptofeed.defines import FUNDING, L2_BOOK, LIQUIDATIONS, TICKER, TRADES
    from cryptofeed.exchanges import BinanceFutures

    channels: list[str] = []
    callbacks: dict[str, Callable[..., Awaitable[None]]] = {}
    if callback is not None:
        channels.append(TRADES)
        callbacks[TRADES] = callback
    if ticker_callback is not None:
        channels.append(TICKER)
        callbacks[TICKER] = ticker_callback
    if book_callback is not None:
        channels.append(L2_BOOK)
        callbacks[L2_BOOK] = book_callback
    if funding_callback is not None:
        channels.append(FUNDING)
        callbacks[FUNDING] = funding_callback
    if liquidation_callback is not None:
        channels.append(LIQUIDATIONS)
        callbacks[LIQUIDATIONS] = liquidation_callback
    if not channels:
        raise ValueError("at least one Futures public channel is required")
    handler = FeedHandler()
    handler.add_feed(
        BinanceFutures(
            symbols=symbols,
            channels=channels,
            callbacks=callbacks,
            retries=-1,
            timeout=60,
            http_proxy=_http_proxy(),
        )
    )
    return handler


async def _shutdown_feed_handler(handler: Any, loop: asyncio.AbstractEventLoop) -> None:
    """Close Cryptofeed resources without nesting the active event loop."""
    await handler.stop_async(loop=loop)


async def stream(
    symbols: list[str],
    *,
    flush_sec: float,
    dsn: str | None = None,
    market: str = "SPOT",
) -> None:
    """Maintain a public ticker stream and persist the latest batch per interval."""
    pending: dict[str, dict[str, Any]] = {}
    pending_events: list[dict[str, Any]] = []
    pending_observations: dict[tuple[str, str], dict[str, Any]] = {}
    local_books: dict[str, LocalOrderBook] = {}
    depth_buffers: dict[str, list[dict[str, Any]]] = {}

    async def on_trade(trade: Any, receipt_timestamp: float) -> None:
        normalized = _stream_market(trade, receipt_timestamp, market=market)
        if normalized is None:
            return
        symbol, snapshot = normalized
        pending[symbol] = snapshot
        event = snapshot.get("market_data_event")
        if event:
            pending_events.append(event)

    async def on_ticker(ticker: Any, receipt_timestamp: float) -> None:
        normalized = _stream_book_ticker(
            ticker, receipt_timestamp, market=market
        )
        if normalized is not None:
            _, event = normalized
            pending_observations[(event["symbol"], event["event_type"])] = event

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
                return None
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
            pending_observations[(event["symbol"], event["event_type"])] = event

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
            report = _stream_report(markets)
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

    if market.upper() == "FUTURES":
        async def on_funding(funding: Any, receipt_timestamp: float) -> None:
            normalized = _stream_funding(funding, receipt_timestamp)
            if normalized is not None:
                _, event = normalized
                pending_observations[(event["symbol"], event["event_type"])] = event

        async def on_liquidation(liquidation: Any, receipt_timestamp: float) -> None:
            normalized = _stream_liquidation(liquidation, receipt_timestamp)
            if normalized is not None:
                _, event = normalized
                pending_events.append(event)

        handler = _build_futures_stream_handler(
            symbols,
            on_trade,
            ticker_callback=on_ticker,
            book_callback=on_book,
            funding_callback=on_funding,
            liquidation_callback=on_liquidation,
        )
    else:
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
        await asyncio.Future()
    finally:
        flush_task.cancel()
        try:
            await flush_task
        except asyncio.CancelledError:
            pass
        await _shutdown_feed_handler(handler, loop)


async def stream_futures_liquidations(
    symbols: list[str],
    *,
    flush_sec: float,
    dsn: str | None = None,
) -> None:
    """Persist public Futures liquidations without starting Futures execution."""
    pending_events: list[dict[str, Any]] = []

    async def on_liquidation(liquidation: Any, receipt_timestamp: float) -> None:
        normalized = _stream_liquidation(liquidation, receipt_timestamp)
        if normalized is not None:
            _, event = normalized
            pending_events.append(event)

    async def flush() -> None:
        while True:
            await asyncio.sleep(flush_sec)
            if not pending_events:
                continue
            events = list(pending_events)
            pending_events.clear()
            report = _stream_report([])
            report["collection_kind"] = "stream_futures_liquidation"
            report["source_url"] = "wss://fstream.binance.com"
            report["events"] = events
            try:
                await asyncio.to_thread(persist, report, dsn)
            except Exception as exc:
                LOGGER.exception(
                    "futures liquidation persistence failed",
                    extra={
                        "collection_kind": "stream_futures_liquidation",
                        "run_id": report["run_id"],
                        "event_count": len(events),
                    },
                )
                try:
                    await asyncio.to_thread(
                        record_collection_failure,
                        report["run_id"],
                        "stream_futures_liquidation",
                        _persistence_error_code(exc),
                        dsn=dsn,
                    )
                except Exception:
                    pass

    handler = _build_futures_stream_handler(
        symbols, liquidation_callback=on_liquidation
    )
    loop = asyncio.get_running_loop()
    flush_task = asyncio.create_task(flush())
    handler.run(start_loop=False, install_signal_handlers=False)
    try:
        await asyncio.Future()
    finally:
        flush_task.cancel()
        try:
            await flush_task
        except asyncio.CancelledError:
            pass
        await _shutdown_feed_handler(handler, loop)


def _candidate_stream_symbols(
    report: dict[str, Any], *, fallback_symbols: list[str]
) -> list[str]:
    """Select Tier 2 symbols from a persisted Tier 1 scanner report.

    BTC and ETH remain explicit benchmarks even if a scanner response is
    incomplete. The returned exchange symbols are stable and deduplicated so
    an observer only reconnects streams when the actual candidate set changes.
    """
    scanner = report.get("universe") or {}
    scanned = scanner.get("candidate_symbols") if isinstance(scanner, dict) else []
    raw_symbols = ["BTCUSDT", "ETHUSDT", *(scanned or [])]
    raw_symbols.extend(symbol.replace("-", "") for symbol in fallback_symbols)
    normalized: list[str] = []
    for raw_symbol in raw_symbols:
        symbol = str(raw_symbol).upper().replace("-", "").strip()
        if not symbol.endswith("USDT") or len(symbol) <= 4:
            continue
        stream_symbol = f"{symbol[:-4]}-USDT"
        if stream_symbol not in normalized:
            normalized.append(stream_symbol)
    return normalized or list(fallback_symbols)


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
    spot_run_id = str(uuid.uuid4())
    try:
        spot_report = collect(limit=normalized_limit, run_id=spot_run_id)
        persist(spot_report, dsn)
    except Exception as exc:
        LOGGER.exception(
            "spot positioning observation failed",
            extra={"collection_kind": "positioning_observe_spot", "run_id": spot_run_id},
        )
        try:
            record_collection_failure(
                spot_run_id,
                "positioning_observe_spot",
                _collection_error_code(exc),
                dsn=dsn,
            )
        except Exception:
            pass
        raise

    candidate_symbols = _candidate_stream_symbols(
        spot_report, fallback_symbols=fallback_symbols
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
        await asyncio.gather(*tasks, return_exceptions=True)


async def observe(
    fallback_symbols: list[str],
    *,
    candidate_limit: int,
    refresh_sec: float,
    flush_sec: float,
    dsn: str | None = None,
) -> None:
    """Continuously observe candidates without enabling any trading decision.

    Tier 1 scanner/Futures observations are refreshed on a bounded cadence.
    Tier 2 Futures trade/book/mark/index/liquidation streams are restarted
    only when the selected candidate universe materially changes.
    """
    fallback = list(dict.fromkeys(fallback_symbols))
    active_symbols: tuple[str, ...] = ()
    stream_tasks: list[asyncio.Task[None]] = []
    try:
        while True:
            try:
                candidates = await asyncio.to_thread(
                    collect_positioning_observations,
                    fallback_symbols=fallback,
                    candidate_limit=candidate_limit,
                    dsn=dsn,
                )
            except Exception:
                # Failure has been persisted with a run ID. Keep existing
                # streams, or use the operator-provided baseline on startup.
                candidates = list(active_symbols) or fallback

            candidate_set = tuple(candidates)
            streams_failed = any(task.done() for task in stream_tasks)
            if candidate_set != active_symbols or streams_failed:
                await _stop_observation_streams(stream_tasks)
                stream_tasks = [
                    asyncio.create_task(
                        stream(
                            candidate_set,
                            flush_sec=flush_sec,
                            dsn=dsn,
                            market="FUTURES",
                        )
                    ),
                ]
                active_symbols = candidate_set
            await asyncio.sleep(max(1.0, refresh_sec))
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
        default=os.environ.get("BIAN_STREAM_SYMBOLS", "BTC-USDT,ETH-USDT"),
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
