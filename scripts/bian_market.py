#!/usr/bin/env python3
"""Public Binance market collector for the independent bian bot."""
from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

from database import configured_dsn, ensure_schema

SPOT_HOSTS = (
    "https://api.binance.com/api/v3",
    "https://data-api.binance.vision/api/v3",
)
PRODUCT_ENDPOINTS = {
    "spot": ("https://api.binance.com/api/v3", "/exchangeInfo", "symbols"),
    "perpetual": ("https://fapi.binance.com/fapi/v1", "/exchangeInfo", "symbols"),
    "delivery": ("https://dapi.binance.com/dapi/v1", "/exchangeInfo", "symbols"),
    "options": ("https://eapi.binance.com/eapi/v1", "/exchangeInfo", "optionSymbols"),
}
USER_AGENT = "bian-market/1.0 (+public-read-only)"


def _get_json(url: str, timeout_sec: float = 8.0) -> Any:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code} from {url}") from exc
    except OSError as exc:
        raise RuntimeError(f"request failed for {url}: {exc}") from exc


def _float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ticker_rows(limit: int) -> tuple[list[dict[str, Any]], str]:
    last_error: Exception | None = None
    for host in SPOT_HOSTS:
        url = f"{host}/ticker/24hr"
        try:
            payload = _get_json(url)
            rows = [
                row
                for row in payload
                if isinstance(row, dict)
                and str(row.get("symbol") or "").endswith("USDT")
                and _float(row.get("lastPrice")) is not None
            ]
            rows.sort(key=lambda row: _float(row.get("quoteVolume")) or 0.0, reverse=True)
            return rows[:limit], url
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"all Binance spot hosts failed: {last_error}")


def _coverage() -> list[dict[str, Any]]:
    captured_at = _now()
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
        except Exception as exc:
            rows.append(
                {
                    "product_type": product_type,
                    "status": "unavailable",
                    "symbol_count": None,
                    "source_url": url,
                    "captured_at": captured_at,
                    "detail": {"error": str(exc)[:240]},
                }
            )
    return rows


def collect(limit: int = 20) -> dict[str, Any]:
    captured_at = _now()
    markets, source_url = _ticker_rows(limit)
    return {
        "captured_at": captured_at,
        "source_url": source_url,
        "markets": [
            {
                "symbol": str(row["symbol"]),
                "last_price": _float(row.get("lastPrice")),
                "price_change_percent": _float(row.get("priceChangePercent")),
                "quote_volume": _float(row.get("quoteVolume")),
                "payload": row,
            }
            for row in markets
        ],
        "product_coverage": _coverage(),
    }


def persist(report: dict[str, Any], dsn: str | None = None) -> None:
    import psycopg2

    dsn = dsn or configured_dsn()
    ensure_schema(dsn)
    with psycopg2.connect(dsn, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            for market in report["markets"]:
                cursor.execute(
                    """
                    INSERT INTO bian_market_snapshots(
                        captured_at, symbol, last_price, price_change_percent,
                        quote_volume, source_url, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, CAST(%s AS JSONB))
                    """,
                    (
                        report["captured_at"],
                        market["symbol"],
                        market["last_price"],
                        market["price_change_percent"],
                        market["quote_volume"],
                        report["source_url"],
                        json.dumps(market["payload"]),
                    ),
                )
            for coverage in report["product_coverage"]:
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
                        coverage["product_type"],
                        coverage["status"],
                        coverage["symbol_count"],
                        coverage["source_url"],
                        coverage["captured_at"],
                        json.dumps(coverage["detail"]),
                    ),
                )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect public Binance market data.")
    parser.add_argument("command", choices=("collect",))
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args(argv)
    report = collect(limit=max(1, min(args.limit, 100)))
    persist(report)
    print(json.dumps({"status": "ok", "markets": len(report["markets"])}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
