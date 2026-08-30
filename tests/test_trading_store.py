from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from trading_store import TradingStore


def _rows(now: datetime):
    received = now - timedelta(seconds=1)
    timestamp = received - timedelta(milliseconds=20)
    return [
        (
            "BTCUSDT", "FUTURES", "MARK_INDEX_FUNDING", timestamp, received,
            20, {"source": "binance_futures_mark_price_stream"},
        ),
        (
            "BTCUSDT", "FUTURES", "ORDERBOOK", timestamp, received,
            20, {"source": "binance_futures_diff_depth", "health_status": "GAP"},
        ),
    ]


def test_market_data_freshness_reports_missing_and_gap_per_required_source():
    now = datetime.now(timezone.utc)
    cursor = MagicMock()
    cursor.fetchall.return_value = _rows(now)
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor

    with patch("psycopg2.connect", return_value=connection), patch(
        "trading_store._now", return_value=now
    ):
        rows = TradingStore("postgresql://test").market_data_freshness(
            max_age_sec=60, symbols=["BTC-USDT"]
        )

    by_source = {row["source"]: row for row in rows}
    assert by_source["FUTURES_MARK_PRICE"]["status"] == "FRESH"
    assert by_source["FUTURES_INDEX_PRICE"]["status"] == "FRESH"
    assert by_source["FUTURES_FUNDING"]["status"] == "FRESH"
    assert by_source["FUTURES_DEPTH"]["status"] == "GAP"
    assert by_source["FUTURES_TRADE"]["status"] == "MISSING"
    assert by_source["FUTURES_TRADE"]["symbol"] == "BTCUSDT"
