from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import UUID

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
    assert by_source["FUTURES_FUNDING_LIVENESS"]["status"] == "FRESH"
    assert by_source["FUTURES_DEPTH"]["status"] == "GAP"
    assert by_source["FUTURES_TRADE"]["status"] == "MISSING"
    assert by_source["FUTURES_TRADE"]["symbol"] == "BTCUSDT"


def test_market_data_freshness_preserves_unsafe_orderbook_state():
    now = datetime.now(timezone.utc)
    rows = _rows(now)
    rows[-1] = (
        "BTCUSDT", "FUTURES", "ORDERBOOK", now - timedelta(seconds=1),
        now, 1000, {"source": "binance_futures_diff_depth", "health_status": "UNSAFE"},
    )
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor

    with patch("psycopg2.connect", return_value=connection), patch(
        "trading_store._now", return_value=now
    ):
        result = TradingStore("postgresql://test").market_data_freshness(
            max_age_sec=60, symbols=["BTCUSDT"]
        )

    by_source = {row["source"]: row for row in result}
    assert by_source["FUTURES_DEPTH"]["status"] == "UNSAFE"


def test_trade_and_order_event_persistence_use_idempotent_conflict_keys():
    cursor = MagicMock()
    cursor.fetchone.return_value = ("00000000-0000-0000-0000-000000000002",)
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    store = TradingStore("postgresql://test")

    with patch("psycopg2.connect", return_value=connection):
        store.append_order_event(
            UUID("00000000-0000-0000-0000-000000000001"),
            event_type="ORDER_TRADE_UPDATE",
            status="FILLED",
            event_id="exchange-event-1",
        )
        store.record_trade(
            UUID("00000000-0000-0000-0000-000000000001"),
            symbol="BTCUSDT",
            side="BUY",
            quantity=Decimal("1"),
            price=Decimal("100"),
            fee=Decimal("0.1"),
            fee_asset="USDT",
            source_event_id="exchange-event-1",
        )

    statements = "\n".join(call.args[0] for call in cursor.execute.call_args_list)
    assert "ON CONFLICT (event_id) DO NOTHING" in statements
    assert "ON CONFLICT (source_event_id) DO UPDATE" in statements


def test_funding_settlement_persistence_is_mode_symbol_time_idempotent():
    cursor = MagicMock()
    cursor.fetchone.return_value = ("paper",)
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    store = TradingStore("postgresql://test")
    settled_at = datetime(2026, 8, 26, 8, 0, tzinfo=timezone.utc)

    with patch("psycopg2.connect", return_value=connection):
        assert store.record_funding_settlement(
            mode="paper",
            symbol="BTCUSDT",
            settlement_timestamp=settled_at,
            rate=Decimal("0.0001"),
            notional=Decimal("100"),
            payment=Decimal("0.01"),
            position_side="LONG",
        ) is True

    statement = cursor.execute.call_args[0][0]
    assert "PRIMARY KEY" not in statement
    assert "ON CONFLICT (mode, symbol, settlement_timestamp) DO NOTHING" in statement


def test_positioning_snapshot_persists_complete_episode_and_evidence_contract():
    from engine import StrategyEngine
    from tests.test_engine import _positioning_frame

    decision = StrategyEngine().positioning_decision(_positioning_frame())
    cursor = MagicMock()
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor

    with patch("psycopg2.connect", return_value=connection):
        TradingStore("postgresql://test").record_positioning_snapshot(
            decision, strategy_version="positioning-v1"
        )

    statements = "\n".join(call.args[0] for call in cursor.execute.call_args_list)
    assert "episode_id" in statements
    assert "episode_direction" in statements
    assert "episode_status" in statements
    assert "evidence_sufficiency" in statements
    assert "data_quality" in statements
    evidence_args = cursor.execute.call_args_list[1].args[1]
    assert '"futures_trade_flow"' in evidence_args[4]
    assert '"is_meme"' in evidence_args[11]


def test_update_episode_ignores_stale_writer():
    from datetime import datetime, timezone
    from uuid import uuid4

    episode_id = uuid4()
    started = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
    newer = datetime(2026, 8, 26, 12, 5, tzinfo=timezone.utc)
    older = datetime(2026, 8, 26, 12, 1, tzinfo=timezone.utc)
    current_row = (
        str(episode_id), "BTCUSDT", "FUTURES", "LONG", started, None,
        "LONG_BUILDING", "OPEN", newer, "positioning-v1", "hash", {},
    )
    cursor = MagicMock()
    cursor.fetchone.side_effect = [None, current_row]
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor

    with patch("psycopg2.connect", return_value=connection):
        result = TradingStore("postgresql://test").update_episode(
            episode_id,
            state="UNKNOWN",
            status="UNRESOLVED",
            observed_at=older,
            metadata={"stale": True},
        )

    assert result["status"] == "OPEN"
    assert result["state"] == "LONG_BUILDING"
    assert result["last_observed_at"] == str(newer)


def test_start_episode_uses_one_active_row_per_symbol_market():
    cursor = MagicMock()
    started = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
    cursor.fetchone.return_value = (
        "11111111-1111-1111-1111-111111111111", "BTCUSDT", "FUTURES", "LONG",
        started, None, "LONG_BUILDING", "OPEN", started, "positioning-v1", "hash", {},
    )
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    store = TradingStore("postgresql://test")

    with patch("psycopg2.connect", return_value=connection):
        first = store.start_episode(
            symbol="BTCUSDT", direction="LONG", state="LONG_BUILDING",
            observed_at=started, strategy_version="positioning-v1", config_hash="hash",
        )
        second = store.start_episode(
            symbol="BTCUSDT", direction="LONG", state="LONG_BUILDING",
            observed_at=started, strategy_version="positioning-v1", config_hash="hash",
        )

    statement = cursor.execute.call_args_list[0].args[0]
    assert "ON CONFLICT (symbol, market)" in statement
    assert "WHERE status IN ('OPEN', 'UNRESOLVED')" in statement
    assert first["episode_id"] == second["episode_id"]


def test_market_data_freshness_reads_nested_health_status():
    now = datetime.now(timezone.utc)
    received = now - timedelta(seconds=1)
    timestamp = received - timedelta(milliseconds=20)
    rows = [
        (
            "BTCUSDT", "FUTURES", "ORDERBOOK", timestamp, received, 20,
            {
                "event_type": "ORDERBOOK",
                "health": "GAP",
                "metadata": {"health_status": "GAP", "state": "GAP"},
            },
        ),
    ]
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    with patch("psycopg2.connect", return_value=connection), patch(
        "trading_store._now", return_value=now
    ):
        result = TradingStore("postgresql://test").market_data_freshness(
            max_age_sec=60, symbols=["BTCUSDT"]
        )
    by_source = {row["source"]: row for row in result}
    assert by_source["FUTURES_DEPTH"]["status"] == "GAP"
