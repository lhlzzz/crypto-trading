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


def test_collector_lifecycle_events_reads_latest_heartbeat():
    event = {
        "old_connection_id": "trade-aaa",
        "new_connection_id": "trade-bbb",
        "channel": "TRADE",
        "disconnect_at": "2026-09-02T00:00:00+00:00",
        "reconnect_at": "2026-09-02T00:00:02+00:00",
        "recovery_ms": 2000,
        "subscriptions_restored": True,
        "reason": "controlled_reconnect",
        "source": "collector_lifecycle",
    }
    cursor = MagicMock()
    cursor.fetchall.return_value = [({"metadata": event},)]
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    with patch("psycopg2.connect", return_value=connection):
        rows = TradingStore("postgresql://test").collector_lifecycle_events()
    assert rows == [event]


def test_market_data_freshness_includes_metadata_for_lifecycle():
    now = datetime.now(timezone.utc)
    received = now - timedelta(seconds=1)
    timestamp = received - timedelta(milliseconds=20)
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        (
            "BTCUSDT", "FUTURES", "LIQUIDATION_HEARTBEAT", timestamp, received, 20,
            {"reconnect_events": [{"source": "collector_lifecycle", "channel": "TRADE"}]},
        ),
    ]
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    with patch("psycopg2.connect", return_value=connection), patch(
        "trading_store._now", return_value=now
    ):
        rows = TradingStore("postgresql://test").market_data_freshness(
            max_age_sec=60, symbols=["BTCUSDT"]
        )
    heartbeat = next(row for row in rows if row["source"] == "FUTURES_LIQUIDATION_LIVENESS")
    assert heartbeat["metadata"]["reconnect_events"][0]["channel"] == "TRADE"


def test_market_data_freshness_ignores_synthetic_stale_markers():
    now = datetime.now(timezone.utc)
    live_received = now - timedelta(seconds=2)
    live_source = live_received - timedelta(milliseconds=20)
    stale_received = now - timedelta(seconds=1)
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        (
            "BTCUSDT", "FUTURES", "BOOK_TICKER", stale_received, stale_received, 0,
            {"health": "STALE", "health_status": "STALE", "metadata": {"health_status": "STALE"}},
        ),
        (
            "BTCUSDT", "FUTURES", "BOOK_TICKER", live_source, live_received, 20,
            {"source": "binance_futures_book_ticker"},
        ),
    ]
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    with patch("psycopg2.connect", return_value=connection), patch(
        "trading_store._now", return_value=now
    ):
        rows = TradingStore("postgresql://test").market_data_freshness(
            max_age_sec=60, symbols=["BTCUSDT"]
        )
    by_source = {row["source"]: row for row in rows}
    assert by_source["FUTURES_BOOK_TICKER"]["status"] == "FRESH"


def test_runtime_acceptance_snapshot_requires_session_and_scopes_mode():
    store = TradingStore("postgresql://test")
    empty = store.runtime_acceptance_snapshot(mode="paper")
    assert empty["observation_count"] == 0
    assert empty["session_id"] is None
    assert empty["restart_recovery"] is False

    executed = []
    cursor = MagicMock()

    def execute(sql, params=()):
        executed.append((sql, params))
        if "FROM validation_sessions" in sql:
            cursor.fetchone.return_value = (
                "sess-1", "paper_24h", "paper", "2026-09-03T00:00:00+00:00",
                None, None, None, "abc", 1, "2026-09-03T00:01:00+00:00", "RUNNING",
                None, None, None, {},
            )
        elif "FROM balances" in sql:
            cursor.fetchone.return_value = None
        else:
            cursor.fetchone.return_value = (0,)
        cursor.fetchall.return_value = []

    cursor.execute.side_effect = execute
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    with patch("psycopg2.connect", return_value=connection):
        snapshot = store.runtime_acceptance_snapshot(mode="paper", session_id="sess-1")
    joined = "\n".join(sql for sql, _ in executed)
    assert "validation_session_observations" in joined
    assert "validation_session_id" in joined
    assert "status = 'OPEN'" not in joined
    assert any(params and "sess-1" in str(params) for _, params in executed)
    assert snapshot["session_id"] == "sess-1"
    assert snapshot["mode"] == "paper"
    assert snapshot["unknown_order"] == 0


def _restart_snapshot(**overrides):
    payload = {
        "positions": [{"symbol": "BTCUSDT", "position_side": "LONG", "position_quantity": "1"}],
        "episodes": [{"active_episode_id": "ep-1"}],
        "wallet_balance": "1000",
        "available_balance": "990",
        "used_margin": "10",
        "unrealized_pnl": "0",
        "equity": "1000",
        "realized_pnl": "0",
        "funding_pnl": "0",
        "fee_pnl": "0",
        "open_orders": [{"client_order_id": "c-1", "exchange_order_id": "x-1", "status": "NEW"}],
    }
    payload.update(overrides)
    return payload


def test_compare_restart_snapshots_require_before_and_after():
    from trading_store import TradingStore as Store

    missing = Store.compare_restart_snapshots(None, {"wallet_balance": "1"})
    assert missing["ok"] is False
    pre = _restart_snapshot()
    assert Store.compare_restart_snapshots(pre, dict(pre))["ok"] is True
    moved = _restart_snapshot(unrealized_pnl="20", equity="1020")
    valid = Store.compare_restart_snapshots(pre, moved)
    assert valid["ok"] is True
    position_mismatch = Store.compare_restart_snapshots(
        pre, _restart_snapshot(positions=[{"symbol": "BTCUSDT", "position_side": "SHORT", "position_quantity": "1"}])
    )
    assert position_mismatch["ok"] is False
    assert "positions" in position_mismatch["mismatched"]
    episode_mismatch = Store.compare_restart_snapshots(
        pre, _restart_snapshot(episodes=[{"active_episode_id": "ep-2"}])
    )
    assert episode_mismatch["ok"] is False
    assert "episodes" in episode_mismatch["mismatched"]
    order_mismatch = Store.compare_restart_snapshots(
        pre, _restart_snapshot(open_orders=[{"client_order_id": "c-2", "exchange_order_id": "x-1", "status": "NEW"}])
    )
    assert order_mismatch["ok"] is False
    assert "open_orders" in order_mismatch["mismatched"]
    accounting = Store.compare_restart_snapshots(
        pre,
        _restart_snapshot(wallet_balance="500", available_balance="490", used_margin="10", equity="500", unrealized_pnl="0"),
    )
    assert accounting["ok"] is False
    assert accounting["reason"] == "UNEXPECTED_BALANCE_DELTA"
    invariant = Store.compare_restart_snapshots(pre, _restart_snapshot(equity="999"))
    assert invariant["ok"] is False
    assert invariant["reason"] == "IMPOSSIBLE_ACCOUNTING"
    encoded = Store.compare_restart_snapshots(
        __import__("json").dumps(pre),
        __import__("json").dumps(_restart_snapshot(unrealized_pnl="20", equity="1020")),
    )
    assert encoded["ok"] is True


def test_derive_testnet_facts_are_session_scoped():
    from trading_store import derive_testnet_lifecycle_facts

    old = derive_testnet_lifecycle_facts(
        session={"session_id": "old"},
        orders=[{
            "order_id": "old-order",
            "client_order_id": "L-old",
            "exchange_order_id": "999",
            "symbol": "BTCUSDT",
            "status": "FILLED",
            "position_side": "LONG",
            "position_action": "OPEN",
        }],
        order_events=[],
        trades=[{"trade_id": "old-trade", "order_id": "old-order", "quantity": "1"}],
        positions=[{"symbol": "BTCUSDT", "quantity": "0", "position_side": "FLAT"}],
        system_events=[],
    )
    current = derive_testnet_lifecycle_facts(
        session={"session_id": "current"},
        orders=[],
        order_events=[],
        trades=[],
        positions=[{"symbol": "BTCUSDT", "quantity": "0", "position_side": "FLAT"}],
        system_events=[],
    )
    assert old["session_id"] == "old"
    assert old["long"]["order"]["exchange_order_id"] == "999"
    assert current["session_id"] == "current"
    assert not current["long"]["order"].get("exchange_order_id")
    local_only = derive_testnet_lifecycle_facts(
        session={"session_id": "flat-local"},
        orders=[],
        order_events=[],
        trades=[],
        positions=[{"symbol": "BTCUSDT", "quantity": "0", "position_side": "FLAT"}],
        system_events=[],
    )
    assert local_only["reconciliation"]["local_flat"] is True
    assert local_only["reconciliation"]["exchange_flat"] is False
    exchange_flat = derive_testnet_lifecycle_facts(
        session={"session_id": "flat-exchange"},
        orders=[],
        order_events=[],
        trades=[],
        positions=[{"symbol": "BTCUSDT", "quantity": "0", "position_side": "FLAT"}],
        system_events=[{
            "event_type": "RECONCILIATION_OK",
            "payload": {
                "exchange_flat": True,
                "exchange_positions": [{"symbol": "BTCUSDT", "position_side": "FLAT", "quantity": "0"}],
            },
        }],
    )
    assert exchange_flat["reconciliation"]["exchange_flat"] is True


def test_collector_lifecycle_events_query_ws_lifecycle():
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    with patch("psycopg2.connect", return_value=connection):
        TradingStore("postgresql://test").collector_lifecycle_events()
    sql = cursor.execute.call_args[0][0]
    assert "WS_LIFECYCLE" in sql
    assert "LIQUIDATION_HEARTBEAT" not in sql


def test_duplicate_exchange_trade_id_is_idempotent():
    cursor = MagicMock()
    cursor.fetchone.return_value = ("00000000-0000-0000-0000-000000000003",)
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    store = TradingStore("postgresql://test")
    with patch("psycopg2.connect", return_value=connection):
        first = store.record_trade(
            UUID("00000000-0000-0000-0000-000000000001"),
            symbol="BTCUSDT",
            side="BUY",
            quantity=Decimal("1"),
            price=Decimal("100"),
            fee=Decimal("0"),
            fee_asset="USDT",
            exchange_trade_id="77",
        )
        second = store.record_trade(
            UUID("00000000-0000-0000-0000-000000000001"),
            symbol="BTCUSDT",
            side="BUY",
            quantity=Decimal("1"),
            price=Decimal("100"),
            fee=Decimal("0"),
            fee_asset="USDT",
            exchange_trade_id="77",
        )
    statement = "\n".join(call.args[0] for call in cursor.execute.call_args_list)
    assert "ON CONFLICT (mode, exchange_trade_id)" in statement
    assert first == second


def test_position_mode_isolation_uses_canonical_mode_key():
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    store = TradingStore("postgresql://test")
    with patch("psycopg2.connect", return_value=connection), patch.dict(
        "os.environ", {"BIAN_MODE": "paper"}, clear=False
    ):
        store.upsert_position(
            "BTCUSDT",
            quantity=Decimal("1"),
            average_price=Decimal("100"),
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            mode="paper",
        )
        store.get_position("BTCUSDT", mode="testnet")
        store.list_positions(mode="live")
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert any("ON CONFLICT (mode, market, symbol)" in sql for sql in statements)
    assert any("WHERE mode = %s AND market = %s AND symbol = %s" in sql for sql in statements)
    assert any("WHERE mode = %s AND market = %s" in sql for sql in statements)


def test_episode_session_isolation_does_not_reuse_foreign_session():
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
    store.validation_session_id = "session-b"
    with patch("psycopg2.connect", return_value=connection):
        store.get_active_episode("BTCUSDT", "session-b")
        store.start_episode(
            symbol="BTCUSDT", direction="LONG", state="LONG_BUILDING",
            observed_at=started, strategy_version="positioning-v1", config_hash="hash",
        )
    get_sql = cursor.execute.call_args_list[0].args[0]
    start_sql = cursor.execute.call_args_list[1].args[0]
    assert "validation_session_id IS NOT DISTINCT FROM %s" in get_sql
    assert "ON CONFLICT (symbol, market, validation_session_id)" in start_sql


def test_runtime_gate_statuses_are_current_session_scoped():
    store = TradingStore("postgresql://test")
    assert store.runtime_gate_statuses()["testnet"] == "NOT_STARTED"
    store.validation_session_id = "sess-current"
    cursor = MagicMock()
    cursor.fetchall.return_value = [("testnet", "PASSED")]
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    with patch("psycopg2.connect", return_value=connection):
        statuses = store.runtime_gate_statuses()
    sql = cursor.execute.call_args.args[0]
    params = cursor.execute.call_args.args[1]
    assert "validation_session_id = %s" in sql
    assert params[0] == "sess-current"
    assert statuses["testnet"] == "PASSED"


def test_record_trade_uses_canonical_mode_not_payload():
    cursor = MagicMock()
    cursor.fetchone.return_value = ("00000000-0000-0000-0000-000000000004",)
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    store = TradingStore("postgresql://test")
    with patch("psycopg2.connect", return_value=connection), patch.dict(
        "os.environ", {"BIAN_MODE": "testnet"}, clear=False
    ):
        store.record_trade(
            UUID("00000000-0000-0000-0000-000000000001"),
            symbol="BTCUSDT",
            side="BUY",
            quantity=Decimal("1"),
            price=Decimal("100"),
            fee=Decimal("0"),
            fee_asset="USDT",
            exchange_trade_id="91",
            payload={"mode": "live"},
        )
    args = cursor.execute.call_args.args[1]
    assert args[11] == "testnet"


def test_list_reconciliation_orders_uses_explicit_mode_and_includes_filled():
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    store = TradingStore("postgresql://test")
    with patch("psycopg2.connect", return_value=connection), patch.dict(
        "os.environ", {"BIAN_MODE": "live"}, clear=False
    ):
        store.list_reconciliation_orders(mode="testnet")
    sql, params = cursor.execute.call_args.args
    assert "WHERE mode = %s AND market = %s" in sql
    assert "LIMIT" not in sql
    assert params[0] == "testnet"
    assert "PENDING" not in sql
    assert "FILLED" not in sql


def _halt_cursor(event_type: str, mode: str, market: str = "FUTURES"):
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        (event_type,),
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000002",),
    ]
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    return cursor, connection


def test_testnet_halt_survives_paper_resume():
    store = TradingStore("postgresql://test")
    cursor, connection = _halt_cursor("TRADING_HALTED", "testnet")
    with patch("psycopg2.connect", return_value=connection):
        halted = store.is_halted(mode="testnet")
        store.set_halt(False, reason="paper resume", source="test", mode="paper")
    sql = cursor.execute.call_args_list[0].args[0]
    params = cursor.execute.call_args_list[0].args[1]
    assert "AND mode = %s" in sql
    assert "AND market = %s" in sql
    assert params == ("testnet", "FUTURES")
    assert halted is True
    resume_params = cursor.execute.call_args_list[1].args[1]
    assert resume_params[4] == "paper"
    assert resume_params[5] == "FUTURES"


def test_live_halt_survives_testnet_resume():
    store = TradingStore("postgresql://test")
    cursor, connection = _halt_cursor("TRADING_HALTED", "live")
    with patch("psycopg2.connect", return_value=connection):
        halted = store.is_halted(mode="live")
        store.set_halt(False, reason="testnet resume", source="test", mode="testnet")
    params = cursor.execute.call_args_list[0].args[1]
    assert params == ("live", "FUTURES")
    assert halted is True
    resume_params = cursor.execute.call_args_list[1].args[1]
    assert resume_params[4] == "testnet"
    assert resume_params[5] == "FUTURES"


def test_paper_halt_survives_testnet_resume():
    store = TradingStore("postgresql://test")
    cursor, connection = _halt_cursor("TRADING_HALTED", "paper")
    with patch("psycopg2.connect", return_value=connection):
        halted = store.is_halted(mode="paper")
        store.set_halt(False, reason="testnet resume", source="test", mode="testnet")
    params = cursor.execute.call_args_list[0].args[1]
    assert params == ("paper", "FUTURES")
    assert halted is True


def test_current_mode_resume_clears_own_halt():
    store = TradingStore("postgresql://test")
    cursor, connection = _halt_cursor("TRADING_RESUMED", "testnet")
    with patch("psycopg2.connect", return_value=connection):
        halted = store.is_halted(mode="testnet")
    params = cursor.execute.call_args.args[1]
    assert params == ("testnet", "FUTURES")
    assert halted is False


def test_spot_event_does_not_clear_futures_halt():
    store = TradingStore("postgresql://test")
    cursor, connection = _halt_cursor("TRADING_HALTED", "testnet")
    with patch("psycopg2.connect", return_value=connection):
        halted = store.is_halted(mode="testnet", market="FUTURES")
        store.set_halt(False, reason="spot resume", source="test", mode="testnet", market="SPOT")
    params = cursor.execute.call_args_list[0].args[1]
    assert params == ("testnet", "FUTURES")
    assert halted is True
    resume_params = cursor.execute.call_args_list[1].args[1]
    assert resume_params[4] == "testnet"
    assert resume_params[5] == "SPOT"


def test_db_unavailable_is_halted_fails_closed():
    store = TradingStore("postgresql://test")
    with patch("psycopg2.connect", side_effect=RuntimeError("db down")):
        assert store.is_halted(mode="paper") is True


def test_migration_does_not_fabricate_liquidation_price():
    from scripts.database import _migrate_futures_columns

    cursor = MagicMock()
    cursor.fetchone.return_value = None
    _migrate_futures_columns(cursor)
    statements = "\n".join(str(call.args[0]) for call in cursor.execute.call_args_list)
    assert "liquidation_price = entry_price" not in statements
    assert "positions_active_liquidation_positive" not in statements or "DROP CONSTRAINT" in statements
