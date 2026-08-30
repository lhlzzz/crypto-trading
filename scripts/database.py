"""Database contract for the independent bian bot."""
from __future__ import annotations

import argparse
import os
import socket
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

DEFAULT_DSN = "postgresql://bian:bian@localhost:5446/bian"
REQUIRED_TABLES = frozenset(
    {
        "bian_collection_runs",
        "bian_market_snapshots",
        "bian_product_coverage",
        "signals",
        "trade_intents",
        "orders",
        "order_events",
        "trades",
        "positions",
        "balances",
        "risk_events",
        "system_events",
        "market_flow_events",
        "positioning_snapshots",
        "evidence_snapshots",
        "liquidation_events",
    }
)


def configured_dsn(environ: Mapping[str, str] | None = None) -> str:
    values = os.environ if environ is None else environ
    return values.get("BIAN_PG_DSN") or values.get("BIAN_DATABASE_URL") or DEFAULT_DSN


def database_target(dsn: str) -> tuple[str, int]:
    parsed = urlparse(dsn)
    if not parsed.hostname:
        raise ValueError("database DSN must include a host")
    return parsed.hostname, parsed.port or 5432


def database_reachable(dsn: str, timeout_sec: float = 1.0) -> bool:
    try:
        with socket.create_connection(database_target(dsn), timeout=timeout_sec):
            return True
    except (OSError, ValueError):
        return False


def _status(
    host: str,
    port: int,
    *,
    state: str,
    missing_tables: list[str],
    error: str | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "status": state,
        "database": f"{host}:{port}",
        "missing_tables": missing_tables,
    }
    if error:
        result["error"] = error
    return result


def _unavailable_status(dsn: str, error: str) -> dict[str, object]:
    try:
        host, port = database_target(dsn)
    except ValueError:
        return {
            "status": "unavailable",
            "database": "invalid",
            "missing_tables": sorted(REQUIRED_TABLES),
            "error": "invalid_dsn",
        }
    return _status(
        host,
        port,
        state="unavailable",
        missing_tables=sorted(REQUIRED_TABLES),
        error=error,
    )


def ensure_schema(dsn: str | None = None) -> None:
    import psycopg2

    with psycopg2.connect(dsn or configured_dsn(), connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bian_collection_runs (
                    run_id UUID PRIMARY KEY,
                    collection_kind TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('running', 'ok', 'failed')),
                    started_at TIMESTAMPTZ NOT NULL,
                    completed_at TIMESTAMPTZ,
                    source_url TEXT,
                    market_count INTEGER NOT NULL DEFAULT 0,
                    coverage_count INTEGER NOT NULL DEFAULT 0,
                    error_code TEXT
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS bian_collection_runs_status_completed_idx
                ON bian_collection_runs(status, completed_at DESC)
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bian_market_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    captured_at TIMESTAMPTZ NOT NULL,
                    symbol TEXT NOT NULL,
                    last_price NUMERIC NOT NULL,
                    price_change_percent NUMERIC,
                    quote_volume NUMERIC,
                    source_url TEXT NOT NULL,
                    payload JSONB NOT NULL DEFAULT CAST('{}' AS JSONB)
                )
                """
            )
            cursor.execute(
                """
                ALTER TABLE bian_market_snapshots
                ADD COLUMN IF NOT EXISTS run_id UUID
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS bian_market_snapshots_symbol_captured_idx
                ON bian_market_snapshots(symbol, captured_at DESC)
                """
            )
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS bian_market_snapshots_run_symbol_idx
                ON bian_market_snapshots(run_id, symbol)
                WHERE run_id IS NOT NULL
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bian_product_coverage (
                    product_type TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    symbol_count INTEGER,
                    source_url TEXT NOT NULL,
                    captured_at TIMESTAMPTZ NOT NULL,
                    detail JSONB NOT NULL DEFAULT CAST('{}' AS JSONB)
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS bian_product_coverage_product_type_idx ON bian_product_coverage(product_type)
                """
            )
            _create_trading_tables(cursor)

def _create_trading_tables(cursor: Any) -> None:
    """Create trading-domain tables; persistence behavior belongs elsewhere."""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS signals (
            id BIGSERIAL PRIMARY KEY,
            signal_id UUID NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL', 'HOLD')),
            confidence NUMERIC NOT NULL,
            reason TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            observed_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL DEFAULT CAST('{}' AS JSONB)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_intents (
            id BIGSERIAL PRIMARY KEY,
            intent_id UUID NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
            direction TEXT NOT NULL CHECK (direction IN ('LONG', 'SHORT')),
            action TEXT NOT NULL CHECK (action IN ('OPEN', 'REDUCE', 'CLOSE')),
            order_type TEXT NOT NULL CHECK (order_type IN ('MARKET', 'LIMIT')),
            quantity NUMERIC,
            price NUMERIC,
            confidence NUMERIC NOT NULL,
            reason TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'CREATED',
            created_at TIMESTAMPTZ NOT NULL,
            client_order_id TEXT,
            reduce_only BOOLEAN NOT NULL DEFAULT FALSE,
            leverage NUMERIC NOT NULL DEFAULT 1,
            margin_type TEXT NOT NULL DEFAULT 'ISOLATED',
            position_mode TEXT NOT NULL DEFAULT 'ONE_WAY',
            positioning_state TEXT,
            previous_state TEXT,
            transition TEXT,
            evidence_snapshot_id UUID,
            payload JSONB NOT NULL DEFAULT CAST('{}' AS JSONB)
        )
        """
    )
    cursor.execute(
        """
        ALTER TABLE trade_intents
        ADD COLUMN IF NOT EXISTS client_order_id TEXT
        """
    )
    cursor.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS trade_intents_client_order_id_idx
        ON trade_intents(client_order_id)
        WHERE client_order_id IS NOT NULL
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id BIGSERIAL PRIMARY KEY,
            order_id UUID NOT NULL UNIQUE,
            intent_id UUID REFERENCES trade_intents(intent_id),
            symbol TEXT NOT NULL,
            side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
            order_type TEXT NOT NULL CHECK (order_type IN ('MARKET', 'LIMIT')),
            client_order_id TEXT NOT NULL UNIQUE,
            exchange_order_id TEXT,
            market TEXT NOT NULL DEFAULT 'FUTURES',
            position_side TEXT,
            position_action TEXT,
            reduce_only BOOLEAN NOT NULL DEFAULT FALSE,
            leverage NUMERIC NOT NULL DEFAULT 1,
            margin_type TEXT NOT NULL DEFAULT 'ISOLATED',
            quantity NUMERIC,
            price NUMERIC,
            executed_quantity NUMERIC NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            mode TEXT NOT NULL CHECK (mode IN ('paper', 'testnet', 'live')),
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            expires_at TIMESTAMPTZ,
            payload JSONB NOT NULL DEFAULT CAST('{}' AS JSONB)
        )
        """
    )
    cursor.execute(
        """
        ALTER TABLE orders
        ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS order_events (
            id BIGSERIAL PRIMARY KEY,
            order_id UUID NOT NULL REFERENCES orders(order_id),
            event_type TEXT NOT NULL,
            status TEXT NOT NULL,
            event_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL DEFAULT CAST('{}' AS JSONB)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id BIGSERIAL PRIMARY KEY,
            trade_id UUID NOT NULL UNIQUE,
            order_id UUID NOT NULL REFERENCES orders(order_id),
            symbol TEXT NOT NULL,
            side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
            quantity NUMERIC NOT NULL,
            price NUMERIC NOT NULL,
            fee NUMERIC NOT NULL DEFAULT 0,
            fee_asset TEXT,
            realized_pnl NUMERIC NOT NULL DEFAULT 0,
            executed_at TIMESTAMPTZ NOT NULL,
            market TEXT NOT NULL DEFAULT 'FUTURES',
            mode TEXT NOT NULL DEFAULT 'paper' CHECK (mode IN ('paper', 'testnet', 'live')),
            position_side TEXT,
            funding NUMERIC NOT NULL DEFAULT 0,
            payload JSONB NOT NULL DEFAULT CAST('{}' AS JSONB)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS positions (
            market TEXT NOT NULL DEFAULT 'FUTURES',
            symbol TEXT NOT NULL,
            position_side TEXT,
            quantity NUMERIC NOT NULL DEFAULT 0,
            entry_price NUMERIC NOT NULL DEFAULT 0,
            average_price NUMERIC NOT NULL DEFAULT 0,
            mark_price NUMERIC,
            index_price NUMERIC,
            notional NUMERIC,
            leverage NUMERIC,
            margin_type TEXT NOT NULL DEFAULT 'ISOLATED',
            initial_margin NUMERIC,
            maintenance_margin NUMERIC,
            liquidation_price NUMERIC,
            realized_pnl NUMERIC NOT NULL DEFAULT 0,
            unrealized_pnl NUMERIC NOT NULL DEFAULT 0,
            funding_pnl NUMERIC NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL DEFAULT CAST('{}' AS JSONB),
            PRIMARY KEY (market, symbol)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS balances (
            asset TEXT NOT NULL,
            free NUMERIC NOT NULL DEFAULT 0,
            locked NUMERIC NOT NULL DEFAULT 0,
            mode TEXT NOT NULL CHECK (mode IN ('paper', 'testnet', 'live')),
            updated_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL DEFAULT CAST('{}' AS JSONB),
            PRIMARY KEY (mode, asset)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_events (
            id BIGSERIAL PRIMARY KEY,
            event_id UUID NOT NULL UNIQUE,
            intent_id UUID,
            decision TEXT NOT NULL CHECK (decision IN ('ALLOW', 'DENY', 'REDUCE', 'HALT')),
            reason TEXT NOT NULL,
            mode TEXT NOT NULL DEFAULT 'paper' CHECK (mode IN ('paper', 'testnet', 'live')),
            market TEXT NOT NULL DEFAULT 'FUTURES',
            event_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL DEFAULT CAST('{}' AS JSONB)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS system_events (
            id BIGSERIAL PRIMARY KEY,
            event_id UUID NOT NULL UNIQUE,
            event_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            message TEXT NOT NULL,
            mode TEXT NOT NULL DEFAULT 'paper' CHECK (mode IN ('paper', 'testnet', 'live')),
            market TEXT NOT NULL DEFAULT 'FUTURES',
            event_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL DEFAULT CAST('{}' AS JSONB)
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS orders_status_updated_idx
        ON orders(status, updated_at DESC)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS order_events_order_event_idx
        ON order_events(order_id, event_at DESC)
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS market_flow_events (
            event_id UUID PRIMARY KEY,
            symbol TEXT NOT NULL,
            market TEXT NOT NULL CHECK (market IN ('SPOT', 'FUTURES')),
            event_type TEXT NOT NULL,
            event_timestamp TIMESTAMPTZ NOT NULL,
            received_timestamp TIMESTAMPTZ NOT NULL,
            latency_ms INTEGER NOT NULL CHECK (latency_ms >= 0),
            price NUMERIC,
            quantity NUMERIC,
            notional NUMERIC,
            direction TEXT,
            metadata JSONB NOT NULL DEFAULT CAST('{}' AS JSONB)
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS market_flow_events_symbol_time_idx
        ON market_flow_events(symbol, event_timestamp DESC)
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS positioning_snapshots (
            snapshot_id UUID PRIMARY KEY,
            symbol TEXT NOT NULL,
            observed_at TIMESTAMPTZ NOT NULL,
            state TEXT NOT NULL,
            transition TEXT NOT NULL,
            direction TEXT NOT NULL CHECK (direction IN ('LONG', 'SHORT', 'FLAT')),
            confidence NUMERIC NOT NULL,
            long_score NUMERIC NOT NULL,
            short_score NUMERIC NOT NULL,
            crowding_score NUMERIC NOT NULL,
            liquidity_score NUMERIC NOT NULL,
            data_quality_score NUMERIC NOT NULL,
            strategy_version TEXT NOT NULL,
            reason_codes JSONB NOT NULL DEFAULT '[]'::JSONB,
            payload JSONB NOT NULL DEFAULT CAST('{}' AS JSONB)
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS positioning_snapshots_symbol_time_idx
        ON positioning_snapshots(symbol, observed_at DESC)
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS evidence_snapshots (
            snapshot_id UUID PRIMARY KEY,
            symbol TEXT NOT NULL,
            observed_at TIMESTAMPTZ NOT NULL,
            source_timestamps JSONB NOT NULL DEFAULT '{}'::JSONB,
            evidence JSONB NOT NULL DEFAULT '{}'::JSONB,
            payload JSONB NOT NULL DEFAULT '{}'::JSONB
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS evidence_snapshots_symbol_time_idx
        ON evidence_snapshots(symbol, observed_at DESC)
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS liquidation_events (
            event_id UUID PRIMARY KEY,
            symbol TEXT NOT NULL,
            market TEXT NOT NULL CHECK (market = 'FUTURES'),
            side TEXT NOT NULL,
            price NUMERIC NOT NULL,
            quantity NUMERIC NOT NULL,
            notional NUMERIC NOT NULL,
            event_timestamp TIMESTAMPTZ NOT NULL,
            received_timestamp TIMESTAMPTZ NOT NULL,
            latency_ms INTEGER NOT NULL CHECK (latency_ms >= 0),
            payload JSONB NOT NULL DEFAULT '{}'::JSONB
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS liquidation_events_symbol_time_idx
        ON liquidation_events(symbol, event_timestamp DESC)
        """
    )
    _migrate_futures_columns(cursor)


def _migrate_futures_columns(cursor: Any) -> None:
    """Add futures fields to the existing trading tables."""
    statements = (
        "ALTER TABLE trade_intents ADD COLUMN IF NOT EXISTS direction TEXT",
        "ALTER TABLE trade_intents ADD COLUMN IF NOT EXISTS action TEXT",
        "ALTER TABLE trade_intents ADD COLUMN IF NOT EXISTS reduce_only BOOLEAN",
        "ALTER TABLE trade_intents ADD COLUMN IF NOT EXISTS leverage NUMERIC",
        "ALTER TABLE trade_intents ADD COLUMN IF NOT EXISTS margin_type TEXT",
        "ALTER TABLE trade_intents ADD COLUMN IF NOT EXISTS position_mode TEXT",
        "ALTER TABLE trade_intents ADD COLUMN IF NOT EXISTS positioning_state TEXT",
        "ALTER TABLE trade_intents ADD COLUMN IF NOT EXISTS previous_state TEXT",
        "ALTER TABLE trade_intents ADD COLUMN IF NOT EXISTS transition TEXT",
        "ALTER TABLE trade_intents ADD COLUMN IF NOT EXISTS evidence_snapshot_id UUID",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS market TEXT",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS position_side TEXT",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS position_action TEXT",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS reduce_only BOOLEAN",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS leverage NUMERIC",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS margin_type TEXT",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS market TEXT",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS mode TEXT DEFAULT 'paper'",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS position_side TEXT",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS funding NUMERIC NOT NULL DEFAULT 0",
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS market TEXT",
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS position_side TEXT",
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_price NUMERIC",
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS mark_price NUMERIC",
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS notional NUMERIC",
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS leverage NUMERIC",
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS margin_type TEXT",
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS initial_margin NUMERIC",
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS maintenance_margin NUMERIC",
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS liquidation_price NUMERIC",
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS funding_pnl NUMERIC NOT NULL DEFAULT 0",
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS index_price NUMERIC",
        "ALTER TABLE balances ADD COLUMN IF NOT EXISTS wallet_balance NUMERIC",
        "ALTER TABLE balances ADD COLUMN IF NOT EXISTS available_balance NUMERIC",
        "ALTER TABLE balances ADD COLUMN IF NOT EXISTS margin_balance NUMERIC",
        "ALTER TABLE balances ADD COLUMN IF NOT EXISTS used_margin NUMERIC",
        "ALTER TABLE balances ADD COLUMN IF NOT EXISTS unrealized_pnl NUMERIC",
        "ALTER TABLE risk_events ADD COLUMN IF NOT EXISTS mode TEXT DEFAULT 'paper'",
        "ALTER TABLE risk_events ADD COLUMN IF NOT EXISTS market TEXT DEFAULT 'FUTURES'",
        "ALTER TABLE system_events ADD COLUMN IF NOT EXISTS mode TEXT DEFAULT 'paper'",
        "ALTER TABLE system_events ADD COLUMN IF NOT EXISTS market TEXT DEFAULT 'FUTURES'",
    )
    for statement in statements:
        cursor.execute(statement)
    cursor.execute("UPDATE positions SET market = 'FUTURES' WHERE market IS NULL")
    cursor.execute("UPDATE orders SET market = 'FUTURES' WHERE market IS NULL")
    cursor.execute("UPDATE trades SET market = 'FUTURES', mode = COALESCE(mode, 'paper') WHERE market IS NULL OR mode IS NULL")
    cursor.execute("UPDATE risk_events SET market = 'FUTURES', mode = COALESCE(mode, 'paper') WHERE market IS NULL OR mode IS NULL")
    cursor.execute("UPDATE system_events SET market = 'FUTURES', mode = COALESCE(mode, 'paper') WHERE market IS NULL OR mode IS NULL")
    cursor.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM balances
                GROUP BY mode, asset
                HAVING COUNT(*) > 1
            ) THEN
                RAISE EXCEPTION 'balances contain duplicate canonical (mode, asset) keys';
            END IF;
        END $$
        """
    )
    cursor.execute("ALTER TABLE balances DROP CONSTRAINT IF EXISTS balances_pkey")
    cursor.execute(
        "ALTER TABLE balances ADD CONSTRAINT balances_pkey PRIMARY KEY (mode, asset)"
    )
    cursor.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM positions
                GROUP BY market, symbol
                HAVING COUNT(*) > 1
            ) THEN
                RAISE EXCEPTION 'positions contain duplicate canonical (market, symbol) keys';
            END IF;
        END $$
        """
    )
    cursor.execute("ALTER TABLE positions DROP CONSTRAINT IF EXISTS positions_pkey")
    cursor.execute(
        "ALTER TABLE positions ADD CONSTRAINT positions_pkey PRIMARY KEY (market, symbol)"
    )
    cursor.execute(
        """
        UPDATE trade_intents
        SET payload = COALESCE(payload, CAST('{}' AS JSONB))
            || CAST('{"legacy": true}' AS JSONB)
        WHERE direction IS NULL
        """
    )
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'trade_intents' AND column_name = 'quote_quantity'
        """
    )
    if cursor.fetchone() is not None:
        cursor.execute(
            """
            UPDATE trade_intents
            SET payload = COALESCE(payload, CAST('{}' AS JSONB))
                || jsonb_build_object('legacy_quote_quantity', quote_quantity::text)
            WHERE quote_quantity IS NOT NULL
            """
        )
        cursor.execute("ALTER TABLE trade_intents DROP COLUMN IF EXISTS quote_quantity")
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'orders' AND column_name = 'quote_quantity'
        """
    )
    if cursor.fetchone() is not None:
        cursor.execute(
            """
            UPDATE orders
            SET payload = COALESCE(payload, CAST('{}' AS JSONB))
                || jsonb_build_object('legacy_quote_quantity', quote_quantity::text)
            WHERE quote_quantity IS NOT NULL
            """
        )
        cursor.execute("ALTER TABLE orders DROP COLUMN IF EXISTS quote_quantity")
    cursor.execute("DROP INDEX IF EXISTS positions_market_symbol_side_uidx")


def schema_status(dsn: str | None = None) -> dict[str, object]:
    dsn = dsn or configured_dsn()
    try:
        host, port = database_target(dsn)
    except ValueError:
        return _unavailable_status(dsn, "invalid_dsn")
    if not database_reachable(dsn):
        return _status(
            host,
            port,
            state="unavailable",
            missing_tables=sorted(REQUIRED_TABLES),
            error="unreachable",
        )

    import psycopg2

    try:
        with psycopg2.connect(dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = ANY(%s)
                    """,
                    (list(REQUIRED_TABLES),),
                )
                tables = {str(row[0]) for row in cursor.fetchall()}
    except (psycopg2.Error, OSError):
        return _status(
            host,
            port,
            state="unavailable",
            missing_tables=sorted(REQUIRED_TABLES),
            error="connection_failed",
        )

    missing = sorted(REQUIRED_TABLES - tables)
    return _status(
        host,
        port,
        state="ok" if not missing else "schema_incomplete",
        missing_tables=missing,
    )


def _serialize_time(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _run_view(row: tuple[Any, ...]) -> tuple[dict[str, object], datetime | None]:
    (
        run_id,
        collection_kind,
        status,
        started_at,
        completed_at,
        source_url,
        market_count,
        coverage_count,
        error_code,
    ) = row
    return (
        {
            "run_id": str(run_id),
            "collection_kind": str(collection_kind),
            "status": str(status),
            "started_at": _serialize_time(started_at),
            "completed_at": _serialize_time(completed_at),
            "source_url": source_url,
            "market_count": int(market_count),
            "coverage_count": int(coverage_count),
            "error_code": error_code,
        },
        completed_at,
    )


def _collection_state(
    cursor: Any,
    *,
    max_data_age_sec: int,
) -> dict[str, object]:
    columns = """
        run_id, collection_kind, status, started_at, completed_at, source_url,
        market_count, coverage_count, error_code
    """
    cursor.execute(
        f"""
        SELECT {columns}
        FROM bian_collection_runs
        WHERE collection_kind IN ('rest_24h', 'stream_trade')
        ORDER BY started_at DESC
        LIMIT 1
        """
    )
    latest_row = cursor.fetchone()
    cursor.execute(
        f"""
        SELECT {columns}
        FROM bian_collection_runs
        WHERE status = 'ok'
          AND collection_kind IN ('rest_24h', 'stream_trade')
        ORDER BY completed_at DESC
        LIMIT 1
        """
    )
    success_row = cursor.fetchone()

    latest = _run_view(latest_row)[0] if latest_row else None
    if not success_row:
        return {
            "status": "missing",
            "max_data_age_sec": max_data_age_sec,
            "age_sec": None,
            "last_attempt": latest,
            "last_success": None,
        }

    success, completed_at = _run_view(success_row)
    if completed_at is None:
        age_sec = None
    else:
        age_sec = max(
            0,
            int((datetime.now(timezone.utc) - completed_at).total_seconds()),
        )
    fresh = age_sec is not None and age_sec <= max_data_age_sec
    return {
        "status": "fresh" if fresh else "stale",
        "max_data_age_sec": max_data_age_sec,
        "age_sec": age_sec,
        "last_attempt": latest,
        "last_success": success,
    }


def read_overview(
    dsn: str | None = None,
    *,
    limit: int = 20,
    max_data_age_sec: int = 900,
) -> dict[str, object]:
    """Read the bounded operator view owned by bian's PostgreSQL schema."""
    import psycopg2

    dsn = dsn or configured_dsn()
    status = schema_status(dsn)
    if status["status"] != "ok":
        return {
            "database_status": status,
            "collection": {
                "status": "unavailable",
                "max_data_age_sec": max_data_age_sec,
                "age_sec": None,
                "last_attempt": None,
                "last_success": None,
            },
            "markets": [],
            "coverage": [],
            "updated_at": None,
        }

    bounded_limit = max(1, min(int(limit), 100))
    bounded_age = max(1, int(max_data_age_sec))
    try:
        with psycopg2.connect(dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                collection = _collection_state(
                    cursor,
                    max_data_age_sec=bounded_age,
                )
                cursor.execute(
                    """
                    SELECT symbol, last_price::text,
                           price_change_percent::text,
                           quote_volume::text, captured_at::text, source_url
                    FROM (
                        SELECT DISTINCT ON (symbol)
                            symbol, last_price, price_change_percent, quote_volume,
                            captured_at, source_url
                        FROM bian_market_snapshots
                        ORDER BY symbol, captured_at DESC, id DESC
                    ) latest
                    ORDER BY quote_volume DESC NULLS LAST, symbol
                    LIMIT %s
                    """,
                    (bounded_limit,),
                )
                market_columns = [str(column[0]) for column in cursor.description]
                markets = [
                    dict(zip(market_columns, row))
                    for row in cursor.fetchall()
                ]
                cursor.execute(
                    """
                    SELECT product_type, status, symbol_count, source_url,
                           captured_at::text, detail
                    FROM bian_product_coverage
                    ORDER BY product_type
                    """
                )
                coverage_columns = [str(column[0]) for column in cursor.description]
                coverage = [
                    dict(zip(coverage_columns, row))
                    for row in cursor.fetchall()
                ]
    except (psycopg2.Error, OSError):
        return {
            "database_status": _unavailable_status(dsn, "query_failed"),
            "collection": {
                "status": "unavailable",
                "max_data_age_sec": bounded_age,
                "age_sec": None,
                "last_attempt": None,
                "last_success": None,
            },
            "markets": [],
            "coverage": [],
            "updated_at": None,
        }

    last_success = collection["last_success"]
    updated_at = (
        last_success.get("completed_at")
        if isinstance(last_success, dict)
        else None
    )
    return {
        "database_status": status,
        "collection": collection,
        "markets": markets,
        "coverage": coverage,
        "updated_at": updated_at,
    }


def record_collection_failure(
    run_id: str,
    collection_kind: str,
    error_code: str,
    *,
    dsn: str | None = None,
) -> None:
    """Persist a safe failure code when storage remains available."""
    import psycopg2

    dsn = dsn or configured_dsn()
    ensure_schema(dsn)
    with psycopg2.connect(dsn, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bian_collection_runs(
                    run_id, collection_kind, status, started_at, completed_at,
                    error_code
                ) VALUES (%s, %s, 'failed', NOW(), NOW(), %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    status = 'failed',
                    completed_at = NOW(),
                    error_code = EXCLUDED.error_code
                """,
                (run_id, collection_kind, error_code),
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect or initialize bian storage.")
    parser.add_argument("action", choices=("ensure", "status"))
    args = parser.parse_args(argv)
    if args.action == "ensure":
        ensure_schema()
    report = read_overview(limit=1)
    print(
        {
            "database": report["database_status"],
            "collection": report["collection"],
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
