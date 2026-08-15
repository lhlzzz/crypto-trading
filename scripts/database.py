"""Database contract for the independent bian bot."""
from __future__ import annotations

import argparse
import os
import socket
from collections.abc import Mapping
from urllib.parse import urlparse

DEFAULT_DSN = "postgresql://bian:bian@localhost:5446/bian"
REQUIRED_TABLES = frozenset({"bian_market_snapshots", "bian_product_coverage"})


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
    except OSError:
        return False


def ensure_schema(dsn: str | None = None) -> None:
    import psycopg2

    with psycopg2.connect(dsn or configured_dsn(), connect_timeout=5) as connection:
        with connection.cursor() as cursor:
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
                CREATE INDEX IF NOT EXISTS bian_market_snapshots_symbol_captured_idx
                ON bian_market_snapshots(symbol, captured_at DESC)
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


def schema_status(dsn: str | None = None) -> dict[str, object]:
    dsn = dsn or configured_dsn()
    host, port = database_target(dsn)
    if not database_reachable(dsn):
        return {
            "status": "unavailable",
            "database": f"{host}:{port}",
            "missing_tables": sorted(REQUIRED_TABLES),
        }

    import psycopg2

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
    missing = sorted(REQUIRED_TABLES - tables)
    return {
        "status": "ok" if not missing else "schema_incomplete",
        "database": f"{host}:{port}",
        "missing_tables": missing,
    }


def read_overview(
    dsn: str | None = None,
    *,
    limit: int = 20,
) -> dict[str, object]:
    """Read the bounded operator view owned by bian's PostgreSQL schema."""
    import psycopg2

    dsn = dsn or configured_dsn()
    status = schema_status(dsn)
    if status["status"] != "ok":
        return {
            "database_status": status,
            "markets": [],
            "coverage": [],
            "updated_at": None,
        }

    bounded_limit = max(1, min(int(limit), 100))
    with psycopg2.connect(dsn, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT symbol, last_price::double precision,
                       price_change_percent::double precision,
                       quote_volume::double precision, captured_at::text,
                       source_url
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

    updated_at = markets[0].get("captured_at") if markets else None
    return {
        "database_status": status,
        "markets": markets,
        "coverage": coverage,
        "updated_at": updated_at,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect or initialize bian storage.")
    parser.add_argument("action", choices=("ensure", "status"))
    args = parser.parse_args(argv)
    if args.action == "ensure":
        ensure_schema()
    print(schema_status())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
