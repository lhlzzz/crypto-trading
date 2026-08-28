from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient
from psycopg2 import OperationalError

import bian_api
from scripts.database import schema_status


def _report() -> dict[str, object]:
    return {
        "database_status": {
            "status": "ok",
            "database": "localhost:5446",
            "missing_tables": [],
        },
        "markets": [
            {
                "symbol": "BTCUSDT",
                "last_price": "100",
                "price_change_percent": "2",
                "quote_volume": "900",
                "captured_at": "2026-08-15T00:00:00+00:00",
                "source_url": "https://api.binance.com/api/v3/ticker/24hr",
                "payload": {"symbol": "BTCUSDT"},
            }
        ],
        "coverage": [
            {
                "product_type": "spot",
                "status": "ok",
                "symbol_count": 1,
                "source_url": "https://api.binance.com/api/v3/exchangeInfo",
                "captured_at": "2026-08-15T00:00:00+00:00",
                "detail": {"symbol_field": "symbols"},
            }
        ],
        "collection": {
            "status": "fresh",
            "max_data_age_sec": 900,
            "age_sec": 1,
            "last_attempt": {"run_id": "run-1", "status": "ok"},
            "last_success": {"run_id": "run-1", "status": "ok"},
        },
        "updated_at": "2026-08-15T00:00:00+00:00",
    }


def test_front_data_exposes_versioned_read_only_contract() -> None:
    with patch.object(bian_api, "read_overview", return_value=_report()):
        response = TestClient(bian_api.app).get("/api/os/front-data?limit=1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["workspace"] == "bian"
    assert payload["mode"] == "PUBLIC_READ_ONLY / NO_TRADE"
    assert payload["database_connected"] is True
    assert payload["markets"][0]["symbol"] == "BTCUSDT"
    assert payload["markets"][0]["last_price"] == "100"
    assert payload["collection"]["status"] == "fresh"
    assert payload["provenance"]["raw_payloads"] is True


def test_health_reports_degraded_database_without_connection() -> None:
    with patch.object(
        bian_api,
        "read_overview",
        return_value={
            "database_status": {
                "status": "unavailable",
                "database": "localhost:5446",
                "missing_tables": ["bian_market_snapshots"],
                "error": "connection_failed",
            },
            "collection": {
                "status": "unavailable",
                "max_data_age_sec": 900,
                "age_sec": None,
                "last_attempt": None,
                "last_success": None,
            },
            "markets": [],
            "coverage": [],
            "updated_at": None,
        },
    ):
        response = TestClient(bian_api.app).get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["service"] == "bian"
    assert payload["status"] == "degraded"
    assert "bian" in payload["database"]["missing_tables"][0]
    assert "password" not in str(payload)


def test_health_reports_stale_data_as_degraded() -> None:
    report = _report()
    report["collection"] = {
        "status": "stale",
        "max_data_age_sec": 900,
        "age_sec": 901,
        "last_attempt": {"run_id": "run-1", "status": "ok"},
        "last_success": {"run_id": "run-1", "status": "ok"},
    }
    with patch.object(bian_api, "read_overview", return_value=report):
        response = TestClient(bian_api.app).get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "degraded"


def test_front_data_keeps_empty_database_result_valid() -> None:
    empty_report = {
        "database_status": {
            "status": "ok",
            "database": "localhost:5446",
            "missing_tables": [],
        },
        "collection": {
            "status": "missing",
            "max_data_age_sec": 900,
            "age_sec": None,
            "last_attempt": None,
            "last_success": None,
        },
        "markets": [],
        "coverage": [],
        "updated_at": None,
    }
    with patch.object(bian_api, "read_overview", return_value=empty_report):
        response = TestClient(bian_api.app).get("/api/dashboard/overview")

    assert response.status_code == 200
    payload = response.json()
    assert payload["database_connected"] is True
    assert payload["markets"] == []
    assert payload["updated_at"] is None


def test_schema_status_hides_database_authentication_failure() -> None:
    with patch(
        "scripts.database.database_reachable",
        return_value=True,
    ), patch(
        "psycopg2.connect",
        side_effect=OperationalError("password authentication failed"),
    ):
        status = schema_status("postgresql://bian:secret@localhost:5446/bian")

    assert status["status"] == "unavailable"
    assert status["error"] == "connection_failed"
    assert "secret" not in str(status)


def test_trading_api_is_read_only_and_exposes_status() -> None:
    store = type(
        "StoreStub",
        (),
        {
            "trading_counts": lambda self: {
                "signals": 1,
                "trade_intents": 1,
                "orders": 1,
                "trades": 1,
                "positions": 1,
                "risk_events": 1,
            },
            "list_orders": lambda self, limit: [{"status": "FILLED", "limit": limit}],
            "list_trades": lambda self, limit: [{"symbol": "BTCUSDT", "limit": limit}],
            "list_positions": lambda self: [{"symbol": "BTCUSDT"}],
            "list_balances": lambda self: [{"asset": "USDT", "free": "1000"}],
            "list_risk_events": lambda self, limit: [{"decision": "ALLOW", "limit": limit}],
            "list_system_events": lambda self, limit: [{"event_type": "SYSTEM_EVENT", "limit": limit}],
            "trading_summary": lambda self: {"account_equity_usdt": "1000", "total_pnl": "0"},
            "is_halted": lambda self: False,
        },
    )
    with patch.object(bian_api, "_trading_store", return_value=store()):
        client = TestClient(bian_api.app)
        assert client.get("/api/trading/status").status_code == 200
        assert client.get("/api/trading/orders?limit=1").json()["items"][0]["status"] == "FILLED"
        assert client.get("/api/trading/trades").json()["items"][0]["symbol"] == "BTCUSDT"
        assert client.get("/api/trading/positions").status_code == 200
        assert client.get("/api/trading/balance").status_code == 200
        assert client.get("/api/trading/risk").status_code == 200
        assert client.get("/api/trading/summary").json()["account_equity_usdt"] == "1000"
        assert client.get("/api/trading/events").json()["items"][0]["event_type"] == "SYSTEM_EVENT"
        assert client.post("/api/trading/orders").status_code == 405
