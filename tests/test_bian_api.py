from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

import bian_api


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
                "last_price": 100.0,
                "price_change_percent": 2.0,
                "quote_volume": 900.0,
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
    assert payload["provenance"]["raw_payloads"] is True


def test_health_reports_degraded_database_without_credentials() -> None:
    with patch.object(
        bian_api,
        "schema_status",
        return_value={
            "status": "unavailable",
            "database": "localhost:5446",
            "missing_tables": ["bian_market_snapshots"],
        },
    ):
        response = TestClient(bian_api.app).get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "degraded"
    assert "bian" in payload["database"]["missing_tables"][0]
    assert "password" not in str(payload)


def test_front_data_keeps_empty_database_result_valid() -> None:
    empty_report = {
        "database_status": {
            "status": "ok",
            "database": "localhost:5446",
            "missing_tables": [],
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
