#!/usr/bin/env python3
"""Read-only FastAPI boundary for Binance public market data."""
from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, ConfigDict, Field

from scripts.database import read_overview
from runtime_gate import GateResult, evaluate_runtime_gate
from trading_store import TradingStore

BIAN_OPERATOR_CONTRACT_VERSION = os.environ.get(
    "BIAN_OPERATOR_CONTRACT_VERSION",
    "2026-08-15",
).strip()
BIAN_RELEASE_VERSION = os.environ.get(
    "BIAN_RELEASE_VERSION",
    BIAN_OPERATOR_CONTRACT_VERSION,
).strip()
BIAN_ENVIRONMENT = os.environ.get("BIAN_ENVIRONMENT", "development").strip().lower()


def _env_enabled(name: str, *, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


BIAN_API_DOCS_ENABLED = _env_enabled(
    "BIAN_API_DOCS_ENABLED",
    default=BIAN_ENVIRONMENT != "production",
)


def _csv_env(name: str, default: str) -> list[str]:
    return [
        value.strip()
        for value in os.environ.get(name, default).split(",")
        if value.strip()
    ]


def _positive_int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, default))
    except ValueError:
        return default
    return max(1, value)


BIAN_MAX_DATA_AGE_SEC = _positive_int_env("BIAN_MAX_DATA_AGE_SEC", 900)


def _trading_store() -> TradingStore:
    return TradingStore()


def _trading_mode() -> str:
    return os.environ.get("BIAN_MODE", "paper").strip().lower()


def _trading_halted() -> bool:
    return _env_enabled("BIAN_TRADING_HALTED")


def _persistent_trading_halt(store: TradingStore) -> bool:
    try:
        return _trading_halted() or store.is_halted()
    except Exception:
        return _trading_halted()


def _runtime_gate(
    *,
    store: TradingStore | None = None,
    data_health_ok: bool | None = None,
    reconciliation_ok: bool | None = None,
) -> GateResult:
    resolved_store = store or _trading_store()
    return evaluate_runtime_gate(
        mode=_trading_mode(),
        store=resolved_store,
        data_health_ok=data_health_ok,
        reconciliation_ok=reconciliation_ok,
        probe_account=False,
    )


class BianFrontData(BaseModel):
    """Versioned response consumed by the Financial OS bian workspace."""

    model_config = ConfigDict(extra="allow")

    contract_version: str
    release_version: str
    environment: str
    workspace: str
    mode: str
    source: str
    database_connected: bool
    database: dict[str, Any]
    collection: dict[str, Any]
    markets: list[dict[str, Any]] = Field(default_factory=list)
    coverage: list[dict[str, Any]] = Field(default_factory=list)
    provenance: dict[str, Any] = Field(default_factory=dict)
    knowledge: dict[str, Any] = Field(default_factory=dict)
    updated_at: str | None = None


class BianHealth(BaseModel):
    model_config = ConfigDict(extra="allow")

    service: str
    status: str
    contract_version: str
    release_version: str
    environment: str
    database: dict[str, Any]
    collection: dict[str, Any]


def _front_data(limit: int) -> dict[str, Any]:
    report = read_overview(
        limit=limit,
        max_data_age_sec=BIAN_MAX_DATA_AGE_SEC,
    )
    database = report["database_status"]
    connected = database.get("status") == "ok"
    gate_store = _trading_store()
    try:
        store = gate_store
        gate = _runtime_gate(
            store=store,
            data_health_ok=report["collection"].get("status") == "fresh",
            reconciliation_ok=not _persistent_trading_halt(store),
        )
        positioning = {
            "enabled": _env_enabled("POSITIONING_DECISION_ENABLED"),
            "shadow_mode": not _env_enabled("POSITIONING_DECISION_ENABLED"),
            "snapshots": store.list_positioning_snapshots(limit),
            "source_freshness": store.market_data_freshness(
                max_age_sec=BIAN_MAX_DATA_AGE_SEC
            ),
        }
    except Exception:
        gate = _runtime_gate(
            store=None,
            data_health_ok=False,
            reconciliation_ok=False,
        )
        positioning = {
            "enabled": _env_enabled("POSITIONING_DECISION_ENABLED"),
            "shadow_mode": not _env_enabled("POSITIONING_DECISION_ENABLED"),
            "snapshots": [],
            "status": "unavailable",
        }
    return {
        "contract_version": BIAN_OPERATOR_CONTRACT_VERSION,
        "release_version": BIAN_RELEASE_VERSION,
        "environment": BIAN_ENVIRONMENT,
        "workspace": "bian",
        "api_mode": "READ_ONLY",
        "mode": _trading_mode(),
        "trading_mode": _trading_mode(),
        "trading_halted": _persistent_trading_halt(_trading_store()),
        "source": "binance_public_api_and_stream",
        "database_connected": connected,
        "database": database,
        "collection": report["collection"],
        "markets": report["markets"],
        "coverage": report["coverage"],
        "provenance": {
            "authority": "bian_postgresql",
            "source_urls": sorted(
                {
                    str(row["source_url"])
                    for row in [*report["markets"], *report["coverage"]]
                    if row.get("source_url")
                }
            ),
            "raw_payloads": True,
            "captured_at": report["updated_at"],
        },
        "knowledge": {
            "kind": "public_market_data_provenance",
            "authority": "bian_postgresql",
            "entries": report["coverage"],
        },
        "positioning": positioning,
        "runtime_gate": gate.as_dict(),
        "updated_at": report["updated_at"],
    }


app = FastAPI(
    title="bian API",
    description="Read-only Binance public market-data service.",
    version=BIAN_RELEASE_VERSION,
    docs_url="/docs" if BIAN_API_DOCS_ENABLED else None,
    redoc_url=None,
    openapi_url="/openapi.json" if BIAN_API_DOCS_ENABLED else None,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_csv_env(
        "BIAN_CORS_ORIGINS",
        "http://127.0.0.1:3000,http://localhost:3000",
    ),
    allow_methods=["GET"],
    allow_headers=["Accept", "Content-Type"],
)
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=_csv_env(
        "BIAN_TRUSTED_HOSTS",
        "127.0.0.1,localhost,testserver",
    ),
)


@app.get("/health", response_model=BianHealth)
def health() -> dict[str, Any]:
    report = read_overview(
        limit=1,
        max_data_age_sec=BIAN_MAX_DATA_AGE_SEC,
    )
    database = report["database_status"]
    collection = report["collection"]
    healthy = (
        database.get("status") == "ok"
        and collection.get("status") == "fresh"
    )
    return {
        "service": "bian",
        "status": "ok" if healthy else "degraded",
        "contract_version": BIAN_OPERATOR_CONTRACT_VERSION,
        "release_version": BIAN_RELEASE_VERSION,
        "environment": BIAN_ENVIRONMENT,
        "database": database,
        "collection": collection,
    }


@app.get("/api/os/front-data", response_model=BianFrontData)
def get_os_front_data(
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return _front_data(limit)


@app.get("/api/dashboard/overview", response_model=BianFrontData)
def get_dashboard_overview(
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return _front_data(limit)


@app.get("/api/trading/status")
def get_trading_status() -> dict[str, Any]:
    store = _trading_store()
    try:
        counts = store.trading_counts()
        summary = store.trading_summary()
        halted = _persistent_trading_halt(store)
        database_status = "ok"
    except Exception:
        counts = {}
        summary = {}
        halted = _trading_halted()
        database_status = "unavailable"
    gate = _runtime_gate(
        store=store,
        data_health_ok=database_status == "ok",
        reconciliation_ok=not halted,
    )
    return {
        "api_mode": "READ_ONLY",
        "trading_mode": gate.mode,
        "database_status": database_status,
        "trading_halted": halted,
        "runtime_gate": gate.as_dict(),
        "risk_status": gate.risk_status,
        "live_orders_allowed": gate.live_allowed,
        "counts": counts,
        "summary": summary,
    }


@app.get("/api/trading/summary")
def get_trading_summary() -> dict[str, Any]:
    try:
        return {"status": "ok", **_trading_store().trading_summary()}
    except Exception:
        return {"status": "unavailable"}


@app.get("/api/trading/orders")
def get_trading_orders(
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    try:
        return {"items": _trading_store().list_orders(limit)}
    except Exception:
        return {"items": [], "status": "unavailable"}


@app.get("/api/trading/trades")
def get_trading_trades(
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    try:
        return {"items": _trading_store().list_trades(limit)}
    except Exception:
        return {"items": [], "status": "unavailable"}


@app.get("/api/trading/positions")
def get_trading_positions() -> dict[str, Any]:
    try:
        return {"items": _trading_store().list_positions()}
    except Exception:
        return {"items": [], "status": "unavailable"}


@app.get("/api/trading/balance")
def get_trading_balance() -> dict[str, Any]:
    try:
        return {"items": _trading_store().list_balances()}
    except Exception:
        return {"items": [], "status": "unavailable"}


@app.get("/api/trading/risk")
def get_trading_risk(
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    try:
        return {
            "halted": _persistent_trading_halt(_trading_store()),
            "items": _trading_store().list_risk_events(limit),
        }
    except Exception:
        return {"halted": _trading_halted(), "items": [], "status": "unavailable"}


@app.get("/api/trading/events")
def get_trading_events(
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    try:
        return {"items": _trading_store().list_system_events(limit)}
    except Exception:
        return {"items": [], "status": "unavailable"}


def _positioning_items(limit: int) -> dict[str, Any]:
    try:
        items = _trading_store().list_positioning_snapshots(limit)
        return {"status": "ok", "items": items}
    except Exception:
        return {"status": "unavailable", "items": []}


@app.get("/api/positioning/snapshots")
def get_positioning_snapshots(
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    return _positioning_items(limit)


@app.get("/api/positioning/candidates")
def get_positioning_candidates(
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    result = _positioning_items(200)
    if result.get("status") != "ok":
        return result
    items = [
        item for item in result["items"]
        if item.get("direction") in {"LONG", "SHORT"}
    ]
    return {"status": "ok", "items": items[:limit]}


@app.get("/api/positioning/transitions")
def get_positioning_transitions(
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    result = _positioning_items(limit)
    if result.get("status") != "ok":
        return result
    return {
        "status": "ok",
        "items": [item for item in result["items"] if item.get("transition") != "NONE"],
    }


@app.get("/api/positioning/evidence")
def get_positioning_evidence(
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    result = _positioning_items(limit)
    if result.get("status") != "ok":
        return result
    return {
        "status": "ok",
        "items": [
            {
                "snapshot_id": item.get("snapshot_id"),
                "symbol": item.get("symbol"),
                "observed_at": item.get("observed_at"),
                "payload": item.get("payload", {}),
            }
            for item in result["items"]
        ],
    }


@app.get("/api/positioning/data-quality")
def get_positioning_data_quality(
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    result = _positioning_items(limit)
    try:
        source_freshness = _trading_store().market_data_freshness(
            max_age_sec=BIAN_MAX_DATA_AGE_SEC
        )
    except Exception:
        source_freshness = []
    if result.get("status") != "ok":
        return {**result, "source_freshness": source_freshness}
    return {
        "status": "ok",
        "source_freshness": source_freshness,
        "items": [
            {
                "snapshot_id": item.get("snapshot_id"),
                "symbol": item.get("symbol"),
                "observed_at": item.get("observed_at"),
                "data_quality_score": item.get("data_quality_score"),
                "state": item.get("state"),
            }
            for item in result["items"]
        ],
    }


@app.get("/api/positioning/status")
def get_positioning_status() -> dict[str, Any]:
    enabled = _env_enabled("POSITIONING_DECISION_ENABLED")
    snapshots = _positioning_items(1)
    items = snapshots.get("items") or []
    latest = items[0] if items else None
    gate = _runtime_gate()
    return {
        "status": "ok" if snapshots.get("status") == "ok" else "unavailable",
        "enabled": enabled,
        "shadow_mode": not enabled,
        "api_mode": "READ_ONLY",
        "trading_mode": gate.mode,
        "latest": latest,
        "trading_halted": _persistent_trading_halt(_trading_store()),
        "runtime_gate": gate.as_dict(),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "bian_api:app",
        host=os.environ.get("BIAN_API_HOST", "0.0.0.0"),
        port=int(os.environ.get("BIAN_API_PORT", "8001")),
        reload=False,
    )
