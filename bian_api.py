#!/usr/bin/env python3
"""Read-only FastAPI boundary for Binance public market data."""
from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, ConfigDict, Field

from scripts.database import read_overview, schema_status

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
    markets: list[dict[str, Any]] = Field(default_factory=list)
    coverage: list[dict[str, Any]] = Field(default_factory=list)
    provenance: dict[str, Any] = Field(default_factory=dict)
    knowledge: dict[str, Any] = Field(default_factory=dict)
    updated_at: str | None = None


class BianHealth(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: str
    contract_version: str
    release_version: str
    environment: str
    database: dict[str, Any]


def _front_data(limit: int) -> dict[str, Any]:
    report = read_overview(limit=limit)
    database = report["database_status"]
    connected = database.get("status") == "ok"
    return {
        "contract_version": BIAN_OPERATOR_CONTRACT_VERSION,
        "release_version": BIAN_RELEASE_VERSION,
        "environment": BIAN_ENVIRONMENT,
        "workspace": "bian",
        "mode": "PUBLIC_READ_ONLY / NO_TRADE",
        "source": "binance_public_api",
        "database_connected": connected,
        "database": database,
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
    status = schema_status()
    return {
        "status": "ok" if status["status"] == "ok" else "degraded",
        "contract_version": BIAN_OPERATOR_CONTRACT_VERSION,
        "release_version": BIAN_RELEASE_VERSION,
        "environment": BIAN_ENVIRONMENT,
        "database": status,
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "bian_api:app",
        host=os.environ.get("BIAN_API_HOST", "0.0.0.0"),
        port=int(os.environ.get("BIAN_API_PORT", "8001")),
        reload=False,
    )
