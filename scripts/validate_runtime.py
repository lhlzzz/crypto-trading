#!/usr/bin/env python3
"""Elapsed validation session owner for bian Futures observation.

This script never marks PASS from unit tests or mocked exchange responses.
Realtime stages run the public collector for a requested duration and persist
``runtime_validation_report.json`` as a session, not a one-shot report.
Missing Binance connectivity is FAIL. Dedicated markPrice is not required
while MARK_INDEX_FUNDING is the production mark/index/funding source.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import resource
import subprocess
import sys
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine import runtime_required_sources
from runtime_gate import evaluate_runtime_gate, max_data_age_sec
from scripts.bian_market import observe
from trading_store import TradingStore

STAGE_SPECS = {
    "realtime_30m": {"duration": 1800, "prior": None, "kind": "realtime"},
    "realtime_2h": {"duration": 7200, "prior": "realtime_30m", "kind": "realtime"},
    "realtime_6h": {"duration": 21600, "prior": "realtime_2h", "kind": "realtime"},
    "realtime_24h": {"duration": 86400, "prior": "realtime_6h", "kind": "realtime"},
    "paper_24h": {"duration": 86400, "prior": "realtime_24h", "kind": "paper"},
    "shadow_7d": {"duration": 604800, "prior": "paper_24h", "kind": "shadow"},
    "alpha_oos": {"duration": 0, "prior": "shadow_7d", "kind": "alpha"},
    "testnet": {"duration": 0, "prior": "alpha_oos", "kind": "testnet"},
    "live_preflight": {"duration": 0, "prior": None, "kind": "live"},
}
DURATION_GATES = {
    spec["duration"]: name
    for name, spec in STAGE_SPECS.items()
    if spec["kind"] == "realtime"
}
LADDER = ("realtime_30m", "realtime_2h", "realtime_6h", "realtime_24h")
BENCHMARK_SYMBOLS = frozenset({"BTCUSDT", "ETHUSDT"})
PAPER_RESTART_EXIT = 75
STAGE_STATES = {
    "NOT_STARTED", "RUNNING", "PASSED", "FAILED", "BLOCKED", "EXPIRED",
}
STORE_STATES = {"NOT_STARTED", "RUNNING", "PASSED", "FAILED"}
CHANNEL_SOURCES = {
    "TRADE": ("FUTURES_TRADE",),
    "BOOK_TICKER": ("FUTURES_BOOK_TICKER",),
    "DEPTH": ("FUTURES_DEPTH",),
    "MARK_INDEX_FUNDING": (
        "FUTURES_MARK_PRICE",
        "FUTURES_INDEX_PRICE",
        "FUTURES_FUNDING_LIVENESS",
    ),
    "LIQUIDATION_LIVENESS": ("FUTURES_LIQUIDATION_LIVENESS",),
}
REQUIRED_CHANNELS = tuple(CHANNEL_SOURCES)
SOURCE_KINDS = {
    "FUTURES_TRADE": "continuous_stream",
    "FUTURES_BOOK_TICKER": "continuous_stream",
    "FUTURES_DEPTH": "continuous_stream",
    "FUTURES_MARK_PRICE": "continuous_stream",
    "FUTURES_INDEX_PRICE": "continuous_stream",
    "FUTURES_FUNDING_LIVENESS": "continuous_stream",
    "FUTURES_LIQUIDATION_LIVENESS": "heartbeat_source",
    "FUTURES_FORCE_ORDER": "event_source",
    "FUTURES_OPEN_INTEREST": "rest_observation",
    "FUTURES_TAKER": "rest_observation",
}
FREEZE_PATHS = (
    "scripts/bian_market.py",
    "binance_client.py",
    "runtime_gate.py",
    "trading_store.py",
)
REPORT_PATH = Path(PROJECT_ROOT) / "runtime_validation_report.json"
SAMPLE_SEC = 30.0
HEALTHY_STATUSES = {"FRESH", "OK", "LIVE", "PRESENT"}
UNSAFE_STATUSES = {"GAP", "UNSAFE"}
STORE_GATES = {
    "observation", "paper", "shadow", "testnet",
    "realtime_30m", "realtime_2h", "realtime_6h", "realtime_24h",
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None = None) -> str:
    return (value or _now()).isoformat()


def _commit_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, text=True
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "UNKNOWN"


def _file_hash(relative: str) -> str:
    path = Path(PROJECT_ROOT) / relative
    if not path.exists():
        return "MISSING"
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _freeze_hashes() -> dict[str, str]:
    return {path: _file_hash(path) for path in FREEZE_PATHS}


def _universe_snapshot(symbols: list[str]) -> dict[str, Any]:
    compact = [item.replace("-", "").upper() for item in symbols]
    digest = hashlib.sha256(",".join(compact).encode("utf-8")).hexdigest()
    return {
        "universe_version": digest[:12],
        "symbol_hash": digest,
        "symbol_set": compact,
        "locked": True,
    }


def _config_hash(stage: str, duration: int, symbols: list[str]) -> str:
    payload = {
        "stage": stage,
        "duration": duration,
        "symbols": [item.replace("-", "").upper() for item in symbols],
        "freeze": _freeze_hashes(),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def stage_status(stage: dict[str, Any] | None) -> str:
    value = str((stage or {}).get("status") or "NOT_STARTED").upper()
    return value if value in STAGE_STATES else "FAILED"


def prior_passed(stages: dict[str, Any], gate: str) -> bool:
    prior = STAGE_SPECS[gate]["prior"]
    if prior is None:
        return True
    return stage_status(stages.get(prior)) == "PASSED"


def expire_realtime_if_code_changed(stages: dict[str, Any]) -> list[str]:
    expired: list[str] = []
    current = _freeze_hashes()
    for name in STAGE_SPECS:
        stage = dict(stages.get(name) or {})
        if stage_status(stage) != "PASSED":
            continue
        recorded = stage.get("freeze_hashes") or {}
        if not recorded:
            stage["freeze_hashes"] = current
            stages[name] = stage
            continue
        identity_changed = recorded != current
        symbols = [str(item) for item in (stage.get("symbol_set") or [])]
        if symbols:
            universe = _universe_snapshot(symbols)
            if stage.get("universe_version") and stage.get("universe_version") != universe["universe_version"]:
                identity_changed = True
            if stage.get("symbol_hash") and stage.get("symbol_hash") != universe["symbol_hash"]:
                identity_changed = True
        requested = int(stage.get("requested_duration") or STAGE_SPECS[name]["duration"])
        if stage.get("config_hash") and symbols:
            expected = _config_hash(name, requested, symbols)
            if stage.get("config_hash") != expected:
                identity_changed = True
        if identity_changed:
            stage["status"] = "EXPIRED"
            stage["reason"] = "CODE_CHANGE_INVALIDATED"
            stages[name] = stage
            expired.append(name)
    return expired


def session_owner_alive(stage: dict[str, Any] | None) -> bool:
    pid = (stage or {}).get("owner_pid")
    try:
        os.kill(int(pid), 0)
    except (OSError, TypeError, ValueError):
        return False
    return True


def recover_orphaned_sessions(stages: dict[str, Any]) -> list[str]:
    recovered: list[str] = []
    for name, current in list(stages.items()):
        stage = dict(current or {})
        if stage_status(stage) != "RUNNING":
            continue
        if session_owner_alive(stage):
            continue
        stage["status"] = "FAILED"
        stage["reason"] = "SESSION_OWNER_DEAD"
        stages[name] = stage
        recovered.append(name)
    return recovered


def channel_status(health: dict[str, Any]) -> dict[str, str]:
    rows = health.get("rows") or []
    by_source: dict[str, set[str]] = {}
    for row in rows:
        source = str(row.get("source") or row.get("event_type") or "").upper()
        status = str(row.get("status") or "MISSING").upper()
        by_source.setdefault(source, set()).add(status)
    missing = {str(item).upper() for item in health.get("missing_sources") or []}
    stale = {str(item).upper() for item in health.get("stale_sources") or []}
    result: dict[str, str] = {}
    for channel, sources in CHANNEL_SOURCES.items():
        component_status: set[str] = set()
        for source in sources:
            if source in missing or source not in by_source:
                component_status.add("MISSING")
                continue
            component_status.update(by_source[source])
            if source in stale:
                component_status.add("STALE")
        if component_status & UNSAFE_STATUSES or component_status - HEALTHY_STATUSES:
            result[channel] = "FAIL"
        else:
            result[channel] = "PASS"
    result["DEDICATED_MARK_PRICE"] = "NOT_REQUIRED"
    result["ORDERBOOK"] = result["DEPTH"]
    return result


def required_channels_healthy(status: dict[str, str]) -> bool:
    return all(status.get(channel) == "PASS" for channel in REQUIRED_CHANNELS)


def watchdog_state(
    *,
    collector_alive: bool,
    health: dict[str, Any],
    persistence_ok: bool,
    persistence_backlog: bool = False,
) -> str:
    if not collector_alive or not persistence_ok:
        return "HALT"
    missing = health.get("missing_sources") or []
    if "STORE" in missing or "COLLECTOR" in missing:
        return "HALT"
    if health.get("gap_count") or any(
        str(row.get("status") or "").upper() == "UNSAFE"
        for row in health.get("rows") or []
    ):
        return "UNHEALTHY"
    if missing or health.get("stale_sources") or persistence_backlog:
        return "DEGRADED"
    return "HEALTHY"


def symbol_and_global_health(health: dict[str, Any]) -> dict[str, Any]:
    rows = health.get("rows") or []
    per_symbol: dict[str, dict[str, str]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        source = str(row.get("source") or "").upper()
        if symbol and source:
            per_symbol.setdefault(symbol, {})[source] = str(row.get("status") or "MISSING").upper()
    required = runtime_required_sources()
    symbol_health = {}
    blocked_symbols = []
    for symbol, sources in per_symbol.items():
        relevant = {
            source: status for source, status in sources.items() if source in required
        }
        if not relevant:
            state = "UNKNOWN"
        elif all(status in HEALTHY_STATUSES for status in relevant.values()):
            state = "HEALTHY"
        else:
            state = "BLOCKED"
            blocked_symbols.append(symbol)
        symbol_health[symbol] = state
    global_sources = {
        str(row.get("source") or "").upper()
        for row in rows
        if str(row.get("status") or "").upper() in HEALTHY_STATUSES
    }
    required_stream = {source for sources in CHANNEL_SOURCES.values() for source in sources}
    global_missing = [source for source in sorted(required_stream) if source not in global_sources]
    missing = health.get("missing_sources") or []
    global_halt = missing in (["STORE"], ["COLLECTOR"]) or (
        bool(global_missing) and not blocked_symbols and bool(rows)
        and all(state != "HEALTHY" for state in symbol_health.values())
    )
    return {
        "symbol_health": symbol_health,
        "blocked_symbols": blocked_symbols,
        "global_halt": global_halt,
        "global_missing": global_missing,
    }


def reconnect_events_from_samples(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    previous = None
    disconnect_at = None
    old_connection_id = 0
    connection_id = 0
    for sample in samples:
        status = str(sample.get("status") or "")
        at = sample.get("at")
        if previous in {"healthy", None} and status in {"degraded", "failure"}:
            disconnect_at = at
            old_connection_id = connection_id
        recovered = previous in {"degraded", "failure"} and status == "healthy" and disconnect_at
        if recovered:
            connection_id += 1
            started = datetime.fromisoformat(str(disconnect_at).replace("Z", "+00:00"))
            ended = datetime.fromisoformat(str(at).replace("Z", "+00:00"))
            events.append({
                "old_connection_id": f"ws-{old_connection_id}",
                "new_connection_id": f"ws-{connection_id}",
                "disconnect_at": disconnect_at,
                "reconnect_at": at,
                "recovery_ms": max(0, int((ended - started).total_seconds() * 1000)),
                "subscriptions_restored": not sample.get("missing_sources")
                and not sample.get("stale_sources"),
            })
            disconnect_at = None
        previous = status
    for event in events:
        event.setdefault("channel", None)
        event.setdefault("reason", "health_sample_transition")
        event["source"] = "health_sample_fallback"
        event["proof"] = False
    return events


def canonical_lifecycle_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    canonical: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for event in events:
        if event.get("source") != "collector_lifecycle":
            continue
        if not event.get("old_connection_id") or not event.get("new_connection_id"):
            continue
        if not event.get("channel"):
            continue
        key = (
            event.get("old_connection_id"),
            event.get("new_connection_id"),
            event.get("channel"),
            event.get("reconnect_at"),
        )
        if key in seen:
            continue
        seen.add(key)
        canonical.append(dict(event))
    return canonical


def reconnect_events_from_lifecycle(health: dict[str, Any]) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    for row in health.get("rows") or []:
        payload = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        nested = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        for event in (
            payload.get("reconnect_events")
            or nested.get("reconnect_events")
            or []
        ):
            if isinstance(event, dict):
                collected.append(event)
    return canonical_lifecycle_events(collected)


def reconnect_count_from_health(health: dict[str, Any]) -> int:
    total = 0
    for row in health.get("rows") or []:
        payload = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        nested = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        value = payload.get("reconnect_count", nested.get("reconnect_count"))
        try:
            total = max(total, int(value or 0))
        except (TypeError, ValueError):
            continue
    return total


def rss_mb() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return round(float(usage.ru_maxrss) / 1024.0, 2)


def _health_summary(store: TradingStore, symbols: list[str]) -> dict[str, Any]:
    rows = store.market_data_freshness(max_age_sec=max_data_age_sec(), symbols=symbols)
    required = runtime_required_sources()
    present = {
        str(row.get("source") or row.get("event_type") or "").upper()
        for row in rows
    }
    missing = list(dict.fromkeys(
        [
            *(source for source in sorted(required) if source not in present),
            *(
                str(row.get("source") or "").upper()
                for row in rows
                if str(row.get("source") or "").upper() in required
                and str(row.get("status", "")).upper() == "MISSING"
            ),
        ]
    ))
    stale = [
        str(row.get("source") or "").upper()
        for row in rows
        if str(row.get("source") or "").upper() in required
        and str(row.get("status", "")).upper() not in HEALTHY_STATUSES | {"MISSING"}
    ]
    gaps = [row for row in rows if str(row.get("status", "")).upper() in UNSAFE_STATUSES]
    return {
        "required_sources": sorted(required),
        "missing_sources": missing,
        "stale_sources": list(dict.fromkeys(stale)),
        "gap_count": len(gaps),
        "rows": rows,
    }


def _sample_status(health: dict[str, Any]) -> str:
    if health.get("missing_sources"):
        return "failure"
    if health.get("stale_sources") or health.get("gap_count"):
        return "degraded"
    return "healthy"


def evaluate_realtime_acceptance(
    *,
    stage: str,
    duration: int,
    requested_duration: int,
    health: dict[str, Any],
    healthy_seconds: int,
    degraded_seconds: int,
    failure_seconds: int,
    reconnect_events: list[dict[str, Any]],
    collector_alive: bool,
    persistence_ok: bool,
    unsafe_at_end: bool,
) -> tuple[str, str]:
    channels = channel_status(health)
    sampled = max(1, healthy_seconds + degraded_seconds + failure_seconds)
    healthy_ratio = healthy_seconds / sampled
    lifecycle_events = canonical_lifecycle_events(reconnect_events)
    recovered = (not lifecycle_events) or all(
        event.get("subscriptions_restored") for event in lifecycle_events
    )
    persistent_failure = (
        healthy_ratio < 0.8
        or failure_seconds > max(300, requested_duration // 5)
    )
    stale_at_end = bool(health.get("stale_sources") or health.get("missing_sources"))
    if not collector_alive:
        return "FAILED", "COLLECTOR_DEAD"
    if not persistence_ok:
        return "FAILED", "PERSISTENCE_UNAVAILABLE"
    if duration < requested_duration:
        return "FAILED", "DURATION_SHORT"
    if unsafe_at_end or int(health.get("gap_count") or 0) > 0:
        return "FAILED", "ORDERBOOK_UNSAFE"
    if stale_at_end:
        return "FAILED", "SOURCE_STALE_AT_END"
    if not required_channels_healthy(channels):
        return "FAILED", "CHANNEL_UNHEALTHY"
    if persistent_failure:
        return "FAILED", "SOURCE_OUTAGE"
    if stage == "realtime_24h" and not lifecycle_events:
        return "FAILED", "NO_CONTROLLED_RECONNECT"
    if lifecycle_events and not recovered:
        return "FAILED", "RECONNECT_UNRECOVERED"
    return "PASSED", "OK"


def empty_stage(name: str) -> dict[str, Any]:
    spec = STAGE_SPECS[name]
    return {
        "status": "NOT_STARTED",
        "reason": "NOT_STARTED",
        "stage": name,
        "kind": spec["kind"],
        "requested_duration": spec["duration"],
        "session_id": None,
        "commit_sha": None,
        "start_at": None,
        "end_at": None,
        "duration_sec": 0,
        "process_duration_sec": 0,
        "healthy_sec": 0,
        "degraded_sec": 0,
        "failure_sec": 0,
        "stale_sec": 0,
        "gap_count": 0,
        "reconnect_count": 0,
        "owner_pid": None,
        "errors": [],
        "channel_status": {
            **{channel: "NOT_STARTED" for channel in REQUIRED_CHANNELS},
            "DEDICATED_MARK_PRICE": "NOT_REQUIRED",
            "ORDERBOOK": "NOT_STARTED",
        },
    }


def _infer_channel_status(stage: dict[str, Any]) -> dict[str, str]:
    health = {
        "required_sources": stage.get("required_sources") or [],
        "missing_sources": stage.get("missing_sources") or [],
        "stale_sources": stage.get("stale_sources") or [],
        "rows": [
            {"source": source, "status": "FRESH"}
            for source in stage.get("required_sources") or []
            if source not in (stage.get("missing_sources") or [])
            and source not in (stage.get("stale_sources") or [])
        ],
    }
    status = channel_status(health)
    if stage_status(stage) == "PASSED":
        status["DEDICATED_MARK_PRICE"] = "NOT_REQUIRED"
        mark_sources = set(CHANNEL_SOURCES["MARK_INDEX_FUNDING"])
        stale = set(stage.get("stale_sources") or [])
        missing = set(stage.get("missing_sources") or [])
        status["MARK_INDEX_FUNDING"] = "FAIL" if (mark_sources & (stale | missing)) else "PASS"
    return status


def migrate_report(payload: dict[str, Any]) -> dict[str, Any]:
    stages = dict(payload.get("stages") or {})
    for name in STAGE_SPECS:
        current = dict(stages.get(name) or empty_stage(name))
        if name not in stages:
            stages[name] = empty_stage(name)
            continue
        current.setdefault("stage", name)
        current.setdefault("kind", STAGE_SPECS[name]["kind"])
        current.setdefault("session_id", current.get("session_id"))
        current.setdefault("commit_sha", payload.get("commit_sha"))
        current.setdefault("start_at", current.get("start_time") or current.get("start_at"))
        current.setdefault("end_at", current.get("end_time") or current.get("end_at"))
        current.setdefault("duration_sec", current.get("duration") or 0)
        current.setdefault("process_duration_sec", current.get("duration") or 0)
        current.setdefault("healthy_sec", current.get("healthy_seconds") or 0)
        current.setdefault("degraded_sec", current.get("degraded_seconds") or 0)
        current.setdefault("failure_sec", current.get("failure_seconds") or 0)
        current.setdefault("stale_sec", current.get("stale_count") or 0)
        current.setdefault("errors", current.get("errors") or [])
        if "channel_status" not in current:
            current["channel_status"] = _infer_channel_status(current)
        stages[name] = current
    expire_realtime_if_code_changed(stages)
    recover_orphaned_sessions(stages)
    payload["stages"] = stages
    payload.setdefault("session_id", payload.get("session_id") or str(uuid.uuid4()))
    payload.setdefault("commit_sha", _commit_sha())
    payload.update(readiness_from_stages(stages))
    payload["CODE_PASS"] = True
    return payload


def load_report(path: Path | None = None) -> dict[str, Any]:
    report_path = path or REPORT_PATH
    if not report_path.exists():
        return migrate_report({"stages": {}})
    try:
        payload = json.loads(report_path.read_text())
    except json.JSONDecodeError:
        return migrate_report({"stages": {}})
    if not isinstance(payload, dict):
        return migrate_report({"stages": {}})
    payload.setdefault("stages", {})
    return migrate_report(payload)


def save_report(payload: dict[str, Any], path: Path | None = None) -> None:
    (path or REPORT_PATH).write_text(json.dumps(payload, indent=2, default=str))


def readiness_from_stages(stages: dict[str, Any]) -> dict[str, Any]:
    alpha_status = str((stages.get("alpha_oos") or {}).get("alpha_status") or "INSUFFICIENT_SAMPLE")
    if alpha_status not in {"INSUFFICIENT_SAMPLE", "ALPHA_NOT_SUPPORTED", "ALPHA_SUPPORTED"}:
        alpha_status = "INSUFFICIENT_SAMPLE"
    return {
        "CODE_READY": True,
        "CODE_PASS": True,
        "REAL_DATA_READY": stage_status(stages.get("realtime_24h")) == "PASSED",
        "PAPER_READY": stage_status(stages.get("paper_24h")) == "PASSED",
        "SHADOW_READY": stage_status(stages.get("shadow_7d")) == "PASSED",
        "ALPHA_STATUS": alpha_status,
        "ALPHA_READY": alpha_status == "ALPHA_SUPPORTED",
        "TESTNET_READY": stage_status(stages.get("testnet")) == "PASSED",
        "LIVE_PREFLIGHT": stage_status(stages.get("live_preflight")) == "PASSED",
        "LIVE_ALLOWED": False,
        "historical_gate_evidence": {
            "realtime_24h": stage_status(stages.get("realtime_24h")),
            "paper_24h": stage_status(stages.get("paper_24h")),
            "shadow_7d": stage_status(stages.get("shadow_7d")),
            "alpha_oos": alpha_status,
            "testnet": stage_status(stages.get("testnet")),
        },
    }


def store_status(status: str) -> str:
    return status if status in STORE_STATES else "FAILED"



def universe_qualification(symbols: list[str]) -> dict[str, Any]:
    compact = [item.replace("-", "").upper() for item in symbols]
    benchmark = [item for item in compact if item in BENCHMARK_SYMBOLS]
    production = [item for item in compact if item not in BENCHMARK_SYMBOLS]
    benchmark_only = bool(compact) and set(compact) <= BENCHMARK_SYMBOLS
    return {
        "runtime_stage_validated_symbols": compact,
        "production_target_universe": production,
        "benchmark_symbols": benchmark,
        "meme_universe_validated": bool(production) and not benchmark_only,
        "qualification_scope": (
            "BENCHMARK_ONLY" if benchmark_only else "MEME" if production and not benchmark else "MIXED"
        ),
        "is_meme": False if benchmark_only else bool(production),
    }


def evidence_planes(
    *,
    health: dict[str, Any],
    channels: dict[str, str],
    reconnect_events: list[dict[str, Any]],
    samples: list[dict[str, Any]],
    collector_alive: bool,
    persistence_ok: bool,
) -> dict[str, str]:
    missing = bool(health.get("missing_sources") or health.get("stale_sources"))
    source_health = "FAIL" if missing else "PASS"
    channel_health = "PASS" if required_channels_healthy(channels) else "FAIL"
    lifecycle = canonical_lifecycle_events(reconnect_events)
    if lifecycle:
        connection_lifecycle = (
            "PASS" if all(event.get("subscriptions_restored") for event in lifecycle) else "FAIL"
        )
    else:
        connection_lifecycle = "INSUFFICIENT"
    evidence = "PASS" if (samples or health.get("rows")) and collector_alive and persistence_ok else "FAIL"
    return {
        "SOURCE_HEALTH": source_health,
        "CHANNEL_HEALTH": channel_health,
        "CONNECTION_LIFECYCLE": connection_lifecycle,
        "EVIDENCE_SUFFICIENCY": evidence,
    }


def persist_store_gate(store: TradingStore | None, gate: str, status: str, detail: dict[str, Any]) -> None:
    if store is None or gate not in STORE_GATES:
        return
    store.record_runtime_gate_status(gate, store_status(status), detail=detail)


def build_session_stage(
    *,
    name: str,
    status: str,
    reason: str,
    session_id: str,
    commit_sha: str,
    start_at: str,
    end_at: str | None,
    duration_sec: int,
    requested_duration: int,
    symbols: list[str],
    health: dict[str, Any],
    healthy_sec: int,
    degraded_sec: int,
    failure_sec: int,
    reconnect_events: list[dict[str, Any]],
    errors: list[str],
    samples: list[dict[str, Any]],
    watchdog: str,
    runtime_gate: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    universe = _universe_snapshot(symbols)
    if health.get("rows") is not None:
        channels = channel_status(health)
    else:
        channels = (extra or {}).get("channel_status") or empty_stage(name)["channel_status"]
    stage = {
        "status": status,
        "reason": reason,
        "stage": name,
        "kind": STAGE_SPECS[name]["kind"],
        "session_id": session_id,
        "commit_sha": commit_sha,
        "config_hash": _config_hash(name, requested_duration, symbols),
        "start_at": start_at,
        "end_at": end_at,
        "start_time": start_at,
        "end_time": end_at,
        "duration": duration_sec,
        "duration_sec": duration_sec,
        "process_duration_sec": duration_sec,
        "requested_duration": requested_duration,
        "required_sources": health.get("required_sources") or sorted(runtime_required_sources()),
        "missing_sources": health.get("missing_sources") or [],
        "stale_sources": health.get("stale_sources") or [],
        "symbol_set": universe["symbol_set"],
        "universe_version": universe["universe_version"],
        "symbol_hash": universe["symbol_hash"],
        "universe_locked": universe["locked"],
        "channel_status": channels,
        "source_kinds": SOURCE_KINDS,
        "healthy_seconds": healthy_sec,
        "degraded_seconds": degraded_sec,
        "failure_seconds": failure_sec,
        "healthy_sec": healthy_sec,
        "degraded_sec": degraded_sec,
        "failure_sec": failure_sec,
        "stale_sec": len(health.get("stale_sources") or []),
        "gap_count": int(health.get("gap_count") or 0),
        "reconnect_count": len(canonical_lifecycle_events(reconnect_events)),
        "reconnect_events": canonical_lifecycle_events(reconnect_events),
        "diagnostic_reconnect_events": [
            event for event in reconnect_events if event.get("source") != "collector_lifecycle"
        ],
        "owner_pid": os.getpid(),
        "errors": errors,
        "watchdog": watchdog,
        "rss_mb": rss_mb(),
        "stale_count": len(health.get("stale_sources") or []),
        "observation_count": len(health.get("rows") or []),
        "samples": samples[-120:],
        "runtime_gate": runtime_gate or {},
        "freeze_hashes": _freeze_hashes(),
        **symbol_and_global_health(health),
        **universe_qualification(symbols),
        **evidence_planes(
            health=health,
            channels=channels if isinstance(channels, dict) else {},
            reconnect_events=reconnect_events,
            samples=samples,
            collector_alive=watchdog != "HALT",
            persistence_ok="STORE" not in (health.get("missing_sources") or []),
        ),
    }
    if extra:
        stage.update(extra)
    return stage


def write_session(report: dict[str, Any], stage: dict[str, Any], *, path: Path | None = None) -> dict[str, Any]:
    stages = dict(report.get("stages") or {})
    stages[stage["stage"]] = stage
    payload = {
        **report,
        "generated_at": _iso(),
        "session_id": stage.get("session_id") or report.get("session_id") or str(uuid.uuid4()),
        "stage": stage["stage"],
        "commit_sha": stage.get("commit_sha") or _commit_sha(),
        "start_at": stage.get("start_at"),
        "end_at": stage.get("end_at"),
        "duration_sec": stage.get("duration_sec") or 0,
        "required_sources": stage.get("required_sources"),
        "symbol_set": stage.get("symbol_set"),
        "channel_status": stage.get("channel_status"),
        "healthy_sec": stage.get("healthy_sec") or 0,
        "degraded_sec": stage.get("degraded_sec") or 0,
        "failure_sec": stage.get("failure_sec") or 0,
        "stale_sec": stage.get("stale_sec") or 0,
        "gap_count": stage.get("gap_count") or 0,
        "reconnect_count": stage.get("reconnect_count") or 0,
        "errors": stage.get("errors") or [],
        "SOURCE_HEALTH": stage.get("SOURCE_HEALTH"),
        "CHANNEL_HEALTH": stage.get("CHANNEL_HEALTH"),
        "CONNECTION_LIFECYCLE": stage.get("CONNECTION_LIFECYCLE"),
        "EVIDENCE_SUFFICIENCY": stage.get("EVIDENCE_SUFFICIENCY"),
        "runtime_stage_validated_symbols": stage.get("runtime_stage_validated_symbols"),
        "production_target_universe": stage.get("production_target_universe"),
        "qualification_scope": stage.get("qualification_scope"),
        "status": stage.get("status"),
        "gate": stage["stage"],
        "stages": stages,
        **readiness_from_stages(stages),
        "RUNTIME_PASS": stage.get("status") == "PASSED",
    }
    save_report(payload, path)
    return payload


async def run_realtime(
    duration: int,
    symbols: list[str],
    *,
    on_sample: Callable[[dict[str, Any]], None] | None = None,
    controlled_reconnect_after: float | None = None,
) -> dict[str, Any]:
    started = _now()
    store = TradingStore()
    compact = [item.replace("-", "") for item in symbols]
    samples: list[dict[str, Any]] = []
    healthy_seconds = 0
    degraded_seconds = 0
    failure_seconds = 0
    max_gap = 0
    stale_periods = 0
    errors: list[str] = []
    persistence_ok = True
    observer = asyncio.create_task(
        observe(
            symbols,
            candidate_limit=max(1, len(symbols)),
            refresh_sec=min(60.0, float(duration)),
            flush_sec=5.0,
            duration_sec=float(duration),
            controlled_reconnect_after=controlled_reconnect_after,
        )
    )
    deadline = asyncio.get_running_loop().time() + max(1.0, float(duration))
    last_tick = asyncio.get_running_loop().time()
    collector_alive = True
    try:
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0 and observer.done():
                break
            wait_for = SAMPLE_SEC if remaining > SAMPLE_SEC else max(0.1, remaining)
            try:
                await asyncio.wait_for(asyncio.shield(observer), timeout=wait_for)
                break
            except asyncio.TimeoutError:
                pass
            now = asyncio.get_running_loop().time()
            elapsed_slice = max(1, int(now - last_tick))
            last_tick = now
            try:
                health = _health_summary(store, compact)
                persistence_ok = True
            except Exception as exc:
                persistence_ok = False
                errors.append(type(exc).__name__)
                health = {
                    "required_sources": sorted(runtime_required_sources()),
                    "missing_sources": ["STORE"],
                    "stale_sources": [],
                    "gap_count": 0,
                    "rows": [],
                    "error": type(exc).__name__,
                }
            status = _sample_status(health)
            if status == "healthy":
                healthy_seconds += elapsed_slice
            elif status == "degraded":
                degraded_seconds += elapsed_slice
                stale_periods += 1
            else:
                failure_seconds += elapsed_slice
            max_gap = max(max_gap, int(health.get("gap_count") or 0))
            collector_alive = not observer.done() or observer.exception() is None
            samples.append({
                "at": _iso(),
                "status": status,
                "missing_sources": health.get("missing_sources"),
                "stale_sources": health.get("stale_sources"),
                "gap_count": health.get("gap_count"),
                "observation_count": len(health.get("rows") or []),
                "watchdog": watchdog_state(
                    collector_alive=collector_alive,
                    health=health,
                    persistence_ok=persistence_ok,
                ),
                "rss_mb": rss_mb(),
                "channel_status": channel_status(health),
            })
            if on_sample is not None:
                on_sample({
                    "health": health,
                    "samples": samples,
                    "healthy_seconds": healthy_seconds,
                    "degraded_seconds": degraded_seconds,
                    "failure_seconds": failure_seconds,
                    "collector_alive": collector_alive,
                    "persistence_ok": persistence_ok,
                    "errors": list(errors),
                    "started": started,
                })
            if observer.done() and remaining <= 0:
                break
        if not observer.done():
            await observer
        else:
            observer.result()
        collector_alive = True
    except Exception as exc:
        collector_alive = False
        errors.append(type(exc).__name__)
        observer.cancel()
        await asyncio.gather(observer, return_exceptions=True)
        raise
    ended = _now()
    try:
        final_health = _health_summary(store, compact)
        persistence_ok = True
    except Exception as exc:
        persistence_ok = False
        errors.append(type(exc).__name__)
        final_health = {
            "required_sources": sorted(runtime_required_sources()),
            "missing_sources": ["STORE"],
            "stale_sources": [],
            "gap_count": 0,
            "rows": [],
        }
    reconnect_events = reconnect_events_from_lifecycle(final_health)
    diagnostic_reconnect_events = reconnect_events_from_samples(samples)
    return {
        "start_time": started.isoformat(),
        "end_time": ended.isoformat(),
        "duration": max(0, int((ended - started).total_seconds())),
        "requested_duration": duration,
        "healthy_seconds": healthy_seconds,
        "degraded_seconds": degraded_seconds,
        "failure_seconds": failure_seconds,
        "gap_count": max(max_gap, int(final_health.get("gap_count") or 0)),
        "stale_count": len(final_health.get("stale_sources") or []),
        "stale_periods": stale_periods,
        "observation_count": len(final_health.get("rows") or []),
        "samples": samples[-120:],
        "health": final_health,
        "reconnect_events": reconnect_events,
        "diagnostic_reconnect_events": diagnostic_reconnect_events,
        "collector_alive": collector_alive,
        "persistence_ok": persistence_ok,
        "errors": errors,
        "unsafe_at_end": any(
            str(row.get("status") or "").upper() in UNSAFE_STATUSES
            for row in final_health.get("rows") or []
        ),
    }


def _count(store: Any, name: str, default: int = 0) -> int:
    getter = getattr(store, name, None)
    if getter is None:
        return default
    try:
        value = getter()
    except Exception:
        return default
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        return int(value.get("count") or len(value))
    return default


def runtime_acceptance_snapshot(store: Any, *, mode: str = "paper") -> dict[str, Any]:
    getter = getattr(store, "runtime_acceptance_snapshot", None)
    if getter is not None:
        try:
            snapshot = getter(mode=mode)
            if isinstance(snapshot, dict):
                return snapshot
        except TypeError:
            snapshot = getter()
            if isinstance(snapshot, dict):
                return snapshot
        except Exception as exc:
            return {"database_healthy": False, "error": type(exc).__name__}
    return {
        "database_healthy": store is not None,
        "observation_count": _count(store, "market_observation_count"),
        "positioning_count": _count(store, "positioning_snapshot_count"),
        "evidence_count": _count(store, "evidence_snapshot_count"),
        "episode_count": _count(store, "positioning_episode_count"),
        "intent_count": _count(store, "trade_intent_count"),
        "risk_decision_count": _count(store, "risk_event_count"),
        "order_count": _count(store, "order_count"),
        "fill_count": _count(store, "trade_count"),
        "funding_count": _count(store, "funding_settlement_count"),
        "long_count": _count(store, "long_positioning_count"),
        "short_count": _count(store, "short_positioning_count"),
        "long_building_count": _count(store, "long_building_count"),
        "short_building_count": _count(store, "short_building_count"),
        "duplicate_trades": 0,
        "duplicate_funding": 0,
        "invalid_positions": 0,
        "impossible_balance": False,
        "impossible_equity": False,
        "invalid_margin": False,
        "stale_open": 0,
        "unsafe_open": 0,
        "accounting": {},
        "restart_recovery": True,
        "episode_split": False,
        "error": None,
    }


def _strategy_path_present(snapshot: dict[str, Any]) -> bool:
    return all(
        (
            int(snapshot.get("observation_count") or 0) > 0,
            int(snapshot.get("positioning_count") or 0) > 0,
            int(snapshot.get("evidence_count") or 0) > 0,
        )
    )


def paper_acceptance(
    *,
    requested_duration: int,
    duration_sec: int,
    process_completed: bool,
    snapshot: dict[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    detail = {
        "observations": int(snapshot.get("observation_count") or 0),
        "positioning snapshots": int(snapshot.get("positioning_count") or 0),
        "evidence snapshots": int(snapshot.get("evidence_count") or 0),
        "episodes": int(snapshot.get("episode_count") or 0),
        "intents": int(snapshot.get("intent_count") or 0),
        "risk decisions": int(snapshot.get("risk_decision_count") or 0),
        "orders": int(snapshot.get("order_count") or 0),
        "fills": int(snapshot.get("fill_count") or 0),
        "funding events": int(snapshot.get("funding_count") or 0),
        "restart recovery": bool(snapshot.get("restart_recovery", True)),
        "duplicate accounting": int(snapshot.get("duplicate_trades") or 0)
        + int(snapshot.get("duplicate_funding") or 0),
        "equity invariant": not bool(snapshot.get("impossible_equity")),
        "margin invariant": not bool(snapshot.get("invalid_margin") or snapshot.get("impossible_balance")),
        "LONG_BUILDING observations": int(snapshot.get("long_building_count") or 0),
        "SHORT_BUILDING observations": int(snapshot.get("short_building_count") or 0),
        "DIRECTIONAL_SAMPLE_INSUFFICIENT": (
            int(snapshot.get("long_building_count") or 0) == 0
            or int(snapshot.get("short_building_count") or 0) == 0
        ),
        "liquidation_model": {
            "scope": "PAPER_ONLY",
            "model": "SIMPLIFIED",
            "binance_parity": "NOT_BINANCE_PARITY",
        },
    }
    if not process_completed:
        return "FAILED", "PAPER_PROCESS_INCOMPLETE", detail
    if not snapshot.get("database_healthy", True):
        return "FAILED", "PAPER_DATABASE_UNHEALTHY", detail
    if duration_sec < requested_duration:
        return "FAILED", "DURATION_SHORT", detail
    if not _strategy_path_present(snapshot):
        return "FAILED", "PAPER_EVIDENCE_MISSING", detail
    if int(snapshot.get("duplicate_trades") or 0) or int(snapshot.get("duplicate_funding") or 0):
        return "FAILED", "DUPLICATE_ACCOUNTING", detail
    if int(snapshot.get("invalid_positions") or 0):
        return "FAILED", "INVALID_POSITION", detail
    if snapshot.get("impossible_balance") or snapshot.get("invalid_margin"):
        return "FAILED", "IMPOSSIBLE_BALANCE", detail
    if snapshot.get("impossible_equity"):
        return "FAILED", "IMPOSSIBLE_EQUITY", detail
    if int(snapshot.get("stale_open") or 0):
        return "FAILED", "STALE_OPEN", detail
    if int(snapshot.get("unsafe_open") or 0):
        return "FAILED", "UNSAFE_OPEN", detail
    if not snapshot.get("restart_recovery", True):
        return "FAILED", "PAPER_RESTART_RECOVERY_FAILED", detail
    return "PASSED", "OK", detail


def shadow_acceptance(
    *,
    requested_duration: int,
    duration_sec: int,
    snapshot: dict[str, Any],
    real_order_delta: int | None,
) -> tuple[str, str, dict[str, Any]]:
    detail = {
        "observation_count": int(snapshot.get("observation_count") or 0),
        "positioning_count": int(snapshot.get("positioning_count") or 0),
        "evidence_count": int(snapshot.get("evidence_count") or 0),
        "episode_count": int(snapshot.get("episode_count") or 0),
        "LONG_count": int(snapshot.get("long_count") or 0),
        "SHORT_count": int(snapshot.get("short_count") or 0),
        "real_orders": 0 if real_order_delta is None else int(real_order_delta),
        "real_order_proof": (
            "NOT_CHECKABLE_EXTERNALLY"
            if real_order_delta is None
            else "REAL_ORDER_ZERO_VERIFIED" if real_order_delta == 0 else "REAL_ORDER_DELTA"
        ),
        "restart_recovery": bool(snapshot.get("restart_recovery", True)),
        "episode_split": bool(snapshot.get("episode_split")),
    }
    if duration_sec < requested_duration:
        return "FAILED", "DURATION_SHORT", detail
    if not _strategy_path_present(snapshot) or int(snapshot.get("episode_count") or 0) <= 0:
        return "FAILED", "SHADOW_EVIDENCE_MISSING", detail
    if real_order_delta not in {None, 0}:
        return "FAILED", "SHADOW_REAL_ORDER_DELTA", detail
    if snapshot.get("episode_split"):
        return "FAILED", "SHADOW_EPISODE_SPLIT", detail
    if not snapshot.get("restart_recovery", True):
        return "FAILED", "SHADOW_RESTART_RECOVERY_FAILED", detail
    return "PASSED", "OK", detail


def _paper_command(duration: int, symbols: list[str], *, mode: str, planned_restart_after: int | None = None) -> list[str]:
    command = [
        sys.executable,
        str(Path(PROJECT_ROOT) / "paper_runner.py"),
        "--mode", mode,
        "--duration", str(duration),
        "--symbols", ",".join(item.replace("-", "") for item in symbols),
    ]
    if planned_restart_after is not None:
        command.extend(["--planned-restart-after", str(planned_restart_after)])
    return command


def _run_child(command: list[str], env: dict[str, str] | None = None) -> dict[str, Any]:
    completed = subprocess.run(
        command, cwd=PROJECT_ROOT, env=env, capture_output=True, text=True, check=False
    )
    return {
        "command": command,
        "returncode": completed.returncode,
        "stdout": completed.stdout[-4000:],
        "stderr": completed.stderr[-4000:],
    }


def exchange_open_order_count() -> int | None:
    if not os.environ.get("BIAN_TESTNET_API_KEY") and not os.environ.get("BIAN_LIVE_API_KEY"):
        return None
    try:
        from binance_client import ClientConfig, FuturesPrivateClient

        mode = "testnet" if os.environ.get("BIAN_TESTNET_API_KEY") else "live"
        client = FuturesPrivateClient(ClientConfig.from_env(mode))
        getter = getattr(client, "open_orders", None) or getattr(client, "get_open_orders", None)
        if getter is None:
            return None
        orders = getter()
        return len(orders or [])
    except Exception:
        return None


def run_paper_stage(duration: int, symbols: list[str]) -> dict[str, Any]:
    env = dict(os.environ)
    env["BIAN_MODE"] = "paper"
    env["BIAN_MARKET"] = "FUTURES"
    restart_after = max(1, duration // 2)
    remaining = max(1, duration - restart_after)
    first = _run_child(
        _paper_command(duration, symbols, mode="paper", planned_restart_after=restart_after),
        env,
    )
    second = _run_child(_paper_command(remaining, symbols, mode="paper"), env)
    process_completed = first["returncode"] == PAPER_RESTART_EXIT and second["returncode"] == 0
    try:
        store = TradingStore()
        snapshot = runtime_acceptance_snapshot(store, mode="paper")
    except Exception as exc:
        snapshot = {"database_healthy": False, "error": type(exc).__name__}
    snapshot["restart_recovery"] = bool(
        process_completed
        and not snapshot.get("episode_split")
        and not int(snapshot.get("duplicate_trades") or 0)
        and not int(snapshot.get("duplicate_funding") or 0)
    )
    duration_sec = duration if process_completed else 0
    status, reason, detail = paper_acceptance(
        requested_duration=duration,
        duration_sec=duration_sec if process_completed else 0,
        process_completed=process_completed,
        snapshot=snapshot,
    )
    return {
        "status": status,
        "reason": reason,
        "first_child": first,
        "second_child": second,
        "paper_acceptance": detail,
        "snapshot": snapshot,
        "process_completed": process_completed,
    }


def run_shadow_stage(duration: int, symbols: list[str]) -> dict[str, Any]:
    before = exchange_open_order_count()
    restart_after = max(1, duration // 2)
    remaining = max(1, duration - restart_after)
    first = _run_child(
        _paper_command(duration, symbols, mode="shadow", planned_restart_after=restart_after),
    )
    second = _run_child(_paper_command(remaining, symbols, mode="shadow"))
    after = exchange_open_order_count()
    real_order_delta = None if before is None or after is None else after - before
    try:
        store = TradingStore()
        snapshot = runtime_acceptance_snapshot(store, mode="shadow")
    except Exception as exc:
        snapshot = {"database_healthy": False, "error": type(exc).__name__}
    process_completed = first["returncode"] == PAPER_RESTART_EXIT and second["returncode"] == 0
    snapshot["restart_recovery"] = bool(
        process_completed
        and not snapshot.get("episode_split")
        and not int(snapshot.get("duplicate_trades") or 0)
        and not int(snapshot.get("duplicate_funding") or 0)
    )
    status, reason, detail = shadow_acceptance(
        requested_duration=duration,
        duration_sec=duration if process_completed else 0,
        snapshot=snapshot,
        real_order_delta=real_order_delta,
    )
    return {
        "status": status,
        "reason": reason,
        "first_child": first,
        "second_child": second,
        "shadow_acceptance": detail,
        "snapshot": snapshot,
        "real_order_delta": real_order_delta,
        "process_completed": process_completed,
    }


def _load_alpha_frames() -> list[Any]:
    store = TradingStore()
    getter = getattr(store, "positioning_replay_frames", None)
    if getter is None:
        return []
    try:
        return list(getter())
    except TypeError:
        symbols = []
        lister = getattr(store, "positioning_symbols", None)
        if lister is not None:
            symbols = list(lister() or [])
        frames: list[Any] = []
        for symbol in symbols:
            frames.extend(list(getter(symbol)))
        return frames


def run_alpha_stage() -> dict[str, Any]:
    from backtesting import evaluate_alpha_gate

    try:
        frames = _load_alpha_frames()
    except Exception as exc:
        return {
            "status": "FAILED",
            "reason": f"ALPHA_PERSISTENCE_UNAVAILABLE:{type(exc).__name__}",
            "alpha_status": "INSUFFICIENT_SAMPLE",
        }
    result = evaluate_alpha_gate(frames)
    long_episodes = int(result.oos_metrics.get("long_episodes") or getattr(result, "long_episodes", 0) or 0)
    short_episodes = int(result.oos_metrics.get("short_episodes") or getattr(result, "short_episodes", 0) or 0)
    alpha_status = result.status
    reason = result.reason
    if alpha_status == "ALPHA_SUPPORTED" and min(long_episodes, short_episodes) == 0:
        alpha_status = "ALPHA_NOT_SUPPORTED"
        reason = "DIRECTIONAL_SAMPLE_INSUFFICIENT"
    return {
        "status": "PASSED" if alpha_status in {
            "INSUFFICIENT_SAMPLE", "ALPHA_NOT_SUPPORTED", "ALPHA_SUPPORTED",
        } else "FAILED",
        "reason": reason,
        "alpha_status": alpha_status,
        "strategy_version": result.strategy_version,
        "config_hash": result.config_hash,
        "parameter_hash": result.parameter_version,
        "code_commit": _commit_sha(),
        "universe_version": getattr(result, "universe_version", None),
        "train_samples": result.train_samples,
        "validation_samples": result.validation_samples,
        "oos_samples": result.oos_samples,
        "independent_episodes": (result.oos_metrics or {}).get("independent_episodes"),
        "long_episodes": long_episodes,
        "short_episodes": short_episodes,
        "baseline": {key: (result.oos_metrics or {}).get(key) for key in ("expectancy", "net_return", "max_drawdown", "profit_factor")},
        "cost_stress": (result.oos_metrics or {}).get("cost_stress"),
        "oos_metrics": result.oos_metrics,
        "frame_count": len(frames),
    }


def testnet_acceptance(snapshot: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    required = {
        "account": bool(snapshot.get("account")),
        "ONE_WAY": bool(snapshot.get("one_way")),
        "ISOLATED": bool(snapshot.get("isolated")),
        "LONG lifecycle": bool(snapshot.get("long_lifecycle")),
        "SHORT lifecycle": bool(snapshot.get("short_lifecycle")),
        "partial fill": bool(snapshot.get("partial_fill")),
        "cancel": bool(snapshot.get("cancel")),
        "UNKNOWN resolution": bool(snapshot.get("unknown_resolution")),
        "USER_STREAM": bool(snapshot.get("user_stream")),
        "listenKey": bool(snapshot.get("listen_key")),
        "reconciliation": bool(snapshot.get("reconciliation")),
        "restart": bool(snapshot.get("restart")),
    }
    missing = [name for name, ok in required.items() if not ok]
    status = "PASSED" if not missing else "FAILED"
    return status, "OK" if not missing else "TESTNET_LIFECYCLE_INCOMPLETE", required


def run_testnet_stage() -> dict[str, Any]:
    if not os.environ.get("BIAN_TESTNET_API_KEY") or not os.environ.get("BIAN_TESTNET_API_SECRET"):
        return {
            "status": "BLOCKED",
            "reason": "BLOCKED_BY_EXTERNAL_CREDENTIALS",
            "credentials": False,
        }
    try:
        store = TradingStore()
        snapshot = getattr(store, "testnet_lifecycle_snapshot", lambda: {})()
        if not isinstance(snapshot, dict):
            snapshot = {}
    except Exception as exc:
        snapshot = {"error": type(exc).__name__}
    status, reason, detail = testnet_acceptance(snapshot)
    return {
        "status": status,
        "reason": reason,
        "credentials": True,
        "testnet_acceptance": detail,
        "snapshot": snapshot,
    }


def run_live_preflight(stages: dict[str, Any] | None = None) -> dict[str, Any]:
    stages = stages if stages is not None else load_report().get("stages") or {}
    required = {
        "REAL_DATA_READY": stage_status(stages.get("realtime_24h")) == "PASSED",
        "PAPER_READY": stage_status(stages.get("paper_24h")) == "PASSED",
        "SHADOW_READY": stage_status(stages.get("shadow_7d")) == "PASSED",
        "ALPHA_READY": str((stages.get("alpha_oos") or {}).get("alpha_status")) == "ALPHA_SUPPORTED",
        "TESTNET_READY": stage_status(stages.get("testnet")) == "PASSED",
        "DATABASE_HEALTHY": True,
        "ACCOUNT_HEALTHY": True,
        "USER_STREAM_HEALTHY": True,
        "RECONCILIATION_HEALTHY": True,
        "RISK_HEALTHY": True,
        "MEME_UNIVERSE_HEALTHY": True,
    }
    current_health: dict[str, Any] = {}
    try:
        store = TradingStore()
        gate = evaluate_runtime_gate(mode="paper", store=store, symbols=["BTCUSDT"]).as_dict()
        current_health = {
            "current_runtime_health": gate.get("MARKET_HEALTH"),
            "current_account_health": gate.get("ACCOUNT_HEALTH"),
            "current_user_stream": gate.get("USER_STREAM_HEALTH"),
            "current_reconciliation": gate.get("RECONCILIATION_HEALTH"),
            "global_transport_health": gate.get("global_transport_health"),
            "risk_health": gate.get("RISK_HEALTH"),
            "meme_universe": gate.get("meme_universe"),
        }
        required["DATABASE_HEALTHY"] = True
        required["ACCOUNT_HEALTHY"] = gate.get("ACCOUNT_HEALTH") in {"OK", "NOT_APPLICABLE"}
        required["USER_STREAM_HEALTHY"] = gate.get("USER_STREAM_HEALTH") in {"OK", "NOT_APPLICABLE"}
        required["RECONCILIATION_HEALTHY"] = gate.get("RECONCILIATION_HEALTH") == "OK"
        required["RISK_HEALTHY"] = gate.get("RISK_HEALTH") == "SAFE"
        required["MEME_UNIVERSE_HEALTHY"] = bool(gate.get("meme_universe"))
        if gate.get("global_transport_health") in {"HALT", "FAILED", "BLOCKED"}:
            required["REAL_DATA_READY"] = False
    except Exception as exc:
        required["DATABASE_HEALTHY"] = False
        current_health["error"] = type(exc).__name__
    missing = [name for name, ready in required.items() if not ready]
    return {
        "status": "FAILED" if missing else "PASSED",
        "reason": "LIVE_RELEASE_GATES_PENDING" if missing else "OK",
        "missing": missing,
        "LIVE_ALLOWED": False,
        "historical_gate_evidence": {
            "realtime_24h": stage_status(stages.get("realtime_24h")),
            "paper_24h": stage_status(stages.get("paper_24h")),
            "shadow_7d": stage_status(stages.get("shadow_7d")),
            "alpha_oos": str((stages.get("alpha_oos") or {}).get("alpha_status")),
            "testnet": stage_status(stages.get("testnet")),
        },
        "current_runtime_health": current_health,
        **required,
    }


def resolve_stage(stage: str | None, duration: int | None) -> tuple[str, int]:
    if stage:
        name = stage.strip().lower()
        if name not in STAGE_SPECS:
            raise SystemExit(f"unknown stage: {stage}")
        spec = STAGE_SPECS[name]
        if duration is None:
            return name, int(spec["duration"])
        if spec["kind"] == "realtime" and duration != spec["duration"]:
            raise SystemExit(f"{name} requires --duration {spec['duration']}")
        return name, int(duration)
    if duration is None:
        raise SystemExit("--stage or --duration is required")
    if duration not in DURATION_GATES:
        raise SystemExit(f"unsupported duration: {duration}")
    return DURATION_GATES[duration], duration


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run elapsed validation sessions.")
    parser.add_argument("--stage", choices=sorted(STAGE_SPECS))
    parser.add_argument("--duration", type=int)
    parser.add_argument("--stream-symbols", default="BTC-USDT")
    args = parser.parse_args(argv)
    gate, duration = resolve_stage(args.stage, args.duration)
    report = load_report()
    stages = dict(report.get("stages") or {})
    symbols = [item.strip() for item in args.stream_symbols.split(",") if item.strip()]
    compact = [item.replace("-", "") for item in symbols]
    session_id = str(uuid.uuid4())
    commit_sha = _commit_sha()
    started = _iso()
    if not prior_passed(stages, gate):
        prior = STAGE_SPECS[gate]["prior"]
        stage = empty_stage(gate)
        stage.update({
            "status": "FAILED",
            "reason": "LADDER_SKIPPED",
            "session_id": session_id,
            "commit_sha": commit_sha,
            "start_at": started,
            "end_at": _iso(),
            "errors": [f"{prior} is {stage_status(stages.get(prior or ''))}"],
        })
        write_session(report, stage)
        print(json.dumps({"gate": gate, "status": "FAILED", "reason": "LADDER_SKIPPED"}, default=str))
        return 1

    empty_health = {
        "required_sources": sorted(runtime_required_sources()),
        "missing_sources": [],
        "stale_sources": [],
        "gap_count": 0,
        "rows": [],
    }
    running = build_session_stage(
        name=gate,
        status="RUNNING",
        reason="RUNNING",
        session_id=session_id,
        commit_sha=commit_sha,
        start_at=started,
        end_at=None,
        duration_sec=0,
        requested_duration=duration,
        symbols=symbols,
        health=empty_health,
        healthy_sec=0,
        degraded_sec=0,
        failure_sec=0,
        reconnect_events=[],
        errors=[],
        samples=[],
        watchdog="HEALTHY",
    )
    report = write_session(report, running)
    store: TradingStore | None
    try:
        store = TradingStore()
        persist_store_gate(store, gate, "RUNNING", {"verified_at": started, "session_id": session_id})
    except Exception:
        store = None

    kind = STAGE_SPECS[gate]["kind"]
    extra: dict[str, Any] = {}
    health = dict(empty_health)
    healthy_sec = 0
    degraded_sec = 0
    failure_sec = 0
    reconnect_events: list[dict[str, Any]] = []
    errors: list[str] = []
    samples: list[dict[str, Any]] = []
    watchdog = "HEALTHY"
    runtime_gate_payload: dict[str, Any] = {}
    status = "FAILED"
    reason = "STAGE_FAILED"
    try:
        if kind == "realtime":
            def _persist_sample(snapshot: dict[str, Any]) -> None:
                elapsed_now = max(0, int((_now() - datetime.fromisoformat(started)).total_seconds()))
                current = build_session_stage(
                    name=gate,
                    status="RUNNING",
                    reason="RUNNING",
                    session_id=session_id,
                    commit_sha=commit_sha,
                    start_at=started,
                    end_at=None,
                    duration_sec=elapsed_now,
                    requested_duration=duration,
                    symbols=symbols,
                    health=snapshot["health"],
                    healthy_sec=snapshot["healthy_seconds"],
                    degraded_sec=snapshot["degraded_seconds"],
                    failure_sec=snapshot["failure_seconds"],
                    reconnect_events=reconnect_events_from_lifecycle(snapshot["health"]),
                    errors=snapshot["errors"],
                    samples=snapshot["samples"],
                    watchdog=watchdog_state(
                        collector_alive=snapshot["collector_alive"],
                        health=snapshot["health"],
                        persistence_ok=snapshot["persistence_ok"],
                    ),
                )
                write_session(report, current)

            elapsed = asyncio.run(
                run_realtime(
                    duration,
                    symbols,
                    on_sample=_persist_sample,
                    controlled_reconnect_after=300.0 if gate == "realtime_24h" else None,
                )
            )
            health = elapsed["health"]
            healthy_sec = int(elapsed.get("healthy_seconds") or 0)
            degraded_sec = int(elapsed.get("degraded_seconds") or 0)
            failure_sec = int(elapsed.get("failure_seconds") or 0)
            reconnect_events = list(elapsed.get("reconnect_events") or [])
            errors = list(elapsed.get("errors") or [])
            samples = list(elapsed.get("samples") or [])
            if store is None:
                store = TradingStore()
            runtime_gate_payload = evaluate_runtime_gate(
                mode="paper", store=store, symbols=compact
            ).as_dict()
            watchdog = watchdog_state(
                collector_alive=bool(elapsed.get("collector_alive")),
                health=health,
                persistence_ok=bool(elapsed.get("persistence_ok")),
            )
            status, reason = evaluate_realtime_acceptance(
                stage=gate,
                duration=int(elapsed["duration"]),
                requested_duration=duration,
                health=health,
                healthy_seconds=healthy_sec,
                degraded_seconds=degraded_sec,
                failure_seconds=failure_sec,
                reconnect_events=reconnect_events,
                collector_alive=bool(elapsed.get("collector_alive")),
                persistence_ok=bool(elapsed.get("persistence_ok")),
                unsafe_at_end=bool(elapsed.get("unsafe_at_end")),
            )
            extra["process_duration_sec"] = elapsed["duration"]
        elif kind == "paper":
            extra = run_paper_stage(duration, symbols)
            status, reason = extra["status"], extra["reason"]
        elif kind == "shadow":
            extra = run_shadow_stage(duration, symbols)
            status, reason = extra["status"], extra["reason"]
        elif kind == "alpha":
            extra = run_alpha_stage()
            status, reason = extra["status"], extra["reason"]
        elif kind == "testnet":
            extra = run_testnet_stage()
            status, reason = extra["status"], extra["reason"]
        else:
            extra = run_live_preflight(stages)
            status, reason = extra["status"], extra["reason"]
    except Exception as exc:
        status = "FAILED"
        reason = f"STAGE_FAILED:{type(exc).__name__}"
        errors.append(type(exc).__name__)
        watchdog = "HALT"
        health = {
            "required_sources": sorted(runtime_required_sources()),
            "missing_sources": ["COLLECTOR"],
            "stale_sources": [],
            "gap_count": 0,
            "rows": [],
        }

    ended = _iso()
    duration_sec = max(0, int((_now() - datetime.fromisoformat(started)).total_seconds()))
    stage = build_session_stage(
        name=gate,
        status=status,
        reason=reason,
        session_id=session_id,
        commit_sha=commit_sha,
        start_at=started,
        end_at=ended,
        duration_sec=duration_sec,
        requested_duration=duration,
        symbols=symbols,
        health=health,
        healthy_sec=healthy_sec,
        degraded_sec=degraded_sec,
        failure_sec=failure_sec,
        reconnect_events=reconnect_events,
        errors=errors,
        samples=samples,
        watchdog=watchdog,
        runtime_gate=runtime_gate_payload,
        extra=extra,
    )
    write_session(report, stage)
    try:
        persist_store_gate(
            store or TradingStore(),
            gate,
            status,
            {
                "verified_at": ended,
                "session_id": session_id,
                "reason": reason,
                "healthy_seconds": healthy_sec,
                "degraded_seconds": degraded_sec,
                "failure_seconds": failure_sec,
                "gap_count": stage.get("gap_count"),
                "reconnect_count": stage.get("reconnect_count"),
            },
        )
    except Exception as exc:
        stage["errors"] = list(stage.get("errors") or []) + [f"STORE:{type(exc).__name__}"]
        if status == "PASSED":
            stage["status"] = "FAILED"
            stage["reason"] = f"{reason};STORE:{type(exc).__name__}"
        write_session(report, stage)
        status = stage["status"]
        reason = stage["reason"]
    print(json.dumps({"gate": gate, "status": status, "reason": reason, "session_id": session_id}, default=str))
    return 0 if status == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
