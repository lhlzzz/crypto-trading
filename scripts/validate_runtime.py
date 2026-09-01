#!/usr/bin/env python3
"""Elapsed realtime validation ladder for bian Futures observation.

This script never marks PASS from unit tests or mocked exchange responses.
It runs the public collector for a requested duration and writes
``runtime_validation_report.json``. Missing Binance connectivity is FAIL.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine import runtime_required_sources
from runtime_gate import evaluate_runtime_gate, max_data_age_sec
from scripts.bian_market import observe
from trading_store import TradingStore

DURATION_GATES = {
    1800: "realtime_30m",
    7200: "realtime_2h",
    21600: "realtime_6h",
    86400: "realtime_24h",
}
LADDER = ("realtime_30m", "realtime_2h", "realtime_6h", "realtime_24h")
REPORT_PATH = Path(PROJECT_ROOT) / "runtime_validation_report.json"
SAMPLE_SEC = 30.0


def _load_report() -> dict:
    if not REPORT_PATH.exists():
        return {"stages": {}}
    try:
        payload = json.loads(REPORT_PATH.read_text())
    except json.JSONDecodeError:
        return {"stages": {}}
    if not isinstance(payload, dict):
        return {"stages": {}}
    payload.setdefault("stages", {})
    return payload


def _prior_passed(stages: dict, gate: str) -> bool:
    if gate == "realtime_30m":
        return True
    index = LADDER.index(gate)
    prior = LADDER[index - 1]
    return str((stages.get(prior) or {}).get("status")) == "PASSED"


def _health_summary(store: TradingStore, symbols: list[str]) -> dict:
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
        and str(row.get("status", "")).upper() not in {"FRESH", "OK", "MISSING"}
    ]
    gaps = [
        row
        for row in rows
        if str(row.get("status", "")).upper() in {"GAP", "UNSAFE"}
    ]
    return {
        "required_sources": sorted(required),
        "missing_sources": missing,
        "stale_sources": list(dict.fromkeys(stale)),
        "gap_count": len(gaps),
        "rows": rows,
    }


def _sample_status(health: dict) -> str:
    if health["missing_sources"]:
        return "failure"
    if health["stale_sources"] or health["gap_count"]:
        return "degraded"
    return "healthy"


async def _run(duration: int, symbols: list[str]) -> dict:
    started = datetime.now(timezone.utc)
    store = TradingStore()
    compact = [item.replace("-", "") for item in symbols]
    samples: list[dict] = []
    healthy_seconds = 0
    degraded_seconds = 0
    failure_seconds = 0
    max_gap = 0
    stale_periods = 0
    observer = asyncio.create_task(
        observe(
            symbols,
            candidate_limit=5,
            refresh_sec=min(60.0, float(duration)),
            flush_sec=5.0,
            duration_sec=float(duration),
        )
    )
    deadline = asyncio.get_running_loop().time() + max(1.0, float(duration))
    last_tick = asyncio.get_running_loop().time()
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
            except Exception as exc:
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
            samples.append({
                "at": datetime.now(timezone.utc).isoformat(),
                "status": status,
                "missing_sources": health.get("missing_sources"),
                "stale_sources": health.get("stale_sources"),
                "gap_count": health.get("gap_count"),
                "observation_count": len(health.get("rows") or []),
            })
            if observer.done() and remaining <= 0:
                break
        if not observer.done():
            await observer
        else:
            observer.result()
    except Exception:
        observer.cancel()
        await asyncio.gather(observer, return_exceptions=True)
        raise
    ended = datetime.now(timezone.utc)
    elapsed = max(0, int((ended - started).total_seconds()))
    final_health = _health_summary(store, compact)
    return {
        "start_time": started.isoformat(),
        "end_time": ended.isoformat(),
        "duration": elapsed,
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
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run elapsed realtime validation.")
    parser.add_argument("--duration", type=int, required=True, choices=sorted(DURATION_GATES))
    parser.add_argument(
        "--stream-symbols",
        default="BTC-USDT",
    )
    args = parser.parse_args(argv)
    gate = DURATION_GATES[args.duration]
    report = _load_report()
    stages = dict(report.get("stages") or {})
    if not _prior_passed(stages, gate):
        payload = {
            "status": "FAILED",
            "reason": "LADDER_SKIPPED",
            "gate": gate,
            "stages": stages,
        }
        REPORT_PATH.write_text(json.dumps(payload, indent=2, default=str))
        print(json.dumps(payload, default=str))
        return 1

    symbols = [item.strip() for item in args.stream_symbols.split(",") if item.strip()]
    compact = [item.replace("-", "") for item in symbols]
    started = datetime.now(timezone.utc)
    try:
        elapsed = asyncio.run(_run(args.duration, symbols))
        store = TradingStore()
        health = elapsed.get("health") or _health_summary(store, compact)
        gate_result = evaluate_runtime_gate(mode="paper", store=store, symbols=compact)
        long_enough = elapsed["duration"] >= args.duration
        unresolved_gap = int(elapsed.get("gap_count") or 0) > 0
        source_ok = not health.get("missing_sources") and not health.get("stale_sources")
        sampled = max(
            1,
            int(elapsed.get("healthy_seconds") or 0)
            + int(elapsed.get("degraded_seconds") or 0)
            + int(elapsed.get("failure_seconds") or 0),
        )
        healthy_ratio = int(elapsed.get("healthy_seconds") or 0) / sampled
        persistent_failure = healthy_ratio < 0.8 or int(elapsed.get("failure_seconds") or 0) > max(300, args.duration // 5)
        status = (
            "PASSED"
            if long_enough and source_ok and not unresolved_gap and not persistent_failure
            else "FAILED"
        )
        if not long_enough:
            reason = "DURATION_SHORT"
        elif unresolved_gap:
            reason = "ORDERBOOK_GAP"
        elif persistent_failure:
            reason = "SOURCE_OUTAGE"
        elif not source_ok:
            reason = "SOURCE_UNHEALTHY"
        else:
            reason = "OK"
        try:
            store.record_runtime_gate_status(
                gate,
                status,
                detail={
                    "verified_at": elapsed["end_time"],
                    "duration": elapsed["duration"],
                    "reason": reason,
                    "healthy_seconds": elapsed.get("healthy_seconds"),
                    "degraded_seconds": elapsed.get("degraded_seconds"),
                    "failure_seconds": elapsed.get("failure_seconds"),
                    "gap_count": elapsed.get("gap_count"),
                    "stale_count": elapsed.get("stale_count"),
                    "observation_count": elapsed.get("observation_count"),
                },
            )
        except Exception as exc:
            reason = f"{reason};STORE:{type(exc).__name__}"
            status = "FAILED"
    except Exception as exc:
        elapsed = {
            "start_time": started.isoformat(),
            "end_time": datetime.now(timezone.utc).isoformat(),
            "duration": int((datetime.now(timezone.utc) - started).total_seconds()),
            "requested_duration": args.duration,
            "healthy_seconds": 0,
            "degraded_seconds": 0,
            "failure_seconds": int((datetime.now(timezone.utc) - started).total_seconds()),
            "gap_count": 0,
            "stale_count": 0,
            "observation_count": 0,
            "samples": [],
        }
        health = {
            "required_sources": sorted(runtime_required_sources()),
            "missing_sources": ["COLLECTOR"],
            "stale_sources": [],
            "rows": [],
        }
        gate_result = evaluate_runtime_gate(mode="paper")
        status = "FAILED"
        reason = f"COLLECTOR_FAILED:{type(exc).__name__}"

    stage = {
        "status": status,
        "reason": reason,
        "start_time": elapsed["start_time"],
        "end_time": elapsed["end_time"],
        "duration": elapsed["duration"],
        "requested_duration": args.duration,
        "required_sources": health.get("required_sources"),
        "missing_sources": health.get("missing_sources"),
        "stale_sources": health.get("stale_sources"),
        "healthy_seconds": elapsed.get("healthy_seconds", 0),
        "degraded_seconds": elapsed.get("degraded_seconds", 0),
        "failure_seconds": elapsed.get("failure_seconds", 0),
        "gap_count": elapsed.get("gap_count", 0),
        "reconnect_count": 0,
        "stale_count": elapsed.get("stale_count", len(health.get("stale_sources") or [])),
        "observation_count": elapsed.get("observation_count", len(health.get("rows") or [])),
        "samples": elapsed.get("samples") or [],
        "runtime_gate": gate_result.as_dict(),
    }
    stages[gate] = stage
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "gate": gate,
        "status": status,
        "CODE_PASS": True,
        "RUNTIME_PASS": status == "PASSED",
        "stages": stages,
        "CODE_READY": True,
        "REAL_DATA_READY": str((stages.get("realtime_24h") or {}).get("status")) == "PASSED",
        "PAPER_READY": False,
        "SHADOW_READY": False,
        "TESTNET_READY": False,
        "ALPHA_READY": False,
        "LIVE_PREFLIGHT": False,
        "LIVE_ALLOWED": False,
    }
    REPORT_PATH.write_text(json.dumps(payload, indent=2, default=str))
    print(json.dumps({"gate": gate, "status": status, "reason": reason}, default=str))
    return 0 if status == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
