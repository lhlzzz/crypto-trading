from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from paper_runner import main as paper_main
from scripts import validate_runtime as vr


def _health(rows, missing=None, stale=None, gap_count=0):
    return {
        "required_sources": sorted({row["source"] for row in rows}),
        "missing_sources": missing or [],
        "stale_sources": stale or [],
        "gap_count": gap_count,
        "rows": rows,
    }


def _fresh(source, symbol="BTCUSDT"):
    return {"source": source, "symbol": symbol, "status": "FRESH"}


def _required_rows(symbol="BTCUSDT"):
    return [_fresh(source, symbol) for source in vr.runtime_required_sources()]


def test_channel_status_splits_mark_index_funding_from_dedicated_mark_price() -> None:
    health = _health(_required_rows())
    status = vr.channel_status(health)
    assert status["MARK_INDEX_FUNDING"] == "PASS"
    assert status["DEDICATED_MARK_PRICE"] == "NOT_REQUIRED"
    assert status["TRADE"] == "PASS"
    assert status["LIQUIDATION_LIVENESS"] == "PASS"


def test_channel_status_fails_when_mark_index_funding_component_is_stale() -> None:
    rows = _required_rows()
    for row in rows:
        if row["source"] == "FUTURES_MARK_PRICE":
            row["status"] = "STALE"
    health = _health(rows, stale=["FUTURES_MARK_PRICE"])
    assert vr.channel_status(health)["MARK_INDEX_FUNDING"] == "FAIL"


def test_stage_progression_requires_30m_before_2h() -> None:
    stages = {"realtime_30m": {"status": "NOT_STARTED"}}
    assert vr.prior_passed(stages, "realtime_2h") is False
    stages["realtime_30m"] = {"status": "PASSED"}
    assert vr.prior_passed(stages, "realtime_2h") is True
    assert vr.prior_passed(stages, "realtime_6h") is False
    assert vr.prior_passed(stages, "paper_24h") is False


def test_paper_requires_24h_realtime(tmp_path: Path, monkeypatch) -> None:
    report = vr.migrate_report({"stages": {"realtime_30m": {"status": "PASSED"}}})
    monkeypatch.setattr(vr, "REPORT_PATH", tmp_path / "report.json")
    vr.save_report(report, tmp_path / "report.json")
    assert vr.main(["--stage", "paper_24h", "--duration", "86400"]) == 1
    payload = json.loads((tmp_path / "report.json").read_text())
    assert payload["stages"]["paper_24h"]["status"] == "FAILED"
    assert payload["stages"]["paper_24h"]["reason"] == "LADDER_SKIPPED"
    assert payload["PAPER_READY"] is False


def test_stage_expiration_on_freeze_hash_change() -> None:
    stages = {
        "realtime_30m": {
            "status": "PASSED",
            "freeze_hashes": {path: "old" for path in vr.FREEZE_PATHS},
        }
    }
    expired = vr.expire_realtime_if_code_changed(stages)
    assert expired == ["realtime_30m"]
    assert stages["realtime_30m"]["status"] == "EXPIRED"


def test_docs_only_change_does_not_expire_30m() -> None:
    current = vr._freeze_hashes()
    stages = {"realtime_30m": {"status": "PASSED", "freeze_hashes": current}}
    assert vr.expire_realtime_if_code_changed(stages) == []
    assert stages["realtime_30m"]["status"] == "PASSED"


def test_transient_reconnect_is_not_stage_failure() -> None:
    samples = [
        {"at": "2026-09-02T00:00:00+00:00", "status": "healthy"},
        {"at": "2026-09-02T00:00:05+00:00", "status": "failure", "missing_sources": ["FUTURES_TRADE"]},
        {"at": "2026-09-02T00:00:08+00:00", "status": "healthy", "missing_sources": [], "stale_sources": []},
    ]
    events = vr.reconnect_events_from_samples(samples)
    assert events and events[0]["subscriptions_restored"] is True
    health = _health(_required_rows())
    status, reason = vr.evaluate_realtime_acceptance(
        stage="realtime_2h",
        duration=7200,
        requested_duration=7200,
        health=health,
        healthy_seconds=7000,
        degraded_seconds=50,
        failure_seconds=5,
        reconnect_events=events,
        collector_alive=True,
        persistence_ok=True,
        unsafe_at_end=False,
    )
    assert status == "PASSED"
    assert reason == "OK"


def test_24h_requires_controlled_reconnect() -> None:
    health = _health(_required_rows())
    status, reason = vr.evaluate_realtime_acceptance(
        stage="realtime_24h",
        duration=86400,
        requested_duration=86400,
        health=health,
        healthy_seconds=86000,
        degraded_seconds=0,
        failure_seconds=0,
        reconnect_events=[],
        collector_alive=True,
        persistence_ok=True,
        unsafe_at_end=False,
    )
    assert status == "FAILED"
    assert reason == "NO_CONTROLLED_RECONNECT"


def test_24h_reconnect_pass_when_subscriptions_restored() -> None:
    health = _health(_required_rows())
    events = [{
        "old_connection_id": "ws-0",
        "new_connection_id": "ws-1",
        "disconnect_at": "2026-09-02T00:00:00+00:00",
        "reconnect_at": "2026-09-02T00:00:02+00:00",
        "recovery_ms": 2000,
        "subscriptions_restored": True,
    }]
    status, reason = vr.evaluate_realtime_acceptance(
        stage="realtime_24h",
        duration=86400,
        requested_duration=86400,
        health=health,
        healthy_seconds=86000,
        degraded_seconds=20,
        failure_seconds=0,
        reconnect_events=events,
        collector_alive=True,
        persistence_ok=True,
        unsafe_at_end=False,
    )
    assert (status, reason) == ("PASSED", "OK")


def test_stale_or_unsafe_at_end_fails() -> None:
    health = _health(_required_rows(), stale=["FUTURES_TRADE"])
    status, reason = vr.evaluate_realtime_acceptance(
        stage="realtime_2h",
        duration=7200,
        requested_duration=7200,
        health=health,
        healthy_seconds=7100,
        degraded_seconds=100,
        failure_seconds=0,
        reconnect_events=[],
        collector_alive=True,
        persistence_ok=True,
        unsafe_at_end=False,
    )
    assert status == "FAILED"
    assert reason == "SOURCE_STALE_AT_END"
    healthy = _health(_required_rows(), gap_count=1)
    status, reason = vr.evaluate_realtime_acceptance(
        stage="realtime_2h",
        duration=7200,
        requested_duration=7200,
        health=healthy,
        healthy_seconds=7200,
        degraded_seconds=0,
        failure_seconds=0,
        reconnect_events=[],
        collector_alive=True,
        persistence_ok=True,
        unsafe_at_end=True,
    )
    assert reason == "ORDERBOOK_UNSAFE"


def test_watchdog_symbol_block_vs_global_halt() -> None:
    one = _health(
        [_fresh(source, "BTCUSDT") for source in vr.runtime_required_sources()]
        + [{"source": "FUTURES_TRADE", "symbol": "DOGEUSDT", "status": "STALE"}]
    )
    split = vr.symbol_and_global_health(one)
    assert split["symbol_health"]["DOGEUSDT"] == "BLOCKED"
    assert split["global_halt"] is False
    dead = _health([], missing=["COLLECTOR"])
    assert vr.watchdog_state(collector_alive=False, health=dead, persistence_ok=True) == "HALT"
    assert vr.symbol_and_global_health(dead)["global_halt"] is True


def test_process_duration_is_distinct_from_healthy_duration() -> None:
    health = _health(_required_rows())
    status, _reason = vr.evaluate_realtime_acceptance(
        stage="realtime_2h",
        duration=7200,
        requested_duration=7200,
        health=health,
        healthy_seconds=6000,
        degraded_seconds=1200,
        failure_seconds=0,
        reconnect_events=[],
        collector_alive=True,
        persistence_ok=True,
        unsafe_at_end=False,
    )
    assert status == "PASSED"
    status, reason = vr.evaluate_realtime_acceptance(
        stage="realtime_2h",
        duration=7200,
        requested_duration=7200,
        health=health,
        healthy_seconds=1000,
        degraded_seconds=1000,
        failure_seconds=5200,
        reconnect_events=[],
        collector_alive=True,
        persistence_ok=True,
        unsafe_at_end=False,
    )
    assert status == "FAILED"
    assert reason == "SOURCE_OUTAGE"


def test_cli_stage_duration_and_ladder_skip(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(vr, "REPORT_PATH", tmp_path / "runtime_validation_report.json")
    assert vr.resolve_stage("realtime_2h", 7200) == ("realtime_2h", 7200)
    with pytest.raises(SystemExit):
        vr.resolve_stage("realtime_2h", 1800)
    assert vr.main(["--stage", "realtime_2h", "--duration", "7200"]) == 1
    payload = json.loads((tmp_path / "runtime_validation_report.json").read_text())
    assert payload["stages"]["realtime_2h"]["reason"] == "LADDER_SKIPPED"


def test_migrate_report_keeps_30m_pass_and_does_not_set_real_data_ready(tmp_path: Path) -> None:
    original = {
        "status": "PASSED",
        "stages": {
            "realtime_30m": {
                "status": "PASSED",
                "reason": "OK",
                "required_sources": [
                    "FUTURES_BOOK_TICKER",
                    "FUTURES_DEPTH",
                    "FUTURES_FUNDING_LIVENESS",
                    "FUTURES_INDEX_PRICE",
                    "FUTURES_LIQUIDATION_LIVENESS",
                    "FUTURES_MARK_PRICE",
                    "FUTURES_TRADE",
                ],
                "missing_sources": [],
                "stale_sources": [],
            }
        },
    }
    migrated = vr.migrate_report(original)
    assert migrated["stages"]["realtime_30m"]["status"] == "PASSED"
    assert migrated["stages"]["realtime_30m"]["channel_status"]["MARK_INDEX_FUNDING"] == "PASS"
    assert migrated["stages"]["realtime_30m"]["channel_status"]["DEDICATED_MARK_PRICE"] == "NOT_REQUIRED"
    assert migrated["REAL_DATA_READY"] is False


def test_testnet_without_credentials_is_blocked(monkeypatch) -> None:
    monkeypatch.delenv("BIAN_TESTNET_API_KEY", raising=False)
    monkeypatch.delenv("BIAN_TESTNET_API_SECRET", raising=False)
    result = vr.run_testnet_stage()
    assert result["status"] == "BLOCKED"
    assert result["reason"] == "TESTNET_BLOCKED_BY_EXTERNAL_CREDENTIALS"


def test_live_preflight_stays_blocked_until_release_gates_pass() -> None:
    result = vr.run_live_preflight({
        "realtime_24h": {"status": "NOT_STARTED"},
        "paper_24h": {"status": "NOT_STARTED"},
        "shadow_7d": {"status": "NOT_STARTED"},
        "alpha_oos": {"alpha_status": "INSUFFICIENT_SAMPLE"},
        "testnet": {"status": "BLOCKED"},
    })
    assert result["status"] == "FAILED"
    assert result["LIVE_ALLOWED"] is False
    assert "REAL_DATA_READY" in result["missing"]


def test_alpha_empty_frames_are_insufficient_sample() -> None:
    result = vr.run_alpha_stage()
    assert result["alpha_status"] == "INSUFFICIENT_SAMPLE"
    assert result["status"] == "PASSED"


def test_paper_runner_accepts_duration_and_shadow_mode(monkeypatch) -> None:
    captured = {}

    def fake_shadow(symbols, *, duration_sec=None):
        captured["symbols"] = symbols
        captured["duration_sec"] = duration_sec

    monkeypatch.setattr("paper_runner.run_shadow_forever", fake_shadow)
    assert paper_main(["--mode", "shadow", "--duration", "604800", "--symbols", "BTCUSDT"]) == 0
    assert captured["duration_sec"] == 604800
    assert captured["symbols"] == ["BTCUSDT"]


def test_session_fields_are_persisted(tmp_path: Path) -> None:
    report = vr.migrate_report({"stages": {}})
    stage = vr.build_session_stage(
        name="realtime_2h",
        status="RUNNING",
        reason="RUNNING",
        session_id="sess-1",
        commit_sha="abc",
        start_at=datetime.now(timezone.utc).isoformat(),
        end_at=None,
        duration_sec=12,
        requested_duration=7200,
        symbols=["BTC-USDT"],
        health=_health(_required_rows()),
        healthy_sec=10,
        degraded_sec=2,
        failure_sec=0,
        reconnect_events=[],
        errors=[],
        samples=[],
        watchdog="HEALTHY",
    )
    payload = vr.write_session(report, stage, path=tmp_path / "session.json")
    assert payload["session_id"] == "sess-1"
    assert payload["stage"] == "realtime_2h"
    assert payload["healthy_sec"] == 10
    assert payload["degraded_sec"] == 2
    assert payload["channel_status"]["DEDICATED_MARK_PRICE"] == "NOT_REQUIRED"
    assert payload["REAL_DATA_READY"] is False
