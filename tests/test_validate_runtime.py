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
        "channel": "TRADE",
        "disconnect_at": "2026-09-02T00:00:00+00:00",
        "reconnect_at": "2026-09-02T00:00:02+00:00",
        "recovery_ms": 2000,
        "subscriptions_restored": True,
        "reason": "disconnect",
        "source": "collector_lifecycle",
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
    assert result["reason"] == "BLOCKED_BY_EXTERNAL_CREDENTIALS"


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


def test_alpha_empty_frames_are_insufficient_sample(monkeypatch) -> None:
    monkeypatch.setattr(vr, "_load_alpha_frames", lambda: [])
    result = vr.run_alpha_stage()
    assert result["alpha_status"] == "INSUFFICIENT_SAMPLE"
    assert result["status"] == "PASSED"


def test_paper_runner_accepts_duration_and_shadow_mode(monkeypatch) -> None:
    captured = {}

    def fake_shadow(symbols, *, duration_sec=None, planned_restart_after=None):
        captured["symbols"] = symbols
        captured["duration_sec"] = duration_sec
        captured["planned_restart_after"] = planned_restart_after

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


def test_inferred_reconnect_is_not_24h_proof() -> None:
    samples = [
        {"at": "2026-09-02T00:00:00+00:00", "status": "healthy"},
        {"at": "2026-09-02T00:00:05+00:00", "status": "failure", "missing_sources": ["FUTURES_TRADE"]},
        {"at": "2026-09-02T00:00:08+00:00", "status": "healthy", "missing_sources": [], "stale_sources": []},
    ]
    inferred = vr.reconnect_events_from_samples(samples)
    assert inferred and inferred[0]["source"] == "health_sample_fallback"
    health = _health(_required_rows())
    status, reason = vr.evaluate_realtime_acceptance(
        stage="realtime_24h",
        duration=86400,
        requested_duration=86400,
        health=health,
        healthy_seconds=86000,
        degraded_seconds=20,
        failure_seconds=0,
        reconnect_events=inferred,
        collector_alive=True,
        persistence_ok=True,
        unsafe_at_end=False,
    )
    assert status == "FAILED"
    assert reason == "NO_CONTROLLED_RECONNECT"


def test_orphaned_running_session_is_failed() -> None:
    stages = {
        "realtime_2h": {
            "status": "RUNNING",
            "owner_pid": 99999999,
            "session_id": "dead",
        }
    }
    recovered = vr.recover_orphaned_sessions(stages)
    assert recovered == ["realtime_2h"]
    assert stages["realtime_2h"]["status"] == "EXPIRED"
    assert stages["realtime_2h"]["reason"] == "STALE_RUNNING_SESSION"


def test_universe_qualification_does_not_treat_btc_as_meme_universe() -> None:
    payload = vr.universe_qualification(["BTC-USDT"])
    assert payload["qualification_scope"] == "BENCHMARK_ONLY"
    assert payload["meme_universe_validated"] is False
    assert payload["is_meme"] is False
    assert payload["runtime_stage_validated_symbols"] == ["BTCUSDT"]
    assert payload["production_target_universe"] == []


def test_paper_acceptance_requires_evidence_not_returncode() -> None:
    status, reason, detail = vr.paper_acceptance(
        requested_duration=10,
        duration_sec=10,
        process_completed=True,
        snapshot={
            "database_healthy": True,
            "observation_count": 0,
            "positioning_count": 0,
            "evidence_count": 0,
            "session_id": "paper-sess",
            "mode": "paper",
        },
    )
    assert status == "FAILED"
    assert reason == "PAPER_EVIDENCE_MISSING"
    status, reason, detail = vr.paper_acceptance(
        requested_duration=10,
        duration_sec=10,
        process_completed=True,
        snapshot={
            "database_healthy": True,
            "observation_count": 4,
            "positioning_count": 4,
            "evidence_count": 4,
            "episode_count": 1,
            "intent_count": 0,
            "risk_decision_count": 2,
            "order_count": 0,
            "fill_count": 0,
            "funding_count": 0,
            "long_building_count": 2,
            "short_building_count": 0,
            "duplicate_trades": 0,
            "duplicate_funding": 0,
            "invalid_positions": 0,
            "impossible_balance": False,
            "impossible_equity": False,
            "invalid_margin": False,
            "stale_open": 0,
            "unsafe_open": 0,
            "restart_recovery": True,
            "session_id": "paper-sess",
            "mode": "paper",
            "unknown_order": 0,
            "stale_pending_order": 0,
            "unsafe_order": 0,
        },
    )
    assert status == "PASSED"
    assert detail["DIRECTIONAL_SAMPLE_INSUFFICIENT"] is True
    assert detail["liquidation_model"]["binance_parity"] == "NOT_BINANCE_PARITY"


def test_shadow_acceptance_requires_evidence_and_does_not_claim_uncheckable_orders() -> None:
    snapshot = {
        "observation_count": 3,
        "positioning_count": 3,
        "evidence_count": 3,
        "episode_count": 1,
        "long_count": 2,
        "short_count": 0,
        "restart_recovery": True,
        "episode_split": False,
        "session_id": "shadow-sess",
        "mode": "shadow",
        "order_count": 0,
    }
    status, reason, detail = vr.shadow_acceptance(
        requested_duration=10,
        duration_sec=10,
        snapshot=snapshot,
        real_order_delta=None,
    )
    assert status == "BLOCKED"
    assert reason == "INSUFFICIENT_EVIDENCE"
    assert detail["real_order_proof"] == "INSUFFICIENT_EVIDENCE"
    status, reason, detail = vr.shadow_acceptance(
        requested_duration=10,
        duration_sec=10,
        snapshot=snapshot,
        real_order_delta=1,
    )
    assert status == "FAILED"
    assert reason == "SHADOW_REAL_ORDER_DELTA"


def test_paper_stage_uses_two_child_processes(monkeypatch) -> None:
    calls = []

    def fake_child(command, env=None):
        calls.append(command)
        return {"command": command, "returncode": 75 if len(calls) == 1 else 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(vr, "_run_child", fake_child)
    monkeypatch.setattr(vr, "runtime_acceptance_snapshot", lambda store, mode="paper", session_id=None: {
        "database_healthy": True,
        "observation_count": 2,
        "positioning_count": 2,
        "evidence_count": 2,
        "restart_recovery": True,
        "session_id": session_id or "paper-sess",
        "mode": mode,
        "unknown_order": 0,
        "stale_pending_order": 0,
        "unsafe_order": 0,
    })
    monkeypatch.setattr(vr, "TradingStore", lambda: object())
    result = vr.run_paper_stage(10, ["BTCUSDT"])
    assert len(calls) == 2
    assert "--planned-restart-after" in calls[0]
    assert result["process_completed"] is True


def test_alpha_stage_loads_persisted_frames(monkeypatch) -> None:
    from backtesting import AlphaGateResult

    monkeypatch.setattr(vr, "_load_alpha_frames", lambda: ["frame"])

    def fake_gate(frames, **kwargs):
        assert frames == ["frame"]
        return AlphaGateResult(
            status="INSUFFICIENT_SAMPLE",
            train_samples=0,
            validation_samples=0,
            oos_samples=0,
            train_metrics={},
            validation_metrics={},
            oos_metrics={"independent_episodes": 0, "long_episodes": 0, "short_episodes": 0},
            strategy_version="positioning-v1",
            parameter_version="abc",
            config_hash="abc",
            reason="no historical frames",
        )

    monkeypatch.setattr("backtesting.evaluate_alpha_gate", fake_gate)
    result = vr.run_alpha_stage()
    assert result["alpha_status"] == "INSUFFICIENT_SAMPLE"
    assert result["frame_count"] == 1


def test_testnet_with_credentials_does_not_pass_preflight_only(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_TESTNET_API_KEY", "k")
    monkeypatch.setenv("BIAN_TESTNET_API_SECRET", "s")
    monkeypatch.setattr(vr, "TradingStore", lambda: type("S", (), {"testnet_lifecycle_snapshot": lambda self: {}})())
    result = vr.run_testnet_stage()
    assert result["status"] == "FAILED"
    assert result["reason"] == "TESTNET_LIFECYCLE_INCOMPLETE"


def test_evidence_planes_are_separated() -> None:
    health = _health(_required_rows())
    planes = vr.evidence_planes(
        health=health,
        channels=vr.channel_status(health),
        reconnect_events=[],
        samples=[{"at": "x", "status": "healthy"}],
        collector_alive=True,
        persistence_ok=True,
    )
    assert planes["SOURCE_HEALTH"] == "PASS"
    assert planes["CHANNEL_HEALTH"] == "PASS"
    assert planes["CONNECTION_LIFECYCLE"] == "INSUFFICIENT"
    assert planes["EVIDENCE_SUFFICIENCY"] == "PASS"


def test_alpha_and_testnet_cannot_skip_prior_gates(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(vr, "REPORT_PATH", tmp_path / "runtime_validation_report.json")
    assert vr.main(["--stage", "alpha_oos"]) == 1
    payload = json.loads((tmp_path / "runtime_validation_report.json").read_text())
    assert payload["stages"]["alpha_oos"]["reason"] == "LADDER_SKIPPED"
    assert vr.main(["--stage", "testnet"]) == 1
    payload = json.loads((tmp_path / "runtime_validation_report.json").read_text())
    assert payload["stages"]["testnet"]["reason"] == "LADDER_SKIPPED"


def test_shadow_stage_uses_two_child_processes(monkeypatch) -> None:
    calls = []

    def fake_child(command, env=None):
        calls.append(command)
        return {"command": command, "returncode": 75 if len(calls) == 1 else 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(vr, "_run_child", fake_child)
    monkeypatch.setattr(vr, "exchange_open_order_count", lambda: None)
    monkeypatch.setattr(vr, "runtime_acceptance_snapshot", lambda store, mode="shadow", session_id=None: {
        "database_healthy": True,
        "observation_count": 2,
        "positioning_count": 2,
        "evidence_count": 2,
        "episode_count": 1,
        "restart_recovery": True,
        "session_id": session_id or "shadow-sess",
        "mode": mode,
        "order_count": 0,
    })
    monkeypatch.setattr(vr, "TradingStore", lambda: object())
    result = vr.run_shadow_stage(10, ["BTCUSDT"])
    assert len(calls) == 2
    assert "--planned-restart-after" in calls[0]
    assert result["process_completed"] is True
    assert result["status"] != "PASSED"
    assert result["actual_duration_sec"] < 10


def test_testnet_acceptance_requires_full_lifecycle() -> None:
    status, reason, detail = vr.testnet_acceptance({})
    assert status == "FAILED"
    assert reason == "TESTNET_LIFECYCLE_INCOMPLETE"
    assert detail["LONG lifecycle"] is False
    assert detail["SHORT lifecycle"] is False
    status, reason, _detail = vr.testnet_acceptance({
        "account": True,
        "one_way": True,
        "isolated": True,
        "long_lifecycle": True,
        "short_lifecycle": True,
        "partial_fill": True,
        "cancel": True,
        "unknown_resolution": True,
        "user_stream": True,
        "listen_key": True,
        "reconciliation": True,
        "restart": True,
    })
    assert status == "FAILED"
    assert reason == "TESTNET_LIFECYCLE_INCOMPLETE"


def test_reconnect_events_from_lifecycle_ignore_health_sample_fallback() -> None:
    inferred = vr.reconnect_events_from_samples([
        {"at": "2026-09-02T00:00:00+00:00", "status": "healthy"},
        {"at": "2026-09-02T00:00:05+00:00", "status": "failure"},
        {"at": "2026-09-02T00:00:08+00:00", "status": "healthy"},
    ])
    health = _health(_required_rows())
    health["rows"][-1]["metadata"] = {"reconnect_events": inferred}
    assert vr.reconnect_events_from_lifecycle(health) == []


def test_reconnect_events_from_lifecycle_read_collector_metadata() -> None:
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
    health = _health(_required_rows())
    health["rows"][-1]["metadata"] = {"reconnect_events": [event]}
    events = vr.reconnect_events_from_lifecycle(health)
    assert events == [event]
    health["rows"][-1]["metadata"] = {}
    health["reconnect_events"] = [event]
    assert vr.reconnect_events_from_lifecycle(health) == [event]


def test_paper_acceptance_fails_accounting_invariants() -> None:
    snapshot = {
        "database_healthy": True,
        "observation_count": 2,
        "positioning_count": 2,
        "evidence_count": 2,
        "impossible_balance": True,
        "impossible_equity": False,
        "invalid_margin": False,
        "restart_recovery": True,
        "session_id": "paper-sess",
        "mode": "paper",
    }
    status, reason, _detail = vr.paper_acceptance(
        requested_duration=10,
        duration_sec=10,
        process_completed=True,
        snapshot=snapshot,
    )
    assert (status, reason) == ("FAILED", "IMPOSSIBLE_BALANCE")
    snapshot["impossible_balance"] = False
    snapshot["impossible_equity"] = True
    status, reason, _detail = vr.paper_acceptance(
        requested_duration=10,
        duration_sec=10,
        process_completed=True,
        snapshot=snapshot,
    )
    assert (status, reason) == ("FAILED", "IMPOSSIBLE_EQUITY")


def test_run_realtime_cancels_collector_after_window(monkeypatch) -> None:
    import asyncio

    calls: dict[str, object] = {}

    async def fake_observe(*args, **kwargs):
        calls["duration_sec"] = kwargs.get("duration_sec")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            calls["cancelled"] = True
            raise

    class FakeStore:
        def market_data_freshness(self, *, max_age_sec=900, symbols=None):
            return _required_rows()

        def collector_lifecycle_events(self):
            return [{
                "old_connection_id": "trade-aaa",
                "new_connection_id": "trade-bbb",
                "channel": "TRADE",
                "disconnect_at": "2026-09-02T00:00:00+00:00",
                "reconnect_at": "2026-09-02T00:00:02+00:00",
                "recovery_ms": 2000,
                "subscriptions_restored": True,
                "reason": "controlled_reconnect",
                "source": "collector_lifecycle",
            }]

    monkeypatch.setattr(vr, "observe", fake_observe)
    monkeypatch.setattr(vr, "TradingStore", FakeStore)
    monkeypatch.setattr(vr, "SAMPLE_SEC", 0.05)
    result = asyncio.run(vr.run_realtime(1, ["BTCUSDT"]))
    assert calls["duration_sec"] is None
    assert calls.get("cancelled") is True
    assert result["collector_alive"] is True
    assert result["reconnect_events"][0]["channel"] == "TRADE"
    assert result["reconnect_events"][0]["source"] == "collector_lifecycle"
    assert all(event.get("proof") is not False for event in result["reconnect_events"])


def _paper_ok_snapshot(**overrides):
    snapshot = {
        "database_healthy": True,
        "session_id": "paper-sess",
        "mode": "paper",
        "observation_count": 4,
        "positioning_count": 4,
        "evidence_count": 4,
        "episode_count": 1,
        "duplicate_trades": 0,
        "duplicate_funding": 0,
        "invalid_positions": 0,
        "impossible_balance": False,
        "impossible_equity": False,
        "invalid_margin": False,
        "unknown_order": 0,
        "stale_pending_order": 0,
        "unsafe_order": 0,
        "restart_recovery": True,
    }
    snapshot.update(overrides)
    return snapshot


def test_paper_acceptance_is_session_scoped_and_rejects_foreign_mode() -> None:
    status, reason, _detail = vr.paper_acceptance(
        requested_duration=10,
        duration_sec=10,
        process_completed=True,
        snapshot=_paper_ok_snapshot(session_id=None),
    )
    assert (status, reason) == ("FAILED", "PAPER_SESSION_MISSING")
    status, reason, _detail = vr.paper_acceptance(
        requested_duration=10,
        duration_sec=10,
        process_completed=True,
        snapshot=_paper_ok_snapshot(mode="testnet"),
    )
    assert (status, reason) == ("FAILED", "PAPER_MODE_MISMATCH")
    status, reason, _detail = vr.paper_acceptance(
        requested_duration=10,
        duration_sec=10,
        process_completed=True,
        snapshot=_paper_ok_snapshot(mode=""),
    )
    assert (status, reason) == ("FAILED", "PAPER_MODE_MISMATCH")


def test_unknown_order_always_fails_paper_acceptance() -> None:
    status, reason, _detail = vr.paper_acceptance(
        requested_duration=10,
        duration_sec=10,
        process_completed=True,
        snapshot=_paper_ok_snapshot(unknown_order=1),
    )
    assert (status, reason) == ("FAILED", "UNKNOWN_ORDER")


def test_stale_pending_order_fails_paper_acceptance() -> None:
    status, reason, _detail = vr.paper_acceptance(
        requested_duration=10,
        duration_sec=10,
        process_completed=True,
        snapshot=_paper_ok_snapshot(stale_pending_order=1),
    )
    assert (status, reason) == ("FAILED", "STALE_PENDING_ORDER")


def test_shadow_zero_delta_can_pass_when_session_has_no_local_orders() -> None:
    snapshot = {
        "observation_count": 3,
        "positioning_count": 3,
        "evidence_count": 3,
        "episode_count": 1,
        "restart_recovery": True,
        "episode_split": False,
        "session_id": "shadow-sess",
        "mode": "shadow",
        "order_count": 0,
    }
    status, reason, _detail = vr.shadow_acceptance(
        requested_duration=10,
        duration_sec=10,
        snapshot=snapshot,
        real_order_delta=0,
    )
    assert (status, reason) == ("PASSED", "OK")
    snapshot["order_count"] = 1
    status, reason, _detail = vr.shadow_acceptance(
        requested_duration=10,
        duration_sec=10,
        snapshot=snapshot,
        real_order_delta=0,
    )
    assert (status, reason) == ("FAILED", "SHADOW_LOCAL_ORDER_PRESENT")


def test_stale_running_session_expires_on_load_report(tmp_path, monkeypatch) -> None:
    path = tmp_path / "report.json"
    path.write_text(json.dumps({
        "stages": {
            "realtime_2h": {
                "status": "RUNNING",
                "owner_pid": 99999999,
                "session_id": "dead",
                "start_at": "2026-01-01T00:00:00+00:00",
                "heartbeat_at": "2026-01-01T00:00:00+00:00",
            }
        }
    }))
    monkeypatch.setattr(vr, "REPORT_PATH", path)
    report = vr.load_report()
    assert report["stages"]["realtime_2h"]["status"] == "EXPIRED"
    assert report["stages"]["realtime_2h"]["reason"] == "STALE_RUNNING_SESSION"


def test_live_preflight_evaluates_live_mode(monkeypatch) -> None:
    captured = {}

    class FakeGate:
        def as_dict(self):
            return {
                "mode": "live",
                "MARKET_HEALTH": "OK",
                "ACCOUNT_HEALTH": "OK",
                "USER_STREAM_HEALTH": "OK",
                "RECONCILIATION_HEALTH": "OK",
                "RISK_HEALTH": "SAFE",
                "meme_universe": True,
                "global_transport_health": "OK",
            }

    def fake_gate(**kwargs):
        captured.update(kwargs)
        return FakeGate()

    monkeypatch.setattr(vr, "TradingStore", lambda: object())
    monkeypatch.setattr(vr, "evaluate_runtime_gate", fake_gate)
    result = vr.run_live_preflight({
        "realtime_24h": {"status": "PASSED"},
        "paper_24h": {"status": "PASSED"},
        "shadow_7d": {"status": "PASSED"},
        "alpha_oos": {"alpha_status": "ALPHA_SUPPORTED"},
        "testnet": {"status": "PASSED"},
    })
    assert captured["mode"] == "live"
    assert result["mode"] == "live"
    assert result["LIVE_ALLOWED"] is False
    assert result["ACCOUNT_HEALTHY"] is True
    assert result["USER_STREAM_HEALTHY"] is True


def test_live_preflight_rejects_not_applicable_account_health(monkeypatch) -> None:
    class FakeGate:
        def as_dict(self):
            return {
                "mode": "live",
                "MARKET_HEALTH": "OK",
                "ACCOUNT_HEALTH": "NOT_APPLICABLE",
                "USER_STREAM_HEALTH": "NOT_APPLICABLE",
                "RECONCILIATION_HEALTH": "OK",
                "RISK_HEALTH": "SAFE",
                "meme_universe": True,
                "global_transport_health": "OK",
            }

    monkeypatch.setattr(vr, "TradingStore", lambda: object())
    monkeypatch.setattr(vr, "evaluate_runtime_gate", lambda **kwargs: FakeGate())
    result = vr.run_live_preflight({
        "realtime_24h": {"status": "PASSED"},
        "paper_24h": {"status": "PASSED"},
        "shadow_7d": {"status": "PASSED"},
        "alpha_oos": {"alpha_status": "ALPHA_SUPPORTED"},
        "testnet": {"status": "PASSED"},
    })
    assert result["ACCOUNT_HEALTHY"] is False
    assert result["USER_STREAM_HEALTHY"] is False
    assert result["LIVE_ALLOWED"] is False


def _testnet_evidence(**overrides):
    evidence = {
        "source": "database",
        "session_id": "tn-1",
        "start_time": "2026-09-03T00:00:00+00:00",
        "end_time": "2026-09-03T00:10:00+00:00",
        "symbol": "BTCUSDT",
        "long": {
            "order": {"order_id": "lo-1", "client_order_id": "L-1", "exchange_order_id": "100"},
            "fills": [{"trade_id": "1", "exchange_trade_id": "t-1", "quantity": "0.01"}],
            "user_stream_observed": True,
            "local_state_updated": True,
            "reconciliation_matches": True,
            "close": {
                "status": "FILLED",
                "exchange_order_id": "104",
                "reconciliation_matches": True,
            },
        },
        "short": {
            "order": {"order_id": "so-1", "client_order_id": "S-1", "exchange_order_id": "101"},
            "fills": [{"trade_id": "2", "exchange_trade_id": "t-2", "quantity": "0.01"}],
            "user_stream_observed": True,
            "local_state_updated": True,
            "reconciliation_matches": True,
            "close": {
                "status": "FILLED",
                "exchange_order_id": "105",
                "reconciliation_matches": True,
            },
        },
        "partial_fill": {
            "status": "PARTIALLY_FILLED",
            "order_id": "p-1",
            "exchange_order_id": "102",
            "event_type": "USER_STREAM_ORDER_UPDATE",
            "event_id": "ev-partial",
        },
        "cancel": {
            "status": "CANCELLED",
            "order_id": "c-1",
            "exchange_order_id": "103",
            "event_type": "ORDER_CANCELLED",
            "event_id": "ev-cancel",
        },
        "unknown_order": {
            "status": "UNKNOWN",
            "resolved_by": "client_order_id",
            "resubmitted": False,
            "resolved_status": "FILLED",
            "halted": False,
        },
        "user_stream": {
            "events": [
                {
                    "event_id": "ev-trade",
                    "exchange_order_id": "100",
                    "symbol": "BTCUSDT",
                    "event_time": "2026-09-03T00:01:00+00:00",
                    "execution_type": "TRADE",
                    "event_type": "ORDER_TRADE_UPDATE",
                },
                {
                    "event_id": "ev-account",
                    "symbol": "BTCUSDT",
                    "event_time": "2026-09-03T00:01:01+00:00",
                    "execution_type": "ACCOUNT_UPDATE",
                    "event_type": "ACCOUNT_UPDATE",
                    "session_id": "tn-1",
                    "position_updates": [{"symbol": "BTCUSDT", "quantity": "0"}],
                },
            ]
        },
        "listen_key": {"created": True, "listen_key": "lk-1"},
        "reconciliation": {
            "ok": True,
            "open_matches": True,
            "orders": [
                {"local_order_id": "L-1", "exchange_order_id": "100", "match": True},
                {"local_order_id": "L-2", "exchange_order_id": "104", "match": True},
                {"local_order_id": "S-1", "exchange_order_id": "101", "match": True},
                {"local_order_id": "S-2", "exchange_order_id": "105", "match": True},
            ],
            "fills": [{"trade_id": "1", "exchange_trade_id": "t-1", "match": True}],
            "local_flat": True,
            "exchange_flat": True,
        },
        "restart": {"ok": True},
    }
    evidence.update(overrides)
    return evidence


def test_testnet_acceptance_requires_structured_lifecycle_evidence() -> None:
    status, reason, _detail = vr.testnet_acceptance(_testnet_evidence())
    assert (status, reason) == ("PASSED", "OK")
    status, reason, _detail = vr.testnet_acceptance(_testnet_evidence(unknown_order={
        "status": "UNKNOWN",
        "resolved_by": "client_order_id",
        "resubmitted": True,
        "resolved_status": "FILLED",
    }))
    assert (status, reason) == ("FAILED", "TESTNET_UNKNOWN_RESUBMITTED")


def test_alpha_stage_reports_explicit_sample_fields(monkeypatch) -> None:
    from backtesting import AlphaGateResult

    monkeypatch.setattr(vr, "_load_alpha_frames", lambda: ["frame"])

    def fake_gate(frames, **kwargs):
        return AlphaGateResult(
            status="INSUFFICIENT_SAMPLE",
            train_samples=1,
            validation_samples=1,
            oos_samples=0,
            train_metrics={},
            validation_metrics={},
            oos_metrics={"independent_episodes": 0, "observation_samples": 2},
            strategy_version="positioning-v1",
            parameter_version="abc",
            config_hash="abc",
            reason="no historical frames",
            oos_frame_count=1,
            oos_observation_samples=2,
            oos_independent_episodes=0,
        )

    monkeypatch.setattr("backtesting.evaluate_alpha_gate", fake_gate)
    result = vr.run_alpha_stage()
    assert result["oos_samples_semantics"] == "independent_episodes"
    assert result["oos_frame_count"] == 1
    assert result["oos_observation_samples"] == 2
    assert result["oos_independent_episodes"] == 0
    assert result["model_training_completed"] is False
    assert result["frozen_strategy"] is True
    assert result["chronological_train"] is True


def test_pending_and_terminal_order_states_are_canonical() -> None:
    from execution import PENDING_ORDER_STATES, TERMINAL_ORDER_STATES

    assert PENDING_ORDER_STATES == {
        "CREATED",
        "RISK_APPROVED",
        "SUBMITTED",
        "ACKNOWLEDGED",
        "PARTIALLY_FILLED",
        "UNKNOWN",
    }
    assert TERMINAL_ORDER_STATES == {
        "FILLED",
        "REJECTED",
        "CANCELLED",
        "EXPIRED",
        "FAILED",
    }
    assert "OPEN" not in PENDING_ORDER_STATES
    assert "OPEN" not in TERMINAL_ORDER_STATES
    for status in TERMINAL_ORDER_STATES:
        snapshot = _paper_ok_snapshot()
        status_name, reason, _detail = vr.paper_acceptance(
            requested_duration=10,
            duration_sec=10,
            process_completed=True,
            snapshot=snapshot,
        )
        assert (status_name, reason) == ("PASSED", "OK"), status
    for status in PENDING_ORDER_STATES:
        if status == "UNKNOWN":
            status_name, reason, _detail = vr.paper_acceptance(
                requested_duration=10,
                duration_sec=10,
                process_completed=True,
                snapshot=_paper_ok_snapshot(unknown_order=1),
            )
            assert (status_name, reason) == ("FAILED", "UNKNOWN_ORDER")
        else:
            status_name, reason, _detail = vr.paper_acceptance(
                requested_duration=10,
                duration_sec=10,
                process_completed=True,
                snapshot=_paper_ok_snapshot(stale_pending_order=1),
            )
            assert (status_name, reason) == ("FAILED", "STALE_PENDING_ORDER")


def _session_times(start, pre, post, end, requested=10):
    return {
        "started_at": start,
        "pre_restart_at": pre,
        "post_restart_at": post,
        "ended_at": end,
        "pre_restart_snapshot": {"positions": []},
        "post_restart_snapshot": {"positions": []},
        "requested_duration": requested,
    }


def test_session_wall_clock_short_fails() -> None:
    proof = vr.session_wall_clock_proof(
        _session_times(
            "2026-09-03T00:00:00+00:00",
            "2026-09-03T00:00:04+00:00",
            "2026-09-03T00:00:05+00:00",
            "2026-09-03T00:00:09+00:00",
        ),
        requested_duration=10,
    )
    assert proof["ok"] is False
    assert proof["reason"] == "DURATION_SHORT"
    assert proof["actual_duration_sec"] == 9


def test_session_wall_clock_exact_and_long_pass() -> None:
    exact = vr.session_wall_clock_proof(
        _session_times(
            "2026-09-03T00:00:00+00:00",
            "2026-09-03T00:00:04+00:00",
            "2026-09-03T00:00:05+00:00",
            "2026-09-03T00:00:10+00:00",
        ),
        requested_duration=10,
    )
    assert exact["ok"] is True
    assert exact["actual_duration_sec"] == 10
    long = vr.session_wall_clock_proof(
        _session_times(
            "2026-09-03T00:00:00+00:00",
            "2026-09-03T00:00:04+00:00",
            "2026-09-03T00:00:05+00:00",
            "2026-09-03T00:00:11+00:00",
        ),
        requested_duration=10,
    )
    assert long["ok"] is True
    assert long["actual_duration_sec"] == 11


def test_session_wall_clock_does_not_round_down_to_pass() -> None:
    proof = vr.session_wall_clock_proof(
        _session_times(
            "2026-09-03T00:00:00+00:00",
            "2026-09-03T12:00:00+00:00",
            "2026-09-03T12:00:01+00:00",
            "2026-09-03T23:59:59+00:00",
        ),
        requested_duration=86400,
    )
    assert proof["ok"] is False
    assert proof["actual_duration_sec"] == 86399
    assert proof["reason"] == "DURATION_SHORT"


def test_restart_total_wall_clock_can_pass() -> None:
    proof = vr.session_wall_clock_proof(
        _session_times(
            "2026-09-03T00:00:00+00:00",
            "2026-09-03T12:00:00+00:00",
            "2026-09-03T12:00:01+00:00",
            "2026-09-04T00:00:00+00:00",
        ),
        requested_duration=86400,
    )
    assert proof["ok"] is True
    assert proof["actual_duration_sec"] == 86400


def test_paper_stage_correct_returncodes_with_short_wall_clock_fail(monkeypatch) -> None:
    calls = []

    def fake_child(command, env=None):
        calls.append(command)
        return {"command": command, "returncode": 75 if len(calls) == 1 else 0, "stdout": "", "stderr": ""}

    class Store:
        def bind_validation_session(self, *args, **kwargs):
            return None

        def expire_stale_validation_sessions(self, **kwargs):
            return 0

        def finish_validation_session(self, *args, **kwargs):
            return None

        def load_validation_session(self, session_id):
            return _session_times(
                "2026-09-03T00:00:00+00:00",
                "2026-09-03T00:00:01+00:00",
                "2026-09-03T00:00:02+00:00",
                "2026-09-03T00:00:03+00:00",
            )

        def compare_restart_snapshots(self, pre, post):
            return {"ok": True, "reason": "OK"}

    monkeypatch.setattr(vr, "_run_child", fake_child)
    monkeypatch.setattr(vr, "runtime_acceptance_snapshot", lambda store, mode="paper", session_id=None: {
        **_paper_ok_snapshot(session_id=session_id or "paper-sess", mode=mode),
        "restart_recovery": True,
    })
    monkeypatch.setattr(vr, "TradingStore", Store)
    result = vr.run_paper_stage(10, ["BTCUSDT"])
    assert result["process_completed"] is True
    assert result["status"] == "FAILED"
    assert result["reason"] == "DURATION_SHORT"
    assert result["actual_duration_sec"] == 3


def test_paper_stage_restart_total_duration_can_pass(monkeypatch) -> None:
    calls = []

    def fake_child(command, env=None):
        calls.append(command)
        return {"command": command, "returncode": 75 if len(calls) == 1 else 0, "stdout": "", "stderr": ""}

    class Store:
        def bind_validation_session(self, *args, **kwargs):
            return None

        def expire_stale_validation_sessions(self, **kwargs):
            return 0

        def finish_validation_session(self, *args, **kwargs):
            return None

        def load_validation_session(self, session_id):
            return _session_times(
                "2026-09-03T00:00:00+00:00",
                "2026-09-03T00:00:05+00:00",
                "2026-09-03T00:00:06+00:00",
                "2026-09-03T00:00:10+00:00",
            )

        def compare_restart_snapshots(self, pre, post):
            return {"ok": True, "reason": "OK"}

    monkeypatch.setattr(vr, "_run_child", fake_child)
    monkeypatch.setattr(vr, "runtime_acceptance_snapshot", lambda store, mode="paper", session_id=None: {
        **_paper_ok_snapshot(session_id=session_id or "paper-sess", mode=mode),
        "restart_recovery": True,
        "long_building_count": 1,
        "short_building_count": 1,
    })
    monkeypatch.setattr(vr, "TradingStore", Store)
    result = vr.run_paper_stage(10, ["BTCUSDT"])
    assert result["process_completed"] is True
    assert result["status"] == "PASSED"
    assert result["actual_duration_sec"] == 10
    assert "--planned-restart-after" in calls[0]


def test_testnet_json_only_evidence_cannot_pass_runtime(monkeypatch) -> None:
    class Store:
        def bind_validation_session(self, *args, **kwargs):
            return None

        def expire_stale_validation_sessions(self, **kwargs):
            return 0

        def testnet_lifecycle_evidence(self, session_id=None):
            return {"session_id": session_id, "source": "database"}

        def record_testnet_lifecycle_evidence(self, evidence):
            return None

        def finish_validation_session(self, *args, **kwargs):
            return None

    monkeypatch.setenv("BIAN_TESTNET_API_KEY", "k")
    monkeypatch.setenv("BIAN_TESTNET_API_SECRET", "s")
    monkeypatch.setattr(vr, "TradingStore", Store)
    result = vr.run_testnet_stage(session_id="new-session")
    assert result["status"] == "FAILED"
    assert result["reason"] == "TESTNET_LIFECYCLE_INCOMPLETE"
    assert result["source"] == "database"


def test_testnet_missing_real_facts_fail() -> None:
    cases = {
        "missing real order": _testnet_evidence(long={"order": {}, "fills": [], "close": {}}),
        "missing exchange_order_id": _testnet_evidence(long={
            "order": {"client_order_id": "L-1"},
            "fills": [{"trade_id": "1"}],
            "user_stream_observed": True,
            "local_state_updated": True,
            "reconciliation_matches": True,
            "close": {"status": "FILLED", "exchange_order_id": "104", "reconciled_flat": True},
        }),
        "missing UserStream event": _testnet_evidence(user_stream={"observed": True}),
        "missing reconciliation": _testnet_evidence(reconciliation={"ok": True}),
        "not flat after close": _testnet_evidence(reconciliation={
            "ok": True,
            "open_matches": True,
            "orders": [{"local_order_id": "L-1", "exchange_order_id": "100", "match": True}],
            "local_flat": False,
            "exchange_flat": False,
        }),
        "exchange not flat": _testnet_evidence(reconciliation={
            "ok": True,
            "open_matches": True,
            "orders": [{"local_order_id": "L-1", "exchange_order_id": "100", "match": True}],
            "local_flat": True,
            "exchange_flat": False,
        }),
        "partial fill missing": _testnet_evidence(partial_fill={}),
        "cancel missing": _testnet_evidence(cancel={}),
    }
    for name, evidence in cases.items():
        status, reason, detail = vr.testnet_acceptance(evidence)
        assert status == "FAILED", name
        assert reason == "TESTNET_LIFECYCLE_INCOMPLETE", name


def test_testnet_unknown_resubmit_and_unresolved_fail() -> None:
    status, reason, _detail = vr.testnet_acceptance(_testnet_evidence(unknown_order={
        "status": "UNKNOWN",
        "resolved_by": "client_order_id",
        "resubmitted": True,
        "resolved_status": "FILLED",
        "halted": False,
    }))
    assert (status, reason) == ("FAILED", "TESTNET_UNKNOWN_RESUBMITTED")
    status, reason, _detail = vr.testnet_acceptance(_testnet_evidence(unknown_order={
        "status": "UNKNOWN",
        "resolved_by": "client_order_id",
        "resubmitted": False,
        "resolved_status": None,
        "halted": True,
    }))
    assert (status, reason) == ("FAILED", "TESTNET_UNKNOWN_UNRESOLVED")


def test_old_session_facts_cannot_pass_new_session() -> None:
    from trading_store import derive_testnet_lifecycle_facts

    old_facts = derive_testnet_lifecycle_facts(
        session={"session_id": "old", "started_at": "2026-01-01T00:00:00+00:00", "ended_at": "2026-01-01T00:10:00+00:00"},
        orders=[{
            "order_id": "o1",
            "client_order_id": "L-1",
            "exchange_order_id": "100",
            "symbol": "BTCUSDT",
            "status": "FILLED",
            "position_side": "LONG",
            "position_action": "OPEN",
        }],
        order_events=[],
        trades=[{"trade_id": "1", "order_id": "o1", "quantity": "0.01"}],
        positions=[{"symbol": "BTCUSDT", "position_side": "FLAT", "quantity": "0"}],
        system_events=[],
    )
    new_facts = derive_testnet_lifecycle_facts(
        session={"session_id": "new", "started_at": "2026-09-03T00:00:00+00:00", "ended_at": "2026-09-03T00:10:00+00:00"},
        orders=[],
        order_events=[],
        trades=[],
        positions=[{"symbol": "BTCUSDT", "position_side": "FLAT", "quantity": "0"}],
        system_events=[],
    )
    assert old_facts["session_id"] == "old"
    assert new_facts["session_id"] == "new"
    status, reason, _detail = vr.testnet_acceptance(new_facts)
    assert status == "FAILED"
    assert reason in {"TESTNET_LIFECYCLE_INCOMPLETE", "TESTNET_UNKNOWN_UNRESOLVED"}


def test_acceptance_path_files_are_freeze_paths() -> None:
    assert "scripts/validate_runtime.py" in vr.FREEZE_PATHS
    assert "scripts/database.py" in vr.FREEZE_PATHS
    stages = {
        "realtime_30m": {
            "status": "PASSED",
            "freeze_hashes": {path: "old" for path in vr.FREEZE_PATHS},
        }
    }
    expired = vr.expire_realtime_if_code_changed(stages)
    assert expired == ["realtime_30m"]


def test_db_expired_session_cannot_keep_report_passed(monkeypatch) -> None:
    class Store:
        def expire_stale_validation_sessions(self, **kwargs):
            return 1

        def load_validation_session(self, session_id):
            return {"session_id": session_id, "status": "EXPIRED"}

    monkeypatch.setattr(vr, "TradingStore", Store)
    stages = {
        "paper_24h": {"status": "PASSED", "session_id": "sess-expired"},
        "shadow_7d": {"status": "NOT_STARTED"},
    }
    mismatched = vr.reconcile_report_with_session_status(stages)
    assert mismatched == ["paper_24h"]
    assert stages["paper_24h"]["status"] == "EXPIRED"
    assert vr.prior_passed(stages, "shadow_7d") is False


def test_testnet_acceptance_requires_exchange_trade_id() -> None:
    evidence = _testnet_evidence()
    evidence["long"]["fills"] = [{"trade_id": "1", "quantity": "0.01"}]
    status, reason, detail = vr.testnet_acceptance(evidence)
    assert status == "FAILED"
    assert detail["LONG lifecycle"] is False


def test_generic_user_stream_event_does_not_satisfy_lifecycle() -> None:
    evidence = _testnet_evidence()
    evidence["user_stream"] = {
        "events": [
            {
                "event_id": "ev-other",
                "exchange_order_id": "999",
                "symbol": "ETHUSDT",
                "event_time": "2026-09-03T00:01:00+00:00",
                "execution_type": "TRADE",
                "event_type": "ORDER_TRADE_UPDATE",
            }
        ]
    }
    status, reason, detail = vr.testnet_acceptance(evidence)
    assert status == "FAILED"
    assert detail["USER_STREAM"] is False


def test_local_flat_does_not_replace_exchange_flat() -> None:
    evidence = _testnet_evidence()
    evidence["reconciliation"] = {
        **evidence["reconciliation"],
        "ok": True,
        "open_matches": True,
        "local_flat": True,
        "exchange_flat": False,
    }
    status, reason, detail = vr.testnet_acceptance(evidence)
    assert status == "FAILED"
    assert detail["reconciliation"] is False


def test_db_unavailable_invalidates_passed_report(monkeypatch) -> None:
    def boom():
        raise RuntimeError("db down")

    monkeypatch.setattr(vr, "TradingStore", boom)
    stages = {"paper_24h": {"status": "PASSED", "session_id": "s1"}}
    mismatched = vr.reconcile_report_with_session_status(stages)
    assert mismatched == ["paper_24h"]
    assert stages["paper_24h"]["status"] == "FAILED"
    assert stages["paper_24h"]["reason"] == "SESSION_STATUS_UNAVAILABLE"


def test_missing_session_invalidates_passed_report(monkeypatch) -> None:
    class Store:
        def load_validation_session(self, session_id):
            return None

    monkeypatch.setattr(vr, "TradingStore", Store)
    stages = {"paper_24h": {"status": "PASSED", "session_id": "missing"}}
    mismatched = vr.reconcile_report_with_session_status(stages)
    assert stages["paper_24h"]["status"] == "FAILED"
    assert stages["paper_24h"]["reason"] == "SESSION_MISSING"
    assert mismatched == ["paper_24h"]


def test_session_lookup_exception_fails_closed(monkeypatch) -> None:
    class Store:
        def load_validation_session(self, session_id):
            raise RuntimeError("timeout")

    monkeypatch.setattr(vr, "TradingStore", Store)
    stages = {"paper_24h": {"status": "PASSED", "session_id": "s1"}}
    mismatched = vr.reconcile_report_with_session_status(stages)
    assert stages["paper_24h"]["reason"] == "SESSION_LOOKUP_FAILED"
    assert mismatched == ["paper_24h"]


def test_session_mode_mismatch_blocks(monkeypatch) -> None:
    class Store:
        def load_validation_session(self, session_id):
            return {"session_id": session_id, "status": "PASSED", "mode": "paper", "stage": "paper_24h"}

    monkeypatch.setattr(vr, "TradingStore", Store)
    stages = {"paper_24h": {"status": "PASSED", "session_id": "s1", "mode": "testnet", "stage": "paper_24h"}}
    mismatched = vr.reconcile_report_with_session_status(stages)
    assert stages["paper_24h"]["reason"] == "SESSION_MODE_MISMATCH"
    assert mismatched == ["paper_24h"]


def test_session_stage_mismatch_blocks(monkeypatch) -> None:
    class Store:
        def load_validation_session(self, session_id):
            return {"session_id": session_id, "status": "PASSED", "mode": "paper", "stage": "shadow_7d"}

    monkeypatch.setattr(vr, "TradingStore", Store)
    stages = {"paper_24h": {"status": "PASSED", "session_id": "s1", "mode": "paper", "stage": "paper_24h"}}
    mismatched = vr.reconcile_report_with_session_status(stages)
    assert stages["paper_24h"]["reason"] == "SESSION_STAGE_MISMATCH"
    assert mismatched == ["paper_24h"]


def test_generic_reconciliation_event_does_not_satisfy_lifecycle() -> None:
    evidence = _testnet_evidence()
    evidence["reconciliation"] = {
        "ok": True,
        "open_matches": True,
        "orders": [{"local_order_id": "x", "exchange_order_id": "999", "match": True}],
        "local_flat": True,
        "exchange_flat": True,
    }
    status, _reason, detail = vr.testnet_acceptance(evidence)
    assert status == "FAILED"
    assert detail["reconciliation"] is False


def test_wrong_exchange_order_id_fails_testnet_acceptance() -> None:
    evidence = _testnet_evidence()
    evidence["user_stream"]["events"][0]["exchange_order_id"] = "999"
    status, _reason, detail = vr.testnet_acceptance(evidence)
    assert status == "FAILED"
    assert detail["USER_STREAM"] is False


def test_wrong_session_id_fails_testnet_acceptance() -> None:
    evidence = _testnet_evidence()
    for event in evidence["user_stream"]["events"]:
        event["session_id"] = "other-session"
    status, _reason, detail = vr.testnet_acceptance(evidence)
    assert status == "FAILED"
    assert detail["USER_STREAM"] is False


def test_stale_account_update_fails_testnet_acceptance() -> None:
    evidence = _testnet_evidence()
    evidence["user_stream"]["events"][1]["event_time"] = "2020-01-01T00:00:00+00:00"
    status, _reason, detail = vr.testnet_acceptance(evidence)
    assert status == "FAILED"
    assert detail["USER_STREAM"] is False


def test_session_commit_mismatch_blocks(monkeypatch) -> None:
    class Store:
        def load_validation_session(self, session_id):
            return {
                "session_id": session_id,
                "status": "PASSED",
                "mode": "paper",
                "stage": "paper_24h",
                "commit_sha": "aaa",
            }

    monkeypatch.setattr(vr, "TradingStore", Store)
    stages = {
        "paper_24h": {
            "status": "PASSED",
            "session_id": "s1",
            "mode": "paper",
            "stage": "paper_24h",
            "commit_sha": "bbb",
        }
    }
    mismatched = vr.reconcile_report_with_session_status(stages)
    assert stages["paper_24h"]["reason"] == "SESSION_COMMIT_MISMATCH"
    assert mismatched == ["paper_24h"]


def test_live_allowed_remains_false(monkeypatch) -> None:
    class FakeGate:
        def as_dict(self):
            return {
                "mode": "live",
                "MARKET_HEALTH": "OK",
                "ACCOUNT_HEALTH": "OK",
                "USER_STREAM_HEALTH": "OK",
                "RECONCILIATION_HEALTH": "OK",
                "RISK_HEALTH": "SAFE",
                "meme_universe": True,
                "global_transport_health": "OK",
            }

    monkeypatch.setattr(vr, "TradingStore", lambda: object())
    monkeypatch.setattr(vr, "evaluate_runtime_gate", lambda **kwargs: FakeGate())
    result = vr.run_live_preflight({})
    assert result["LIVE_ALLOWED"] is False
