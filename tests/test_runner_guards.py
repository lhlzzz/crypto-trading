from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from engine import MarketFrame
from paper_runner import (
    _startup_recovery,
    _test_only_signal_injection,
    run_cycle,
    run_shadow_cycle,
)


def _frame() -> MarketFrame:
    from datetime import datetime, timezone
    from decimal import Decimal

    return MarketFrame(
        symbol="BTCUSDT",
        closes=(Decimal("100"), Decimal("101")),
        captured_at=datetime.now(timezone.utc),
    )


class StoreStub:
    def __init__(self, halted: bool = False) -> None:
        self.halted = halted
        self.initialized = False

    def initialize(self):
        self.initialized = True

    def is_halted(self):
        return self.halted

    def list_open_local_orders(self):
        return []


def test_paper_startup_recovery_allows_clean_store() -> None:
    store = StoreStub()
    _startup_recovery("paper", store)
    assert store.initialized is True


def test_live_startup_requires_explicit_confirmation(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_MARKET", "FUTURES")
    monkeypatch.setenv("POSITIONING_DECISION_ENABLED", "true")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("LIVE_CONFIRMATION_TOKEN", "secret-token")
    monkeypatch.delenv("BIAN_LIVE_CONFIRMATION", raising=False)

    with pytest.raises(SystemExit, match="confirmation token"):
        _startup_recovery("live", StoreStub())


def test_live_shell_guard_rejects_non_live_mode() -> None:
    script = Path(__file__).parents[1] / "start_live.sh"
    result = subprocess.run(
        ["bash", str(script)],
        env={**os.environ, "BIAN_MODE": "paper", "LIVE_TRADING_ENABLED": "false"},
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "BIAN_MODE" in result.stderr


def test_test_only_signal_injection_is_off_by_default(monkeypatch) -> None:
    monkeypatch.delenv("TEST_ONLY_SIGNAL_INJECTION", raising=False)

    assert _test_only_signal_injection(_frame(), mode="paper") is None


def test_test_only_signal_injection_still_creates_intent_in_paper(monkeypatch) -> None:
    monkeypatch.setenv("TEST_ONLY_SIGNAL_INJECTION", "true")
    monkeypatch.setenv("BIAN_ENVIRONMENT", "development")

    intent = _test_only_signal_injection(_frame(), mode="paper")

    assert intent is not None
    assert intent.reason == "TEST_ONLY_SIGNAL_INJECTION"
    assert intent.direction == "LONG"
    assert intent.action == "OPEN"


def test_test_only_signal_injection_is_hard_blocked_outside_paper(monkeypatch) -> None:
    monkeypatch.setenv("TEST_ONLY_SIGNAL_INJECTION", "true")

    with pytest.raises(RuntimeError, match="BIAN_MODE=paper"):
        _test_only_signal_injection(_frame(), mode="testnet")


def test_test_only_signal_injection_is_hard_blocked_in_production(monkeypatch) -> None:
    monkeypatch.setenv("TEST_ONLY_SIGNAL_INJECTION", "true")
    monkeypatch.setenv("BIAN_ENVIRONMENT", "production")

    with pytest.raises(RuntimeError, match="production"):
        _test_only_signal_injection(_frame(), mode="paper")


def test_no_signal_is_recorded_without_lowering_strategy_threshold(monkeypatch) -> None:
    from datetime import datetime, timezone

    class NoSignalStore:
        def __init__(self):
            self.events = []

        def record_system_event(self, **fields):
            self.events.append(fields)

    class NoSignalEngine:
        class Config:
            strategy_version = "test-no-signal"

        config = Config()

        def evaluate(self, frame):
            return None

    class UnusedExecutor:
        def submit(self, *args, **kwargs):
            raise AssertionError("no signal must not submit")

    monkeypatch.setattr(
        "paper_runner.bian_market.get_klines",
        lambda symbol, interval, limit: {
            "symbol": symbol,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "klines": [[0, 0, 0, 0, "100", 0, 0, "1000"]],
        },
    )
    store = NoSignalStore()
    result = run_cycle(
        "BTCUSDT",
        store=store,
        engine=NoSignalEngine(),
        executor=UnusedExecutor(),
        mode="paper",
    )

    assert result["status"] == "no_signal"
    assert store.events[0]["event_type"] == "NO_SIGNAL"


def test_shadow_cycle_records_decision_without_risk_or_execution(monkeypatch) -> None:
    from datetime import datetime, timezone

    class ShadowStore:
        def __init__(self):
            self.snapshots = []
            self.events = []

        def positioning_events(self, *args, **kwargs):
            return []

        def market_universe_context(self, *args, **kwargs):
            return {}

        def record_positioning_snapshot(self, decision, **kwargs):
            self.snapshots.append((decision, kwargs))

        def record_system_event(self, **fields):
            self.events.append(fields)

    captured_at = datetime(2026, 8, 26, 20, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(
        "paper_runner.bian_market.get_klines",
        lambda symbol, interval, limit: {
            "symbol": symbol,
            "captured_at": captured_at.isoformat(),
            "source_timestamp": captured_at.isoformat(),
            "received_timestamp": captured_at.isoformat(),
            "klines": [[0, 0, 0, 0, "100", 0, 0, "1000"]],
        },
    )
    store = ShadowStore()

    result = run_shadow_cycle("BTCUSDT", store=store)

    assert result["status"] == "shadow_recorded"
    assert result["positioning_decision"] == "FLAT"
    assert len(store.snapshots) == 1
    assert store.events[0]["event_type"] == "POSITIONING_SHADOW_DECISION"


def test_shadow_cycle_recovers_the_persisted_predecessor_state(monkeypatch) -> None:
    from datetime import datetime, timezone

    class ShadowStore:
        def __init__(self):
            self.snapshots = []
            self.events = []

        def positioning_events(self, *args, **kwargs):
            return []

        def market_universe_context(self, *args, **kwargs):
            return {}

        def latest_positioning_state(self, symbol, *, before):
            assert symbol == "BTCUSDT"
            assert before == captured_at
            return "SHORT_COVERING"

        def record_positioning_snapshot(self, decision, **kwargs):
            self.snapshots.append((decision, kwargs))

        def record_system_event(self, **fields):
            self.events.append(fields)

    captured_at = datetime(2026, 8, 26, 20, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(
        "paper_runner.bian_market.get_klines",
        lambda symbol, interval, limit: {
            "symbol": symbol,
            "captured_at": captured_at.isoformat(),
            "source_timestamp": captured_at.isoformat(),
            "received_timestamp": captured_at.isoformat(),
            "klines": [[0, 0, 0, 0, "100", 0, 0, "1000"]],
        },
    )
    store = ShadowStore()

    run_shadow_cycle("BTCUSDT", store=store)

    decision = store.snapshots[0][0]
    assert decision.previous_state == "SHORT_COVERING"
    assert decision.transition == "SHORT_COVERING->UNKNOWN"
