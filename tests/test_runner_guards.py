from __future__ import annotations

import os
import subprocess
from pathlib import Path
from decimal import Decimal

import pytest

from engine import MarketFrame
from paper_runner import (
    _startup_recovery,
    _risk_context,
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
        meme_risk_tier="TRADEABLE",
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


def test_non_paper_cycle_requires_canonical_runtime_gate() -> None:
    with pytest.raises(RuntimeError, match="canonical runtime gate"):
        run_cycle("BTCUSDT", mode="testnet")


def test_paper_risk_context_uses_symbol_aware_model_liquidation_price() -> None:
    from decimal import Decimal
    from execution import ExecutionConfig, MarketSnapshot, PaperExecutor
    from trade_intent import TradeIntent

    class ContextStore:
        def __init__(self):
            self.balances = {}
            self.positions = {}

        def initialize(self):
            return None

        def is_halted(self):
            return False

        def get_balance(self, asset):
            return self.balances.get(asset)

        def upsert_balance(self, asset, **fields):
            self.balances[asset] = {"asset": asset, **fields}

        def get_position(self, symbol):
            return self.positions.get(symbol)

    store = ContextStore()
    executor = PaperExecutor(store=store, config=ExecutionConfig(mode="paper"))
    intent = TradeIntent(
        symbol="BTCUSDT",
        direction="LONG",
        action="OPEN",
        reduce_only=False,
        leverage=Decimal("2"),
        order_type="MARKET",
        quantity=Decimal("1"),
        confidence=Decimal("1"),
        reason="test",
        strategy_version="test",
    )
    context = _risk_context(
        intent,
        MarketSnapshot(last_price=Decimal("100"), mark_price=Decimal("100")),
        account_snapshot=executor.account_snapshot(),
        mode="paper",
        paper_executor=executor,
    )

    assert context.liquidation_price is not None
    assert context.liquidation_distance_percent is not None


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


def test_no_signal_is_recorded_without_lowering_strategy_threshold() -> None:
    from datetime import datetime, timezone
    from risk import FuturesAccountSnapshot
    from engine import StrategyConfig, StrategyEngine

    class NoSignalStore:
        def __init__(self):
            self.events = []

        def latest_market_observation(self, *args, **kwargs):
            return _frame()

        def latest_positioning_state(self, *args, **kwargs):
            return None

        def get_active_episode(self, *args, **kwargs):
            return None

        def record_positioning_snapshot(self, *args, **kwargs):
            return None

        def record_system_event(self, **fields):
            self.events.append(fields)

    class UnusedExecutor:
        def account_snapshot(self):
            return FuturesAccountSnapshot(
                mode="paper",
                wallet_balance=Decimal("1000"),
                available_balance=Decimal("1000"),
                total_margin=Decimal("1000"),
                used_margin=Decimal("0"),
                unrealized_pnl=Decimal("0"),
                realized_pnl=Decimal("0"),
                positions=(),
                open_orders=(),
                leverage={},
                margin_mode="ISOLATED",
                position_mode="ONE_WAY",
                captured_at=datetime.now(timezone.utc),
                source="test",
            )

        def submit(self, *args, **kwargs):
            raise AssertionError("no signal must not submit")

    store = NoSignalStore()
    result = run_cycle(
        "BTCUSDT",
        store=store,
        engine=StrategyEngine(
            StrategyConfig(
                positioning_decision_enabled=False,
                legacy_execution_enabled=False,
            )
        ),
        executor=UnusedExecutor(),
        mode="paper",
    )

    assert result["status"] == "no_signal"
    assert store.events[-1]["event_type"] == "NO_SIGNAL"


def test_disabled_positioning_does_not_open_from_sufficient_evidence() -> None:
    from datetime import datetime, timezone
    from engine import SourceFreshness, StrategyConfig, StrategyEngine
    from risk import FuturesAccountSnapshot

    captured = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
    source_timestamps = {
        source: {
            "source_timestamp": captured.isoformat(),
            "received_timestamp": captured.isoformat(),
            "latency_ms": 0,
        }
        for source in (
            "futures_open_interest",
            "futures_funding",
            "futures_trade_flow",
            "futures_taker_ratio",
        )
    }
    frame = MarketFrame(
        symbol="BTCUSDT",
        closes=(Decimal("100"), Decimal("101")),
        captured_at=captured,
        futures_trade_flow=Decimal("8"),
        cvd_change=Decimal("8"),
        taker_buy_volume=Decimal("10"),
        taker_sell_volume=Decimal("3"),
        oi_change=Decimal("0.03"),
        funding_rate=Decimal("0.0001"),
        spread_bps=Decimal("2"),
        depth_25bps=Decimal("100"),
        market_regime="RISK_ON",
        meme_risk_tier="TRADEABLE",
        is_meme=True,
        freshness=(SourceFreshness("futures_trade_flow", captured, captured, 900, captured),),
        source_timestamps=source_timestamps,
    )

    class Store:
        def __init__(self):
            self.events = []

        def latest_market_observation(self, *args, **kwargs):
            return frame

        def latest_positioning_state(self, *args, **kwargs):
            return None

        def get_active_episode(self, *args, **kwargs):
            return None

        def start_episode(self, **kwargs):
            return {
                "episode_id": "00000000-0000-0000-0000-000000000001",
                "direction": kwargs["direction"],
                "status": "OPEN",
                "started_at": captured,
            }

        def record_positioning_snapshot(self, *args, **kwargs):
            return None

        def record_system_event(self, **fields):
            self.events.append(fields)

    class UnusedExecutor:
        def account_snapshot(self):
            return FuturesAccountSnapshot(
                mode="paper",
                wallet_balance=Decimal("1000"),
                available_balance=Decimal("1000"),
                total_margin=Decimal("1000"),
                used_margin=Decimal("0"),
                unrealized_pnl=Decimal("0"),
                realized_pnl=Decimal("0"),
                positions=(),
                open_orders=(),
                leverage={},
                margin_mode="ISOLATED",
                position_mode="ONE_WAY",
                captured_at=captured,
                source="test",
            )

        def submit(self, *args, **kwargs):
            raise AssertionError("disabled positioning must not submit")

    store = Store()
    result = run_cycle(
        "BTCUSDT",
        store=store,
        engine=StrategyEngine(
            StrategyConfig(
                positioning_decision_enabled=False,
                legacy_execution_enabled=True,
            )
        ),
        executor=UnusedExecutor(),
        mode="paper",
    )

    assert result["status"] == "no_signal"
    assert store.events[-1]["event_type"] == "NO_SIGNAL"


def test_persisted_episode_survives_restart_and_unresolved_states() -> None:
    from dataclasses import replace
    from datetime import datetime, timezone
    from uuid import uuid4

    from engine import StrategyEngine
    from paper_runner import _with_persisted_episode

    episode_id = uuid4()
    started_at = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)

    class EpisodeStore:
        def __init__(self) -> None:
            self.episode = {
                "episode_id": episode_id,
                "symbol": "BTCUSDT",
                "market": "FUTURES",
                "direction": "LONG",
                "started_at": started_at,
                "ended_at": None,
                "state": "LONG_BUILDING",
                "status": "OPEN",
                "last_observed_at": started_at,
                "strategy_version": "positioning-v1",
                "config_hash": "hash",
                "metadata": {},
            }

        def get_active_episode(self, *args, **kwargs):
            return self.episode

        def update_episode(self, *args, state, status, observed_at, metadata, **kwargs):
            self.episode.update(
                state=state,
                status=status,
                last_observed_at=observed_at,
                metadata=metadata,
            )
            return self.episode

    engine = StrategyEngine()
    decision = engine.positioning_decision(_frame())
    store = EpisodeStore()

    directional = replace(decision, state="LONG_BUILDING")
    restarted = _with_persisted_episode(directional, store=store, engine=engine)
    unknown = _with_persisted_episode(
        replace(decision, state="UNKNOWN"),
        store=store,
        engine=engine,
    )
    conflicted = _with_persisted_episode(
        replace(decision, state="CONFLICTED"),
        store=store,
        engine=engine,
    )

    assert restarted.episode_id == episode_id
    assert restarted.episode_status == "OPEN"
    assert unknown.episode_id == episode_id
    assert unknown.episode_status == "UNRESOLVED"
    assert conflicted.episode_id == episode_id
    assert conflicted.episode_status == "UNRESOLVED"


def test_shadow_cycle_records_decision_without_risk_or_execution() -> None:

    class ShadowStore:
        def __init__(self):
            self.snapshots = []
            self.events = []

        def latest_market_observation(self, *args, **kwargs):
            return _frame()

        def latest_positioning_state(self, *args, **kwargs):
            return None

        def get_active_episode(self, *args, **kwargs):
            return None

        def record_positioning_snapshot(self, decision, **kwargs):
            self.snapshots.append((decision, kwargs))

        def record_system_event(self, **fields):
            self.events.append(fields)

    store = ShadowStore()

    result = run_shadow_cycle("BTCUSDT", store=store)

    assert result["status"] == "shadow_recorded"
    assert result["positioning_decision"] == "FLAT"
    assert len(store.snapshots) == 1
    assert store.events[0]["event_type"] == "POSITIONING_SHADOW_DECISION"


def test_shadow_cycle_recovers_the_persisted_predecessor_state() -> None:
    from datetime import datetime, timezone

    class ShadowStore:
        def __init__(self):
            self.snapshots = []
            self.events = []

        def latest_market_observation(self, *args, **kwargs):
            return frame

        def latest_positioning_state(self, symbol, *, before):
            assert symbol == "BTCUSDT"
            assert before == frame.captured_at
            return "SHORT_COVERING"

        def get_active_episode(self, *args, **kwargs):
            return None

        def record_positioning_snapshot(self, decision, **kwargs):
            self.snapshots.append((decision, kwargs))

        def record_system_event(self, **fields):
            self.events.append(fields)

    frame = _frame()
    store = ShadowStore()

    run_shadow_cycle("BTCUSDT", store=store)

    decision = store.snapshots[0][0]
    assert decision.previous_state == "SHORT_COVERING"
    assert decision.transition == "SHORT_COVERING->UNKNOWN"
