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
        evidence_status={"orderbook": "VALID"},
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
            "futures_mark_price",
            "futures_orderbook",
            "futures_book_ticker",
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
        evidence_status={"orderbook": "VALID"},
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


def test_paper_cycle_does_not_call_rest_klines(monkeypatch) -> None:
    from datetime import datetime, timezone
    from engine import StrategyEngine, StrategyConfig
    from risk import FuturesAccountSnapshot

    class Store:
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

        def record_system_event(self, **payload):
            self.events.append(payload)

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

    def explode(*args, **kwargs):
        raise AssertionError("paper cycle must not fetch REST klines")

    monkeypatch.setattr("scripts.bian_market.get_klines", explode)
    monkeypatch.setattr("binance_client.FuturesPublicClient.get_klines", explode)
    result = run_cycle(
        "BTCUSDT",
        store=Store(),
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


def test_paper_cycle_global_halt_when_observation_store_fails() -> None:
    class Store:
        def latest_market_observation(self, *args, **kwargs):
            raise RuntimeError("db down")

        def set_halt(self, halted, reason="", source=""):
            self.reason = reason
            self.halted = halted

    class UnusedExecutor:
        def account_snapshot(self):
            raise AssertionError("global halt must not reach the executor")

        def submit(self, *args, **kwargs):
            raise AssertionError("global halt must not submit")

    result = run_cycle(
        "BTCUSDT",
        store=Store(),
        executor=UnusedExecutor(),
        mode="paper",
    )
    assert result["status"] == "halted"
    assert result["reason"].startswith("GLOBAL_HALT")


def _testnet_gate(**overrides):
    from runtime_gate import GateResult

    payload = {
        "mode": "testnet",
        "positioning_enabled": False,
        "credentials_ok": True,
        "account_mode_ok": True,
        "margin_mode_ok": True,
        "data_health_ok": True,
        "reconciliation_ok": True,
        "risk_config_ok": True,
        "confirmation_ok": True,
        "kill_switch_ok": True,
        "live_allowed": False,
        "testnet_ready": True,
        "current_user_stream": "OK",
    }
    payload.update(overrides)
    return GateResult(**payload)


class _CycleStore:
    def __init__(self, stream_health: str = "OK") -> None:
        self.events = []
        self.risk_events = []
        self.halted = False
        self._user_stream_health = stream_health

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

    def record_risk_event(self, **fields):
        self.risk_events.append(fields)

    def record_intent(self, *args, **kwargs):
        raise AssertionError("OPEN must not be recorded when UserStream is unhealthy")

    def update_intent_status(self, *args, **kwargs):
        return None

    def user_stream_health(self) -> str:
        return self._user_stream_health

    def set_user_stream_health(self, status: str, *, reason: str | None = None) -> None:
        self._user_stream_health = status

    def is_halted(self) -> bool:
        return self.halted

    def set_halt(self, halted: bool, *, reason: str, source: str) -> None:
        self.halted = halted
        self.events.append({"halted": halted, "reason": reason, "source": source})


def _open_engine():
    from engine import StrategyConfig, StrategyEngine
    from trade_intent import TradeIntent
    from uuid import uuid4

    class Engine(StrategyEngine):
        def _intent_from_positioning(self, *args, **kwargs):
            return TradeIntent(
                symbol="BTCUSDT",
                direction="LONG",
                action="OPEN",
                reduce_only=False,
                leverage=Decimal("1"),
                order_type="MARKET",
                quantity=Decimal("0.001"),
                confidence=Decimal("1"),
                reason="test-open",
                strategy_version="test",
                evidence_snapshot_id=uuid4(),
            )

    return Engine(StrategyConfig(positioning_decision_enabled=True, legacy_execution_enabled=False))


def _reduce_engine():
    from engine import StrategyConfig, StrategyEngine
    from trade_intent import TradeIntent
    from uuid import uuid4

    class Engine(StrategyEngine):
        def _intent_from_positioning(self, *args, **kwargs):
            return TradeIntent(
                symbol="BTCUSDT",
                direction="LONG",
                action="CLOSE",
                reduce_only=True,
                leverage=Decimal("1"),
                order_type="MARKET",
                quantity=Decimal("0.001"),
                confidence=Decimal("1"),
                reason="test-close",
                strategy_version="test",
                evidence_snapshot_id=uuid4(),
            )

    return Engine(StrategyConfig(positioning_decision_enabled=True, legacy_execution_enabled=False))


def _account_executor():
    from datetime import datetime, timezone
    from risk import FuturesAccountSnapshot

    class Executor:
        submitted = []

        def account_snapshot(self):
            return FuturesAccountSnapshot(
                mode="testnet",
                wallet_balance=Decimal("1000"),
                available_balance=Decimal("1000"),
                total_margin=Decimal("1000"),
                used_margin=Decimal("0"),
                unrealized_pnl=Decimal("0"),
                realized_pnl=Decimal("0"),
                positions=(),
                open_orders=(),
                leverage={"BTCUSDT": Decimal("1")},
                margin_mode="ISOLATED",
                position_mode="ONE_WAY",
                captured_at=datetime.now(timezone.utc),
                source="test",
            )

        def submit(self, intent, decision, market=None):
            self.submitted.append(intent)
            raise AssertionError("submit must not run for blocked OPEN")

    return Executor()


def test_user_stream_failure_blocks_open() -> None:
    store = _CycleStore("FAILED")
    result = run_cycle(
        "BTCUSDT",
        store=store,
        engine=_open_engine(),
        executor=_account_executor(),
        mode="testnet",
        runtime_gate=_testnet_gate(current_user_stream="OK", testnet_ready=True),
    )
    assert result["status"] == "blocked"
    assert result["reason"] == "USER_STREAM_UNHEALTHY"
    assert store.risk_events[-1]["decision"] == "DENY"


def test_user_stream_degraded_blocks_open() -> None:
    store = _CycleStore("DEGRADED")
    result = run_cycle(
        "BTCUSDT",
        store=store,
        engine=_open_engine(),
        executor=_account_executor(),
        mode="testnet",
        runtime_gate=_testnet_gate(current_user_stream="OK"),
    )
    assert result["reason"] == "USER_STREAM_UNHEALTHY"


@pytest.mark.parametrize(
    "state",
    ["DISCONNECTED", "CONNECTING", "RECONNECTING", "UNKNOWN", "FAILED", "DEGRADED"],
)
def test_unhealthy_user_stream_states_cannot_use_stale_gate_to_open(state: str) -> None:
    store = _CycleStore(state)
    result = run_cycle(
        "BTCUSDT",
        store=store,
        engine=_open_engine(),
        executor=_account_executor(),
        mode="testnet",
        runtime_gate=_testnet_gate(current_user_stream="LIVE", testnet_ready=True),
    )
    assert result["status"] == "blocked"
    assert result["reason"] == "USER_STREAM_UNHEALTHY"


def test_reduce_close_remains_available_under_stream_failure() -> None:
    store = _CycleStore("FAILED")

    def record_intent(*args, **kwargs):
        return None

    store.record_intent = record_intent  # type: ignore[method-assign]
    executor = _account_executor()

    def submit(intent, decision, market=None):
        executor.submitted.append(intent)
        class Result:
            status = "SUBMITTED"
            order_id = "1"
            client_order_id = "c-1"
            executed_quantity = Decimal("0")
            executed_price = Decimal("0")
        return Result()

    executor.submit = submit  # type: ignore[method-assign]
    public_client = type(
        "Pub",
        (),
        {
            "get_symbol_rules": staticmethod(
                lambda symbol: {
                    "status": "TRADING",
                    "min_qty": "0.001",
                    "max_qty": "100",
                    "step_size": "0.001",
                    "tick_size": "0.01",
                    "min_notional": "5",
                }
            )
        },
    )()
    result = run_cycle(
        "BTCUSDT",
        store=store,
        engine=_reduce_engine(),
        risk_gate=type("Risk", (), {"evaluate": staticmethod(lambda *args, **kwargs: type("D", (), {"decision": "ALLOW"})())})(),
        executor=executor,
        public_client=public_client,
        mode="testnet",
        runtime_gate=_testnet_gate(current_user_stream="FAILED"),
    )
    assert result["status"] == "submitted"
    assert executor.submitted[0].action == "CLOSE"


def test_stream_recovery_requires_explicit_healthy_state() -> None:
    store = _CycleStore("RECONNECTING")
    blocked = run_cycle(
        "BTCUSDT",
        store=store,
        engine=_open_engine(),
        executor=_account_executor(),
        mode="testnet",
        runtime_gate=_testnet_gate(current_user_stream="OK"),
    )
    assert blocked["reason"] == "USER_STREAM_UNHEALTHY"
    store._user_stream_health = "LIVE"
    store.record_intent = lambda *args, **kwargs: None  # type: ignore[method-assign]
    executor = _account_executor()
    executor.submit = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("risk not reached"))
    # LIVE is healthy for OPEN; the cycle may continue into risk/execution.
    from runtime_gate import user_stream_allows_open
    assert user_stream_allows_open(store.user_stream_health()) is True


def test_user_stream_task_exception_halts_runtime(monkeypatch) -> None:
    import asyncio
    from paper_runner import _run_private_forever

    class Stream:
        state = "LIVE"

        def __init__(self, *args, **kwargs):
            self.on_halt = kwargs.get("on_halt")

        async def run_forever(self):
            self.state = "FAILED"
            if self.on_halt:
                self.on_halt("task boom")
            raise RuntimeError("task boom")

        async def close(self):
            return None

    store = _CycleStore("LIVE")
    monkeypatch.setattr("user_stream.UserStreamClient", Stream)

    async def run():
        with pytest.raises(RuntimeError, match="USER_STREAM FAILED"):
            await _run_private_forever(
                ["BTCUSDT"],
                mode="testnet",
                store=store,
                executor=type("E", (), {"client": None})(),
                public_client=object(),
                runtime_gate=_testnet_gate(),
            )

    asyncio.run(run())
    assert store.halted is True


def test_startup_gate_recomputes_after_reconciliation_success(monkeypatch) -> None:
    from dataclasses import replace
    from runtime_gate import GateResult

    calls = []

    def fake_gate(**kwargs):
        calls.append(kwargs)
        recon = bool(kwargs.get("reconciliation_ok"))
        return GateResult(
            mode="testnet",
            positioning_enabled=False,
            credentials_ok=True,
            account_mode_ok=True,
            margin_mode_ok=True,
            data_health_ok=True,
            reconciliation_ok=recon,
            risk_config_ok=True,
            confirmation_ok=True,
            kill_switch_ok=True,
            live_allowed=False,
            account_reachable=True,
            testnet_ready=recon,
        )

    class Recovered:
        safe_to_trade = True
        status = "SAFE"

    monkeypatch.setattr("paper_runner.evaluate_runtime_gate", fake_gate)
    monkeypatch.setattr("paper_runner.Reconciler", lambda *args, **kwargs: type("R", (), {"recover": lambda self: Recovered})())
    monkeypatch.setattr("binance_client.FuturesPrivateClient", lambda config: object())
    monkeypatch.setenv("BIAN_TESTNET_API_KEY", "k")
    monkeypatch.setenv("BIAN_TESTNET_API_SECRET", "s")
    monkeypatch.setenv("BIAN_TESTNET_SYMBOLS", "BTCUSDT")
    gate = _startup_recovery("testnet", StoreStub())
    assert calls[0].get("reconciliation_ok") in {None, False}
    assert calls[1]["reconciliation_ok"] is True
    assert gate.reconciliation_ok is True


def test_startup_reconciliation_failure_halts(monkeypatch) -> None:
    from runtime_gate import GateResult

    monkeypatch.setattr(
        "paper_runner.evaluate_runtime_gate",
        lambda **kwargs: GateResult(
            mode="testnet",
            positioning_enabled=False,
            credentials_ok=True,
            account_mode_ok=True,
            margin_mode_ok=True,
            data_health_ok=True,
            reconciliation_ok=False,
            risk_config_ok=True,
            confirmation_ok=True,
            kill_switch_ok=True,
            live_allowed=False,
            account_reachable=True,
        ),
    )
    monkeypatch.setattr(
        "paper_runner.Reconciler",
        lambda *args, **kwargs: type("R", (), {"recover": lambda self: type("X", (), {"safe_to_trade": False, "status": "HALT"})()})(),
    )
    monkeypatch.setattr("binance_client.FuturesPrivateClient", lambda config: object())
    monkeypatch.setenv("BIAN_TESTNET_API_KEY", "k")
    monkeypatch.setenv("BIAN_TESTNET_API_SECRET", "s")
    monkeypatch.setenv("BIAN_TESTNET_SYMBOLS", "BTCUSDT")
    with pytest.raises(SystemExit, match="startup reconciliation blocked"):
        _startup_recovery("testnet", StoreStub())
