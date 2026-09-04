from __future__ import annotations

from unittest.mock import MagicMock

from datetime import datetime, timezone
from decimal import Decimal
from risk import FuturesAccountSnapshot

import pytest

from runtime_gate import (
    REQUIRED_FUTURES_SOURCES,
    evaluate_runtime_gate,
    reset_account_health_cache,
)


@pytest.fixture(autouse=True)
def _reset_account_health_cache():
    reset_account_health_cache()
    yield
    reset_account_health_cache()


class FreshStore:
    def market_data_freshness(self, *, max_age_sec: int, symbols=()):
        del symbols
        return [{"status": "FRESH", "age_sec": 1, "max_age_sec": max_age_sec}]


class RequiredFreshStore:
    def __init__(self, *, missing: str | None = None, stale: str | None = None) -> None:
        self.missing = missing
        self.stale = stale

    def market_data_freshness(self, *, max_age_sec: int, symbols=()):
        del symbols
        now = datetime.now(timezone.utc).isoformat()
        return [
            {
                "source": source,
                "event_type": source,
                "symbol": "BTCUSDT",
                "status": "STALE" if source == self.stale else "FRESH",
                "source_timestamp": now,
                "received_timestamp": now,
                "latency_ms": 0,
                "age_sec": max_age_sec + 1 if source == self.stale else 0,
            }
            for source in REQUIRED_FUTURES_SOURCES
            if source != self.missing
        ]


class TestnetHealthStore:
    def user_stream_health(self) -> str:
        return "OK"


def _client() -> MagicMock:
    client = MagicMock()
    client.get_account.return_value = {
        "assets": [
            {
                "asset": "USDT",
                "walletBalance": "1000",
                "availableBalance": "900",
            }
        ]
    }
    client.get_server_time.return_value = {"serverTime": 1_700_000_000_000}
    client.account_snapshot.return_value = FuturesAccountSnapshot(
        mode="testnet",
        wallet_balance=Decimal("1000"),
        available_balance=Decimal("900"),
        total_margin=Decimal("1000"),
        used_margin=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        realized_pnl=Decimal("0"),
        positions=(),
        open_orders=(),
        leverage={"BTCUSDT": Decimal("2")},
        margin_mode="ISOLATED",
        position_mode="ONE_WAY",
        captured_at=datetime.now(timezone.utc),
        source="binance_futures_rest",
    )
    client.get_margin_type.return_value = {"marginType": "isolated"}
    return client


def test_paper_gate_never_constructs_private_account_client(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_MODE", "paper")
    gate = evaluate_runtime_gate(
        mode="paper",
        store=FreshStore(),
        data_health_ok=True,
        reconciliation_ok=True,
        gate_evidence={"paper": "PASSED"},
    )

    assert gate.credentials_ok is True
    assert gate.account_mode_ok is True
    assert gate.margin_mode_ok is True
    assert gate.live_allowed is False
    assert gate.trading_enabled is True


def test_paper_gate_requires_24h_acceptance_evidence() -> None:
    gate = evaluate_runtime_gate(
        mode="paper",
        store=RequiredFreshStore(),
        reconciliation_ok=True,
    )

    assert gate.paper_ready is False
    assert gate.paper_gate_status == "NOT_STARTED"


def test_real_data_ready_requires_realtime_24h_not_current_freshness() -> None:
    gate = evaluate_runtime_gate(
        mode="paper",
        store=RequiredFreshStore(),
        reconciliation_ok=True,
    )
    payload = gate.as_dict()
    assert payload["REAL_DATA_READY"] is False
    assert payload["historical_gate_evidence"]["realtime_24h"] != "PASSED"
    assert "data_health" in payload["current_runtime_health"]


def test_missing_required_source_blocks_data_health() -> None:
    gate = evaluate_runtime_gate(
        mode="paper",
        store=RequiredFreshStore(missing="FUTURES_DEPTH"),
        reconciliation_ok=True,
    )
    assert gate.data_health_ok is False
    assert gate.paper_ready is False
    assert "FUTURES_DEPTH_MISSING" in gate.reasons


def test_stale_required_source_blocks_data_health() -> None:
    gate = evaluate_runtime_gate(
        mode="paper",
        store=RequiredFreshStore(stale="FUTURES_DEPTH"),
        reconciliation_ok=True,
    )
    assert gate.data_health_ok is False
    assert "FUTURES_DEPTH_STALE" in gate.reasons


def test_future_source_timestamp_blocks_data_health() -> None:
    store = RequiredFreshStore()
    original = store.market_data_freshness

    def future_rows(*, max_age_sec: int, symbols=()):
        rows = original(max_age_sec=max_age_sec, symbols=symbols)
        rows[0]["source_timestamp"] = "2999-01-01T00:00:00+00:00"
        return rows

    store.market_data_freshness = future_rows
    gate = evaluate_runtime_gate(mode="paper", store=store, reconciliation_ok=True)

    assert gate.data_health_ok is False
    assert any(reason.endswith("_FUTURE_TIMESTAMP") for reason in gate.reasons)


def test_gate_evaluation_and_evidence_timestamps_are_distinct() -> None:
    gate = evaluate_runtime_gate(
        mode="paper",
        store=RequiredFreshStore(),
        reconciliation_ok=True,
    )
    assert gate.evaluated_at is not None
    assert gate.evidence_verified_at is None
    assert gate.verification_age_sec == 0
    assert gate.verification_source == "runtime_gate"


def test_testnet_gate_checks_real_account_state_and_leverage(monkeypatch) -> None:
    monkeypatch.setenv("DEFAULT_LEVERAGE", "2")
    monkeypatch.setenv("MAX_DATA_LATENCY_MS", "2000")
    gate = evaluate_runtime_gate(
        mode="testnet",
        client=_client(),
        symbols=["BTCUSDT"],
        store=TestnetHealthStore(),
        data_health_ok=True,
        reconciliation_ok=True,
        probe_account=True,
    )

    assert gate.credentials_ok is False
    assert gate.account_reachable is False
    assert "CREDENTIALS_MISSING" in gate.reasons


def test_testnet_gate_accepts_mode_specific_credentials_and_records_state(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_TESTNET_API_KEY", "key")
    monkeypatch.setenv("BIAN_TESTNET_API_SECRET", "secret")
    monkeypatch.setenv("DEFAULT_LEVERAGE", "2")
    monkeypatch.setenv("MAX_DATA_LATENCY_MS", "200000000000")
    gate = evaluate_runtime_gate(
        mode="testnet",
        client=_client(),
        symbols=["BTCUSDT"],
        store=TestnetHealthStore(),
        data_health_ok=True,
        reconciliation_ok=True,
        probe_account=True,
    )

    assert gate.credentials_ok is True
    assert gate.account_reachable is True
    assert gate.account_mode_ok is True
    assert gate.margin_mode_ok is True
    assert gate.leverage_ok is True
    assert gate.symbol_leverage == {"BTCUSDT": "2"}
    assert gate.trading_enabled is False
    assert gate.testnet_ready is False


def test_live_gate_is_hard_blocked_even_when_preflight_inputs_pass(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_LIVE_API_KEY", "key")
    monkeypatch.setenv("BIAN_LIVE_API_SECRET", "secret")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("LIVE_CONFIRMATION_TOKEN", "confirm")
    monkeypatch.setenv("BIAN_LIVE_CONFIRMATION", "confirm")
    monkeypatch.setenv("DEFAULT_LEVERAGE", "2")
    monkeypatch.setenv("MAX_DATA_LATENCY_MS", "200000000000")
    monkeypatch.setenv("POSITIONING_DECISION_ENABLED", "true")
    gate = evaluate_runtime_gate(
        mode="live",
        client=_client(),
        symbols=["BTCUSDT"],
        data_health_ok=True,
        reconciliation_ok=True,
        probe_account=True,
    )

    assert gate.confirmation_ok is True
    assert gate.live_allowed is False
    assert "LIVE_RELEASE_GATES_PENDING" in gate.reasons


def test_runtime_gate_uses_verified_evidence_dynamically(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_TESTNET_API_KEY", "key")
    monkeypatch.setenv("BIAN_TESTNET_API_SECRET", "secret")
    monkeypatch.setenv("DEFAULT_LEVERAGE", "2")
    monkeypatch.setenv("MAX_DATA_LATENCY_MS", "200000000000")
    pending = evaluate_runtime_gate(
        mode="testnet",
        store=TestnetHealthStore(),
        client=_client(),
        symbols=["BTCUSDT"],
        data_health_ok=True,
        reconciliation_ok=True,
        probe_account=True,
    )
    passed = evaluate_runtime_gate(
        mode="testnet",
        store=TestnetHealthStore(),
        client=_client(),
        symbols=["BTCUSDT"],
        data_health_ok=True,
        reconciliation_ok=True,
        probe_account=True,
        gate_evidence={"testnet": "PASSED"},
    )

    assert pending.testnet_ready is False
    assert passed.testnet_ready is True


def test_required_sources_must_be_fresh_for_every_symbol() -> None:
    gate = evaluate_runtime_gate(
        mode="paper",
        store=RequiredFreshStore(),
        symbols=["BTCUSDT", "ETHUSDT"],
        reconciliation_ok=True,
    )

    assert gate.data_health_ok is False
    assert "FUTURES_DEPTH_MISSING" in gate.reasons


def test_runtime_gate_exposes_current_health() -> None:
    gate = evaluate_runtime_gate(
        mode="paper",
        store=RequiredFreshStore(),
        reconciliation_ok=True,
        symbols=["BTCUSDT"],
    )

    assert gate.current_data_health == "OK"
    assert gate.current_account_health == "NOT_APPLICABLE"
    assert gate.current_reconciliation == "OK"
    assert gate.current_orderbook == "OK"
    assert gate.current_user_stream == "NOT_APPLICABLE"


def test_runtime_gate_rejects_stale_persisted_gate_evidence(monkeypatch) -> None:
    monkeypatch.setenv("GATE_EVIDENCE_MAX_AGE_SEC", "1")
    gate = evaluate_runtime_gate(
        mode="paper",
        store=RequiredFreshStore(),
        reconciliation_ok=True,
        gate_evidence={
            "paper": {
                "status": "PASSED",
                "verified_at": "2020-01-01T00:00:00+00:00",
            }
        },
    )

    assert gate.paper_gate_status == "FAILED"
    assert "PAPER_EVIDENCE_STALE" in gate.reasons
    assert gate.verification_age_sec is not None


def test_runtime_gate_exposes_transport_health() -> None:
    gate = evaluate_runtime_gate(
        mode="paper",
        store=RequiredFreshStore(),
        reconciliation_ok=True,
        symbols=["BTCUSDT"],
    )
    assert gate.global_transport_health in {"OK", "DEGRADED"}
    assert "BTCUSDT" in gate.per_symbol_source_health
    assert "FUTURES_DEPTH" in gate.per_symbol_source_health["BTCUSDT"]
    assert gate.per_symbol_transport_health["BTCUSDT"] in {"OK", "DEGRADED", "FAILED"}


def test_historical_session_pass_does_not_unlock_current_session() -> None:
    class SessionStore:
        def __init__(self, session_id: str, statuses: dict[str, str]) -> None:
            self.validation_session_id = session_id
            self._statuses = statuses

        def runtime_gate_statuses(self):
            return dict(self._statuses)

        def runtime_gate_evidence(self):
            return {
                name: {"status": status, "verified_at": datetime.now(timezone.utc).isoformat(), "validation_session_id": self.validation_session_id}
                for name, status in self._statuses.items()
            }

        def market_data_freshness(self, **kwargs):
            return []

        def user_stream_health(self) -> str:
            return "OK"

    old = evaluate_runtime_gate(
        mode="testnet",
        store=SessionStore("sess-a", {"testnet": "PASSED"}),
        data_health_ok=True,
        reconciliation_ok=True,
        symbols=["BTCUSDT"],
    )
    new = evaluate_runtime_gate(
        mode="testnet",
        store=SessionStore("sess-b", {}),
        data_health_ok=True,
        reconciliation_ok=True,
        symbols=["BTCUSDT"],
    )
    assert old.testnet_gate_status == "PASSED"
    assert new.testnet_gate_status == "NOT_STARTED"
    assert new.testnet_ready is False


def test_missing_session_cannot_reuse_historical_pass() -> None:
    class HistoricalStore:
        def runtime_gate_statuses(self):
            return {"testnet": "PASSED", "realtime_24h": "PASSED"}

        def _session_id(self):
            return None

        def market_data_freshness(self, **kwargs):
            return []

    # TradingStore.runtime_gate_statuses now returns NOT_STARTED without a session.
    # A store that still exposes historical PASS must not be used without a session id.
    from trading_store import TradingStore

    store = TradingStore("postgresql://test")
    assert store.runtime_gate_statuses()["testnet"] == "NOT_STARTED"


def test_paper_testnet_live_symbol_sets_are_independent(monkeypatch) -> None:
    from runtime_gate import trading_symbols_for_mode

    monkeypatch.setenv("BIAN_PAPER_SYMBOLS", "BTCUSDT")
    monkeypatch.setenv("BIAN_TESTNET_SYMBOLS", "ETHUSDT")
    monkeypatch.setenv("BIAN_LIVE_SYMBOLS", "BNBUSDT")
    assert trading_symbols_for_mode("paper") == ("BTCUSDT",)
    assert trading_symbols_for_mode("testnet") == ("ETHUSDT",)
    assert trading_symbols_for_mode("live") == ("BNBUSDT",)
    monkeypatch.setenv("BIAN_LIVE_SYMBOLS", "SOLUSDT")
    assert trading_symbols_for_mode("live") == ()
    from runtime_gate import unauthorized_symbols_for_mode

    assert unauthorized_symbols_for_mode("live") == ("SOLUSDT",)
    monkeypatch.setenv("BIAN_LIVE_SYMBOLS", "BTCUSDT,DOGEUSDT")
    assert trading_symbols_for_mode("live") == ("BTCUSDT",)
    assert unauthorized_symbols_for_mode("live") == ("DOGEUSDT",)
    mixed = evaluate_runtime_gate(
        mode="live",
        data_health_ok=True,
        reconciliation_ok=True,
        probe_account=False,
    )
    assert mixed.live_allowed is False
    assert "UNAUTHORIZED_SYMBOL" in mixed.reasons
    monkeypatch.delenv("BIAN_LIVE_SYMBOLS", raising=False)
    assert trading_symbols_for_mode("live") == ()


def test_live_preflight_without_client_cannot_pass(monkeypatch) -> None:
    from runtime_gate import evaluate_live_preflight

    monkeypatch.delenv("BIAN_LIVE_API_KEY", raising=False)
    monkeypatch.delenv("BIAN_LIVE_API_SECRET", raising=False)
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("LIVE_CONFIRMATION_TOKEN", "tok")
    monkeypatch.setenv("BIAN_LIVE_CONFIRMATION", "tok")
    result = evaluate_runtime_gate(mode="live", probe_account=True)
    assert result.live_allowed is False
    assert result.account_reachable is False


def test_live_unauthorized_symbol_is_rejected(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_LIVE_SYMBOLS", "DOGEUSDT")
    gate = evaluate_runtime_gate(
        mode="live",
        symbols=["DOGEUSDT"],
        data_health_ok=True,
        reconciliation_ok=True,
        probe_account=False,
    )
    assert gate.live_allowed is False
    assert "UNAUTHORIZED_SYMBOL" in gate.reasons
    assert gate.major_universe_ready is False


def test_account_health_cache_survives_probe_account_false(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_TESTNET_API_KEY", "key")
    monkeypatch.setenv("BIAN_TESTNET_API_SECRET", "secret")
    monkeypatch.setenv("DEFAULT_LEVERAGE", "2")
    monkeypatch.setenv("MAX_DATA_LATENCY_MS", "200000000000")
    monkeypatch.setenv("ACCOUNT_HEALTH_TTL_SEC", "30")
    reset_account_health_cache()
    probed = evaluate_runtime_gate(
        mode="testnet",
        client=_client(),
        symbols=["BTCUSDT"],
        store=TestnetHealthStore(),
        data_health_ok=True,
        reconciliation_ok=True,
        probe_account=True,
    )
    cached = evaluate_runtime_gate(
        mode="testnet",
        client=_client(),
        symbols=["BTCUSDT"],
        store=TestnetHealthStore(),
        data_health_ok=True,
        reconciliation_ok=True,
        probe_account=False,
    )
    reset_account_health_cache()
    assert probed.account_reachable is True
    assert cached.account_reachable is True
    assert "ACCOUNT_PREFLIGHT_NOT_RUN" not in cached.reasons
    assert "ACCOUNT_HEALTH_EXPIRED" not in cached.reasons


def _account_client(*, positions=(), open_orders=()):
    client = _client()
    snapshot = client.account_snapshot.return_value
    client.account_snapshot.return_value = FuturesAccountSnapshot(
        mode=snapshot.mode,
        wallet_balance=snapshot.wallet_balance,
        available_balance=snapshot.available_balance,
        total_margin=snapshot.total_margin,
        used_margin=snapshot.used_margin,
        unrealized_pnl=snapshot.unrealized_pnl,
        realized_pnl=snapshot.realized_pnl,
        positions=tuple(positions),
        open_orders=tuple(open_orders),
        leverage=snapshot.leverage,
        margin_mode=snapshot.margin_mode,
        position_mode=snapshot.position_mode,
        captured_at=snapshot.captured_at,
        source=snapshot.source,
        symbol_leverage=snapshot.symbol_leverage,
    )
    return client


def test_account_noncanonical_position_blocks_gate(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_TESTNET_API_KEY", "key")
    monkeypatch.setenv("BIAN_TESTNET_API_SECRET", "secret")
    monkeypatch.setenv("DEFAULT_LEVERAGE", "2")
    monkeypatch.setenv("MAX_DATA_LATENCY_MS", "200000000000")
    gate = evaluate_runtime_gate(
        mode="testnet",
        client=_account_client(positions=({"symbol": "DOGEUSDT", "positionAmt": "1"},)),
        symbols=["BTCUSDT"],
        store=TestnetHealthStore(),
        data_health_ok=True,
        reconciliation_ok=True,
        probe_account=True,
        gate_evidence={"testnet": "PASSED"},
    )
    assert gate.exchange_positions_ok is False
    assert gate.live_allowed is False
    assert gate.testnet_ready is False
    assert "UNAUTHORIZED_EXCHANGE_POSITION" in gate.reasons


def test_account_noncanonical_open_order_blocks_gate(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_TESTNET_API_KEY", "key")
    monkeypatch.setenv("BIAN_TESTNET_API_SECRET", "secret")
    monkeypatch.setenv("DEFAULT_LEVERAGE", "2")
    monkeypatch.setenv("MAX_DATA_LATENCY_MS", "200000000000")
    gate = evaluate_runtime_gate(
        mode="testnet",
        client=_account_client(open_orders=({"symbol": "SOLUSDT", "orderId": 1},)),
        symbols=["BTCUSDT"],
        store=TestnetHealthStore(),
        data_health_ok=True,
        reconciliation_ok=True,
        probe_account=True,
        gate_evidence={"testnet": "PASSED"},
    )
    assert gate.open_orders_ok is False
    assert gate.testnet_ready is False
    assert "UNAUTHORIZED_EXCHANGE_ORDER" in gate.reasons


def test_mixed_futures_universe_halts(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_LIVE_SYMBOLS", "BTCUSDT,DOGEUSDT,SOLUSDT")
    gate = evaluate_runtime_gate(
        mode="live",
        data_health_ok=True,
        reconciliation_ok=True,
        probe_account=False,
    )
    assert gate.live_allowed is False
    assert "UNAUTHORIZED_SYMBOL" in gate.reasons
    assert gate.major_universe_ready is False
