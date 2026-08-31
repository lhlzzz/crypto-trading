from __future__ import annotations

from unittest.mock import MagicMock

from datetime import datetime, timezone

from runtime_gate import REQUIRED_FUTURES_SOURCES, evaluate_runtime_gate


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
    client.get_position_mode.return_value = {"dualSidePosition": "false"}
    client.get_margin_type.return_value = {"marginType": "isolated"}
    client.get_leverage.return_value = {"leverage": "2"}
    client.get_positions.return_value = []
    client.get_open_orders.return_value = []
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


def test_gate_verification_timestamp() -> None:
    gate = evaluate_runtime_gate(
        mode="paper",
        store=RequiredFreshStore(),
        reconciliation_ok=True,
    )
    assert gate.verified_at is not None
    assert gate.verification_age_sec == 0
    assert gate.verification_source == "runtime_gate"


def test_testnet_gate_checks_real_account_state_and_leverage(monkeypatch) -> None:
    monkeypatch.setenv("DEFAULT_LEVERAGE", "2")
    monkeypatch.setenv("MAX_DATA_LATENCY_MS", "2000")
    gate = evaluate_runtime_gate(
        mode="testnet",
        client=_client(),
        symbols=["BTCUSDT"],
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
        client=_client(),
        symbols=["BTCUSDT"],
        data_health_ok=True,
        reconciliation_ok=True,
        probe_account=True,
    )
    passed = evaluate_runtime_gate(
        mode="testnet",
        client=_client(),
        symbols=["BTCUSDT"],
        data_health_ok=True,
        reconciliation_ok=True,
        probe_account=True,
        gate_evidence={"testnet": "PASSED"},
    )

    assert pending.testnet_ready is False
    assert passed.testnet_ready is True


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
