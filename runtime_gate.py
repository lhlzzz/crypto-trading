"""Canonical runtime readiness gate for paper, Testnet, and Live.

This module only evaluates whether a runtime may proceed. It never creates a
TradeIntent, calls an executor, or mutates Binance account configuration.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping
import os

from binance_client import ClientConfig
from engine import runtime_required_sources, validate_source_timestamps
from risk import FuturesAccountSnapshot, RiskLimits


def _enabled(name: str, *, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _mode() -> str:
    return os.environ.get("BIAN_MODE", "paper").strip().lower()


def _symbols(symbols: Iterable[str] | None) -> tuple[str, ...]:
    configured = symbols or os.environ.get("BIAN_PAPER_SYMBOLS", "BTCUSDT").split(",")
    return tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in configured if str(symbol).strip()))


def _expected_position_mode() -> str:
    return os.environ.get("FUTURES_POSITION_MODE", "ONE_WAY").strip().upper()


def _expected_margin_mode() -> str:
    return os.environ.get("FUTURES_MARGIN_MODE", "ISOLATED").strip().upper()


def max_data_age_sec() -> int:
    """Canonical freshness budget shared by observation, runtime, and API."""
    try:
        return max(1, int(os.environ.get("BIAN_MAX_DATA_AGE_SEC", "900")))
    except ValueError:
        return 900


def gate_evidence_max_age_sec() -> int:
    """Bound how long an externally verified gate may be reused."""
    try:
        return max(1, int(os.environ.get("GATE_EVIDENCE_MAX_AGE_SEC", "86400")))
    except ValueError:
        return 86400


_GATE_STATUSES = {"NOT_STARTED", "RUNNING", "PASSED", "FAILED"}
_EXTERNAL_GATES = ("observation", "paper", "shadow", "testnet")
REQUIRED_FUTURES_SOURCES = runtime_required_sources()
_REALTIME_FUTURES_SOURCES = frozenset(
    {
        "FUTURES_TRADE",
        "FUTURES_BOOK_TICKER",
        "FUTURES_DEPTH",
        "FUTURES_MARK_PRICE",
        "FUTURES_INDEX_PRICE",
        "FUTURES_FUNDING_LIVENESS",
        "FUTURES_LIQUIDATION_LIVENESS",
    }
)


def _persisted_gate_statuses(
    store: Any | None,
    supplied: Mapping[str, str] | None,
) -> dict[str, str]:
    """Read verified evidence; configuration alone is never sufficient."""
    values = {name: "NOT_STARTED" for name in _EXTERNAL_GATES}
    if supplied is not None:
        values.update({
            key: str(value.get("status", "NOT_STARTED")).upper()
            if isinstance(value, Mapping)
            else str(value).upper()
            for key, value in supplied.items()
        })
    getter = getattr(store, "runtime_gate_statuses", None) if store is not None else None
    if getter is None:
        getter = getattr(store, "get_runtime_gate_statuses", None) if store is not None else None
    if getter is not None:
        try:
            persisted = getter()
            if isinstance(persisted, Mapping):
                values.update({
                    key: str(value.get("status", "NOT_STARTED")).upper()
                    if isinstance(value, Mapping)
                    else str(value).upper()
                    for key, value in persisted.items()
                })
        except Exception:
            return {name: "FAILED" for name in _EXTERNAL_GATES}
    for name in _EXTERNAL_GATES:
        if values[name] not in _GATE_STATUSES:
            values[name] = "FAILED"
    return values


def _data_health_report(
    store: Any | None,
    *,
    max_age_sec: int,
    symbols: Iterable[str],
) -> tuple[bool, tuple[str, ...]]:
    """Consume canonical normalized source health for every active symbol."""
    if store is None or not hasattr(store, "market_data_freshness"):
        return False, ("DATA_HEALTH_NOT_AVAILABLE",)
    try:
        rows = store.market_data_freshness(max_age_sec=max_age_sec, symbols=symbols)
    except Exception as exc:
        return False, (f"DATA_HEALTH_ERROR:{type(exc).__name__}",)
    now = datetime.now(timezone.utc)
    reasons: list[str] = []
    expected_symbols = {
        str(symbol).strip().upper()
        for symbol in symbols
        if str(symbol).strip()
    }
    max_latency_ms = int(os.environ.get("MAX_DATA_LATENCY_MS", "2000"))
    for source in sorted(REQUIRED_FUTURES_SOURCES):
        source_rows = [
            row for row in rows
            if str(row.get("source") or row.get("event_type") or "").upper() == source
        ]
        target_rows = (
            [
                row
                for symbol in sorted(expected_symbols)
                for row in [
                    next(
                        (
                            candidate for candidate in source_rows
                            if str(candidate.get("symbol") or "").strip().upper() == symbol
                        ),
                        None,
                    )
                ]
                if row is not None
            ]
            if expected_symbols
            else source_rows
        )
        if expected_symbols:
            present_symbols = {
                str(row.get("symbol") or "").strip().upper() for row in target_rows
            }
            for symbol in sorted(expected_symbols - present_symbols):
                reasons.append(f"{source}_MISSING")
        for row in target_rows:
            if not str(row.get("symbol", "")).strip():
                reasons.append(f"{source}_MISSING")
                continue
            status = str(row.get("status", "MISSING")).upper()
            if status != "FRESH":
                reasons.append(f"{source}_{status}")
                continue
            validation = validate_source_timestamps(
                {
                    source.lower(): {
                        "source_timestamp": row.get("source_timestamp"),
                        "received_timestamp": row.get("received_timestamp"),
                        "latency_ms": row.get("latency_ms"),
                        "max_age_sec": max_age_sec,
                    }
                },
                now,
                required_core_sources=frozenset(),
            )
            if validation.status_for(source) != "AVAILABLE":
                issue = validation.issues.get(source.lower(), "ERROR")
                reasons.append(f"{source}_{issue}")
                continue
            if (
                source in _REALTIME_FUTURES_SOURCES
                and int(row["latency_ms"]) > max_latency_ms
            ):
                reasons.append(f"{source}_LATENCY_EXCEEDED")
    unique = tuple(dict.fromkeys(reasons))
    return not unique, unique


@dataclass(frozen=True)
class GateResult:
    """Single canonical answer to runtime readiness questions."""

    mode: str
    positioning_enabled: bool
    credentials_ok: bool
    account_mode_ok: bool
    margin_mode_ok: bool
    data_health_ok: bool
    reconciliation_ok: bool
    risk_config_ok: bool
    confirmation_ok: bool
    kill_switch_ok: bool
    live_allowed: bool
    reasons: tuple[str, ...] = field(default_factory=tuple)
    symbol_leverage: dict[str, str] = field(default_factory=dict)
    account_reachable: bool = False
    exchange_positions_ok: bool = False
    open_orders_ok: bool = False
    wallet_balance_ok: bool = False
    available_balance_ok: bool = False
    server_time_ok: bool = False
    leverage_ok: bool = False
    observation_gates_ok: bool = False
    testnet_lifecycle_ok: bool = False
    paper_ready: bool = False
    testnet_ready: bool = False
    live_ready: bool = False
    observation_gate_status: str = "NOT_STARTED"
    paper_gate_status: str = "NOT_STARTED"
    shadow_gate_status: str = "NOT_STARTED"
    testnet_gate_status: str = "NOT_STARTED"
    paper_db_ok: bool = False
    paper_accounting_ok: bool = False
    alpha_gate_status: str = "INSUFFICIENT_SAMPLE"
    meme_universe_ready: bool = False
    evaluated_at: str | None = None
    evidence_verified_at: str | None = None
    verification_age_sec: int | None = None
    verification_source: str = "runtime_gate"
    current_data_health: str = "BLOCKED"
    current_account_health: str = "UNKNOWN"
    current_reconciliation: str = "BLOCKED"
    current_orderbook: str = "UNKNOWN"
    current_user_stream: str = "UNKNOWN"

    @property
    def trading_enabled(self) -> bool:
        if self.mode == "paper":
            return self.paper_ready
        if self.mode == "testnet":
            return self.testnet_ready
        return self.live_allowed

    @property
    def verified_at(self) -> str | None:
        """Compatibility read for consumers that only expose evidence time."""
        return self.evidence_verified_at

    @property
    def _base_ready(self) -> bool:
        return all(
            (
                self.positioning_enabled if self.mode == "live" else True,
                self.credentials_ok,
                self.account_reachable,
                self.account_mode_ok,
                self.margin_mode_ok,
                self.exchange_positions_ok,
                self.open_orders_ok,
                self.wallet_balance_ok,
                self.available_balance_ok,
                self.server_time_ok,
                self.leverage_ok,
                self.data_health_ok,
                self.reconciliation_ok,
                self.risk_config_ok,
                self.confirmation_ok,
                self.kill_switch_ok,
            )
        )

    @property
    def data_health(self) -> str:
        return "OK" if self.data_health_ok else "BLOCKED"

    @property
    def reconciliation(self) -> str:
        return "OK" if self.reconciliation_ok else "BLOCKED"

    @property
    def risk_status(self) -> str:
        return "SAFE" if self.risk_config_ok and self.kill_switch_ok else "HALT"

    @property
    def account_mode(self) -> str:
        return "ONE_WAY" if self.account_mode_ok else "UNKNOWN"

    @property
    def margin_mode(self) -> str:
        return "ISOLATED" if self.margin_mode_ok else "UNKNOWN"

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "trading_enabled": self.trading_enabled,
            "positioning_enabled": self.positioning_enabled,
            "credentials": self.credentials_ok,
            "credentials_ok": self.credentials_ok,
            "account_reachable": self.account_reachable,
            "account_mode": self.account_mode,
            "account_mode_ok": self.account_mode_ok,
            "margin_mode": self.margin_mode,
            "margin_mode_ok": self.margin_mode_ok,
            "symbol_leverage": dict(self.symbol_leverage),
            "exchange_positions_ok": self.exchange_positions_ok,
            "open_orders_ok": self.open_orders_ok,
            "wallet_balance_ok": self.wallet_balance_ok,
            "available_balance_ok": self.available_balance_ok,
            "server_time_ok": self.server_time_ok,
            "leverage_ok": self.leverage_ok,
            "data_health": self.data_health,
            "data_health_ok": self.data_health_ok,
            "reconciliation": self.reconciliation,
            "reconciliation_ok": self.reconciliation_ok,
            "risk_status": self.risk_status,
            "risk_config_ok": self.risk_config_ok,
            "confirmation_ok": self.confirmation_ok,
            "live_confirmation": self.confirmation_ok,
            "kill_switch_ok": self.kill_switch_ok,
            "observation_gates_ok": self.observation_gates_ok,
            "testnet_lifecycle_ok": self.testnet_lifecycle_ok,
            "paper_ready": self.paper_ready,
            "testnet_ready": self.testnet_ready,
            "live_ready": self.live_ready,
            "observation_gate_status": self.observation_gate_status,
            "paper_gate_status": self.paper_gate_status,
            "shadow_gate_status": self.shadow_gate_status,
            "testnet_gate_status": self.testnet_gate_status,
            "paper_db_ok": self.paper_db_ok,
            "paper_accounting_ok": self.paper_accounting_ok,
            "alpha_gate_status": self.alpha_gate_status,
            "meme_universe_ready": self.meme_universe_ready,
            "evaluated_at": self.evaluated_at,
            "evidence_verified_at": self.evidence_verified_at,
            "verification_age_sec": self.verification_age_sec,
            "verification_source": self.verification_source,
            "current_data_health": self.current_data_health,
            "current_account_health": self.current_account_health,
            "current_reconciliation": self.current_reconciliation,
            "current_orderbook": self.current_orderbook,
            "current_user_stream": self.current_user_stream,
            "live_allowed": self.live_allowed,
            "CODE_READY": True,
            "REAL_DATA_READY": self.observation_gate_status == "PASSED",
            "PAPER_READY": self.paper_ready,
            "SHADOW_READY": self.shadow_gate_status == "PASSED",
            "TESTNET_READY": self.testnet_ready,
            "ALPHA_STATUS": self.alpha_gate_status,
            "LIVE_PREFLIGHT": self.live_ready,
            "LIVE_ALLOWED": self.live_allowed,
            "orderbook_health": self.current_orderbook,
            "account_health": self.current_account_health,
            "user_stream": self.current_user_stream,
            "risk": self.risk_status,
            "meme_universe": self.meme_universe_ready,
            "reasons": list(self.reasons),
        }


def _data_health(
    store: Any | None,
    *,
    max_age_sec: int,
    symbols: Iterable[str],
) -> bool:
    ok, _ = _data_health_report(
        store, max_age_sec=max_age_sec, symbols=symbols
    )
    return ok


def _current_user_stream_health(store: Any | None, *, mode: str) -> str:
    if mode == "paper":
        return "NOT_APPLICABLE"
    for name in ("user_stream_health", "user_stream_status"):
        getter = getattr(store, name, None) if store is not None else None
        if getter is None:
            continue
        try:
            value = getter()
        except Exception:
            return "ERROR"
        if isinstance(value, Mapping):
            value = value.get("status", value.get("health", "UNKNOWN"))
        status = str(value).upper()
        return status if status else "UNKNOWN"
    return "UNKNOWN"


def _risk_config_ok() -> bool:
    try:
        limits = RiskLimits.from_env()
    except Exception:
        return False
    return (
        limits.max_leverage > 0
        and limits.max_margin_usdt > 0
        and limits.max_order_usdt > 0
        and limits.max_position_usdt > 0
    )


def evaluate_runtime_gate(
    *,
    mode: str | None = None,
    store: Any | None = None,
    client: Any | None = None,
    symbols: Iterable[str] | None = None,
    reconciliation_ok: bool | None = None,
    data_health_ok: bool | None = None,
    probe_account: bool = False,
    gate_evidence: Mapping[str, str] | None = None,
    alpha_gate_status: str | None = None,
    meme_universe_ready: bool | None = None,
) -> GateResult:
    """Evaluate the canonical runtime gate from configuration and evidence."""
    resolved_mode = (mode or _mode()).strip().lower()
    if resolved_mode not in {"paper", "testnet", "live"}:
        return GateResult(
            mode=resolved_mode,
            positioning_enabled=False,
            credentials_ok=False,
            account_mode_ok=False,
            margin_mode_ok=False,
            data_health_ok=False,
            reconciliation_ok=False,
            risk_config_ok=False,
            confirmation_ok=False,
            kill_switch_ok=False,
            live_allowed=False,
            reasons=("INVALID_MODE",),
        )

    config = ClientConfig.from_env(resolved_mode)
    selected_symbols = _symbols(symbols)
    reasons: list[str] = []
    positioning_enabled = _enabled("POSITIONING_DECISION_ENABLED")
    alpha_status = str(
        alpha_gate_status or os.environ.get("ALPHA_GATE_STATUS", "INSUFFICIENT_SAMPLE")
    ).upper()
    if alpha_status not in {"INSUFFICIENT_SAMPLE", "ALPHA_NOT_SUPPORTED", "ALPHA_SUPPORTED"}:
        alpha_status = "ALPHA_NOT_SUPPORTED"
    meme_ready = (
        _enabled("MEME_UNIVERSE_READY")
        if meme_universe_ready is None
        else bool(meme_universe_ready)
    )
    if resolved_mode == "live" and not meme_ready:
        reasons.append("MEME_UNIVERSE_NOT_READY")
    credentials_ok = resolved_mode == "paper" or bool(config.api_key and config.api_secret)
    # These fields are Binance account evidence. Paper must not present local
    # state as a verified exchange account.
    account_reachable = False
    # Paper can validate its local execution configuration, but these are not
    # exchange-account observations. ``account_reachable`` and the account
    # balance/position probes remain false in paper mode.
    account_mode_ok = resolved_mode == "paper"
    margin_mode_ok = resolved_mode == "paper"
    exchange_positions_ok = False
    open_orders_ok = False
    wallet_balance_ok = False
    available_balance_ok = False
    server_time_ok = False
    leverage_ok = False
    symbol_leverage: dict[str, str] = {}

    if resolved_mode != "paper" and not credentials_ok:
        reasons.append("CREDENTIALS_MISSING")
    if resolved_mode != "paper" and client is not None and probe_account and credentials_ok:
        try:
            snapshot_getter = getattr(client, "account_snapshot", None)
            if snapshot_getter is None:
                raise RuntimeError("FuturesAccountSnapshot is unavailable")
            snapshot = snapshot_getter()
            if not isinstance(snapshot, FuturesAccountSnapshot):
                raise RuntimeError("invalid FuturesAccountSnapshot")
            account_reachable = snapshot.is_fresh(
                now=datetime.now(timezone.utc),
                max_age_sec=int(os.environ.get("ACCOUNT_SNAPSHOT_MAX_AGE_SEC", "30")),
            ) and snapshot.mode == resolved_mode
            wallet_balance_ok = account_reachable and snapshot.wallet_balance >= 0
            available_balance_ok = (
                account_reachable
                and snapshot.available_balance >= 0
                and snapshot.available_balance <= snapshot.wallet_balance
            )
            server_time = client.get_server_time()
            server_ms = int(server_time.get("serverTime"))
            local_ms = int(__import__("time").time() * 1000)
            server_time_ok = abs(server_ms - local_ms) <= int(
                os.environ.get("MAX_DATA_LATENCY_MS", "2000")
            )
            account_mode_ok = snapshot.position_mode == _expected_position_mode()
            margin_mode_ok = bool(selected_symbols)
            for symbol in selected_symbols:
                margin = client.get_margin_type(symbol)
                actual_margin = str(margin.get("marginType", "")).upper()
                snapshot_margin = snapshot.margin_mode.upper()
                if actual_margin != _expected_margin_mode() or (
                    snapshot_margin != "UNKNOWN"
                    and snapshot_margin != _expected_margin_mode()
                ):
                    margin_mode_ok = False
                configured = snapshot.symbol_leverage or snapshot.leverage
                symbol_leverage[symbol] = str(configured.get(symbol, ""))
            expected_leverage = os.environ.get("DEFAULT_LEVERAGE", "1")
            leverage_ok = bool(symbol_leverage) and all(
                value == expected_leverage for value in symbol_leverage.values()
            )
            margin_mode_ok = bool(selected_symbols) and margin_mode_ok
            exchange_positions_ok = isinstance(snapshot.positions, tuple)
            open_orders_ok = isinstance(snapshot.open_orders, tuple)
        except Exception as exc:
            reasons.append(f"ACCOUNT_PREFLIGHT_FAILED:{type(exc).__name__}")
            account_reachable = False
            account_mode_ok = False
            margin_mode_ok = False
            exchange_positions_ok = False
            open_orders_ok = False
            wallet_balance_ok = False
            available_balance_ok = False
            server_time_ok = False
            leverage_ok = False
    elif resolved_mode != "paper":
        reasons.append("ACCOUNT_PREFLIGHT_NOT_RUN")

    if resolved_mode != "paper" and not account_mode_ok:
        reasons.append("ACCOUNT_MODE_NOT_VERIFIED")
    if resolved_mode != "paper" and not margin_mode_ok:
        reasons.append("MARGIN_MODE_NOT_VERIFIED")
    if resolved_mode != "paper" and not account_reachable:
        reasons.append("ACCOUNT_UNREACHABLE")
    if resolved_mode != "paper" and not server_time_ok:
        reasons.append("SERVER_TIME_NOT_VERIFIED")
    if resolved_mode != "paper" and not leverage_ok:
        reasons.append("LEVERAGE_PARITY_NOT_VERIFIED")

    health_rows: list[Mapping[str, Any]] = []
    if data_health_ok is None:
        data_health_ok, health_reasons = _data_health_report(
            store, max_age_sec=max_data_age_sec(), symbols=selected_symbols
        )
        reasons.extend(health_reasons)
        try:
            health_rows = store.market_data_freshness(  # type: ignore[union-attr]
                max_age_sec=max_data_age_sec(), symbols=selected_symbols
            )
        except Exception:
            health_rows = []
    if not data_health_ok:
        reasons.append("DATA_HEALTH_NOT_VERIFIED")

    if reconciliation_ok is None:
        reconciliation_ok = resolved_mode == "paper"
    if not reconciliation_ok:
        reasons.append("RECONCILIATION_NOT_VERIFIED")

    risk_config_ok = _risk_config_ok()
    if not risk_config_ok:
        reasons.append("RISK_CONFIG_INVALID")
    kill_switch_ok = not _enabled("BIAN_KILL_SWITCH") and not _enabled("BIAN_TRADING_HALTED")
    if not kill_switch_ok:
        reasons.append("KILL_SWITCH_ACTIVE")

    expected_token = os.environ.get("LIVE_CONFIRMATION_TOKEN")
    supplied_token = os.environ.get("BIAN_LIVE_CONFIRMATION")
    confirmation_ok = resolved_mode != "live" or bool(expected_token and supplied_token == expected_token)
    if not confirmation_ok:
        reasons.append("LIVE_CONFIRMATION_MISSING")

    statuses = _persisted_gate_statuses(store, gate_evidence)
    evidence_ages: list[int] = []
    evidence_verified_times: list[datetime] = []
    supplied_evidence: Mapping[str, Any] = gate_evidence or {}
    evidence_records: dict[str, Any] = dict(supplied_evidence)
    evidence_getter = getattr(store, "runtime_gate_evidence", None) if store is not None else None
    if evidence_getter is not None:
        try:
            evidence = evidence_getter()
        except Exception:
            evidence = {}
        if isinstance(evidence, Mapping):
            evidence_records.update(evidence)
    now = datetime.now(timezone.utc)
    for gate_name, row in evidence_records.items():
        if gate_name not in statuses or not isinstance(row, Mapping):
            continue
        raw_verified_at = row.get("verified_at") or row.get("event_at")
        if not raw_verified_at:
            continue
        try:
            verified_at_dt = datetime.fromisoformat(
                str(raw_verified_at).replace("Z", "+00:00")
            )
            if verified_at_dt.tzinfo is None:
                verified_at_dt = verified_at_dt.replace(tzinfo=timezone.utc)
            age = max(0, int((now - verified_at_dt).total_seconds()))
            evidence_ages.append(age)
            evidence_verified_times.append(verified_at_dt)
            if age > gate_evidence_max_age_sec() and statuses[gate_name] == "PASSED":
                statuses[gate_name] = "FAILED"
                reasons.append(f"{gate_name.upper()}_EVIDENCE_STALE")
        except ValueError:
            statuses[gate_name] = "FAILED"
            reasons.append(f"{gate_name.upper()}_EVIDENCE_INVALID")
    observation_gate_status = statuses["observation"]
    paper_gate_status = statuses["paper"]
    shadow_gate_status = statuses["shadow"]
    testnet_gate_status = statuses["testnet"]
    observation_gates_ok = (
        observation_gate_status == "PASSED" and shadow_gate_status == "PASSED"
    )
    testnet_lifecycle_ok = testnet_gate_status == "PASSED"

    paper_db_ok = store is not None
    accounting_checker = getattr(store, "paper_accounting_ready", None) if store is not None else None
    if accounting_checker is None:
        paper_accounting_ok = store is not None
    else:
        try:
            paper_accounting_ok = bool(accounting_checker())
        except Exception:
            paper_accounting_ok = False
    paper_ready = all(
        (paper_db_ok, paper_accounting_ok, bool(data_health_ok), bool(reconciliation_ok),
         risk_config_ok, kill_switch_ok, paper_gate_status == "PASSED")
    )
    current_user_stream = _current_user_stream_health(store, mode=resolved_mode)
    user_stream_ok = current_user_stream == "OK"
    if resolved_mode != "paper" and not user_stream_ok:
        reasons.append("USER_STREAM_NOT_VERIFIED")
    exchange_ready = all(
        (
            credentials_ok, account_reachable, account_mode_ok, margin_mode_ok,
            exchange_positions_ok, open_orders_ok, wallet_balance_ok,
            available_balance_ok, server_time_ok, leverage_ok, bool(data_health_ok),
            bool(reconciliation_ok), risk_config_ok, kill_switch_ok, user_stream_ok,
        )
    )
    testnet_ready = exchange_ready and testnet_lifecycle_ok
    live_trading_enabled = _enabled("LIVE_TRADING_ENABLED")
    if resolved_mode == "live" and not live_trading_enabled:
        reasons.append("LIVE_TRADING_DISABLED")
    live_ready = all(
        (
            exchange_ready,
            paper_ready,
            positioning_enabled,
            observation_gates_ok,
            testnet_lifecycle_ok,
            alpha_status == "ALPHA_SUPPORTED",
            meme_ready,
            live_trading_enabled,
            confirmation_ok,
        )
    )
    if resolved_mode in {"testnet", "live"} and not testnet_lifecycle_ok:
        reasons.append("TESTNET_LIFECYCLE_NOT_VERIFIED")
    if resolved_mode == "live" and not observation_gates_ok:
        reasons.append("OBSERVATION_SHADOW_GATES_NOT_VERIFIED")
    if resolved_mode == "live" and not live_ready:
        reasons.append("LIVE_RELEASE_GATES_PENDING")
    live_allowed = resolved_mode == "live" and live_ready
    evaluated_at = datetime.now(timezone.utc).isoformat()
    orderbook_rows = [
        row
        for row in health_rows
        if str(row.get("source") or row.get("event_type") or "").upper()
        == "FUTURES_DEPTH"
    ]
    current_orderbook = (
        "UNKNOWN"
        if not orderbook_rows
        else "OK"
        if all(str(row.get("status", "")).upper() == "FRESH" for row in orderbook_rows)
        else str(orderbook_rows[0].get("status", "UNKNOWN")).upper()
    )
    return GateResult(
        mode=resolved_mode,
        positioning_enabled=positioning_enabled,
        credentials_ok=credentials_ok,
        account_mode_ok=account_mode_ok,
        margin_mode_ok=margin_mode_ok,
        data_health_ok=data_health_ok,
        reconciliation_ok=reconciliation_ok,
        risk_config_ok=risk_config_ok,
        confirmation_ok=confirmation_ok,
        kill_switch_ok=kill_switch_ok,
        live_allowed=live_allowed,
        reasons=tuple(dict.fromkeys(reasons)),
        symbol_leverage=symbol_leverage,
        account_reachable=account_reachable,
        exchange_positions_ok=exchange_positions_ok,
        open_orders_ok=open_orders_ok,
        wallet_balance_ok=wallet_balance_ok,
        available_balance_ok=available_balance_ok,
        server_time_ok=server_time_ok,
        leverage_ok=leverage_ok,
        observation_gates_ok=observation_gates_ok,
        testnet_lifecycle_ok=testnet_lifecycle_ok,
        paper_ready=paper_ready,
        testnet_ready=testnet_ready,
        live_ready=live_ready,
        observation_gate_status=observation_gate_status,
        paper_gate_status=paper_gate_status,
        shadow_gate_status=shadow_gate_status,
        testnet_gate_status=testnet_gate_status,
        paper_db_ok=paper_db_ok,
        paper_accounting_ok=paper_accounting_ok,
        alpha_gate_status=alpha_status,
        meme_universe_ready=meme_ready,
        evaluated_at=evaluated_at,
        evidence_verified_at=(
            max(evidence_verified_times).isoformat()
            if evidence_verified_times
            else None
        ),
        verification_age_sec=max(evidence_ages) if evidence_ages else 0,
        verification_source=(
            "persisted_gate_evidence" if evidence_ages else "runtime_gate"
        ),
        current_data_health="OK" if data_health_ok else "BLOCKED",
        current_account_health=(
            "NOT_APPLICABLE"
            if resolved_mode == "paper"
            else "OK"
            if account_reachable
            else "BLOCKED"
        ),
        current_reconciliation="OK" if reconciliation_ok else "BLOCKED",
        current_orderbook=current_orderbook,
        current_user_stream=current_user_stream,
    )
