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
from engine import ObservationFreshnessPolicy, runtime_required_sources, validate_source_timestamps
from risk import FuturesAccountSnapshot, RiskLimits, reject_legacy_meme_risk_env
from trade_intent import CANONICAL_FUTURES_SYMBOLS, is_canonical_futures_symbol


def _account_row_symbol(row: Any) -> str:
    if not isinstance(row, Mapping):
        return ""
    return str(
        row.get("symbol")
        or row.get("s")
        or row.get("origSymbol")
        or ""
    ).upper().replace("-PERP", "").removesuffix("PERP").replace("-", "")


def _unauthorized_account_symbols(rows: Any) -> tuple[str, ...]:
    if not isinstance(rows, (tuple, list)):
        return ()
    unauthorized: list[str] = []
    for row in rows:
        symbol = _account_row_symbol(row)
        if not symbol:
            continue
        if not is_canonical_futures_symbol(symbol):
            unauthorized.append(symbol)
    return tuple(dict.fromkeys(unauthorized))


def _enabled(name: str, *, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _mode() -> str:
    return os.environ.get("BIAN_MODE", "paper").strip().lower()


class LiveAuthorizationError(RuntimeError):
    """Live order mutation is blocked until confirmation matches."""


def live_confirmation_ok() -> bool:
    """Canonical live confirmation: both tokens present and identical."""
    expected = os.environ.get("LIVE_CONFIRMATION_TOKEN")
    supplied = os.environ.get("BIAN_LIVE_CONFIRMATION")
    return bool(expected and supplied and supplied == expected)


def authorize_live_order_mutation(*, mode: str) -> None:
    """Fail closed before any live order mutation. Non-live is a no-op."""
    resolved = str(mode or "").strip().lower()
    if resolved != "live":
        return
    if not _enabled("LIVE_TRADING_ENABLED"):
        raise LiveAuthorizationError(
            "live mode is hard-blocked unless LIVE_TRADING_ENABLED=true"
        )
    expected = os.environ.get("LIVE_CONFIRMATION_TOKEN")
    supplied = os.environ.get("BIAN_LIVE_CONFIRMATION")
    if not expected or not str(expected).strip():
        raise LiveAuthorizationError("live mode requires LIVE_CONFIRMATION_TOKEN")
    if not supplied or not str(supplied).strip():
        raise LiveAuthorizationError("live mode requires BIAN_LIVE_CONFIRMATION")
    if supplied != expected:
        raise LiveAuthorizationError("live confirmation token mismatch")


USER_STREAM_OPEN_STATES = frozenset({"LIVE", "OK"})
USER_STREAM_BLOCKED_STATES = frozenset(
    {"DISCONNECTED", "CONNECTING", "RECONNECTING", "DEGRADED", "FAILED", "UNKNOWN"}
)


def requested_symbols_for_mode(mode: str) -> tuple[str, ...]:
    """Mode-local symbol request. Never falls back to another mode's env."""
    resolved = str(mode or "").strip().lower()
    env_name = {
        "paper": "BIAN_PAPER_SYMBOLS",
        "shadow": "BIAN_PAPER_SYMBOLS",
        "testnet": "BIAN_TESTNET_SYMBOLS",
        "live": "BIAN_LIVE_SYMBOLS",
    }.get(resolved)
    if env_name is None:
        return ()
    raw = os.environ.get(env_name, "")
    if not str(raw).strip():
        if resolved in {"paper", "shadow"}:
            raw = ",".join(sorted(CANONICAL_FUTURES_SYMBOLS))
        else:
            return ()
    return tuple(
        dict.fromkeys(
            str(symbol).strip().upper()
            for symbol in str(raw).split(",")
            if str(symbol).strip()
        )
    )


def unauthorized_symbols_for_mode(mode: str) -> tuple[str, ...]:
    """Symbols requested for a mode that are outside the major-coin universe."""
    return tuple(
        symbol
        for symbol in requested_symbols_for_mode(mode)
        if not is_canonical_futures_symbol(symbol)
    )


def trading_symbols_for_mode(mode: str | None = None) -> tuple[str, ...]:
    """Canonical major-coin universe for one runtime mode. No cross-mode fallback.

    Unauthorized symbols are never silently dropped into this result. Callers
    must also inspect ``unauthorized_symbols_for_mode``; a mixed request such
    as ``BTCUSDT,DOGEUSDT`` fails the runtime gate instead of running BTC only.
    """
    if mode is None or not str(mode).strip():
        raise ValueError("trading_symbols_for_mode requires an explicit mode")
    return tuple(
        symbol
        for symbol in requested_symbols_for_mode(mode)
        if is_canonical_futures_symbol(symbol)
    )


def _symbols(symbols: Iterable[str] | None, *, mode: str) -> tuple[str, ...]:
    if symbols is not None:
        return tuple(
            dict.fromkeys(
                str(symbol).strip().upper()
                for symbol in symbols
                if str(symbol).strip()
            )
        )
    return trading_symbols_for_mode(mode)


def user_stream_allows_open(status: str | None) -> bool:
    """Only an explicitly healthy private stream may open a new position."""
    return str(status or "UNKNOWN").strip().upper() in USER_STREAM_OPEN_STATES


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


def account_health_ttl_sec() -> int:
    """Bound how long a probed exchange-account snapshot may be reused."""
    try:
        return max(1, int(os.environ.get("ACCOUNT_HEALTH_TTL_SEC", "30")))
    except ValueError:
        return 30


_ACCOUNT_HEALTH: dict[str, tuple[datetime, dict[str, Any]]] = {}


def reset_account_health_cache() -> None:
    """Drop cached account preflight evidence. Tests and fail-closed restarts."""
    _ACCOUNT_HEALTH.clear()


def account_health_probe_due(mode: str) -> bool:
    """True when testnet/live must refresh the canonical account snapshot."""
    resolved = str(mode or "").strip().lower()
    if resolved == "paper":
        return False
    row = _ACCOUNT_HEALTH.get(resolved)
    if row is None:
        return True
    captured, _fields = row
    age = (datetime.now(timezone.utc) - captured).total_seconds()
    return age >= account_health_ttl_sec()


def _store_account_health(mode: str, fields: Mapping[str, Any]) -> None:
    _ACCOUNT_HEALTH[mode] = (datetime.now(timezone.utc), dict(fields))


def _cached_account_health(mode: str) -> tuple[str, dict[str, Any] | None]:
    row = _ACCOUNT_HEALTH.get(mode)
    if row is None:
        return "MISSING", None
    captured, fields = row
    age = (datetime.now(timezone.utc) - captured).total_seconds()
    payload = {**fields, "captured_at": captured, "age_sec": int(max(0, age))}
    if age > account_health_ttl_sec():
        return "EXPIRED", payload
    return "FRESH", payload


def gate_evidence_max_age_sec() -> int:
    """Bound how long an externally verified gate may be reused."""
    try:
        return max(1, int(os.environ.get("GATE_EVIDENCE_MAX_AGE_SEC", "86400")))
    except ValueError:
        return 86400


_GATE_STATUSES = {"NOT_STARTED", "RUNNING", "PASSED", "FAILED"}
_RUNTIME_GATES = (
    "realtime_30m",
    "realtime_2h",
    "realtime_6h",
    "realtime_24h",
)
_EXTERNAL_GATES = ("observation", "paper", "shadow", "testnet", *_RUNTIME_GATES)
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
    policy = ObservationFreshnessPolicy.from_env()
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
                        "max_age_sec": policy.max_age_for(source),
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
    major_universe_ready: bool = False
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
    global_transport_health: str = "UNKNOWN"
    per_symbol_transport_health: dict[str, str] = field(default_factory=dict)
    per_symbol_source_health: dict[str, dict[str, str]] = field(default_factory=dict)
    realtime_30m_status: str = "NOT_STARTED"
    realtime_2h_status: str = "NOT_STARTED"
    realtime_6h_status: str = "NOT_STARTED"
    realtime_24h_status: str = "NOT_STARTED"
    code_ready: bool = True

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
            "major_universe_ready": self.major_universe_ready,
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
            "global_transport_health": self.global_transport_health,
            "per_symbol_transport_health": dict(self.per_symbol_transport_health),
            "per_symbol_source_health": {
                symbol: dict(rows)
                for symbol, rows in self.per_symbol_source_health.items()
            },
            "live_allowed": self.live_allowed,
            "CODE_READY": True,
            "CODE_PASS": True,
            "ORDERBOOK_CODE_PASS": True,
            "ORDERBOOK_RUNTIME_PASS": self.current_orderbook == "OK",
            "REAL_DATA_READY": self.realtime_24h_status == "PASSED",
            "historical_gate_evidence": {
                "realtime_24h": self.realtime_24h_status,
                "paper": self.paper_gate_status,
                "shadow": self.shadow_gate_status,
                "alpha": self.alpha_gate_status,
                "testnet": self.testnet_gate_status,
            },
            "current_runtime_health": {
                "data_health": self.current_data_health,
                "account": self.current_account_health,
                "user_stream": self.current_user_stream,
                "reconciliation": self.current_reconciliation,
                "orderbook": self.current_orderbook,
                "transport": self.global_transport_health,
            },
            "PAPER_READY": self.paper_ready,
            "SHADOW_READY": self.shadow_gate_status == "PASSED",
            "TESTNET_READY": self.testnet_ready,
            "ALPHA_STATUS": self.alpha_gate_status,
            "ALPHA_READY": self.alpha_gate_status == "ALPHA_SUPPORTED",
            "LIVE_PREFLIGHT": self.live_ready,
            "LIVE_ALLOWED": self.live_allowed,
            "MARKET_HEALTH": self.current_data_health,
            "OBSERVATION_HEALTH": self.observation_gate_status,
            "EVIDENCE_HEALTH": self.current_data_health,
            "ACCOUNT_HEALTH": self.current_account_health,
            "RECONCILIATION_HEALTH": self.current_reconciliation,
            "USER_STREAM_HEALTH": self.current_user_stream,
            "RISK_HEALTH": self.risk_status,
            "PAPER_HEALTH": self.paper_gate_status,
            "TESTNET_HEALTH": self.testnet_gate_status,
            "ALPHA_HEALTH": self.alpha_gate_status,
            "realtime_30m_status": self.realtime_30m_status,
            "realtime_2h_status": self.realtime_2h_status,
            "realtime_6h_status": self.realtime_6h_status,
            "realtime_24h_status": self.realtime_24h_status,
            "orderbook_health": self.current_orderbook,
            "account_health": self.current_account_health,
            "user_stream": self.current_user_stream,
            "risk": self.risk_status,
            "major_universe": self.major_universe_ready,
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


def _risk_config_ok(*, mode: str | None = None) -> bool:
    try:
        reject_legacy_meme_risk_env(mode=mode)
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
    major_universe_ready: bool | None = None,
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
    selected_symbols = _symbols(symbols, mode=resolved_mode)
    reasons: list[str] = []
    positioning_enabled = _enabled("POSITIONING_DECISION_ENABLED")
    alpha_status = str(
        alpha_gate_status or os.environ.get("ALPHA_GATE_STATUS", "INSUFFICIENT_SAMPLE")
    ).upper()
    if alpha_status not in {"INSUFFICIENT_SAMPLE", "ALPHA_NOT_SUPPORTED", "ALPHA_SUPPORTED"}:
        alpha_status = "ALPHA_NOT_SUPPORTED"
    unauthorized = tuple(
        dict.fromkeys(
            (
                *unauthorized_symbols_for_mode(resolved_mode),
                *(
                    symbol
                    for symbol in selected_symbols
                    if not is_canonical_futures_symbol(symbol)
                ),
            )
        )
    )
    if unauthorized:
        reasons.append("UNAUTHORIZED_SYMBOL")
    if resolved_mode in {"testnet", "live"} and not selected_symbols and not unauthorized:
        reasons.append("MAJOR_UNIVERSE_EMPTY")
    major_ready = (
        bool(major_universe_ready)
        if major_universe_ready is not None
        else (bool(selected_symbols) and not unauthorized)
    )
    if not major_ready:
        reasons.append("MAJOR_UNIVERSE_NOT_READY")
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

    def _apply_account_fields(fields: Mapping[str, Any]) -> None:
        nonlocal account_reachable, account_mode_ok, margin_mode_ok
        nonlocal exchange_positions_ok, open_orders_ok, wallet_balance_ok
        nonlocal available_balance_ok, server_time_ok, leverage_ok, symbol_leverage
        account_reachable = bool(fields.get("account_reachable"))
        account_mode_ok = bool(fields.get("account_mode_ok"))
        margin_mode_ok = bool(fields.get("margin_mode_ok"))
        exchange_positions_ok = bool(fields.get("exchange_positions_ok"))
        open_orders_ok = bool(fields.get("open_orders_ok"))
        wallet_balance_ok = bool(fields.get("wallet_balance_ok"))
        available_balance_ok = bool(fields.get("available_balance_ok"))
        server_time_ok = bool(fields.get("server_time_ok"))
        leverage_ok = bool(fields.get("leverage_ok"))
        symbol_leverage = dict(fields.get("symbol_leverage") or {})

    if resolved_mode != "paper" and not credentials_ok:
        reasons.append("CREDENTIALS_MISSING")
    should_probe_account = (
        resolved_mode != "paper"
        and client is not None
        and credentials_ok
        and (probe_account or account_health_probe_due(resolved_mode))
    )
    if should_probe_account:
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
            unauthorized_positions = _unauthorized_account_symbols(snapshot.positions)
            unauthorized_orders = _unauthorized_account_symbols(snapshot.open_orders)
            exchange_positions_ok = (
                isinstance(snapshot.positions, tuple) and not unauthorized_positions
            )
            open_orders_ok = (
                isinstance(snapshot.open_orders, tuple) and not unauthorized_orders
            )
            if unauthorized_positions:
                reasons.append("UNAUTHORIZED_EXCHANGE_POSITION")
            if unauthorized_orders:
                reasons.append("UNAUTHORIZED_EXCHANGE_ORDER")
            _store_account_health(
                resolved_mode,
                {
                    "account_reachable": account_reachable,
                    "account_mode_ok": account_mode_ok,
                    "margin_mode_ok": margin_mode_ok,
                    "exchange_positions_ok": exchange_positions_ok,
                    "open_orders_ok": open_orders_ok,
                    "wallet_balance_ok": wallet_balance_ok,
                    "available_balance_ok": available_balance_ok,
                    "server_time_ok": server_time_ok,
                    "leverage_ok": leverage_ok,
                    "symbol_leverage": dict(symbol_leverage),
                },
            )
        except Exception as exc:
            reasons.append(f"ACCOUNT_PREFLIGHT_FAILED:{type(exc).__name__}")
            reset_account_health_cache()
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
        cache_status, cached = _cached_account_health(resolved_mode)
        if cache_status == "FRESH" and cached is not None:
            _apply_account_fields(cached)
        else:
            reasons.append(
                "ACCOUNT_HEALTH_EXPIRED"
                if cache_status == "EXPIRED"
                else "ACCOUNT_PREFLIGHT_NOT_RUN"
            )

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
    if resolved_mode != "paper" and not exchange_positions_ok:
        if "UNAUTHORIZED_EXCHANGE_POSITION" not in reasons:
            reasons.append("UNAUTHORIZED_EXCHANGE_POSITION")
    if resolved_mode != "paper" and not open_orders_ok:
        if "UNAUTHORIZED_EXCHANGE_ORDER" not in reasons:
            reasons.append("UNAUTHORIZED_EXCHANGE_ORDER")

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

    risk_config_ok = _risk_config_ok(mode=resolved_mode)
    if not risk_config_ok:
        reasons.append("RISK_CONFIG_INVALID")
    kill_switch_ok = not _enabled("BIAN_KILL_SWITCH") and not _enabled("BIAN_TRADING_HALTED")
    if not kill_switch_ok:
        reasons.append("KILL_SWITCH_ACTIVE")

    confirmation_ok = resolved_mode != "live" or live_confirmation_ok()
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
    realtime_30m_status = statuses["realtime_30m"]
    realtime_2h_status = statuses["realtime_2h"]
    realtime_6h_status = statuses["realtime_6h"]
    realtime_24h_status = statuses["realtime_24h"]
    for prior, current in (
        ("realtime_30m", "realtime_2h"),
        ("realtime_2h", "realtime_6h"),
        ("realtime_6h", "realtime_24h"),
    ):
        if statuses[current] == "PASSED" and statuses[prior] != "PASSED":
            statuses[current] = "FAILED"
            reasons.append(f"{current.upper()}_SKIPPED_LADDER")
    realtime_2h_status = statuses["realtime_2h"]
    realtime_6h_status = statuses["realtime_6h"]
    realtime_24h_status = statuses["realtime_24h"]
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
    user_stream_ok = (
        resolved_mode == "paper"
        or user_stream_allows_open(current_user_stream)
    )
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
            major_ready,
            live_trading_enabled,
            confirmation_ok,
            realtime_24h_status == "PASSED",
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
    per_symbol_source_health: dict[str, dict[str, str]] = {}
    per_symbol_transport_health: dict[str, str] = {}
    for row in health_rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        source = str(row.get("source") or row.get("event_type") or "").upper()
        if not symbol or not source:
            continue
        per_symbol_source_health.setdefault(symbol, {})[source] = str(
            row.get("status") or "MISSING"
        ).upper()
    realtime = {str(source).upper() for source in _REALTIME_FUTURES_SOURCES}
    for symbol, sources in per_symbol_source_health.items():
        relevant = {
            source: status
            for source, status in sources.items()
            if source in realtime
        }
        if not relevant:
            per_symbol_transport_health[symbol] = "UNKNOWN"
        elif all(status == "FRESH" for status in relevant.values()):
            per_symbol_transport_health[symbol] = "OK"
        elif any(status in {"GAP", "UNSAFE", "ERROR", "FAILED"} for status in relevant.values()):
            per_symbol_transport_health[symbol] = "FAILED"
        else:
            per_symbol_transport_health[symbol] = "DEGRADED"
    if store is None:
        global_transport_health = "UNKNOWN"
    elif not health_rows and data_health_ok is False:
        global_transport_health = "HALT"
    elif per_symbol_transport_health and all(
        value == "FAILED" for value in per_symbol_transport_health.values()
    ):
        global_transport_health = "HALT"
    elif any(value != "OK" for value in per_symbol_transport_health.values()):
        global_transport_health = "DEGRADED"
    elif data_health_ok:
        global_transport_health = "OK"
    else:
        global_transport_health = "DEGRADED"
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
        major_universe_ready=major_ready,
        meme_universe_ready=False,
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
        realtime_30m_status=realtime_30m_status,
        realtime_2h_status=realtime_2h_status,
        realtime_6h_status=realtime_6h_status,
        realtime_24h_status=realtime_24h_status,
        code_ready=True,
        global_transport_health=global_transport_health,
        per_symbol_transport_health=per_symbol_transport_health,
        per_symbol_source_health=per_symbol_source_health,
    )


def evaluate_live_preflight(store: Any | None = None) -> GateResult:
    """Canonical Live preflight: TradingStore + FuturesPrivateClient + gate."""
    if store is None:
        from trading_store import TradingStore

        store = TradingStore()
    initialize = getattr(store, "initialize", None)
    if callable(initialize):
        initialize()
    symbols = trading_symbols_for_mode("live")
    client = None
    try:
        from binance_client import FuturesPrivateClient

        client = FuturesPrivateClient(ClientConfig.from_env("live"))
    except Exception:
        client = None
    return evaluate_runtime_gate(
        mode="live",
        store=store,
        client=client,
        symbols=symbols,
        probe_account=True,
    )


class RuntimeGate:
    """Single production readiness owner."""

    evaluate = staticmethod(evaluate_runtime_gate)
