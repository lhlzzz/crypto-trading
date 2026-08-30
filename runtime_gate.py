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
from risk import RiskLimits


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


_GATE_STATUSES = {"NOT_STARTED", "RUNNING", "PASSED", "FAILED"}
_EXTERNAL_GATES = ("observation", "paper", "shadow", "testnet")
REQUIRED_FUTURES_SOURCES = frozenset(
    {
        "FUTURES_TRADE",
        "FUTURES_BOOK_TICKER",
        "FUTURES_DEPTH",
        "FUTURES_MARK_PRICE",
        "FUTURES_INDEX_PRICE",
        "FUTURES_OPEN_INTEREST",
        "FUTURES_FUNDING",
        "FUTURES_TAKER",
        "FUTURES_LIQUIDATION",
    }
)
_REALTIME_FUTURES_SOURCES = frozenset(
    {
        "FUTURES_TRADE",
        "FUTURES_BOOK_TICKER",
        "FUTURES_DEPTH",
        "FUTURES_MARK_PRICE",
        "FUTURES_INDEX_PRICE",
        "FUTURES_FUNDING",
        "FUTURES_LIQUIDATION",
    }
)


def _persisted_gate_statuses(
    store: Any | None,
    supplied: Mapping[str, str] | None,
) -> dict[str, str]:
    """Read verified evidence; configuration alone is never sufficient."""
    values = {name: "NOT_STARTED" for name in _EXTERNAL_GATES}
    if supplied is not None:
        values.update({key: str(value).upper() for key, value in supplied.items()})
    getter = getattr(store, "runtime_gate_statuses", None) if store is not None else None
    if getter is None:
        getter = getattr(store, "get_runtime_gate_statuses", None) if store is not None else None
    if getter is not None:
        try:
            persisted = getter()
            if isinstance(persisted, Mapping):
                values.update({key: str(value).upper() for key, value in persisted.items()})
        except Exception:
            return {name: "FAILED" for name in _EXTERNAL_GATES}
    for name in _EXTERNAL_GATES:
        if values[name] not in _GATE_STATUSES:
            values[name] = "FAILED"
    return values


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
    verified_at: str | None = None
    verification_age_sec: int | None = None
    verification_source: str = "runtime_gate"

    @property
    def trading_enabled(self) -> bool:
        if self.mode == "paper":
            return self.paper_ready
        if self.mode == "testnet":
            return self.testnet_ready
        return self.live_allowed

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
            "verified_at": self.verified_at,
            "verification_age_sec": self.verification_age_sec,
            "verification_source": self.verification_source,
            "live_allowed": self.live_allowed,
            "reasons": list(self.reasons),
        }


def _data_health(
    store: Any | None,
    *,
    max_age_sec: int,
    symbols: Iterable[str],
) -> bool:
    if store is None or not hasattr(store, "market_data_freshness"):
        return False
    rows = store.market_data_freshness(max_age_sec=max_age_sec, symbols=symbols)
    now = datetime.now(timezone.utc)
    for source in REQUIRED_FUTURES_SOURCES:
        source_rows = [
            row for row in rows
            if str(row.get("source") or row.get("event_type") or "").upper() == source
        ]
        if not source_rows or any(not str(row.get("symbol", "")).strip() for row in source_rows):
            return False
        for row in source_rows:
            if str(row.get("status", "")).upper() != "FRESH":
                return False
            if row.get("latency_ms") is None:
                return False
            try:
                source_at = datetime.fromisoformat(str(row["source_timestamp"]))
                received_at = datetime.fromisoformat(str(row["received_timestamp"]))
                if source_at.tzinfo is None:
                    source_at = source_at.replace(tzinfo=timezone.utc)
                if received_at.tzinfo is None:
                    received_at = received_at.replace(tzinfo=timezone.utc)
                if received_at < source_at or source_at > now or received_at > now:
                    return False
                if int(row["latency_ms"]) != int((received_at - source_at).total_seconds() * 1000):
                    return False
                if source in _REALTIME_FUTURES_SOURCES and int(row["latency_ms"]) > int(
                    os.environ.get("MAX_DATA_LATENCY_MS", "2000")
                ):
                    return False
            except (KeyError, TypeError, ValueError):
                return False
    return True


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
            account = client.get_account()
            account_reachable = isinstance(account, dict)
            assets = account.get("assets") or account.get("balances") or []
            usdt = next((row for row in assets if str(row.get("asset", "")).upper() == "USDT"), None)
            wallet_balance_ok = usdt is not None and "walletBalance" in usdt
            available_balance_ok = usdt is not None and (
                "availableBalance" in usdt or "available" in usdt
            )
            server_time = client.get_server_time()
            server_ms = int(server_time.get("serverTime"))
            local_ms = int(__import__("time").time() * 1000)
            server_time_ok = abs(server_ms - local_ms) <= int(
                os.environ.get("MAX_DATA_LATENCY_MS", "2000")
            )
            position_mode = client.get_position_mode()
            actual_position_mode = "HEDGE" if str(position_mode.get("dualSidePosition", "")).lower() in {"true", "1"} else "ONE_WAY"
            account_mode_ok = actual_position_mode == _expected_position_mode()
            margin_mode_ok = bool(selected_symbols)
            for symbol in selected_symbols:
                margin = client.get_margin_type(symbol)
                leverage = client.get_leverage(symbol)
                actual_margin = str(margin.get("marginType", "")).upper()
                if actual_margin != _expected_margin_mode():
                    margin_mode_ok = False
                symbol_leverage[symbol] = str(leverage.get("leverage", ""))
            expected_leverage = os.environ.get("DEFAULT_LEVERAGE", "1")
            leverage_ok = bool(symbol_leverage) and all(
                value == expected_leverage for value in symbol_leverage.values()
            )
            margin_mode_ok = bool(selected_symbols) and margin_mode_ok
            positions = client.get_positions()
            open_orders = client.get_open_orders()
            exchange_positions_ok = isinstance(positions, list)
            open_orders_ok = isinstance(open_orders, list)
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

    if data_health_ok is None:
        data_health_ok = _data_health(
            store, max_age_sec=max_data_age_sec(), symbols=selected_symbols
        )
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
    exchange_ready = all(
        (
            credentials_ok, account_reachable, account_mode_ok, margin_mode_ok,
            exchange_positions_ok, open_orders_ok, wallet_balance_ok,
            available_balance_ok, server_time_ok, leverage_ok, bool(data_health_ok),
            bool(reconciliation_ok), risk_config_ok, kill_switch_ok,
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
    verified_at = datetime.now(timezone.utc).isoformat()
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
        verified_at=verified_at,
        verification_age_sec=0,
        verification_source="runtime_gate",
    )
