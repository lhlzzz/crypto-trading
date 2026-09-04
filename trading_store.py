"""Trading persistence owner, separate from the schema/connection owner."""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable
from uuid import UUID, uuid4

from scripts.database import configured_dsn, ensure_schema
from trade_intent import TradeIntent, is_canonical_futures_symbol


def _json(value: Any) -> str:
    return json.dumps(value or {}, default=str)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _row_dict(columns: tuple[str, ...], row: tuple[Any, ...]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for column, value in zip(columns, row):
        if isinstance(value, Decimal):
            result[column] = str(value)
        elif isinstance(value, (UUID, datetime)):
            result[column] = str(value)
        else:
            result[column] = value
    return result


def _as_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value))
    return result if result.tzinfo is not None else result.replace(tzinfo=timezone.utc)


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def empty_runtime_acceptance(
    *,
    mode: str,
    session_id: str | None = None,
    stage: str | None = None,
    scoped_start: str | None = None,
    scoped_end: str | None = None,
    commit_sha: str | None = None,
    database_healthy: bool = True,
) -> dict[str, Any]:
    """Fail-closed acceptance counts when a validation session is missing."""
    return {
        "database_healthy": database_healthy,
        "session_id": session_id,
        "stage": stage,
        "mode": mode,
        "scoped_start": scoped_start,
        "scoped_end": scoped_end,
        "commit_sha": commit_sha,
        "observation_count": 0,
        "session_market_flow_count": 0,
        "evidence_scope": {
            "orders": "session",
            "trades": "session",
            "observations": "session",
            "market_flow_events": "session",
            "balances": "current_exchange_truth",
            "positions": "current_exchange_truth",
        },
        "balance_scope": "current_exchange_truth",
        "position_scope": "current_exchange_truth",
        "positioning_count": 0,
        "evidence_count": 0,
        "episode_count": 0,
        "intent_count": 0,
        "risk_decision_count": 0,
        "order_count": 0,
        "fill_count": 0,
        "funding_count": 0,
        "long_count": 0,
        "short_count": 0,
        "long_building_count": 0,
        "short_building_count": 0,
        "duplicate_trades": 0,
        "duplicate_funding": 0,
        "invalid_positions": 0,
        "impossible_balance": False,
        "impossible_equity": False,
        "invalid_margin": False,
        "unknown_order": 0,
        "stale_pending_order": 0,
        "unsafe_order": 0,
        "stale_open": 0,
        "unsafe_open": 0,
        "accounting": {},
        "restart_recovery": False,
        "episode_split": False,
        "restart_comparison": {"ok": False, "reason": "RESTART_SNAPSHOT_MISSING"},
    }


def derive_testnet_lifecycle_facts(
    *,
    session: dict[str, Any] | None,
    orders: list[dict[str, Any]],
    order_events: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    system_events: list[dict[str, Any]],
) -> dict[str, Any]:
    """Derive Testnet acceptance facts from session-scoped database rows."""
    row = session if isinstance(session, dict) else {}
    session_id = str(row.get("session_id") or "")
    events_by_order: dict[str, list[dict[str, Any]]] = {}
    for event in order_events:
        order_id = str(event.get("order_id") or "")
        events_by_order.setdefault(order_id, []).append(event)
    trades_by_order: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        trades_by_order.setdefault(str(trade.get("order_id") or ""), []).append(trade)

    def _direction(order: dict[str, Any]) -> str:
        return str(order.get("position_side") or "").upper()

    def _action(order: dict[str, Any]) -> str:
        return str(order.get("position_action") or "").upper()

    def _fills(order: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            {
                "trade_id": str(trade.get("trade_id") or ""),
                "exchange_trade_id": str(
                    trade.get("exchange_trade_id")
                    or _json_obj(trade.get("payload")).get("exchange_trade_id")
                    or ""
                ),
                "quantity": str(trade.get("quantity") or "0"),
                "source_event_id": trade.get("source_event_id"),
            }
            for trade in trades_by_order.get(str(order.get("order_id") or ""), [])
        ]

    def _user_stream_for(order: dict[str, Any]) -> list[dict[str, Any]]:
        observed = []
        expected_exchange_id = str(order.get("exchange_order_id") or "")
        for event in events_by_order.get(str(order.get("order_id") or ""), []):
            if str(event.get("event_type") or "") != "USER_STREAM_ORDER_UPDATE":
                continue
            payload = _json_obj(event.get("payload"))
            observed_exchange_id = str(
                event.get("exchange_order_id")
                or payload.get("exchange_order_id")
                or payload.get("i")
                or order.get("exchange_order_id")
                or ""
            )
            if expected_exchange_id and observed_exchange_id != expected_exchange_id:
                continue
            observed.append(
                {
                    "event_id": event.get("event_id") or payload.get("event_id"),
                    "exchange_order_id": observed_exchange_id,
                    "symbol": order.get("symbol") or payload.get("symbol"),
                    "event_time": str(event.get("event_at") or payload.get("event_time") or ""),
                    "execution_type": payload.get("execution_type") or payload.get("x") or event.get("status"),
                    "event_type": payload.get("e") or payload.get("event_type") or "ORDER_TRADE_UPDATE",
                }
            )
        return observed

    def _reconciled(order: dict[str, Any]) -> bool:
        return any(
            str(event.get("event_type") or "") in {"ORDER_RECONCILED", "ORDER_RECONCILED_AFTER_UNKNOWN"}
            for event in events_by_order.get(str(order.get("order_id") or ""), [])
        )

    def _leg(direction: str) -> dict[str, Any]:
        opens = [
            order for order in orders
            if _direction(order) == direction and _action(order) == "OPEN"
        ]
        closes = [
            order for order in orders
            if _direction(order) == direction and _action(order) in {"CLOSE", "REDUCE"}
        ]
        open_order = next((order for order in opens if order.get("exchange_order_id")), opens[0] if opens else {})
        close_order = next((order for order in closes if order.get("exchange_order_id")), closes[0] if closes else {})
        stream = _user_stream_for(open_order) + _user_stream_for(close_order)
        fills = _fills(open_order)
        local_flat = all(
            Decimal(str(position.get("quantity") or 0)) == 0
            or str(position.get("position_side") or "FLAT").upper() == "FLAT"
            for position in positions
        )
        return {
            "order": {
                "order_id": open_order.get("order_id"),
                "client_order_id": open_order.get("client_order_id"),
                "exchange_order_id": open_order.get("exchange_order_id"),
                "symbol": open_order.get("symbol"),
                "status": open_order.get("status"),
            },
            "fills": fills,
            "user_stream_observed": bool(stream),
            "local_state_updated": bool(open_order.get("status")),
            "reconciliation_matches": _reconciled(open_order) and (
                not close_order or _reconciled(close_order)
            ),
            "close": {
                "order_id": close_order.get("order_id"),
                "exchange_order_id": close_order.get("exchange_order_id"),
                "status": close_order.get("status"),
                "reconciled_flat": False,
                "reconciliation_matches": _reconciled(close_order),
            },
        }

    partial = next(
        (
            {
                "status": "PARTIALLY_FILLED",
                "order_id": order.get("order_id"),
                "exchange_order_id": order.get("exchange_order_id"),
                "event_type": event.get("event_type"),
                "event_id": event.get("event_id"),
            }
            for order in orders
            for event in events_by_order.get(str(order.get("order_id") or ""), [])
            if str(order.get("status") or "").upper() == "PARTIALLY_FILLED"
            or str(event.get("status") or "").upper() == "PARTIALLY_FILLED"
        ),
        {},
    )
    cancel = next(
        (
            {
                "status": "CANCELLED",
                "order_id": order.get("order_id"),
                "exchange_order_id": order.get("exchange_order_id"),
                "event_type": event.get("event_type"),
                "event_id": event.get("event_id"),
            }
            for order in orders
            for event in events_by_order.get(str(order.get("order_id") or ""), [])
            if str(order.get("status") or "").upper() in {"CANCELLED", "CANCELED"}
            and str(event.get("event_type") or "") in {"ORDER_CANCELLED", "CANCEL_RECONCILED"}
        ),
        {},
    )
    unknown = {}
    for order in orders:
        events = events_by_order.get(str(order.get("order_id") or ""), [])
        unknown_events = [
            event for event in events
            if str(event.get("event_type") or "") in {"ORDER_UNKNOWN", "CANCEL_UNKNOWN"}
            or str(event.get("status") or "").upper() == "UNKNOWN"
        ]
        if not unknown_events and str(order.get("status") or "").upper() != "UNKNOWN":
            continue
        resubmitted = any("RESUBMIT" in str(event.get("event_type") or "").upper() for event in events)
        resolved = next(
            (
                event for event in events
                if str(event.get("event_type") or "") in {
                    "ORDER_RECONCILED_AFTER_UNKNOWN",
                    "ORDER_RECONCILED",
                    "CANCEL_RECONCILED",
                }
            ),
            None,
        )
        resolved_by = ""
        if resolved is not None:
            payload = _json_obj(resolved.get("payload"))
            if order.get("exchange_order_id") or payload.get("orderId"):
                resolved_by = "exchange_order_id"
            else:
                resolved_by = "client_order_id"
        unknown = {
            "status": "UNKNOWN",
            "order_id": order.get("order_id"),
            "client_order_id": order.get("client_order_id"),
            "exchange_order_id": order.get("exchange_order_id"),
            "resolved_by": resolved_by,
            "resubmitted": resubmitted,
            "resolved_status": None if resolved is None else str(order.get("status") or resolved.get("status") or ""),
            "halted": resolved is None,
        }
        break

    user_stream_events: list[dict[str, Any]] = []
    for order in orders:
        user_stream_events.extend(_user_stream_for(order))
    lifecycle_symbol = next(
        (str(order.get("symbol") or "").upper() for order in orders if order.get("symbol")),
        "",
    )
    lifecycle_exchange_ids = {
        str(order.get("exchange_order_id") or "")
        for order in orders
        if order.get("exchange_order_id")
    }
    for event in system_events:
        payload = _json_obj(event.get("payload"))
        if str(event.get("event_type") or "") != "USER_STREAM_ACCOUNT_UPDATE":
            continue
        event_symbol = str(payload.get("symbol") or "").upper()
        position_symbols = {
            str(item.get("symbol") or "").upper()
            for item in (payload.get("position_updates") or [])
            if isinstance(item, dict) and item.get("symbol")
        }
        if lifecycle_symbol and event_symbol not in {lifecycle_symbol, ""} and lifecycle_symbol not in position_symbols:
            continue
        if lifecycle_symbol and not event_symbol and lifecycle_symbol not in position_symbols:
            continue
        event_time = str(event.get("event_at") or payload.get("event_time") or "")
        if not event_time:
            continue
        user_stream_events.append(
            {
                "event_id": event.get("event_id") or payload.get("event_id"),
                "exchange_order_id": payload.get("exchange_order_id"),
                "symbol": event_symbol or lifecycle_symbol,
                "event_time": event_time,
                "execution_type": payload.get("execution_type") or "ACCOUNT_UPDATE",
                "event_type": "ACCOUNT_UPDATE",
                "position_updates": payload.get("position_updates") or [],
            }
        )
    listen = next(
        (
            {
                "created": True,
                "listen_key": _json_obj(event.get("payload")).get("listen_key"),
                "event_id": event.get("event_id"),
            }
            for event in system_events
            if str(event.get("event_type") or "") == "LISTEN_KEY_CREATED"
            and _json_obj(event.get("payload")).get("listen_key")
        ),
        {},
    )
    recon_ok = any(str(event.get("event_type") or "") == "RECONCILIATION_OK" for event in system_events)
    recon_events = [
        event for event in order_events
        if str(event.get("event_type") or "") in {"ORDER_RECONCILED", "ORDER_RECONCILED_AFTER_UNKNOWN"}
    ]
    recon_exchange_ids = {
        str(event.get("exchange_order_id") or _json_obj(event.get("payload")).get("exchange_order_id") or "")
        for event in recon_events
        if event.get("exchange_order_id") or _json_obj(event.get("payload")).get("exchange_order_id")
    }
    open_close_ids = {
        str(order.get("exchange_order_id") or "")
        for order in orders
        if str(order.get("position_action") or "").upper() in {"OPEN", "CLOSE", "REDUCE"}
        and order.get("exchange_order_id")
    }
    lifecycle_reconciled = bool(open_close_ids) and open_close_ids.issubset(recon_exchange_ids | lifecycle_exchange_ids) and bool(recon_events)
    local_flat = all(
        Decimal(str(position.get("quantity") or 0)) == 0
        or str(position.get("position_side") or "FLAT").upper() == "FLAT"
        for position in positions
    )
    exchange_flat_flags: list[bool] = []
    for event in system_events:
        payload = _json_obj(event.get("payload"))
        event_type = str(event.get("event_type") or "")
        if event_type == "RECONCILIATION_OK":
            rows = payload.get("exchange_positions")
            if isinstance(rows, list):
                exchange_flat_flags.append(
                    all(
                        Decimal(str(item.get("quantity") or 0)) == 0
                        or str(item.get("position_side") or "FLAT").upper() == "FLAT"
                        for item in rows
                        if isinstance(item, dict)
                        and (
                            not lifecycle_symbol
                            or str(item.get("symbol") or "").upper() == lifecycle_symbol
                        )
                    )
                )
            elif "exchange_flat" in payload:
                exchange_flat_flags.append(bool(payload.get("exchange_flat")))
    exchange_flat = bool(exchange_flat_flags) and all(exchange_flat_flags)
    comparison = TradingStore.compare_restart_snapshots(
        row.get("pre_restart_snapshot"),
        row.get("post_restart_snapshot"),
    )
    return {
        "source": "database",
        "session_id": session_id,
        "start_time": row.get("started_at"),
        "started_at": row.get("started_at"),
        "end_time": row.get("ended_at"),
        "ended_at": row.get("ended_at"),
        "symbol": next((str(order.get("symbol") or "") for order in orders if order.get("symbol")), None),
        "long": _leg("LONG"),
        "short": _leg("SHORT"),
        "partial_fill": partial,
        "cancel": cancel,
        "unknown_order": unknown,
        "user_stream": {"events": user_stream_events, "observed": bool(user_stream_events)},
        "listen_key": listen,
        "reconciliation": {
            "ok": recon_ok and lifecycle_reconciled and exchange_flat,
            "open_matches": lifecycle_reconciled,
            "orders": [
                {
                    "local_order_id": event.get("order_id"),
                    "exchange_order_id": event.get("exchange_order_id"),
                    "match": True,
                }
                for event in recon_events
            ],
            "fills": [
                {
                    "trade_id": trade.get("trade_id"),
                    "exchange_trade_id": trade.get("exchange_trade_id") or trade.get("source_event_id"),
                    "match": True,
                }
                for trade in trades
            ],
            "local_flat": local_flat,
            "exchange_flat": exchange_flat,
        },
        "restart": comparison,
        "positions_scope": "current_exchange_truth",
        "balance_scope": "current_exchange_truth",
    }


class TradingStore:
    """Persist trading facts and audit events without owning schema creation."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or configured_dsn()

    @staticmethod
    def _mode(mode: str | None = None) -> str:
        resolved = str(mode or "").strip().lower()
        if resolved not in {"paper", "shadow", "testnet", "live"}:
            raise ValueError("store operations require an explicit mode")
        return resolved

    def _session_id(self) -> str | None:
        bound = getattr(self, "validation_session_id", None)
        if bound:
            return str(bound)
        env = os.environ.get("BIAN_VALIDATION_SESSION_ID", "").strip()
        return env or None

    def bind_validation_session(
        self,
        session_id: str,
        *,
        mode: str,
        stage: str,
        commit_sha: str | None = None,
        status: str = "RUNNING",
        started_at: datetime | None = None,
    ) -> None:
        """Bind this store to one canonical validation session."""
        self.validation_session_id = str(session_id)
        self.validation_mode = str(mode).strip().lower()
        self.upsert_validation_session(
            session_id=self.validation_session_id,
            stage=stage,
            mode=self.validation_mode,
            commit_sha=commit_sha,
            status=status,
            started_at=started_at,
        )

    def initialize(self) -> None:
        ensure_schema(self.dsn)

    def upsert_validation_session(
        self,
        *,
        session_id: str,
        stage: str,
        mode: str,
        commit_sha: str | None = None,
        status: str = "RUNNING",
        started_at: datetime | None = None,
        ended_at: datetime | None = None,
        owner_pid: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        import psycopg2

        now = _now()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO validation_sessions(
                        session_id, stage, mode, started_at, ended_at, commit_sha,
                        owner_pid, heartbeat_at, status, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB))
                    ON CONFLICT (session_id) DO UPDATE SET
                        stage = EXCLUDED.stage,
                        mode = EXCLUDED.mode,
                        ended_at = COALESCE(EXCLUDED.ended_at, validation_sessions.ended_at),
                        commit_sha = COALESCE(EXCLUDED.commit_sha, validation_sessions.commit_sha),
                        owner_pid = COALESCE(EXCLUDED.owner_pid, validation_sessions.owner_pid),
                        heartbeat_at = EXCLUDED.heartbeat_at,
                        status = EXCLUDED.status,
                        payload = COALESCE(EXCLUDED.payload, validation_sessions.payload)
                    """,
                    (
                        str(session_id),
                        stage,
                        str(mode).strip().lower(),
                        started_at or now,
                        ended_at,
                        commit_sha,
                        owner_pid if owner_pid is not None else os.getpid(),
                        now,
                        status,
                        _json(payload),
                    ),
                )

    def heartbeat_validation_session(self, session_id: str | None = None) -> None:
        import psycopg2

        resolved = session_id or self._session_id()
        if not resolved:
            return
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE validation_sessions
                    SET heartbeat_at = %s
                    WHERE session_id = %s
                    """,
                    (_now(), resolved),
                )

    def expire_stale_validation_sessions(self, *, stale_after_sec: int = 600) -> int:
        """Mark stale RUNNING validation sessions EXPIRED. Heartbeat is the owner."""
        import psycopg2

        cutoff = _now() - timedelta(seconds=max(1, int(stale_after_sec)))
        now = _now()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE validation_sessions
                    SET status = 'EXPIRED',
                        ended_at = COALESCE(ended_at, %s),
                        payload = COALESCE(payload, CAST('{}' AS JSONB))
                            || CAST(%s AS JSONB)
                    WHERE status = 'RUNNING'
                      AND COALESCE(heartbeat_at, started_at) < %s
                    """,
                    (now, _json({"expired_reason": "STALE_RUNNING_SESSION"}), cutoff),
                )
                return int(cursor.rowcount or 0)

    def finish_validation_session(
        self,
        session_id: str | None = None,
        *,
        status: str | None = None,
        ended_at: datetime | None = None,
    ) -> None:
        """Persist session end time without resetting started_at."""
        import psycopg2

        resolved = session_id or self._session_id()
        if not resolved:
            return
        now = _now()
        ended = ended_at or now
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                if status is None:
                    cursor.execute(
                        """
                        UPDATE validation_sessions
                        SET ended_at = %s,
                            heartbeat_at = %s
                        WHERE session_id = %s
                        """,
                        (ended, now, resolved),
                    )
                else:
                    cursor.execute(
                        """
                        UPDATE validation_sessions
                        SET ended_at = %s,
                            heartbeat_at = %s,
                            status = %s
                        WHERE session_id = %s
                        """,
                        (ended, now, status, resolved),
                    )

    def record_session_observation(
        self,
        *,
        symbol: str,
        observed_at: datetime,
        observation_id: str | None = None,
        source: str = "consumed_frame",
    ) -> None:
        import psycopg2

        session_id = self._session_id()
        if not session_id:
            return
        observed_at = _as_utc(observed_at)
        identity = observation_id or f"{symbol.upper()}:{observed_at.isoformat()}"
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO validation_session_observations(
                        session_id, observation_id, symbol, observed_at, source
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (session_id, observation_id) DO NOTHING
                    """,
                    (session_id, identity, symbol.upper(), observed_at, source),
                )

    def record_restart_snapshot(self, phase: str, snapshot: dict[str, Any]) -> None:
        import psycopg2

        session_id = self._session_id()
        if not session_id:
            raise ValueError("restart snapshot requires a bound validation session")
        if phase not in {"pre_restart", "post_restart"}:
            raise ValueError("restart phase must be pre_restart or post_restart")
        column = "pre_restart_snapshot" if phase == "pre_restart" else "post_restart_snapshot"
        ts_column = "pre_restart_at" if phase == "pre_restart" else "post_restart_at"
        now = _now()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE validation_sessions
                    SET {column} = CAST(%s AS JSONB),
                        {ts_column} = COALESCE({ts_column}, %s),
                        heartbeat_at = %s
                    WHERE session_id = %s
                    """,
                    (_json(snapshot), now, now, session_id),
                )

    def load_validation_session(self, session_id: str) -> dict[str, Any] | None:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT session_id, stage, mode, started_at, pre_restart_at,
                           post_restart_at, ended_at, commit_sha, owner_pid,
                           heartbeat_at, status, pre_restart_snapshot,
                           post_restart_snapshot, testnet_evidence, payload
                    FROM validation_sessions
                    WHERE session_id = %s
                    """,
                    (str(session_id),),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        result = _row_dict(
            (
                "session_id", "stage", "mode", "started_at", "pre_restart_at",
                "post_restart_at", "ended_at", "commit_sha", "owner_pid",
                "heartbeat_at", "status", "pre_restart_snapshot",
                "post_restart_snapshot", "testnet_evidence", "payload",
            ),
            row,
        )
        for key in ("pre_restart_snapshot", "post_restart_snapshot", "testnet_evidence", "payload"):
            value = result.get(key)
            if value is None or isinstance(value, dict):
                continue
            result[key] = _json_obj(value)
        return result

    def record_testnet_lifecycle_evidence(self, evidence: dict[str, Any]) -> None:
        import psycopg2

        session_id = str(evidence.get("session_id") or self._session_id() or "")
        if not session_id:
            raise ValueError("testnet evidence requires a validation session")
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE validation_sessions
                    SET testnet_evidence = CAST(%s AS JSONB), heartbeat_at = %s
                    WHERE session_id = %s
                    """,
                    (_json(evidence), _now(), session_id),
                )

    def testnet_lifecycle_evidence(self, session_id: str | None = None) -> dict[str, Any]:
        """Load session-scoped Testnet facts from PostgreSQL, never JSON cache."""
        import psycopg2

        resolved = session_id or self._session_id()
        if not resolved:
            return {"source": "database", "session_id": None}
        session = self.load_validation_session(resolved) or {"session_id": resolved}
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT order_id, client_order_id, exchange_order_id, symbol, side,
                           status, position_side, position_action, reduce_only,
                           executed_quantity, quantity, mode, payload
                    FROM orders
                    WHERE validation_session_id = %s AND mode = 'testnet'
                    ORDER BY created_at ASC
                    """,
                    (resolved,),
                )
                order_rows = cursor.fetchall()
                cursor.execute(
                    """
                    SELECT e.event_id, e.order_id, e.event_type, e.status, e.event_at,
                           e.payload, o.exchange_order_id, o.symbol, o.client_order_id
                    FROM order_events e
                    JOIN orders o ON o.order_id = e.order_id
                    WHERE o.validation_session_id = %s AND o.mode = 'testnet'
                    ORDER BY e.event_at ASC
                    """,
                    (resolved,),
                )
                event_rows = cursor.fetchall()
                cursor.execute(
                    """
                    SELECT t.trade_id, t.order_id, t.symbol, t.quantity, t.price,
                           t.source_event_id, t.exchange_trade_id, t.payload
                    FROM trades t
                    JOIN orders o ON o.order_id = t.order_id
                    WHERE o.validation_session_id = %s AND t.mode = 'testnet'
                    ORDER BY t.executed_at ASC
                    """,
                    (resolved,),
                )
                trade_rows = cursor.fetchall()
                cursor.execute(
                    """
                    SELECT event_id, event_type, message, event_at, payload
                    FROM system_events
                    WHERE validation_session_id = %s
                    ORDER BY event_at ASC
                    """,
                    (resolved,),
                )
                system_rows = cursor.fetchall()
                cursor.execute(
                    """
                    SELECT symbol, position_side, quantity
                    FROM positions
                    WHERE mode = 'testnet'
                    """
                )
                position_rows = cursor.fetchall()
        orders = [
            _row_dict(
                (
                    "order_id", "client_order_id", "exchange_order_id", "symbol", "side",
                    "status", "position_side", "position_action", "reduce_only",
                    "executed_quantity", "quantity", "mode", "payload",
                ),
                row,
            )
            for row in order_rows
        ]
        order_events = [
            _row_dict(
                (
                    "event_id", "order_id", "event_type", "status", "event_at",
                    "payload", "exchange_order_id", "symbol", "client_order_id",
                ),
                row,
            )
            for row in event_rows
        ]
        trades = [
            _row_dict(
                (
                    "trade_id", "order_id", "symbol", "quantity", "price",
                    "source_event_id", "exchange_trade_id", "payload",
                ),
                row,
            )
            for row in trade_rows
        ]
        system_events = [
            _row_dict(("event_id", "event_type", "message", "event_at", "payload"), row)
            for row in system_rows
        ]
        positions = [
            _row_dict(("symbol", "position_side", "quantity"), row)
            for row in position_rows
        ]
        return derive_testnet_lifecycle_facts(
            session=session,
            orders=orders,
            order_events=order_events,
            trades=trades,
            positions=positions,
            system_events=system_events,
        )

    def account_continuity_snapshot(
        self,
        *,
        mode: str,
        symbols: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        """Capture restart-comparable paper/shadow account state."""
        resolved_mode = self._mode(mode)
        symbol_list = [item.replace("-", "").upper() for item in (symbols or [])]
        if not symbol_list:
            symbol_list = ["BTCUSDT"]
        positions = []
        episodes = []
        for symbol in symbol_list:
            position = self.get_position(symbol, mode=resolved_mode) or {}
            quantity = Decimal(str(position.get("quantity") or 0))
            side = str(position.get("position_side") or ("FLAT" if quantity == 0 else "UNKNOWN"))
            positions.append(
                {
                    "symbol": symbol,
                    "active_position": quantity > 0,
                    "position_side": side,
                    "position_quantity": str(quantity),
                    "entry_price": str(position.get("entry_price") or 0),
                }
            )
            episode = self.get_active_episode(symbol) or {}
            episodes.append(
                {
                    "symbol": symbol,
                    "active_episode_id": episode.get("episode_id"),
                    "episode_direction": episode.get("direction"),
                    "episode_status": episode.get("status"),
                }
            )
        balance = self.get_balance("USDT", mode=resolved_mode) or {}
        wallet = Decimal(str(balance.get("wallet_balance") or 0))
        unrealized = Decimal(str(balance.get("unrealized_pnl") or 0))
        payload = _json_obj(balance.get("payload"))
        open_orders = [
            {
                "client_order_id": row.get("client_order_id"),
                "exchange_order_id": row.get("exchange_order_id"),
                "status": row.get("status"),
                "symbol": row.get("symbol"),
                "quantity": str(row.get("quantity") or 0),
            }
            for row in self.list_open_local_orders(mode=resolved_mode)
        ]
        open_orders.sort(key=lambda row: str(row.get("client_order_id") or ""))
        return {
            "mode": resolved_mode,
            "positions": positions,
            "episodes": episodes,
            "wallet_balance": str(wallet),
            "available_balance": str(balance.get("available_balance") or 0),
            "used_margin": str(balance.get("used_margin") or 0),
            "unrealized_pnl": str(unrealized),
            "equity": str(wallet + unrealized),
            "realized_pnl": str(payload.get("realized_pnl") or 0),
            "funding_pnl": str(payload.get("funding_pnl") or 0),
            "fee_pnl": str(payload.get("fee_pnl") or 0),
            "open_orders": open_orders,
        }

    @staticmethod
    def compare_restart_snapshots(
        pre: dict[str, Any] | None,
        post: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Compare restart continuity by identity and accounting invariants."""
        if not isinstance(pre, dict):
            pre = _json_obj(pre)
        if not isinstance(post, dict):
            post = _json_obj(post)
        if not pre or not post:
            return {"ok": False, "reason": "RESTART_SNAPSHOT_MISSING", "mismatched": []}

        def _dec(value: Any) -> Decimal:
            try:
                return Decimal(str(value if value is not None else "0"))
            except Exception:
                return Decimal("0")

        def _position_key(item: dict[str, Any]) -> tuple[Any, ...]:
            return (
                str(item.get("symbol") or "").upper(),
                str(item.get("position_side") or "").upper(),
                _dec(item.get("position_quantity")),
            )

        def _episode_key(item: dict[str, Any]) -> tuple[str, ...]:
            return (str(item.get("active_episode_id") or ""),)

        def _order_key(item: dict[str, Any]) -> tuple[str, ...]:
            return (
                str(item.get("client_order_id") or ""),
                str(item.get("exchange_order_id") or ""),
                str(item.get("status") or ""),
            )

        mismatched: list[str] = []
        pre_positions = sorted(_position_key(item) for item in (pre.get("positions") or []) if isinstance(item, dict))
        post_positions = sorted(_position_key(item) for item in (post.get("positions") or []) if isinstance(item, dict))
        if pre_positions != post_positions:
            mismatched.append("positions")
        pre_episodes = sorted(_episode_key(item) for item in (pre.get("episodes") or []) if isinstance(item, dict))
        post_episodes = sorted(_episode_key(item) for item in (post.get("episodes") or []) if isinstance(item, dict))
        if pre_episodes != post_episodes:
            mismatched.append("episodes")
        pre_orders = sorted(_order_key(item) for item in (pre.get("open_orders") or []) if isinstance(item, dict))
        post_orders = sorted(_order_key(item) for item in (post.get("open_orders") or []) if isinstance(item, dict))
        if pre_orders != post_orders:
            mismatched.append("open_orders")

        def _invariants(snap: dict[str, Any]) -> list[str]:
            wallet = _dec(snap.get("wallet_balance"))
            used = _dec(snap.get("used_margin"))
            available = _dec(snap.get("available_balance"))
            unrealized = _dec(snap.get("unrealized_pnl"))
            equity = _dec(snap.get("equity"))
            problems: list[str] = []
            if available != wallet - used:
                problems.append("available_balance")
            if equity != wallet + unrealized:
                problems.append("equity")
            return problems

        pre_problems = _invariants(pre)
        post_problems = _invariants(post)
        if pre_problems or post_problems:
            return {
                "ok": False,
                "reason": "IMPOSSIBLE_ACCOUNTING",
                "mismatched": mismatched,
                "pre_invariants": pre_problems,
                "post_invariants": post_problems,
            }

        wallet_delta = _dec(post.get("wallet_balance")) - _dec(pre.get("wallet_balance"))
        explained = (
            (_dec(post.get("realized_pnl")) - _dec(pre.get("realized_pnl")))
            + (_dec(post.get("funding_pnl")) - _dec(pre.get("funding_pnl")))
            + (_dec(post.get("fee_pnl")) - _dec(pre.get("fee_pnl")))
        )
        if abs(wallet_delta - explained) > Decimal("0.00000001"):
            mismatched.append("wallet_balance")
            return {
                "ok": False,
                "reason": "UNEXPECTED_BALANCE_DELTA",
                "mismatched": mismatched,
                "wallet_delta": str(wallet_delta),
                "explained_delta": str(explained),
            }
        if mismatched:
            return {"ok": False, "reason": "RESTART_STATE_MISMATCH", "mismatched": mismatched}
        return {"ok": True, "reason": "OK", "mismatched": []}

    def is_halted(self, *, mode: str | None = None, market: str = "FUTURES") -> bool:
        import os
        import psycopg2

        if os.environ.get("BIAN_TRADING_HALTED", "false").strip().lower() in {
            "1", "true", "yes", "on"
        }:
            return True
        resolved_mode = self._mode(mode)
        resolved_market = str(market or "FUTURES").upper()
        try:
            with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT event_type
                        FROM system_events
                        WHERE event_type IN ('TRADING_HALTED', 'TRADING_RESUMED')
                          AND mode = %s
                          AND market = %s
                        ORDER BY event_at DESC
                        LIMIT 1
                        """,
                        (resolved_mode, resolved_market),
                    )
                    row = cursor.fetchone()
        except Exception:
            return True
        return row is not None and row[0] == "TRADING_HALTED"

    def set_halt(
        self,
        halted: bool,
        *,
        reason: str,
        source: str,
        mode: str | None = None,
        market: str = "FUTURES",
    ) -> UUID:
        resolved_mode = self._mode(mode)
        resolved_market = str(market or "FUTURES").upper()
        return self.record_system_event(
            event_type="TRADING_HALTED" if halted else "TRADING_RESUMED",
            severity="CRITICAL" if halted else "INFO",
            message=reason,
            payload={
                "source": source,
                "halted": halted,
                "mode": resolved_mode,
                "market": resolved_market,
            },
            mode=resolved_mode,
        )

    def set_user_stream_health(
        self,
        status: str,
        *,
        reason: str | None = None,
        mode: str,
    ) -> None:
        resolved_mode = self._mode(mode)
        normalized = str(status or "UNKNOWN").strip().upper() or "UNKNOWN"
        previous = str(getattr(self, "_user_stream_health", "") or "").upper()
        self._user_stream_health = normalized
        if previous == normalized and reason is None:
            return
        self.record_system_event(
            event_type="USER_STREAM_HEALTH",
            severity="CRITICAL" if normalized in {"FAILED", "DEGRADED", "UNKNOWN"} else "INFO",
            message=reason or f"user stream {normalized}",
            payload={
                "status": normalized,
                "health": normalized,
                "reason": reason,
                "mode": resolved_mode,
            },
            mode=resolved_mode,
        )

    def user_stream_health(self) -> str:
        current = getattr(self, "_user_stream_health", None)
        if current:
            return str(current).upper()
        return "UNKNOWN"

    def record_intent(self, intent: TradeIntent, *, status: str = "CREATED") -> None:
        import psycopg2

        if intent.evidence_snapshot_id is None:
            raise ValueError("production TradeIntent requires an evidence snapshot")
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO trade_intents(
                        intent_id, symbol, side, order_type, quantity,
                        price, confidence, reason,
                        strategy_version, status, created_at, client_order_id,
                        direction, action, reduce_only, leverage, margin_type,
                        position_mode, positioning_state, previous_state,
                        transition, evidence_snapshot_id, payload, validation_session_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, CAST(%s AS JSONB), %s)
                    ON CONFLICT (intent_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        client_order_id = EXCLUDED.client_order_id,
                        payload = EXCLUDED.payload
                    """,
                    (
                        str(intent.id),
                        intent.symbol,
                        intent.exchange_side(),
                        intent.order_type,
                        intent.quantity,
                        intent.price,
                        intent.confidence,
                        intent.reason,
                        intent.strategy_version,
                        status,
                        intent.created_at,
                        intent.client_order_id,
                        intent.direction,
                        intent.action,
                        intent.reduce_only,
                        intent.leverage,
                        intent.margin_type,
                        intent.position_mode,
                        intent.positioning_state,
                        intent.previous_state,
                        intent.transition,
                        str(intent.evidence_snapshot_id) if intent.evidence_snapshot_id else None,
                        _json(intent.model_dump(mode="json")),
                        self._session_id(),
                    ),
                )

    def record_market_flow_event(
        self,
        *,
        event_id: UUID | None = None,
        symbol: str,
        market: str,
        event_type: str,
        event_timestamp: datetime,
        received_timestamp: datetime,
        latency_ms: int,
        price: Decimal | None = None,
        quantity: Decimal | None = None,
        notional: Decimal | None = None,
        direction: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> UUID:
        import psycopg2

        event_id = event_id or uuid4()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO market_flow_events(
                        event_id, symbol, market, event_type, event_timestamp,
                        received_timestamp, latency_ms, price, quantity, notional,
                        direction, metadata, validation_session_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              CAST(%s AS JSONB), %s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (
                        str(event_id), symbol.upper(), market, event_type,
                        event_timestamp, received_timestamp, latency_ms, price,
                        quantity, notional, direction, _json(metadata),
                        self._session_id(),
                    ),
                )
        return event_id


    _EPISODE_COLUMNS = (
        "episode_id", "symbol", "market", "direction", "started_at",
        "ended_at", "state", "status", "last_observed_at",
        "strategy_version", "config_hash", "metadata",
    )

    def _episode_from_row(self, row: Any) -> dict[str, Any]:
        return _row_dict(self._EPISODE_COLUMNS, row)

    def _load_episode(self, episode_id: UUID | str) -> dict[str, Any] | None:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT episode_id, symbol, market, direction, started_at, ended_at,
                           state, status, last_observed_at, strategy_version,
                           config_hash, metadata
                    FROM positioning_episodes
                    WHERE episode_id = %s
                    """,
                    (str(episode_id),),
                )
                row = cursor.fetchone()
        return self._episode_from_row(row) if row is not None else None

    def get_active_episode(
        self, symbol: str, session_id: str | None = None, *, market: str = "FUTURES"
    ) -> dict[str, Any] | None:
        """Return the single persisted directional lifecycle for a symbol."""
        import psycopg2

        resolved_session = session_id if session_id is not None else self._session_id()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT episode_id, symbol, market, direction, started_at, ended_at,
                           state, status, last_observed_at, strategy_version,
                           config_hash, metadata
                    FROM positioning_episodes
                    WHERE symbol = %s
                      AND market = %s
                      AND status IN ('OPEN', 'UNRESOLVED')
                      AND validation_session_id IS NOT DISTINCT FROM %s
                    ORDER BY started_at DESC, episode_id DESC
                    LIMIT 1
                    """,
                    (symbol.upper(), market.upper(), resolved_session),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        return _row_dict(
            (
                "episode_id", "symbol", "market", "direction", "started_at",
                "ended_at", "state", "status", "last_observed_at",
                "strategy_version", "config_hash", "metadata",
            ),
            row,
        )

    def start_episode(
        self,
        *,
        symbol: str,
        direction: str,
        state: str,
        observed_at: datetime,
        strategy_version: str,
        config_hash: str,
        metadata: dict[str, Any] | None = None,
        market: str = "FUTURES",
    ) -> dict[str, Any]:
        """Create one active lifecycle, or atomically return the concurrent one."""
        import psycopg2

        if direction not in {"LONG", "SHORT"}:
            raise ValueError("episodes require a directional regime")
        episode_id = uuid4()
        observed_at = _as_utc(observed_at)
        session_id = self._session_id()
        if session_id:
            conflict = """
                    ON CONFLICT (symbol, market, validation_session_id)
                    WHERE status IN ('OPEN', 'UNRESOLVED')
                      AND validation_session_id IS NOT NULL
            """
        else:
            conflict = """
                    ON CONFLICT (symbol, market)
                    WHERE status IN ('OPEN', 'UNRESOLVED')
                      AND validation_session_id IS NULL
            """
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO positioning_episodes(
                        episode_id, symbol, market, direction, started_at, state,
                        status, last_observed_at, strategy_version, config_hash,
                        metadata, validation_session_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, 'OPEN', %s, %s, %s,
                              CAST(%s AS JSONB), %s)
                    """
                    + conflict
                    + """
                    DO UPDATE SET
                        last_observed_at = GREATEST(
                            positioning_episodes.last_observed_at,
                            EXCLUDED.last_observed_at
                        )
                    RETURNING episode_id, symbol, market, direction, started_at,
                              ended_at, state, status, last_observed_at,
                              strategy_version, config_hash, metadata
                    """,
                    (
                        str(episode_id), symbol.upper(), market.upper(), direction,
                        observed_at, state, observed_at, strategy_version, config_hash,
                        _json(metadata),
                        session_id,
                    ),
                )
                row = cursor.fetchone()
        return _row_dict(
            (
                "episode_id", "symbol", "market", "direction", "started_at",
                "ended_at", "state", "status", "last_observed_at",
                "strategy_version", "config_hash", "metadata",
            ),
            row,
        )

    def update_episode(
        self,
        episode_id: UUID | str,
        *,
        state: str,
        status: str,
        observed_at: datetime,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        import psycopg2

        if status not in {"OPEN", "UNRESOLVED"}:
            raise ValueError("use close_episode for terminal episode status")
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE positioning_episodes
                    SET state = %s,
                        status = %s,
                        last_observed_at = %s,
                        metadata = metadata || CAST(%s AS JSONB)
                    WHERE episode_id = %s
                      AND status IN ('OPEN', 'UNRESOLVED')
                      AND last_observed_at <= %s
                    RETURNING episode_id, symbol, market, direction, started_at,
                              ended_at, state, status, last_observed_at,
                              strategy_version, config_hash, metadata
                    """,
                    (
                        state, status, _as_utc(observed_at), _json(metadata),
                        str(episode_id), _as_utc(observed_at),
                    ),
                )
                row = cursor.fetchone()
        if row is None:
            current = self._load_episode(episode_id)
            if current is None:
                raise RuntimeError("active positioning episode disappeared")
            return current
        return self._episode_from_row(row)

    def close_episode(
        self,
        episode_id: UUID | str,
        *,
        state: str,
        observed_at: datetime,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        import psycopg2

        observed_at = _as_utc(observed_at)
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE positioning_episodes
                    SET state = %s,
                        status = 'CLOSED',
                        ended_at = %s,
                        last_observed_at = %s,
                        metadata = metadata || CAST(%s AS JSONB)
                    WHERE episode_id = %s
                      AND status IN ('OPEN', 'UNRESOLVED')
                      AND last_observed_at <= %s
                    RETURNING episode_id, symbol, market, direction, started_at,
                              ended_at, state, status, last_observed_at,
                              strategy_version, config_hash, metadata
                    """,
                    (
                        state, observed_at, observed_at, _json(metadata),
                        str(episode_id), observed_at,
                    ),
                )
                row = cursor.fetchone()
        if row is None:
            current = self._load_episode(episode_id)
            if current is None:
                raise RuntimeError("active positioning episode disappeared")
            return current
        return self._episode_from_row(row)

    def record_positioning_snapshot(
        self, decision: Any, *, strategy_version: str
    ) -> UUID:
        import psycopg2

        snapshot_id = decision.evidence_snapshot_id
        payload = decision.as_dict()
        session_id = self._session_id()
        if session_id:
            snapshot_conflict = """
                    ON CONFLICT (snapshot_id, validation_session_id)
                    WHERE validation_session_id IS NOT NULL
            """
        else:
            snapshot_conflict = """
                    ON CONFLICT (snapshot_id)
                    WHERE validation_session_id IS NULL
            """
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO positioning_snapshots(
                            snapshot_id, symbol, observed_at, state, transition,
                            direction, confidence, long_score, short_score,
                            crowding_score, liquidity_score, data_quality_score,
                            strategy_version, reason_codes, episode_id,
                            episode_direction, episode_status, episode_started_at,
                            episode_ended_at, evidence_sufficiency, is_meme,
                            meme_classification_source, meme_classification_version,
                            meme_classified_at, meme_reason_codes, payload
                            , validation_session_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                  %s, %s, %s, %s, %s, %s, %s,
                                  CAST(%s AS JSONB), %s, %s, %s, %s, %s,
                                  CAST(%s AS JSONB), CAST(%s AS JSONB), %s)
                        """
                        + snapshot_conflict
                        + """
                        DO UPDATE SET
                            episode_id = EXCLUDED.episode_id,
                            episode_direction = EXCLUDED.episode_direction,
                            episode_status = EXCLUDED.episode_status,
                            episode_started_at = EXCLUDED.episode_started_at,
                            episode_ended_at = EXCLUDED.episode_ended_at,
                            evidence_sufficiency = EXCLUDED.evidence_sufficiency,
                            is_meme = EXCLUDED.is_meme,
                            meme_classification_source = EXCLUDED.meme_classification_source,
                            meme_classification_version = EXCLUDED.meme_classification_version,
                            meme_classified_at = EXCLUDED.meme_classified_at,
                            meme_reason_codes = EXCLUDED.meme_reason_codes,
                            payload = EXCLUDED.payload
                        """,
                        (
                            str(snapshot_id), decision.symbol, decision.timestamp,
                            decision.state, decision.transition, decision.direction,
                            decision.confidence, decision.long_score, decision.short_score,
                            decision.crowding_score, decision.liquidity_score,
                            decision.data_quality_score, strategy_version,
                            _json(list(decision.reason_codes)),
                            str(decision.episode_id) if decision.episode_id else None,
                            decision.episode_direction, decision.episode_status,
                            decision.episode_started_at, decision.episode_ended_at,
                            _json(decision.evidence_sufficiency.as_dict()),
                            decision.is_meme, decision.meme_classification_source,
                            decision.meme_classification_version,
                            decision.meme_classified_at,
                            _json(list(decision.meme_reason_codes)), _json(payload),
                            session_id,
                        ),
                    )
                    cursor.execute(
                        """
                        INSERT INTO evidence_snapshots(
                            snapshot_id, symbol, observed_at, source_timestamps,
                            evidence, data_quality, episode_id, episode_started_at,
                            episode_ended_at, episode_direction, episode_status,
                            universe_classification, payload, validation_session_id
                        ) VALUES (%s, %s, %s, CAST(%s AS JSONB), CAST(%s AS JSONB),
                                  CAST(%s AS JSONB), %s, %s, %s, %s, %s, %s,
                                  CAST(%s AS JSONB), CAST(%s AS JSONB), %s)
                        """
                        + snapshot_conflict
                        + """
                        DO UPDATE SET
                            source_timestamps = EXCLUDED.source_timestamps,
                            evidence = EXCLUDED.evidence,
                            data_quality = EXCLUDED.data_quality,
                            episode_id = EXCLUDED.episode_id,
                            episode_started_at = EXCLUDED.episode_started_at,
                            episode_ended_at = EXCLUDED.episode_ended_at,
                            episode_direction = EXCLUDED.episode_direction,
                            episode_status = EXCLUDED.episode_status,
                            universe_classification = EXCLUDED.universe_classification,
                            payload = EXCLUDED.payload
                        """,
                        (
                            str(snapshot_id), decision.symbol, decision.timestamp,
                            _json(dict(decision.source_timestamps)),
                            _json(decision.input_features),
                            _json({
                                "quality": decision.data_quality_score,
                                "freshness": decision.evidence.freshness,
                                "sufficiency": decision.evidence_sufficiency.as_dict(),
                            }),
                            str(decision.episode_id) if decision.episode_id else None,
                            decision.episode_started_at, decision.episode_ended_at,
                            decision.episode_direction, decision.episode_status,
                            _json({
                                "is_meme": decision.is_meme,
                                "source": decision.meme_classification_source,
                                "version": decision.meme_classification_version,
                                "classified_at": decision.meme_classified_at,
                                "reason_codes": list(decision.meme_reason_codes),
                            }),
                            _json(payload),
                            session_id,
                        ),
                    )
                    if session_id:
                        observed_at = _as_utc(decision.timestamp)
                        identity = str(snapshot_id)
                        cursor.execute(
                            """
                            INSERT INTO validation_session_observations(
                                session_id, observation_id, symbol, observed_at, source
                            ) VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (session_id, observation_id) DO NOTHING
                            """,
                            (
                                session_id,
                                identity,
                                str(decision.symbol).upper(),
                                observed_at,
                                "positioning_pipeline",
                            ),
                        )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return snapshot_id

    def record_liquidation_event(
        self,
        *,
        event_id: UUID | None = None,
        symbol: str,
        side: str,
        price: Decimal,
        quantity: Decimal,
        event_timestamp: datetime,
        received_timestamp: datetime,
        latency_ms: int,
        payload: dict[str, Any] | None = None,
    ) -> UUID:
        import psycopg2

        event_id = event_id or uuid4()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO liquidation_events(
                        event_id, symbol, market, side, price, quantity,
                        notional, event_timestamp, received_timestamp, latency_ms,
                        payload
                    ) VALUES (%s, %s, 'FUTURES', %s, %s, %s, %s, %s, %s, %s,
                              CAST(%s AS JSONB))
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (
                        str(event_id), symbol.upper(), side, price, quantity,
                        price * quantity, event_timestamp, received_timestamp,
                        latency_ms, _json(payload),
                    ),
                )
        return event_id

    def list_positioning_snapshots(self, limit: int = 50) -> list[dict[str, Any]]:
        import psycopg2

        bounded_limit = max(1, min(int(limit), 200))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT snapshot_id, symbol, observed_at, state, transition,
                           direction, confidence, long_score, short_score,
                           crowding_score, liquidity_score, data_quality_score,
                           strategy_version, reason_codes, payload
                    FROM positioning_snapshots
                    ORDER BY observed_at DESC
                    LIMIT %s
                    """, (bounded_limit,),
                )
                rows = cursor.fetchall()
        columns = (
            "snapshot_id", "symbol", "observed_at", "state", "transition",
            "direction", "confidence", "long_score", "short_score",
            "crowding_score", "liquidity_score", "data_quality_score",
            "strategy_version", "reason_codes", "payload",
        )
        return [_row_dict(columns, row) for row in rows]

    def latest_positioning_state(
        self,
        symbol: str,
        *,
        before: datetime,
        validation_session_id: str | None = None,
    ) -> str | None:
        """Return the persisted predecessor state for a timestamp-bounded decision.

        Validation paths are session-scoped: session B cannot read session A's
        previous_state even when the content-addressed snapshot_id matches.
        """
        import psycopg2

        session_id = (
            validation_session_id
            if validation_session_id is not None
            else self._session_id()
        )
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT state
                    FROM positioning_snapshots
                    WHERE symbol = %s
                      AND observed_at < %s
                      AND validation_session_id IS NOT DISTINCT FROM %s
                    ORDER BY observed_at DESC, snapshot_id DESC
                    LIMIT 1
                    """,
                    (symbol.upper(), _as_utc(before), session_id),
                )
                row = cursor.fetchone()
        return str(row[0]) if row is not None else None

    def list_market_flow_events(self, limit: int = 50) -> list[dict[str, Any]]:
        import psycopg2

        bounded_limit = max(1, min(int(limit), 200))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT event_id, symbol, market, event_type, event_timestamp,
                           received_timestamp, latency_ms, price, quantity,
                           notional, direction, metadata
                    FROM market_flow_events
                    ORDER BY event_timestamp DESC
                    LIMIT %s
                    """, (bounded_limit,),
                )
                rows = cursor.fetchall()
        columns = (
            "event_id", "symbol", "market", "event_type", "event_timestamp",
            "received_timestamp", "latency_ms", "price", "quantity",
            "notional", "direction", "metadata",
        )
        return [_row_dict(columns, row) for row in rows]

    def positioning_events(
        self,
        symbol: str,
        *,
        as_of: datetime,
        lookback_seconds: int = 86_400,
    ) -> list[dict[str, Any]]:
        """Return only observations available at a historical decision time.

        Both exchange event time and local received time are constrained. The
        latter is the no-lookahead boundary: an old exchange event that arrived
        after ``as_of`` is unavailable to the decision and must be excluded.
        """
        import psycopg2

        cutoff = _as_utc(as_of)
        start = cutoff - timedelta(seconds=max(1, lookback_seconds))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT event_id, symbol, market, event_type, event_timestamp,
                           received_timestamp, latency_ms, price, quantity,
                           notional, direction, metadata
                    FROM market_flow_events
                    WHERE symbol = %s
                      AND market = 'FUTURES'
                      AND event_timestamp >= %s
                      AND event_timestamp <= %s
                      AND received_timestamp <= %s
                    ORDER BY event_timestamp, received_timestamp, event_id
                    """,
                    (symbol.upper(), start, cutoff, cutoff),
                )
                rows = cursor.fetchall()
        columns = (
            "event_id", "symbol", "market", "event_type", "event_timestamp",
            "received_timestamp", "latency_ms", "price", "quantity",
            "notional", "direction", "metadata",
        )
        return [_row_dict(columns, row) for row in rows]

    def latest_market_observation(
        self,
        symbol: str,
        *,
        source_ttl_sec: int = 900,
        lookback_seconds: int = 86_400,
    ) -> Any:
        """Build one MarketFrame from persisted public observations only."""
        import psycopg2
        from engine import MarketFrame, SourceFreshness
        from scripts.bian_market import positioning_feature_values

        normalized_symbol = symbol.upper()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT captured_at, last_price, quote_volume
                    FROM bian_market_snapshots
                    WHERE symbol = %s
                      AND market = 'FUTURES'
                    ORDER BY captured_at DESC, id DESC
                    LIMIT 100
                    """,
                    (normalized_symbol,),
                )
                rows = list(reversed(cursor.fetchall()))
        if not rows:
            raise RuntimeError(
                f"canonical market observation is unavailable for {normalized_symbol}"
            )
        captured_at = _as_utc(rows[-1][0])
        closes = tuple(Decimal(str(row[1])) for row in rows if row[1] is not None)
        if not closes:
            raise RuntimeError(
                f"canonical market observation has no price for {normalized_symbol}"
            )
        events = self.positioning_events(
            normalized_symbol,
            as_of=captured_at,
            lookback_seconds=lookback_seconds,
        )
        features = positioning_feature_values(events, as_of=captured_at)
        universe = self.market_universe_context(normalized_symbol, as_of=captured_at)
        universe_timestamps = dict(universe.pop("source_timestamps", {}))
        features.update({
            name: value
            for name, value in universe.items()
            if name in {
                "relative_strength", "relative_strength_1m",
                "relative_strength_5m", "relative_strength_15m",
                "relative_strength_1h", "breadth_score",
                "advance_decline_ratio", "market_regime", "meme_risk_tier",
                "is_meme", "meme_classification_source",
                "meme_classification_version", "meme_classified_at",
                "meme_reason_codes",
            }
        })
        source_timestamps = {
            "futures_snapshot": {
                "source_timestamp": captured_at.isoformat(),
                "received_timestamp": captured_at.isoformat(),
                "latency_ms": 0,
            },
            **dict(features.pop("source_timestamps", {})),
            **universe_timestamps,
        }
        freshness = tuple(
            SourceFreshness(
                source=source,
                source_timestamp=_as_utc(timestamps["source_timestamp"]),
                received_timestamp=_as_utc(timestamps["received_timestamp"]),
                max_age_sec=max(1, source_ttl_sec),
                now=captured_at,
                latency_ms=int(timestamps["latency_ms"]),
            )
            for source, timestamps in source_timestamps.items()
        )
        health = self.market_data_freshness(
            max_age_sec=source_ttl_sec,
            symbols=[normalized_symbol],
        )
        evidence_status: dict[str, str] = {}
        for row in health:
            if row["source"] == "FUTURES_DEPTH":
                status = str(row["status"]).upper()
                if status in {"GAP", "UNSAFE", "ERROR"}:
                    evidence_status["orderbook"] = "UNSAFE"
                elif status == "FRESH":
                    evidence_status["orderbook"] = "VALID"
        allowed = set(MarketFrame.__dataclass_fields__) - {
            "symbol", "closes", "captured_at", "quote_volume", "freshness",
            "source_timestamps", "evidence_status",
        }
        values = {name: value for name, value in features.items() if name in allowed}
        if values.get("meme_classified_at") is not None:
            values["meme_classified_at"] = _as_utc(values["meme_classified_at"])
        if values.get("meme_reason_codes") is not None:
            values["meme_reason_codes"] = tuple(values["meme_reason_codes"])
        return MarketFrame(
            symbol=normalized_symbol,
            closes=closes,
            captured_at=captured_at,
            quote_volume=(
                Decimal(str(rows[-1][2])) if rows[-1][2] is not None else None
            ),
            freshness=freshness,
            evidence_status=evidence_status,
            source_timestamps=source_timestamps,
            **values,
        )

    def market_data_freshness(
        self,
        *,
        max_age_sec: int = 900,
        symbols: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return one source-health record per required Futures source/symbol.

        Health tracks whether an observation is arriving, while source payloads
        retain their own measurement or settlement timestamps. In particular,
        the mark-price stream is the liveness source for funding; a prior
        funding settlement is not a stale realtime observation.
        """
        import psycopg2
        from engine import runtime_required_sources

        requested_symbols = {
            str(symbol).upper().replace("-PERP", "").removesuffix("PERP").replace("-", "")
            for symbol in (symbols or ())
            if str(symbol).strip()
        }
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL statement_timeout = '5000'")
                if requested_symbols:
                    cursor.execute(
                        """
                        SELECT DISTINCT ON (symbol, event_type)
                               symbol, market, event_type, event_timestamp,
                               received_timestamp, latency_ms, metadata
                        FROM market_flow_events
                        WHERE market = 'FUTURES'
                          AND symbol = ANY(%s)
                        ORDER BY symbol, event_type, received_timestamp DESC,
                                 event_timestamp DESC
                        """,
                        (list(requested_symbols),),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT DISTINCT ON (symbol, event_type)
                               symbol, market, event_type, event_timestamp,
                               received_timestamp, latency_ms, metadata
                        FROM market_flow_events
                        WHERE market = 'FUTURES'
                        ORDER BY symbol, event_type, received_timestamp DESC,
                                 event_timestamp DESC
                        """
                    )
                rows = cursor.fetchall()
        now = _now()
        source_events = {
            "FUTURES_TRADE": ("FUTURES_TRADE",),
            "BOOK_TICKER": ("FUTURES_BOOK_TICKER",),
            "ORDERBOOK": ("FUTURES_DEPTH",),
            "OPEN_INTEREST": ("FUTURES_OPEN_INTEREST",),
            "TAKER_RATIO": ("FUTURES_TAKER",),
            "MARK_INDEX_FUNDING": (
                "FUTURES_MARK_PRICE",
                "FUTURES_INDEX_PRICE",
                "FUTURES_FUNDING_LIVENESS",
            ),
            "LIQUIDATION_HEARTBEAT": ("FUTURES_LIQUIDATION_LIVENESS",),
        }
        canonical: dict[str, dict[str, tuple[str, Any, Any, int, dict[str, Any]]]] = {}
        symbols: set[str] = set()
        for symbol, market, event_type, source_at, received_at, latency_ms, metadata in rows:
            normalized_symbol = str(symbol).upper()
            if requested_symbols and normalized_symbol not in requested_symbols:
                continue
            symbols.add(normalized_symbol)
            payload = metadata if isinstance(metadata, dict) else {}
            aliases = {
                **source_events,
                "FORCE_ORDER": (),
            }.get(str(event_type), ())
            for source in aliases:
                existing = canonical.setdefault(source, {}).get(normalized_symbol)
                if existing is None or _as_utc(received_at) > _as_utc(existing[2]):
                    canonical[source][normalized_symbol] = (
                        str(market), source_at, received_at, int(latency_ms), payload
                    )

        result = []
        for source in sorted(runtime_required_sources()):
            for symbol in sorted(requested_symbols or symbols):
                record = canonical.get(source, {}).get(symbol)
                if record is None:
                    result.append({
                        "source": source,
                        "event_type": source,
                        "symbol": symbol,
                        "market": "FUTURES",
                        "source_timestamp": None,
                        "received_timestamp": None,
                        "latency_ms": None,
                        "age_sec": None,
                        "status": "MISSING",
                    })
                    continue
                market, source_at, received_at, latency_ms, payload = record
                source_dt = _as_utc(source_at)
                received_dt = _as_utc(received_at)
                age_sec = max(0, int((now - received_dt).total_seconds()))
                valid_timestamps = received_dt >= source_dt and source_dt <= now and received_dt <= now
                valid_latency = latency_ms >= 0 and latency_ms == int((received_dt - source_dt).total_seconds() * 1000)
                nested = (
                    payload.get("metadata")
                    if isinstance(payload.get("metadata"), dict)
                    else {}
                )
                reported_status = str(
                    payload.get("health_status")
                    or payload.get("health")
                    or nested.get("health_status")
                    or nested.get("health")
                    or ""
                ).upper()
                reported_state = str(
                    payload.get("state") or nested.get("state") or ""
                ).upper()
                reported_unhealthy = reported_status in {
                    "GAP", "UNSAFE", "ERROR", "STALE", "SYNCING", "UNINITIALIZED",
                    "FAILED", "RECONNECTING", "STARTING",
                } or reported_state in {
                    "GAP", "UNSAFE", "ERROR", "SYNCING", "UNINITIALIZED", "FAILED",
                }
                if reported_status in {
                    "GAP", "UNSAFE", "ERROR", "STALE", "SYNCING", "UNINITIALIZED",
                    "FAILED", "RECONNECTING", "STARTING",
                }:
                    status = reported_status
                elif reported_state in {
                    "GAP", "UNSAFE", "ERROR", "SYNCING", "UNINITIALIZED", "FAILED",
                }:
                    status = reported_state
                elif not valid_timestamps or not valid_latency:
                    status = "ERROR"
                elif age_sec > max(1, max_age_sec):
                    status = "STALE"
                else:
                    status = "FRESH"
                spec = None
                try:
                    from engine import _source_spec
                    spec = _source_spec(source)
                except Exception:
                    spec = None
                expected_interval = spec.expected_interval_sec if spec else None
                ttl_sec = spec.ttl_sec if spec else max_age_sec
                gap_duration = (
                    max(0, age_sec - expected_interval)
                    if expected_interval is not None else 0
                )
                consecutive_gap_count = int(payload.get("consecutive_gap_count") or 0)
                if expected_interval is not None and age_sec > expected_interval:
                    consecutive_gap_count = max(1, consecutive_gap_count)
                elif expected_interval is not None:
                    consecutive_gap_count = 0
                if (
                    spec is not None
                    and spec.event_driven
                    and status == "STALE"
                    and not reported_unhealthy
                    and age_sec <= max(1, ttl_sec)
                ):
                    status = "FRESH"
                result.append({
                    "source": source,
                    "event_type": source,
                    "symbol": symbol,
                    "market": market,
                    "source_timestamp": source_dt.isoformat(),
                    "received_timestamp": received_dt.isoformat(),
                    "latency_ms": latency_ms,
                    "age_sec": age_sec,
                    "status": status,
                    "last_seen": received_dt.isoformat(),
                    "expected_interval": expected_interval,
                    "gap_duration": gap_duration,
                    "consecutive_gap_count": consecutive_gap_count,
                    "transport_source": payload.get("source"),
                    "metadata": payload,
                })
        # Liveness and sparse event existence are separate observations. A
        # quiet force-order channel is healthy when its heartbeat is fresh.
        for symbol in sorted(requested_symbols or symbols):
            _heartbeat = canonical.get("FUTURES_LIQUIDATION_LIVENESS", {}).get(symbol)
            heartbeat_row = next(
                (
                    row for row in result
                    if row["source"] == "FUTURES_LIQUIDATION_LIVENESS"
                    and row["symbol"] == symbol
                ),
                None,
            )
            if heartbeat_row is not None:
                heartbeat_row["liveness_source"] = "FUTURES_LIQUIDATION_LIVENESS"
            event_record = next(
                (
                    (source_at, received_at, payload)
                    for row_symbol, market, event_type, source_at, received_at, _, payload
                    in rows
                    if str(row_symbol).upper() == symbol
                    and str(event_type).upper() == "FORCE_ORDER"
                ),
                None,
            )
            result.append({
                "source": "FUTURES_FORCE_ORDER",
                "event_type": "FORCE_ORDER",
                "symbol": symbol,
                "market": "FUTURES",
                "source_timestamp": (
                    _as_utc(event_record[0]).isoformat() if event_record else None
                ),
                "received_timestamp": (
                    _as_utc(event_record[1]).isoformat() if event_record else None
                ),
                "latency_ms": None,
                "age_sec": None,
                "status": "PRESENT" if event_record else "MISSING",
                "semantics": "observed_liquidation_not_total_market_volume",
            })
        return result

    def collector_lifecycle_events(self) -> list[dict[str, Any]]:
        """Return canonical websocket lifecycle events from WS_LIFECYCLE rows."""
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT metadata
                    FROM market_flow_events
                    WHERE market = 'FUTURES'
                      AND event_type = 'WS_LIFECYCLE'
                    ORDER BY received_timestamp ASC
                    LIMIT 200
                    """
                )
                rows = cursor.fetchall()
        events: list[dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()
        for (metadata,) in rows:
            payload = metadata if isinstance(metadata, dict) else {}
            nested = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
            event = nested if nested else payload
            if not isinstance(event, dict):
                continue
            key = (
                event.get("channel"),
                event.get("action"),
                event.get("old_connection_id"),
                event.get("new_connection_id"),
                event.get("disconnect_at"),
                event.get("reconnect_at"),
            )
            if key in seen:
                continue
            seen.add(key)
            events.append(dict(event))
        return events

    @staticmethod
    def _datetime(value: Any) -> datetime:
        return _as_utc(value)

    def market_universe_context(
        self,
        symbol: str,
        *,
        as_of: datetime,
        windows_seconds: tuple[int, ...] = (60, 300, 900, 3600),
    ) -> dict[str, Any]:
        """Return timestamp-bounded breadth and relative-strength aggregates.

        The method reads only the latest observations received by ``as_of``.
        It deliberately returns missing values when the scanner has not yet
        accumulated enough history for a requested window.
        """
        import psycopg2

        cutoff = _as_utc(as_of)
        normalized_symbol = symbol.upper()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT event_timestamp, received_timestamp, latency_ms, metadata
                    FROM market_flow_events
                    WHERE symbol = '__MARKET__'
                      AND market = 'FUTURES'
                      AND event_type = 'UNIVERSE_BREADTH'
                      AND event_timestamp <= %s
                      AND received_timestamp <= %s
                    ORDER BY event_timestamp DESC, received_timestamp DESC
                    LIMIT 1
                    """,
                    (cutoff, cutoff),
                )
                breadth_row = cursor.fetchone()
                if breadth_row is None:
                    return {}
                source_at, received_at, latency_ms, stored_metadata = breadth_row
                event_metadata = dict(stored_metadata or {})
                scanner = event_metadata.get("metadata", event_metadata)
                if not isinstance(scanner, dict):
                    return {}

                def price_return(asset: str, window_seconds: int) -> Decimal | None:
                    cursor.execute(
                        """
                        WITH current AS (
                            SELECT last_price, captured_at
                            FROM bian_market_snapshots
                            WHERE symbol = %s
                              AND market = 'FUTURES'
                              AND captured_at <= %s
                            ORDER BY captured_at DESC, id DESC
                            LIMIT 1
                        ), baseline AS (
                            SELECT snapshot.last_price
                            FROM bian_market_snapshots snapshot, current
                            WHERE snapshot.symbol = %s
                              AND snapshot.market = 'FUTURES'
                              AND snapshot.captured_at <= current.captured_at
                                  - make_interval(secs => %s)
                            ORDER BY snapshot.captured_at DESC, snapshot.id DESC
                            LIMIT 1
                        )
                        SELECT (current.last_price - baseline.last_price) / baseline.last_price
                        FROM current, baseline
                        WHERE baseline.last_price > 0
                        """,
                        (asset, cutoff, asset, max(1, window_seconds)),
                    )
                    row = cursor.fetchone()
                    return Decimal(str(row[0])) if row and row[0] is not None else None

                def basket_return(window_seconds: int) -> Decimal | None:
                    cursor.execute(
                        """
                        WITH current AS (
                            SELECT DISTINCT ON (symbol) symbol, last_price, captured_at
                            FROM bian_market_snapshots
                            WHERE captured_at <= %s
                              AND market = 'FUTURES'
                            ORDER BY symbol, captured_at DESC, id DESC
                        ), returns AS (
                            SELECT (current.last_price - baseline.last_price)
                                   / baseline.last_price AS value
                            FROM current
                            JOIN LATERAL (
                                SELECT last_price
                                FROM bian_market_snapshots snapshot
                                WHERE snapshot.symbol = current.symbol
                                  AND snapshot.market = 'FUTURES'
                                  AND snapshot.captured_at <= current.captured_at
                                      - make_interval(secs => %s)
                                ORDER BY snapshot.captured_at DESC, snapshot.id DESC
                                LIMIT 1
                            ) baseline ON baseline.last_price > 0
                        )
                        SELECT AVG(value) FROM returns
                        """,
                        (cutoff, max(1, window_seconds)),
                    )
                    row = cursor.fetchone()
                    return Decimal(str(row[0])) if row and row[0] is not None else None

                relative: dict[str, Decimal] = {}
                for window in windows_seconds:
                    asset = price_return(normalized_symbol, window)
                    references = [
                        value for value in (
                            price_return("BTCUSDT", window),
                            price_return("ETHUSDT", window),
                            basket_return(window),
                        )
                        if value is not None
                    ]
                    if asset is not None and references:
                        relative[f"relative_strength_{window // 60}m"] = (
                            asset - sum(references, Decimal("0")) / Decimal(len(references))
                        )

        values = list(relative.values())
        symbol_rows = scanner.get("symbols") if isinstance(scanner.get("symbols"), list) else []
        symbol_row = next(
            (
                row for row in symbol_rows
                if isinstance(row, dict)
                and str(row.get("symbol", "")).upper() == normalized_symbol
            ),
            {},
        )
        return {
            "relative_strength": (
                sum(values, Decimal("0")) / Decimal(len(values)) if values else None
            ),
            "relative_strength_1m": relative.get("relative_strength_1m"),
            "relative_strength_5m": relative.get("relative_strength_5m"),
            "relative_strength_15m": relative.get("relative_strength_15m"),
            "relative_strength_1h": relative.get("relative_strength_60m"),
            "breadth_score": Decimal(str(scanner["breadth_score"]))
            if scanner.get("breadth_score") is not None else None,
            "advance_decline_ratio": Decimal(str(scanner["advance_decline_ratio"]))
            if scanner.get("advance_decline_ratio") is not None else None,
            "market_regime": str(scanner.get("market_regime", "NEUTRAL")),
            "meme_risk_tier": str(symbol_row.get("meme_risk_tier", "OBSERVE")),
            "is_meme": symbol_row.get("is_meme"),
            "meme_classification_source": symbol_row.get(
                "meme_classification_source",
                symbol_row.get("classification_source"),
            ),
            "meme_classification_version": symbol_row.get(
                "meme_classification_version",
                symbol_row.get("classification_version"),
            ),
            "meme_classified_at": symbol_row.get(
                "meme_classified_at", symbol_row.get("classified_at")
            ),
            "meme_reason_codes": tuple(
                symbol_row.get("meme_reason_codes")
                or symbol_row.get("reason_codes")
                or ()
            ),
            "source_timestamps": {
                "market_universe": {
                    "source_timestamp": _as_utc(source_at).isoformat(),
                    "received_timestamp": _as_utc(received_at).isoformat(),
                    "latency_ms": int(latency_ms),
                }
            },
        }

    def market_flow_storage_metrics(self) -> dict[str, Any]:
        """Measure raw-event throughput before an operator chooses retention."""
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*), MIN(event_timestamp), MAX(event_timestamp),
                           pg_total_relation_size('market_flow_events')
                    FROM market_flow_events
                    """
                )
                count, earliest, latest, bytes_used = cursor.fetchone()
                cursor.execute(
                    """
                    SELECT event_type, COUNT(*), MIN(event_timestamp), MAX(event_timestamp)
                    FROM market_flow_events
                    GROUP BY event_type
                    ORDER BY event_type
                    """
                )
                event_type_rows = cursor.fetchall()
                cursor.execute(
                    """
                    SELECT DATE_TRUNC('day', event_timestamp), COUNT(*)
                    FROM market_flow_events
                    GROUP BY 1
                    ORDER BY 1 DESC
                    LIMIT 30
                    """
                )
                daily = [
                    {"day": _as_utc(day).date().isoformat(), "events": int(rows)}
                    for day, rows in cursor.fetchall()
                ]
        bytes_per_event = (
            Decimal(str(bytes_used)) / Decimal(str(count)) if count else Decimal("0")
        )

        def annualized_rate(
            event_count: int,
            first_seen: datetime | None,
            last_seen: datetime | None,
        ) -> tuple[int, Decimal | None]:
            if first_seen is None or last_seen is None:
                return 0, None
            span_seconds = max(
                0,
                int((_as_utc(last_seen) - _as_utc(first_seen)).total_seconds()),
            )
            if span_seconds == 0:
                return span_seconds, None
            return (
                span_seconds,
                Decimal(str(event_count)) * Decimal("86400") / Decimal(str(span_seconds)),
            )

        observed_span_seconds, estimated_events_per_day = annualized_rate(
            int(count), earliest, latest
        )
        by_type = {str(event_type): int(rows) for event_type, rows, _, _ in event_type_rows}
        event_type_throughput = {}
        for event_type, rows, first_seen, last_seen in event_type_rows:
            span_seconds, events_per_day = annualized_rate(
                int(rows), first_seen, last_seen
            )
            event_type_throughput[str(event_type)] = {
                "events": int(rows),
                "earliest_event_timestamp": _as_utc(first_seen).isoformat()
                if first_seen else None,
                "latest_event_timestamp": _as_utc(last_seen).isoformat()
                if last_seen else None,
                "observed_span_seconds": span_seconds,
                "estimated_events_per_day": events_per_day,
                "estimated_storage_bytes_per_day": (
                    bytes_per_event * events_per_day
                    if events_per_day is not None else None
                ),
            }
        return {
            "events": int(count),
            "earliest_event_timestamp": _as_utc(earliest).isoformat() if earliest else None,
            "latest_event_timestamp": _as_utc(latest).isoformat() if latest else None,
            "storage_bytes": int(bytes_used),
            "storage_bytes_per_event": bytes_per_event,
            "observed_span_seconds": observed_span_seconds,
            "estimated_events_per_day": estimated_events_per_day,
            "estimated_storage_bytes_per_day": (
                bytes_per_event * estimated_events_per_day
                if estimated_events_per_day is not None else None
            ),
            "by_event_type": by_type,
            "event_type_throughput": event_type_throughput,
            "daily": daily,
        }

    def prune_raw_market_flow_events(self, *, retention_days: int) -> int:
        """Delete only raw high-rate observations after explicit operator input."""
        import psycopg2

        if retention_days < 1:
            raise ValueError("retention_days must be at least one day")
        cutoff = _now() - timedelta(days=retention_days)
        raw_event_types = ("TRADE", "BOOK_TICKER", "ORDERBOOK")
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM market_flow_events
                    WHERE event_type = ANY(%s) AND event_timestamp < %s
                    """,
                    (list(raw_event_types), cutoff),
                )
                return int(cursor.rowcount)

    def positioning_replay_frames(
        self,
        symbol: str | None = None,
        *,
        limit: int = 10_000,
        source_ttl_sec: int = 900,
        validation_session_id: str | None = None,
        research_session_ids: Iterable[str] | None = None,
    ) -> list[Any]:
        """Load historical normalized frames for one explicit dataset scope.

        An explicit ``validation_session_id`` reads that session only. A
        research session list is the only allowed multi-session scope. The
        bound store session is used when neither is supplied. Mixing every
        snapshot in the table is forbidden.
        """
        import psycopg2
        from engine import MarketFrame

        bounded_limit = max(1, min(int(limit), 100_000))
        if research_session_ids is not None:
            session_ids = tuple(
                str(value).strip()
                for value in research_session_ids
                if str(value).strip()
            )
            if not session_ids:
                raise ValueError("research_session_ids must not be empty")
        elif validation_session_id is not None:
            session_id = str(validation_session_id).strip()
            if not session_id:
                raise ValueError("validation_session_id is required")
            session_ids = (session_id,)
        else:
            bound = self._session_id()
            if not bound:
                raise ValueError(
                    "positioning_replay_frames requires validation_session_id"
                )
            session_ids = (bound,)
        clauses = ["validation_session_id = ANY(%s)"]
        params: list[Any] = [list(session_ids)]
        if symbol:
            clauses.append("symbol = %s")
            params.append(symbol.upper())
        params.append(bounded_limit)
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT payload, strategy_version, symbol
                    FROM positioning_snapshots
                    WHERE """
                    + " AND ".join(clauses)
                    + """
                    ORDER BY observed_at, snapshot_id
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cursor.fetchall()
        versions: set[str] = set()
        markets: set[str] = set()
        frames: list[Any] = []
        for payload, strategy_version, stored_symbol in rows:
            if not isinstance(payload, dict):
                continue
            if not is_canonical_futures_symbol(stored_symbol):
                raise ValueError("ALPHA_DATASET_UNAUTHORIZED_SYMBOL")
            market = str(payload.get("market") or "").upper()
            inputs = payload.get("input_features") or {}
            if isinstance(inputs, dict) and not market:
                market = str(inputs.get("market") or "").upper()
            timestamps = payload.get("source_timestamps") or {}
            if (
                not market
                and isinstance(timestamps, dict)
                and "spot_trade" in timestamps
                and "futures_trade_flow" not in timestamps
            ):
                market = "SPOT"
            if market in {"SPOT", "UNKNOWN"}:
                raise ValueError("ALPHA_DATASET_WRONG_MARKET")
            if market:
                markets.add(market)
            if strategy_version:
                versions.add(str(strategy_version))
            frames.append(
                MarketFrame.from_evidence_snapshot(
                    payload, source_ttl_sec=source_ttl_sec
                )
            )
        if len(versions) > 1:
            raise ValueError("ALPHA_DATASET_MIXED_STRATEGY_VERSION")
        if any(item and item != "FUTURES" for item in markets):
            raise ValueError("ALPHA_DATASET_WRONG_MARKET")
        return frames

    def update_intent_status(self, intent_id: UUID, status: str) -> None:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE trade_intents SET status = %s WHERE intent_id = %s",
                    (status, str(intent_id)),
                )

    def create_order(
        self,
        intent: TradeIntent,
        *,
        mode: str,
        status: str,
        exchange_order_id: str | None = None,
        order_id: UUID | None = None,
        expires_at: datetime | None = None,
    ) -> UUID:
        import psycopg2

        order_id = order_id or uuid4()
        now = _now()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO orders(
                        order_id, intent_id, symbol, side, order_type,
                        client_order_id, exchange_order_id, quantity,
                        price, status, mode, created_at,
                        updated_at, expires_at, market, position_side,
                        position_action, reduce_only, leverage, margin_type,
                        payload, validation_session_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              CAST(%s AS JSONB), %s)
                    ON CONFLICT (mode, client_order_id) DO UPDATE SET
                        updated_at = EXCLUDED.updated_at
                    RETURNING order_id
                    """,
                    (
                        str(order_id),
                        str(intent.id),
                        intent.symbol,
                        intent.exchange_side(),
                        intent.order_type,
                        intent.client_order_id,
                        exchange_order_id,
                        intent.quantity,
                        intent.price,
                        status,
                        mode,
                        now,
                        now,
                        expires_at,
                        "FUTURES",
                        intent.direction,
                        intent.action,
                        intent.reduce_only,
                        intent.leverage,
                        intent.margin_type,
                        _json(intent.model_dump(mode="json")),
                        self._session_id(),
                    ),
                )
                row = cursor.fetchone()
                return UUID(str(row[0]))

    def update_order(
        self,
        order_id: UUID,
        *,
        status: str,
        executed_quantity: Decimal | None = None,
        exchange_order_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        import psycopg2

        fields = ["status = %s", "updated_at = %s"]
        values: list[Any] = [status, _now()]
        if executed_quantity is not None:
            fields.append("executed_quantity = %s")
            values.append(executed_quantity)
        if exchange_order_id is not None:
            fields.append("exchange_order_id = %s")
            values.append(exchange_order_id)
        if payload is not None:
            fields.append("payload = CAST(%s AS JSONB)")
            values.append(_json(payload))
        values.append(str(order_id))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"UPDATE orders SET {', '.join(fields)} WHERE order_id = %s",
                    values,
                )

    def get_order_by_client_order_id(self, client_order_id: str, *, mode: str | None = None) -> dict[str, Any] | None:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT order_id, intent_id, symbol, side, order_type,
                           client_order_id, exchange_order_id, quantity,
                           price, executed_quantity, status,
                           mode, created_at, updated_at, expires_at,
                           position_side, position_action, reduce_only, leverage
                    FROM orders
                    WHERE client_order_id = %s AND mode = %s
                    """,
                    (client_order_id, self._mode(mode)),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        columns = (
            "order_id", "intent_id", "symbol", "side", "order_type",
            "client_order_id", "exchange_order_id", "quantity",
            "price", "executed_quantity", "status",
            "mode", "created_at", "updated_at", "expires_at",
            "position_side", "position_action", "reduce_only", "leverage",
        )
        return _row_dict(columns, row)

    def get_order(self, order_id: UUID, *, mode: str | None = None) -> dict[str, Any] | None:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT order_id, intent_id, symbol, side, order_type,
                           client_order_id, exchange_order_id, quantity,
                           price, executed_quantity, status,
                           mode, created_at, updated_at, expires_at,
                           position_side, position_action, reduce_only, leverage
                    FROM orders
                    WHERE order_id = %s AND mode = %s
                    """,
                    (str(order_id), self._mode(mode)),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        columns = (
            "order_id", "intent_id", "symbol", "side", "order_type",
            "client_order_id", "exchange_order_id", "quantity",
            "price", "executed_quantity", "status",
            "mode", "created_at", "updated_at", "expires_at",
            "position_side", "position_action", "reduce_only", "leverage",
        )
        return _row_dict(columns, row)

    def append_order_event(
        self,
        order_id: UUID,
        *,
        event_type: str,
        status: str,
        event_id: UUID | str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO order_events(
                        order_id, event_id, event_type, status, event_at, payload
                    ) VALUES (%s, %s, %s, %s, %s, CAST(%s AS JSONB))
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (
                        str(order_id),
                        str(event_id) if event_id is not None else None,
                        event_type,
                        status,
                        _now(),
                        _json(payload),
                    ),
                )

    def record_trade(
        self,
        order_id: UUID,
        *,
        symbol: str,
        side: str,
        quantity: Decimal,
        price: Decimal,
        fee: Decimal,
        fee_asset: str,
        realized_pnl: Decimal = Decimal("0"),
        market: str = "FUTURES",
        position_side: str | None = None,
        funding: Decimal = Decimal("0"),
        source_event_id: str | None = None,
        exchange_trade_id: str | None = None,
        mode: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> UUID:
        import psycopg2

        trade_id = uuid4()
        mode = self._mode(mode)
        if exchange_trade_id:
            conflict_sql = """
                    ON CONFLICT (mode, exchange_trade_id)
                    WHERE exchange_trade_id IS NOT NULL
                    DO UPDATE SET exchange_trade_id = EXCLUDED.exchange_trade_id
            """
        elif source_event_id:
            conflict_sql = """
                    ON CONFLICT (mode, source_event_id)
                    WHERE source_event_id IS NOT NULL
                    DO UPDATE SET source_event_id = EXCLUDED.source_event_id
            """
        else:
            conflict_sql = ""
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO trades(
                        trade_id, order_id, symbol, side, quantity, price,
                        fee, fee_asset, realized_pnl, executed_at, market, mode,
                        position_side, funding, source_event_id, exchange_trade_id,
                        payload, validation_session_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB), %s)
                    """
                    + conflict_sql
                    + """
                    RETURNING trade_id
                    """,
                    (
                        str(trade_id),
                        str(order_id),
                        symbol,
                        side,
                        quantity,
                        price,
                        fee,
                        fee_asset,
                        realized_pnl,
                        _now(),
                        market,
                        mode,
                        position_side,
                        funding,
                        source_event_id,
                        None if exchange_trade_id is None else str(exchange_trade_id),
                        _json(payload),
                        self._session_id(),
                    ),
                )
                row = cursor.fetchone()
                return UUID(str(row[0]))

    def funding_settlement_exists(
        self,
        *,
        mode: str,
        symbol: str,
        settlement_timestamp: datetime,
    ) -> bool:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT 1
                    FROM funding_settlements
                    WHERE mode = %s AND symbol = %s AND settlement_timestamp = %s
                    """,
                    (self._mode(mode), symbol.upper(), _as_utc(settlement_timestamp)),
                )
                return cursor.fetchone() is not None

    def record_funding_settlement(
        self,
        *,
        mode: str,
        symbol: str,
        settlement_timestamp: datetime,
        rate: Decimal,
        notional: Decimal,
        payment: Decimal,
        position_side: str,
    ) -> bool:
        """Persist the mode/symbol/settlement idempotency boundary."""
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO funding_settlements(
                        mode, symbol, settlement_timestamp, rate, notional,
                        payment, position_side, recorded_at, payload, validation_session_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB), %s)
                    ON CONFLICT (mode, symbol, settlement_timestamp) DO NOTHING
                    RETURNING mode
                    """,
                    (
                        self._mode(mode),
                        symbol.upper(),
                        _as_utc(settlement_timestamp),
                        rate,
                        notional,
                        payment,
                        position_side,
                        _now(),
                        _json({
                            "mode": self._mode(mode),
                            "symbol": symbol.upper(),
                            "settlement_timestamp": _as_utc(settlement_timestamp).isoformat(),
                        }),
                        self._session_id(),
                    ),
                )
                return cursor.fetchone() is not None

    def upsert_position(
        self,
        symbol: str,
        *,
        quantity: Decimal,
        average_price: Decimal,
        realized_pnl: Decimal,
        unrealized_pnl: Decimal,
        market: str = "FUTURES",
        position_side: str | None = None,
        entry_price: Decimal | None = None,
        mark_price: Decimal | None = None,
        index_price: Decimal | None = None,
        notional: Decimal | None = None,
        leverage: Decimal | None = None,
        margin_type: str = "ISOLATED",
        initial_margin: Decimal | None = None,
        maintenance_margin: Decimal | None = None,
        liquidation_price: Decimal | None = None,
        funding_pnl: Decimal = Decimal("0"),
        payload: dict[str, Any] | None = None,
        mode: str | None = None,
    ) -> None:
        import psycopg2

        resolved_entry = entry_price if entry_price is not None else average_price
        resolved_mode = self._mode(mode)
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO positions(
                        mode, market, symbol, quantity, average_price, realized_pnl,
                        unrealized_pnl, updated_at, payload, position_side,
                        entry_price, mark_price, index_price, notional,
                        leverage, margin_type, initial_margin,
                        maintenance_margin, liquidation_price, funding_pnl
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB),
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (mode, market, symbol) DO UPDATE SET
                        quantity = EXCLUDED.quantity,
                        average_price = EXCLUDED.average_price,
                        realized_pnl = EXCLUDED.realized_pnl,
                        unrealized_pnl = EXCLUDED.unrealized_pnl,
                        updated_at = EXCLUDED.updated_at,
                        payload = EXCLUDED.payload,
                        market = EXCLUDED.market,
                        mode = EXCLUDED.mode,
                        position_side = EXCLUDED.position_side,
                        entry_price = EXCLUDED.entry_price,
                        mark_price = EXCLUDED.mark_price,
                        index_price = EXCLUDED.index_price,
                        notional = EXCLUDED.notional,
                        leverage = EXCLUDED.leverage,
                        margin_type = EXCLUDED.margin_type,
                        initial_margin = EXCLUDED.initial_margin,
                        maintenance_margin = EXCLUDED.maintenance_margin,
                        liquidation_price = EXCLUDED.liquidation_price,
                        funding_pnl = EXCLUDED.funding_pnl
                    """,
                    (
                        resolved_mode,
                        market,
                        symbol,
                        quantity,
                        average_price,
                        realized_pnl,
                        unrealized_pnl,
                        _now(),
                        _json(payload),
                        position_side,
                        resolved_entry,
                        mark_price,
                        index_price,
                        notional,
                        leverage,
                        margin_type,
                        initial_margin,
                        maintenance_margin,
                        liquidation_price,
                        funding_pnl,
                    ),
                )

    def get_position(
        self, symbol: str, *, market: str = "FUTURES", mode: str | None = None
    ) -> dict[str, Any] | None:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT mode, market, symbol, quantity, average_price, realized_pnl,
                           unrealized_pnl, updated_at, position_side,
                           entry_price, mark_price, index_price, notional, leverage,
                           margin_type, initial_margin, maintenance_margin,
                           liquidation_price, funding_pnl, payload
                    FROM positions
                    WHERE mode = %s AND market = %s AND symbol = %s
                    """,
                    (self._mode(mode), market, symbol),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        return {
            "mode": row[0],
            "market": row[1],
            "symbol": row[2],
            "quantity": Decimal(str(row[3])),
            "average_price": Decimal(str(row[4])),
            "realized_pnl": Decimal(str(row[5])),
            "unrealized_pnl": Decimal(str(row[6])),
            "updated_at": row[7],
            "position_side": row[8],
            "entry_price": Decimal(str(row[9] if row[9] is not None else row[4])),
            "mark_price": Decimal(str(row[10])) if row[10] is not None else None,
            "index_price": Decimal(str(row[11])) if row[11] is not None else None,
            "notional": Decimal(str(row[12])) if row[12] is not None else None,
            "leverage": Decimal(str(row[13])) if row[13] is not None else None,
            "margin_type": row[14],
            "initial_margin": Decimal(str(row[15])) if row[15] is not None else None,
            "maintenance_margin": Decimal(str(row[16])) if row[16] is not None else None,
            "liquidation_price": Decimal(str(row[17])) if row[17] is not None else None,
            "funding_pnl": Decimal(str(row[18] if row[18] is not None else "0")),
            "payload": row[19] if isinstance(row[19], dict) else {},
        }

    def upsert_balance(
        self,
        asset: str,
        *,
        free: Decimal,
        locked: Decimal = Decimal("0"),
        mode: str,
        wallet_balance: Decimal | None = None,
        available_balance: Decimal | None = None,
        margin_balance: Decimal | None = None,
        used_margin: Decimal | None = None,
        unrealized_pnl: Decimal | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        import psycopg2

        resolved_wallet = wallet_balance if wallet_balance is not None else free + locked
        resolved_available = available_balance if available_balance is not None else free
        resolved_used = used_margin if used_margin is not None else locked
        resolved_margin = margin_balance if margin_balance is not None else resolved_wallet
        resolved_upnl = unrealized_pnl if unrealized_pnl is not None else Decimal("0")
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO balances(
                        asset, free, locked, mode, updated_at, payload,
                        wallet_balance, available_balance, margin_balance,
                        used_margin, unrealized_pnl
                    ) VALUES (%s, %s, %s, %s, %s, CAST(%s AS JSONB),
                              %s, %s, %s, %s, %s)
                    ON CONFLICT (mode, asset) DO UPDATE SET
                        free = EXCLUDED.free,
                        locked = EXCLUDED.locked,
                        mode = EXCLUDED.mode,
                        updated_at = EXCLUDED.updated_at,
                        payload = EXCLUDED.payload,
                        wallet_balance = EXCLUDED.wallet_balance,
                        available_balance = EXCLUDED.available_balance,
                        margin_balance = EXCLUDED.margin_balance,
                        used_margin = EXCLUDED.used_margin,
                        unrealized_pnl = EXCLUDED.unrealized_pnl
                    """,
                    (
                        asset, free, locked, mode, _now(), _json(payload),
                        resolved_wallet, resolved_available, resolved_margin,
                        resolved_used, resolved_upnl,
                    ),
                )

    def get_balance(self, asset: str, *, mode: str | None = None) -> dict[str, Any] | None:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT asset, free, locked, mode, updated_at,
                           wallet_balance, available_balance, margin_balance,
                           used_margin, unrealized_pnl, payload
                    FROM balances
                    WHERE asset = %s AND mode = %s
                    """,
                    (asset, self._mode(mode)),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        return {
            "asset": row[0],
            "free": Decimal(str(row[1])),
            "locked": Decimal(str(row[2])),
            "mode": row[3],
            "updated_at": row[4],
            "wallet_balance": Decimal(str(row[5] if row[5] is not None else row[1])),
            "available_balance": Decimal(str(row[6] if row[6] is not None else row[1])),
            "margin_balance": Decimal(str(row[7] if row[7] is not None else row[1])),
            "used_margin": Decimal(str(row[8] if row[8] is not None else row[2])),
            "unrealized_pnl": Decimal(str(row[9] if row[9] is not None else "0")),
            "payload": row[10] if isinstance(row[10], dict) else {},
        }

    def list_orders(self, limit: int = 50, *, mode: str | None = None, market: str = "FUTURES") -> list[dict[str, Any]]:
        import psycopg2

        bounded_limit = max(1, min(int(limit), 200))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT order_id, intent_id, symbol, side, order_type,
                           client_order_id, exchange_order_id, quantity,
                           price, executed_quantity, status,
                           mode, created_at, updated_at, expires_at,
                           position_side, position_action, reduce_only, leverage
                    FROM orders
                    WHERE mode = %s AND market = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (self._mode(mode), market.upper(), bounded_limit),
                )
                rows = cursor.fetchall()
        columns = (
            "order_id", "intent_id", "symbol", "side", "order_type",
            "client_order_id", "exchange_order_id", "quantity",
            "price", "executed_quantity", "status",
            "mode", "created_at", "updated_at", "expires_at",
            "position_side", "position_action", "reduce_only", "leverage",
        )
        return [_row_dict(columns, row) for row in rows]

    def list_open_local_orders(self, *, mode: str | None = None, market: str = "FUTURES") -> list[dict[str, Any]]:
        from execution import PENDING_ORDER_STATES

        return [
            row
            for row in self.list_orders(limit=200, mode=mode, market=market)
            if row["status"] in PENDING_ORDER_STATES
        ]

    def list_reconciliation_orders(
        self,
        *,
        mode: str,
        market: str = "FUTURES",
        session_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Load canonical Futures orders for one explicit runtime mode.

        Recovery must see FILLED/CANCELLED/EXPIRED identities, not only pending
        rows. Session filtering is opt-in so historical canonical orders remain
        visible to exchange truth.
        """
        import psycopg2

        resolved_mode = str(mode).strip().lower()
        if resolved_mode not in {"paper", "testnet", "live"}:
            raise ValueError("reconciliation orders require a canonical mode")
        params: list[Any] = [resolved_mode, market.upper()]
        session_sql = ""
        if session_id:
            session_sql = " AND validation_session_id = %s"
            params.append(str(session_id))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT order_id, intent_id, symbol, side, order_type,
                           client_order_id, exchange_order_id, quantity,
                           price, executed_quantity, status,
                           mode, created_at, updated_at, expires_at,
                           position_side, position_action, reduce_only, leverage
                    FROM orders
                    WHERE mode = %s AND market = %s
                    """ + session_sql + """
                    ORDER BY created_at DESC
                    """,
                    tuple(params),
                )
                rows = cursor.fetchall()
        columns = (
            "order_id", "intent_id", "symbol", "side", "order_type",
            "client_order_id", "exchange_order_id", "quantity",
            "price", "executed_quantity", "status",
            "mode", "created_at", "updated_at", "expires_at",
            "position_side", "position_action", "reduce_only", "leverage",
        )
        return [_row_dict(columns, row) for row in rows]

    def list_trades(self, limit: int = 50, *, mode: str | None = None, market: str = "FUTURES") -> list[dict[str, Any]]:
        import psycopg2

        bounded_limit = max(1, min(int(limit), 200))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_id, order_id, symbol, side, quantity, price,
                           fee, fee_asset, realized_pnl, executed_at, market, mode,
                           position_side, funding, exchange_trade_id, payload
                    FROM trades
                    WHERE market = %s AND mode = %s
                    ORDER BY executed_at DESC
                    LIMIT %s
                    """,
                    (market.upper(), self._mode(mode), bounded_limit),
                )
                rows = cursor.fetchall()
        columns = (
            "trade_id", "order_id", "symbol", "side", "quantity", "price",
            "fee", "fee_asset", "realized_pnl", "executed_at", "market",
            "mode", "position_side", "funding", "exchange_trade_id", "payload",
        )
        return [_row_dict(columns, row) for row in rows]

    def list_positions(
        self, *, market: str = "FUTURES", mode: str | None = None
    ) -> list[dict[str, Any]]:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT mode, market, symbol, quantity, average_price, realized_pnl,
                           unrealized_pnl, updated_at, position_side,
                           entry_price, mark_price, index_price, notional, leverage,
                           margin_type, initial_margin, maintenance_margin,
                           liquidation_price, funding_pnl
                    FROM positions
                    WHERE mode = %s AND market = %s
                    ORDER BY market, symbol
                    """, (self._mode(mode), market.upper())
                )
                rows = cursor.fetchall()
        columns = (
            "mode", "market", "symbol", "quantity", "average_price", "realized_pnl",
            "unrealized_pnl", "updated_at", "position_side",
            "entry_price", "mark_price", "index_price", "notional", "leverage",
            "margin_type", "initial_margin", "maintenance_margin",
            "liquidation_price", "funding_pnl",
        )
        return [_row_dict(columns, row) for row in rows]

    def list_balances(self, *, mode: str | None = None) -> list[dict[str, Any]]:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT asset, free, locked, mode, updated_at,
                           wallet_balance, available_balance, margin_balance,
                           used_margin, unrealized_pnl, payload
                    FROM balances
                    WHERE mode = %s
                    ORDER BY asset
                    """, (self._mode(mode),)
                )
                rows = cursor.fetchall()
        columns = (
            "asset", "free", "locked", "mode", "updated_at",
            "wallet_balance", "available_balance", "margin_balance",
            "used_margin", "unrealized_pnl", "payload",
        )
        return [_row_dict(columns, row) for row in rows]

    def list_risk_events(self, limit: int = 50, *, mode: str | None = None, market: str = "FUTURES") -> list[dict[str, Any]]:
        import psycopg2

        bounded_limit = max(1, min(int(limit), 200))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT event_id, intent_id, decision, reason, event_at, mode, market
                    FROM risk_events
                    WHERE mode = %s AND market = %s
                    ORDER BY event_at DESC
                    LIMIT %s
                    """,
                    (self._mode(mode), market.upper(), bounded_limit),
                )
                rows = cursor.fetchall()
        columns = ("event_id", "intent_id", "decision", "reason", "event_at", "mode", "market")
        return [_row_dict(columns, row) for row in rows]

    def list_system_events(self, limit: int = 50, *, mode: str | None = None, market: str = "FUTURES") -> list[dict[str, Any]]:
        import psycopg2

        bounded_limit = max(1, min(int(limit), 200))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT event_id, event_type, severity, message, event_at, mode, market
                    FROM system_events
                    WHERE mode = %s AND market = %s
                    ORDER BY event_at DESC
                    LIMIT %s
                    """,
                    (self._mode(mode), market.upper(), bounded_limit),
                )
                rows = cursor.fetchall()
        columns = ("event_id", "event_type", "severity", "message", "event_at", "mode", "market")
        return [_row_dict(columns, row) for row in rows]

    def runtime_acceptance_snapshot(
        self,
        *,
        mode: str = "paper",
        session_id: str | None = None,
    ) -> dict[str, Any]:
        """Read session-scoped paper/shadow acceptance counts from PostgreSQL."""
        import psycopg2
        from execution import PENDING_ORDER_STATES, TERMINAL_ORDER_STATES

        resolved_mode = str(mode or "paper").strip().lower()
        resolved_session = session_id or self._session_id()
        if not resolved_session:
            return empty_runtime_acceptance(mode=resolved_mode, database_healthy=True)
        session_row = self.load_validation_session(resolved_session) or {}
        scoped_start = session_row.get("started_at")
        scoped_end = session_row.get("ended_at")
        pending = tuple(sorted(PENDING_ORDER_STATES - {"UNKNOWN"}))
        known = tuple(sorted(PENDING_ORDER_STATES | TERMINAL_ORDER_STATES))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                def count(sql: str, params: tuple[Any, ...] = ()) -> int:
                    cursor.execute(sql, params)
                    row = cursor.fetchone()
                    return int(row[0] or 0)

                session_params = (resolved_session,)
                session_mode = (resolved_session, resolved_mode)
                observation_count = count(
                    """
                    SELECT COUNT(*) FROM validation_session_observations
                    WHERE session_id = %s
                    """,
                    session_params,
                )
                session_market_flow_count = count(
                    """
                    SELECT COUNT(*) FROM market_flow_events
                    WHERE validation_session_id = %s
                    """,
                    session_params,
                )
                positioning_count = count(
                    "SELECT COUNT(*) FROM positioning_snapshots WHERE validation_session_id = %s",
                    session_params,
                )
                evidence_count = count(
                    "SELECT COUNT(*) FROM evidence_snapshots WHERE validation_session_id = %s",
                    session_params,
                )
                episode_count = count(
                    "SELECT COUNT(*) FROM positioning_episodes WHERE validation_session_id = %s",
                    session_params,
                )
                intent_count = count(
                    "SELECT COUNT(*) FROM trade_intents WHERE validation_session_id = %s",
                    session_params,
                )
                risk_decision_count = count(
                    """
                    SELECT COUNT(*) FROM risk_events
                    WHERE validation_session_id = %s AND mode = %s
                    """,
                    session_mode,
                )
                order_count = count(
                    "SELECT COUNT(*) FROM orders WHERE validation_session_id = %s AND mode = %s",
                    session_mode,
                )
                fill_count = count(
                    "SELECT COUNT(*) FROM trades WHERE validation_session_id = %s AND mode = %s",
                    session_mode,
                )
                funding_count = count(
                    """
                    SELECT COUNT(*) FROM funding_settlements
                    WHERE validation_session_id = %s AND mode = %s
                    """,
                    session_mode,
                )
                long_count = count(
                    """
                    SELECT COUNT(*) FROM positioning_snapshots
                    WHERE validation_session_id = %s AND direction = 'LONG'
                    """,
                    session_params,
                )
                short_count = count(
                    """
                    SELECT COUNT(*) FROM positioning_snapshots
                    WHERE validation_session_id = %s AND direction = 'SHORT'
                    """,
                    session_params,
                )
                long_building_count = count(
                    """
                    SELECT COUNT(*) FROM positioning_snapshots
                    WHERE validation_session_id = %s AND state = 'LONG_BUILDING'
                    """,
                    session_params,
                )
                short_building_count = count(
                    """
                    SELECT COUNT(*) FROM positioning_snapshots
                    WHERE validation_session_id = %s AND state = 'SHORT_BUILDING'
                    """,
                    session_params,
                )
                duplicate_trades = count(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT source_event_id FROM trades
                        WHERE validation_session_id = %s
                          AND mode = %s
                          AND source_event_id IS NOT NULL
                        GROUP BY source_event_id HAVING COUNT(*) > 1
                    ) duplicated
                    """,
                    session_mode,
                )
                duplicate_funding = count(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT mode, symbol, settlement_timestamp
                        FROM funding_settlements
                        WHERE validation_session_id = %s AND mode = %s
                        GROUP BY mode, symbol, settlement_timestamp
                        HAVING COUNT(*) > 1
                    ) duplicated
                    """,
                    session_mode,
                )
                invalid_positions = count(
                    """
                    SELECT COUNT(*) FROM positions
                    WHERE mode = %s
                      AND (
                        quantity < 0
                       OR (quantity > 0 AND COALESCE(entry_price, 0) <= 0)
                       OR (quantity > 0 AND COALESCE(leverage, 0) <= 0)
                       OR COALESCE(position_side, 'FLAT') NOT IN ('LONG', 'SHORT', 'FLAT')
                      )
                    """
                    ,
                    (resolved_mode,),
                )
                unknown_order = count(
                    """
                    SELECT COUNT(*) FROM orders
                    WHERE validation_session_id = %s AND mode = %s AND status = 'UNKNOWN'
                    """,
                    session_mode,
                )
                stale_pending_order = count(
                    """
                    SELECT COUNT(*) FROM orders
                    WHERE validation_session_id = %s
                      AND mode = %s
                      AND status = ANY(%s)
                      AND updated_at < NOW() - INTERVAL '1 hour'
                    """,
                    (resolved_session, resolved_mode, list(pending)),
                )
                unsafe_order = count(
                    """
                    SELECT COUNT(*) FROM orders
                    WHERE validation_session_id = %s
                      AND mode = %s
                      AND (status = 'UNKNOWN' OR status <> ALL(%s))
                    """,
                    (resolved_session, resolved_mode, list(known)),
                )
                cursor.execute(
                    """
                    SELECT wallet_balance, used_margin, available_balance, unrealized_pnl, margin_balance, payload
                    FROM balances
                    WHERE mode = %s AND asset = 'USDT'
                    """,
                    (resolved_mode if resolved_mode != "shadow" else "paper",),
                )
                balance = cursor.fetchone()
                impossible_balance = False
                impossible_equity = False
                invalid_margin = False
                accounting: dict[str, str] = {}
                if balance is not None and len(balance) >= 5:
                    wallet, used, available, unrealized = (
                        Decimal(str(balance[0] or 0)),
                        Decimal(str(balance[1] or 0)),
                        Decimal(str(balance[2] or 0)),
                        Decimal(str(balance[3] or 0)),
                    )
                    stored_equity = Decimal(str(balance[4])) if balance[4] is not None else wallet + unrealized
                    payload = _json_obj(balance[5])
                    realized = Decimal(str(payload.get("realized_pnl") or 0))
                    funding = Decimal(str(payload.get("funding_pnl") or 0))
                    fee = Decimal(str(payload.get("fee_pnl") or 0))
                    accounting = {
                        "wallet_balance": str(wallet),
                        "used_margin": str(used),
                        "available_balance": str(available),
                        "unrealized_pnl": str(unrealized),
                        "equity": str(wallet + unrealized),
                        "realized_pnl": str(realized),
                        "funding_pnl": str(funding),
                        "fee_pnl": str(fee),
                        "net_pnl": str(realized + unrealized + funding + fee),
                        "formula": {
                            "available_balance": "wallet_balance - used_margin",
                            "equity": "wallet_balance + unrealized_pnl",
                            "net_pnl": "realized_pnl + unrealized_pnl + funding_pnl + fee_pnl",
                        },
                    }
                    impossible_balance = available != wallet - used
                    impossible_equity = stored_equity != wallet + unrealized
                    invalid_margin = used < 0 or wallet < 0 or available < 0
                episode_split = count(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT symbol, market FROM positioning_episodes
                        WHERE validation_session_id = %s
                          AND status IN ('OPEN', 'UNRESOLVED')
                        GROUP BY symbol, market HAVING COUNT(*) > 1
                    ) split_rows
                    """,
                    session_params,
                ) > 0
        comparison = self.compare_restart_snapshots(
            session_row.get("pre_restart_snapshot"),
            session_row.get("post_restart_snapshot"),
        )
        restart_recovery = (
            bool(comparison.get("ok"))
            and not episode_split
            and duplicate_trades == 0
            and duplicate_funding == 0
        )
        return {
            "database_healthy": True,
            "session_id": resolved_session,
            "stage": session_row.get("stage"),
            "mode": resolved_mode,
            "scoped_start": str(scoped_start) if scoped_start else None,
            "scoped_end": str(scoped_end) if scoped_end else None,
            "commit_sha": session_row.get("commit_sha"),
            "observation_count": observation_count,
            "session_market_flow_count": session_market_flow_count,
            "evidence_scope": {
                "orders": "session",
                "trades": "session",
                "observations": "session",
                "market_flow_events": "session",
                "balances": "current_exchange_truth",
                "positions": "current_exchange_truth",
            },
            "balance_scope": "current_exchange_truth",
            "position_scope": "current_exchange_truth",
            "positioning_count": positioning_count,
            "evidence_count": evidence_count,
            "episode_count": episode_count,
            "intent_count": intent_count,
            "risk_decision_count": risk_decision_count,
            "order_count": order_count,
            "fill_count": fill_count,
            "funding_count": funding_count,
            "long_count": long_count,
            "short_count": short_count,
            "long_building_count": long_building_count,
            "short_building_count": short_building_count,
            "duplicate_trades": duplicate_trades,
            "duplicate_funding": duplicate_funding,
            "invalid_positions": invalid_positions,
            "impossible_balance": impossible_balance,
            "impossible_equity": impossible_equity,
            "invalid_margin": invalid_margin,
            "unknown_order": unknown_order,
            "stale_pending_order": stale_pending_order,
            "unsafe_order": unsafe_order,
            "stale_open": stale_pending_order,
            "unsafe_open": unsafe_order,
            "accounting": accounting,
            "restart_recovery": restart_recovery,
            "episode_split": episode_split,
            "restart_comparison": comparison,
        }

    def record_runtime_gate_status(
        self,
        gate: str,
        status: str,
        *,
        detail: dict[str, Any] | None = None,
        mode: str | None = None,
    ) -> UUID:
        """Persist externally verified readiness evidence for one gate."""
        normalized_gate = gate.strip().lower()
        normalized_status = status.strip().upper()
        if normalized_gate not in {
            "observation",
            "paper",
            "shadow",
            "testnet",
            "realtime_30m",
            "realtime_2h",
            "realtime_6h",
            "realtime_24h",
        }:
            raise ValueError("unsupported runtime gate")
        if normalized_status not in {"NOT_STARTED", "RUNNING", "PASSED", "FAILED"}:
            raise ValueError("unsupported runtime gate status")
        payload = {
            "gate": normalized_gate,
            "status": normalized_status,
            "validation_session_id": self._session_id(),
            "verified_at": _now().isoformat(),
            "commit_sha": os.environ.get("BIAN_VALIDATION_COMMIT") or os.environ.get("GITHUB_SHA"),
            **(detail or {}),
        }
        return self.record_system_event(
            event_type="RUNTIME_GATE_EVIDENCE",
            severity="INFO" if normalized_status != "FAILED" else "CRITICAL",
            message=f"runtime gate {normalized_gate} is {normalized_status}",
            payload=payload,
            mode=self._mode(mode if mode is not None else payload.get("mode")),
        )

    def runtime_gate_statuses(self) -> dict[str, str]:
        """Return the latest persisted status for each external acceptance gate."""
        import psycopg2

        values = {
            "observation": "NOT_STARTED",
            "paper": "NOT_STARTED",
            "shadow": "NOT_STARTED",
            "testnet": "NOT_STARTED",
            "realtime_30m": "NOT_STARTED",
            "realtime_2h": "NOT_STARTED",
            "realtime_6h": "NOT_STARTED",
            "realtime_24h": "NOT_STARTED",
        }
        session_id = self._session_id()
        if not session_id:
            return values
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT ON ((payload->>'gate')) payload->>'gate', payload->>'status'
                    FROM system_events
                    WHERE event_type = 'RUNTIME_GATE_EVIDENCE'
                      AND payload->>'gate' IN ('observation', 'paper', 'shadow', 'testnet', 'realtime_30m', 'realtime_2h', 'realtime_6h', 'realtime_24h')
                      AND validation_session_id = %s
                    ORDER BY (payload->>'gate'), event_at DESC
                    """,
                    (session_id,),
                )
                for gate, status in cursor.fetchall():
                    if gate in values and status:
                        values[gate] = str(status).upper()
        return values

    def runtime_gate_evidence(self) -> dict[str, dict[str, Any]]:
        """Return latest persisted status and verification timestamp per gate."""
        import psycopg2

        values: dict[str, dict[str, Any]] = {}
        session_id = self._session_id()
        if not session_id:
            return values
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT ON ((payload->>'gate'))
                           payload->>'gate', payload->>'status', event_at, payload
                    FROM system_events
                    WHERE event_type = 'RUNTIME_GATE_EVIDENCE'
                      AND payload->>'gate' IN ('observation', 'paper', 'shadow', 'testnet', 'realtime_30m', 'realtime_2h', 'realtime_6h', 'realtime_24h')
                      AND validation_session_id = %s
                    ORDER BY (payload->>'gate'), event_at DESC
                    """,
                    (session_id,),
                )
                for gate, status, event_at, payload in cursor.fetchall():
                    if gate:
                        values[str(gate)] = {
                            "status": str(status or "NOT_STARTED").upper(),
                            "event_at": event_at,
                            "verified_at": (
                                payload.get("verified_at")
                                if isinstance(payload, dict)
                                else None
                            ),
                            "validation_session_id": (
                                payload.get("validation_session_id")
                                if isinstance(payload, dict)
                                else session_id
                            ),
                            "commit_sha": (
                                payload.get("commit_sha")
                                if isinstance(payload, dict)
                                else None
                            ),
                        }
        return values

    def trading_summary(self, *, mode: str) -> dict[str, Any]:
        from datetime import datetime, timezone

        resolved_mode = self._mode(mode)
        balances = self.list_balances(mode=resolved_mode)
        usdt = next((row for row in balances if row["asset"] == "USDT"), None)
        trades = self.list_trades(limit=200, mode=resolved_mode)
        positions = self.list_positions(mode=resolved_mode)
        now = datetime.now(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        def decimal_value(row: dict[str, Any], key: str) -> Decimal:
            return Decimal(str(row.get(key, "0")))

        realized = sum((decimal_value(row, "realized_pnl") for row in trades), Decimal("0"))
        fees = sum((decimal_value(row, "fee") for row in trades), Decimal("0"))
        unrealized = sum(
            (decimal_value(row, "unrealized_pnl") for row in positions), Decimal("0")
        )
        position_market_value = sum(
            (
                decimal_value(row, "quantity") * decimal_value(row, "average_price")
                + decimal_value(row, "unrealized_pnl")
                for row in positions
            ),
            Decimal("0"),
        )
        daily = sum(
            (
                decimal_value(row, "realized_pnl")
                for row in trades
                if _as_utc(row.get("executed_at")) >= day_start
            ),
            Decimal("0"),
        )
        return {
            "available_balance_usdt": str(usdt["free"]) if usdt else "0",
            "locked_balance_usdt": str(usdt["locked"]) if usdt else "0",
            "realized_pnl": str(realized),
            "unrealized_pnl": str(unrealized),
            "total_pnl": str(realized + unrealized),
            "daily_pnl": str(daily),
            "fees": str(fees),
            "drawdown": None,
            "positions": len(positions),
            "trades": len(trades),
            "position_market_value_usdt": str(position_market_value),
            "account_equity_usdt": (
                str(
                    Decimal(str(usdt["free"]))
                    + Decimal(str(usdt["locked"]))
                    + position_market_value
                )
                if usdt
                else str(position_market_value)
            ),
        }

    def trading_counts(self) -> dict[str, int]:
        import psycopg2

        tables = ("signals", "trade_intents", "orders", "trades", "positions", "risk_events")
        counts: dict[str, int] = {}
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    counts[table] = int(cursor.fetchone()[0])
        return counts

    def record_risk_event(
        self,
        *,
        intent_id: UUID | None,
        decision: str,
        reason: str,
        payload: dict[str, Any] | None = None,
        mode: str | None = None,
    ) -> UUID:
        import psycopg2

        event_id = uuid4()
        mode = self._mode(mode if mode is not None else (payload or {}).get("mode"))
        market = str((payload or {}).get("market", "FUTURES")).upper()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO risk_events(
                        event_id, intent_id, decision, reason, mode, market, event_at, payload, validation_session_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB), %s)
                    RETURNING event_id
                    """,
                    (
                        str(event_id),
                        str(intent_id) if intent_id is not None else None,
                        decision,
                        reason,
                        mode,
                        market,
                        _now(),
                        _json(payload),
                        self._session_id(),
                    ),
                )
                row = cursor.fetchone()
                return UUID(str(row[0]))

    def record_system_event(
        self,
        *,
        event_type: str,
        severity: str,
        message: str,
        payload: dict[str, Any] | None = None,
        mode: str | None = None,
    ) -> UUID:
        import psycopg2

        event_id = uuid4()
        mode = self._mode(mode if mode is not None else (payload or {}).get("mode"))
        market = str((payload or {}).get("market", "FUTURES")).upper()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO system_events(
                        event_id, event_type, severity, message, mode, market, event_at, payload, validation_session_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB), %s)
                    RETURNING event_id
                    """,
                    (
                        str(event_id),
                        event_type,
                        severity,
                        message,
                        mode,
                        market,
                        _now(),
                        _json(payload),
                        self._session_id(),
                    ),
                )
                row = cursor.fetchone()
                return UUID(str(row[0]))
