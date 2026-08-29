"""Trading persistence owner, separate from the schema/connection owner."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable
from uuid import UUID, uuid4

from scripts.database import configured_dsn, ensure_schema
from trade_intent import TradeIntent


def _json(value: dict[str, Any] | None) -> str:
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


class TradingStore:
    """Persist trading facts and audit events without owning schema creation."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or configured_dsn()

    def initialize(self) -> None:
        ensure_schema(self.dsn)

    def is_halted(self) -> bool:
        import os
        import psycopg2

        if os.environ.get("BIAN_TRADING_HALTED", "false").strip().lower() in {
            "1", "true", "yes", "on"
        }:
            return True
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT event_type
                    FROM system_events
                    WHERE event_type IN ('TRADING_HALTED', 'TRADING_RESUMED')
                    ORDER BY event_at DESC
                    LIMIT 1
                    """
                )
                row = cursor.fetchone()
        return row is not None and row[0] == "TRADING_HALTED"

    def set_halt(self, halted: bool, *, reason: str, source: str) -> UUID:
        return self.record_system_event(
            event_type="TRADING_HALTED" if halted else "TRADING_RESUMED",
            severity="CRITICAL" if halted else "INFO",
            message=reason,
            payload={"source": source, "halted": halted},
        )

    def record_intent(self, intent: TradeIntent, *, status: str = "CREATED") -> None:
        import psycopg2

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
                        transition, evidence_snapshot_id, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, CAST(%s AS JSONB))
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
                    ),
                )

    def record_signal(
        self,
        *,
        symbol: str,
        side: str,
        confidence: Decimal,
        reason: str,
        strategy_version: str,
        observed_at: datetime,
        payload: dict[str, Any] | None = None,
    ) -> UUID:
        import psycopg2

        signal_id = uuid4()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO signals(
                        signal_id, symbol, side, confidence, reason,
                        strategy_version, observed_at, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB))
                    RETURNING signal_id
                    """,
                    (
                        str(signal_id),
                        symbol,
                        side,
                        confidence,
                        reason,
                        strategy_version,
                        observed_at,
                        _json(payload),
                    ),
                )
                row = cursor.fetchone()
                return UUID(str(row[0]))

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
                        direction, metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              CAST(%s AS JSONB))
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (
                        str(event_id), symbol.upper(), market, event_type,
                        event_timestamp, received_timestamp, latency_ms, price,
                        quantity, notional, direction, _json(metadata),
                    ),
                )
        return event_id

    def record_positioning_snapshot(
        self, decision: Any, *, strategy_version: str
    ) -> UUID:
        import psycopg2

        snapshot_id = decision.evidence_snapshot_id
        payload = decision.as_dict()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO positioning_snapshots(
                        snapshot_id, symbol, observed_at, state, transition,
                        direction, confidence, long_score, short_score,
                        crowding_score, liquidity_score, data_quality_score,
                        strategy_version, reason_codes, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, CAST(%s AS JSONB), CAST(%s AS JSONB))
                    ON CONFLICT (snapshot_id) DO UPDATE SET payload = EXCLUDED.payload
                    """,
                    (
                        str(snapshot_id), decision.symbol, decision.timestamp,
                        decision.state, decision.transition, decision.direction,
                        decision.confidence, decision.long_score, decision.short_score,
                        decision.crowding_score, decision.liquidity_score,
                        decision.data_quality_score, strategy_version,
                        _json(list(decision.reason_codes)), _json(payload),
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO evidence_snapshots(
                        snapshot_id, symbol, observed_at, source_timestamps,
                        evidence, payload
                    ) VALUES (%s, %s, %s, CAST(%s AS JSONB), CAST(%s AS JSONB),
                              CAST(%s AS JSONB))
                    ON CONFLICT (snapshot_id) DO UPDATE SET
                        source_timestamps = EXCLUDED.source_timestamps,
                        evidence = EXCLUDED.evidence,
                        payload = EXCLUDED.payload
                    """,
                    (
                        str(snapshot_id), decision.symbol, decision.timestamp,
                        _json(dict(decision.source_timestamps)),
                        _json(decision.evidence.__dict__), _json(payload),
                    ),
                )
        return snapshot_id

    def record_evidence_snapshot(
        self,
        *,
        snapshot_id: UUID,
        symbol: str,
        observed_at: datetime,
        evidence: dict[str, Any],
        source_timestamps: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> UUID:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO evidence_snapshots(
                        snapshot_id, symbol, observed_at, source_timestamps,
                        evidence, payload
                    ) VALUES (%s, %s, %s, CAST(%s AS JSONB), CAST(%s AS JSONB),
                              CAST(%s AS JSONB))
                    ON CONFLICT (snapshot_id) DO UPDATE SET payload = EXCLUDED.payload
                    """,
                    (
                        str(snapshot_id), symbol.upper(), observed_at,
                        _json(source_timestamps), _json(evidence), _json(payload),
                    ),
                )
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
        self, symbol: str, *, before: datetime
    ) -> str | None:
        """Return the persisted predecessor state for a timestamp-bounded decision."""
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT state
                    FROM positioning_snapshots
                    WHERE symbol = %s
                      AND observed_at < %s
                    ORDER BY observed_at DESC, snapshot_id DESC
                    LIMIT 1
                    """,
                    (symbol.upper(), _as_utc(before)),
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

    def market_data_freshness(self, *, max_age_sec: int = 900) -> list[dict[str, Any]]:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT market, event_type, MAX(event_timestamp),
                           MAX(received_timestamp), MAX(latency_ms)
                    FROM market_flow_events
                    GROUP BY market, event_type
                    ORDER BY market, event_type
                    """
                )
                rows = cursor.fetchall()
        now = _now()
        result = []
        for market, event_type, source_at, received_at, latency_ms in rows:
            age_sec = max(0, int((now - _as_utc(source_at)).total_seconds()))
            result.append(
                {
                    "market": market,
                    "event_type": event_type,
                    "source_timestamp": source_at.isoformat(),
                    "received_timestamp": received_at.isoformat(),
                    "latency_ms": int(latency_ms),
                    "age_sec": age_sec,
                    "status": "FRESH" if age_sec <= max(1, max_age_sec) else "STALE",
                }
            )
        return result

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
                            WHERE symbol = %s AND captured_at <= %s
                            ORDER BY captured_at DESC, id DESC
                            LIMIT 1
                        ), baseline AS (
                            SELECT snapshot.last_price
                            FROM bian_market_snapshots snapshot, current
                            WHERE snapshot.symbol = %s
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
                            ORDER BY symbol, captured_at DESC, id DESC
                        ), returns AS (
                            SELECT (current.last_price - baseline.last_price)
                                   / baseline.last_price AS value
                            FROM current
                            JOIN LATERAL (
                                SELECT last_price
                                FROM bian_market_snapshots snapshot
                                WHERE snapshot.symbol = current.symbol
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
        symbol: str,
        *,
        limit: int = 10_000,
        source_ttl_sec: int = 900,
    ) -> list[Any]:
        """Load historical normalized frames in deterministic timestamp order."""
        import psycopg2
        from engine import MarketFrame

        bounded_limit = max(1, min(int(limit), 100_000))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT payload
                    FROM positioning_snapshots
                    WHERE symbol = %s
                    ORDER BY observed_at, snapshot_id
                    LIMIT %s
                    """,
                    (symbol.upper(), bounded_limit),
                )
                payloads = [dict(row[0]) for row in cursor.fetchall()]
        return [
            MarketFrame.from_evidence_snapshot(payload, source_ttl_sec=source_ttl_sec)
            for payload in payloads
        ]

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
                        payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              CAST(%s AS JSONB))
                    ON CONFLICT (client_order_id) DO UPDATE SET
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

    def get_order_by_client_order_id(self, client_order_id: str) -> dict[str, Any] | None:
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
                    WHERE client_order_id = %s
                    """,
                    (client_order_id,),
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

    def get_order(self, order_id: UUID) -> dict[str, Any] | None:
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
                    WHERE order_id = %s
                    """,
                    (str(order_id),),
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
        payload: dict[str, Any] | None = None,
    ) -> None:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO order_events(
                        order_id, event_type, status, event_at, payload
                    ) VALUES (%s, %s, %s, %s, CAST(%s AS JSONB))
                    """,
                    (str(order_id), event_type, status, _now(), _json(payload)),
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
        payload: dict[str, Any] | None = None,
    ) -> UUID:
        import psycopg2

        trade_id = uuid4()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO trades(
                        trade_id, order_id, symbol, side, quantity, price,
                        fee, fee_asset, realized_pnl, executed_at, market,
                        position_side, funding, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, CAST(%s AS JSONB))
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
                        position_side,
                        funding,
                        _json(payload),
                    ),
                )
                row = cursor.fetchone()
                return UUID(str(row[0]))

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
        notional: Decimal | None = None,
        leverage: Decimal | None = None,
        margin_type: str = "ISOLATED",
        initial_margin: Decimal | None = None,
        maintenance_margin: Decimal | None = None,
        liquidation_price: Decimal | None = None,
        funding_pnl: Decimal = Decimal("0"),
        payload: dict[str, Any] | None = None,
    ) -> None:
        import psycopg2

        resolved_entry = entry_price if entry_price is not None else average_price
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO positions(
                        symbol, quantity, average_price, realized_pnl,
                        unrealized_pnl, updated_at, payload, market,
                        position_side, entry_price, mark_price, notional,
                        leverage, margin_type, initial_margin,
                        maintenance_margin, liquidation_price, funding_pnl
                    ) VALUES (%s, %s, %s, %s, %s, %s, CAST(%s AS JSONB),
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol) DO UPDATE SET
                        quantity = EXCLUDED.quantity,
                        average_price = EXCLUDED.average_price,
                        realized_pnl = EXCLUDED.realized_pnl,
                        unrealized_pnl = EXCLUDED.unrealized_pnl,
                        updated_at = EXCLUDED.updated_at,
                        payload = EXCLUDED.payload,
                        market = EXCLUDED.market,
                        position_side = EXCLUDED.position_side,
                        entry_price = EXCLUDED.entry_price,
                        mark_price = EXCLUDED.mark_price,
                        notional = EXCLUDED.notional,
                        leverage = EXCLUDED.leverage,
                        margin_type = EXCLUDED.margin_type,
                        initial_margin = EXCLUDED.initial_margin,
                        maintenance_margin = EXCLUDED.maintenance_margin,
                        liquidation_price = EXCLUDED.liquidation_price,
                        funding_pnl = EXCLUDED.funding_pnl
                    """,
                    (
                        symbol,
                        quantity,
                        average_price,
                        realized_pnl,
                        unrealized_pnl,
                        _now(),
                        _json(payload),
                        market,
                        position_side,
                        resolved_entry,
                        mark_price,
                        notional,
                        leverage,
                        margin_type,
                        initial_margin,
                        maintenance_margin,
                        liquidation_price,
                        funding_pnl,
                    ),
                )

    def get_position(self, symbol: str) -> dict[str, Any] | None:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT symbol, quantity, average_price, realized_pnl,
                           unrealized_pnl, updated_at, market, position_side,
                           entry_price, mark_price, notional, leverage,
                           margin_type, initial_margin, maintenance_margin,
                           liquidation_price, funding_pnl, payload
                    FROM positions
                    WHERE symbol = %s
                    """,
                    (symbol,),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        return {
            "symbol": row[0],
            "quantity": Decimal(str(row[1])),
            "average_price": Decimal(str(row[2])),
            "realized_pnl": Decimal(str(row[3])),
            "unrealized_pnl": Decimal(str(row[4])),
            "updated_at": row[5],
            "market": row[6],
            "position_side": row[7],
            "entry_price": Decimal(str(row[8] if row[8] is not None else row[2])),
            "mark_price": Decimal(str(row[9])) if row[9] is not None else None,
            "notional": Decimal(str(row[10])) if row[10] is not None else None,
            "leverage": Decimal(str(row[11])) if row[11] is not None else None,
            "margin_type": row[12],
            "initial_margin": Decimal(str(row[13])) if row[13] is not None else None,
            "maintenance_margin": Decimal(str(row[14])) if row[14] is not None else None,
            "liquidation_price": Decimal(str(row[15])) if row[15] is not None else None,
            "funding_pnl": Decimal(str(row[16] if row[16] is not None else "0")),
            "payload": row[17] if isinstance(row[17], dict) else {},
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
                    ON CONFLICT (asset) DO UPDATE SET
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

    def get_balance(self, asset: str) -> dict[str, Any] | None:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT asset, free, locked, mode, updated_at,
                           wallet_balance, available_balance, margin_balance,
                           used_margin, unrealized_pnl
                    FROM balances
                    WHERE asset = %s
                    """,
                    (asset,),
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
        }

    def list_orders(self, limit: int = 50) -> list[dict[str, Any]]:
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
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (bounded_limit,),
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

    def list_open_local_orders(self) -> list[dict[str, Any]]:
        return [
            row
            for row in self.list_orders(limit=200)
            if row["status"] in {"CREATED", "RISK_APPROVED", "SUBMITTED", "ACKNOWLEDGED", "PARTIALLY_FILLED", "UNKNOWN"}
        ]

    def list_trades(self, limit: int = 50) -> list[dict[str, Any]]:
        import psycopg2

        bounded_limit = max(1, min(int(limit), 200))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_id, order_id, symbol, side, quantity, price,
                           fee, fee_asset, realized_pnl, executed_at
                    FROM trades
                    ORDER BY executed_at DESC
                    LIMIT %s
                    """,
                    (bounded_limit,),
                )
                rows = cursor.fetchall()
        columns = (
            "trade_id", "order_id", "symbol", "side", "quantity", "price",
            "fee", "fee_asset", "realized_pnl", "executed_at",
        )
        return [_row_dict(columns, row) for row in rows]

    def list_positions(self) -> list[dict[str, Any]]:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT symbol, quantity, average_price, realized_pnl,
                           unrealized_pnl, updated_at, market, position_side,
                           entry_price, mark_price, notional, leverage,
                           margin_type, initial_margin, maintenance_margin,
                           liquidation_price, funding_pnl
                    FROM positions
                    ORDER BY symbol
                    """
                )
                rows = cursor.fetchall()
        columns = (
            "symbol", "quantity", "average_price", "realized_pnl",
            "unrealized_pnl", "updated_at", "market", "position_side",
            "entry_price", "mark_price", "notional", "leverage",
            "margin_type", "initial_margin", "maintenance_margin",
            "liquidation_price", "funding_pnl",
        )
        return [_row_dict(columns, row) for row in rows]

    def list_balances(self) -> list[dict[str, Any]]:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT asset, free, locked, mode, updated_at,
                           wallet_balance, available_balance, margin_balance,
                           used_margin, unrealized_pnl
                    FROM balances
                    ORDER BY asset
                    """
                )
                rows = cursor.fetchall()
        columns = (
            "asset", "free", "locked", "mode", "updated_at",
            "wallet_balance", "available_balance", "margin_balance",
            "used_margin", "unrealized_pnl",
        )
        return [_row_dict(columns, row) for row in rows]

    def list_risk_events(self, limit: int = 50) -> list[dict[str, Any]]:
        import psycopg2

        bounded_limit = max(1, min(int(limit), 200))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT event_id, intent_id, decision, reason, event_at
                    FROM risk_events
                    ORDER BY event_at DESC
                    LIMIT %s
                    """,
                    (bounded_limit,),
                )
                rows = cursor.fetchall()
        columns = ("event_id", "intent_id", "decision", "reason", "event_at")
        return [_row_dict(columns, row) for row in rows]

    def list_system_events(self, limit: int = 50) -> list[dict[str, Any]]:
        import psycopg2

        bounded_limit = max(1, min(int(limit), 200))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT event_id, event_type, severity, message, event_at
                    FROM system_events
                    ORDER BY event_at DESC
                    LIMIT %s
                    """,
                    (bounded_limit,),
                )
                rows = cursor.fetchall()
        columns = ("event_id", "event_type", "severity", "message", "event_at")
        return [_row_dict(columns, row) for row in rows]

    def trading_summary(self) -> dict[str, Any]:
        from datetime import datetime, timezone

        balances = self.list_balances()
        usdt = next((row for row in balances if row["asset"] == "USDT"), None)
        trades = self.list_trades(limit=200)
        positions = self.list_positions()
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
    ) -> UUID:
        import psycopg2

        event_id = uuid4()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO risk_events(
                        event_id, intent_id, decision, reason, event_at, payload
                    ) VALUES (%s, %s, %s, %s, %s, CAST(%s AS JSONB))
                    RETURNING event_id
                    """,
                    (
                        str(event_id),
                        str(intent_id) if intent_id is not None else None,
                        decision,
                        reason,
                        _now(),
                        _json(payload),
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
    ) -> UUID:
        import psycopg2

        event_id = uuid4()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO system_events(
                        event_id, event_type, severity, message, event_at, payload
                    ) VALUES (%s, %s, %s, %s, %s, CAST(%s AS JSONB))
                    RETURNING event_id
                    """,
                    (
                        str(event_id),
                        event_type,
                        severity,
                        message,
                        _now(),
                        _json(payload),
                    ),
                )
                row = cursor.fetchone()
                return UUID(str(row[0]))
