"""Trading persistence owner, separate from the schema/connection owner."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable
from uuid import UUID, uuid4

from scripts.database import configured_dsn, ensure_schema
from trade_intent import TradeIntent


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


class TradingStore:
    """Persist trading facts and audit events without owning schema creation."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or configured_dsn()

    @staticmethod
    def _mode(mode: str | None = None) -> str:
        import os
        return (mode or os.environ.get("BIAN_MODE", "paper")).strip().lower()

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
        self, symbol: str, *, market: str = "FUTURES"
    ) -> dict[str, Any] | None:
        """Return the single persisted directional lifecycle for a symbol."""
        import psycopg2

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
                    ORDER BY started_at DESC, episode_id DESC
                    LIMIT 1
                    """,
                    (symbol.upper(), market.upper()),
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
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO positioning_episodes(
                        episode_id, symbol, market, direction, started_at, state,
                        status, last_observed_at, strategy_version, config_hash,
                        metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s, 'OPEN', %s, %s, %s,
                              CAST(%s AS JSONB))
                    ON CONFLICT (symbol, market)
                    WHERE status IN ('OPEN', 'UNRESOLVED')
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
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
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
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s,
                              CAST(%s AS JSONB), %s, %s, %s, %s, %s,
                              CAST(%s AS JSONB), CAST(%s AS JSONB))
                    ON CONFLICT (snapshot_id) DO UPDATE SET
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
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO evidence_snapshots(
                        snapshot_id, symbol, observed_at, source_timestamps,
                        evidence, data_quality, episode_id, episode_started_at,
                        episode_ended_at, episode_direction, episode_status,
                        universe_classification, payload
                    ) VALUES (%s, %s, %s, CAST(%s AS JSONB), CAST(%s AS JSONB),
                              CAST(%s AS JSONB), %s, %s, %s, %s, %s, %s,
                              CAST(%s AS JSONB), CAST(%s AS JSONB))
                    ON CONFLICT (snapshot_id) DO UPDATE SET
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

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT ON (symbol, event_type)
                           symbol, market, event_type, event_timestamp,
                           received_timestamp, latency_ms, metadata
                    FROM market_flow_events
                    WHERE market = 'FUTURES'
                      AND COALESCE(metadata->>'health', '') IS DISTINCT FROM 'STALE'
                      AND COALESCE(metadata->>'health_status', '') IS DISTINCT FROM 'STALE'
                      AND COALESCE(metadata->'metadata'->>'health', '') IS DISTINCT FROM 'STALE'
                      AND COALESCE(metadata->'metadata'->>'health_status', '') IS DISTINCT FROM 'STALE'
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
        requested_symbols = {
            str(symbol).upper().replace("-PERP", "").removesuffix("PERP").replace("-", "")
            for symbol in (symbols or ())
            if str(symbol).strip()
        }
        canonical: dict[str, dict[str, tuple[str, Any, Any, int, dict[str, Any]]]] = {}
        symbols: set[str] = set()
        for symbol, market, event_type, source_at, received_at, latency_ms, metadata in rows:
            normalized_symbol = str(symbol).upper()
            if requested_symbols and normalized_symbol not in requested_symbols:
                continue
            symbols.add(normalized_symbol)
            payload = metadata if isinstance(metadata, dict) else {}
            nested = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
            marker = str(
                payload.get("health")
                or payload.get("health_status")
                or nested.get("health")
                or nested.get("health_status")
                or ""
            ).upper()
            if marker == "STALE":
                continue
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
        """Return canonical websocket reconnect events from the latest heartbeat."""
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT metadata
                    FROM market_flow_events
                    WHERE market = 'FUTURES'
                      AND event_type IN ('LIQUIDATION_HEARTBEAT', 'FUTURES_LIQUIDATION_LIVENESS')
                    ORDER BY received_timestamp DESC
                    LIMIT 20
                    """
                )
                rows = cursor.fetchall()
        for (metadata,) in rows:
            payload = metadata if isinstance(metadata, dict) else {}
            nested = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
            found = payload.get("reconnect_events") or nested.get("reconnect_events") or []
            events = [event for event in found if isinstance(event, dict)]
            if events:
                return events
        return []

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
    ) -> list[Any]:
        """Load historical normalized frames in deterministic timestamp order."""
        import psycopg2
        from engine import MarketFrame

        bounded_limit = max(1, min(int(limit), 100_000))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                if symbol:
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
                else:
                    cursor.execute(
                        """
                        SELECT payload
                        FROM positioning_snapshots
                        ORDER BY observed_at, snapshot_id
                        LIMIT %s
                        """,
                        (bounded_limit,),
                    )
                payloads = [dict(row[0]) for row in cursor.fetchall() if isinstance(row[0], dict)]
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
                        fee, fee_asset, realized_pnl, executed_at, market, mode,
                        position_side, funding, source_event_id, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, CAST(%s AS JSONB))
                    ON CONFLICT (source_event_id) DO UPDATE
                    SET source_event_id = EXCLUDED.source_event_id
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
                        self._mode((payload or {}).get("mode")),
                        position_side,
                        funding,
                        source_event_id,
                        _json(payload),
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
                        payment, position_side, recorded_at, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB))
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
    ) -> None:
        import psycopg2

        resolved_entry = entry_price if entry_price is not None else average_price
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO positions(
                        market, symbol, quantity, average_price, realized_pnl,
                        unrealized_pnl, updated_at, payload, position_side,
                        entry_price, mark_price, index_price, notional,
                        leverage, margin_type, initial_margin,
                        maintenance_margin, liquidation_price, funding_pnl
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB),
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (market, symbol) DO UPDATE SET
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

    def get_position(self, symbol: str, *, market: str = "FUTURES") -> dict[str, Any] | None:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT market, symbol, quantity, average_price, realized_pnl,
                           unrealized_pnl, updated_at, position_side,
                           entry_price, mark_price, index_price, notional, leverage,
                           margin_type, initial_margin, maintenance_margin,
                           liquidation_price, funding_pnl, payload
                    FROM positions
                    WHERE market = %s AND symbol = %s
                    """,
                    (market, symbol),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        return {
            "market": row[0],
            "symbol": row[1],
            "quantity": Decimal(str(row[2])),
            "average_price": Decimal(str(row[3])),
            "realized_pnl": Decimal(str(row[4])),
            "unrealized_pnl": Decimal(str(row[5])),
            "updated_at": row[6],
            "position_side": row[7],
            "entry_price": Decimal(str(row[8] if row[8] is not None else row[3])),
            "mark_price": Decimal(str(row[9])) if row[9] is not None else None,
            "index_price": Decimal(str(row[10])) if row[10] is not None else None,
            "notional": Decimal(str(row[11])) if row[11] is not None else None,
            "leverage": Decimal(str(row[12])) if row[12] is not None else None,
            "margin_type": row[13],
            "initial_margin": Decimal(str(row[14])) if row[14] is not None else None,
            "maintenance_margin": Decimal(str(row[15])) if row[15] is not None else None,
            "liquidation_price": Decimal(str(row[16])) if row[16] is not None else None,
            "funding_pnl": Decimal(str(row[17] if row[17] is not None else "0")),
            "payload": row[18] if isinstance(row[18], dict) else {},
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
        return [
            row
            for row in self.list_orders(limit=200, mode=mode, market=market)
            if row["status"] in {"CREATED", "RISK_APPROVED", "SUBMITTED", "ACKNOWLEDGED", "PARTIALLY_FILLED", "UNKNOWN"}
        ]

    def list_trades(self, limit: int = 50, *, mode: str | None = None, market: str = "FUTURES") -> list[dict[str, Any]]:
        import psycopg2

        bounded_limit = max(1, min(int(limit), 200))
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_id, order_id, symbol, side, quantity, price,
                           fee, fee_asset, realized_pnl, executed_at, market, mode,
                           position_side, funding, payload
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
            "mode", "position_side", "funding", "payload",
        )
        return [_row_dict(columns, row) for row in rows]

    def list_positions(self, *, market: str = "FUTURES") -> list[dict[str, Any]]:
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT market, symbol, quantity, average_price, realized_pnl,
                           unrealized_pnl, updated_at, position_side,
                           entry_price, mark_price, index_price, notional, leverage,
                           margin_type, initial_margin, maintenance_margin,
                           liquidation_price, funding_pnl
                    FROM positions
                    WHERE market = %s
                    ORDER BY market, symbol
                    """, (market.upper(),)
                )
                rows = cursor.fetchall()
        columns = (
            "market", "symbol", "quantity", "average_price", "realized_pnl",
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

    def runtime_acceptance_snapshot(self, *, mode: str = "paper") -> dict[str, Any]:
        """Read canonical paper/shadow acceptance counts from PostgreSQL."""
        import psycopg2

        resolved = str(mode or "paper").strip().lower()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                def count(sql: str, params: tuple[Any, ...] = ()) -> int:
                    cursor.execute(sql, params)
                    row = cursor.fetchone()
                    return int(row[0] or 0)

                observation_count = count("SELECT COUNT(*) FROM market_flow_events WHERE market = 'FUTURES'")
                positioning_count = count("SELECT COUNT(*) FROM positioning_snapshots")
                evidence_count = count("SELECT COUNT(*) FROM evidence_snapshots")
                episode_count = count("SELECT COUNT(*) FROM positioning_episodes")
                intent_count = count("SELECT COUNT(*) FROM trade_intents")
                risk_decision_count = count("SELECT COUNT(*) FROM risk_events")
                order_count = count("SELECT COUNT(*) FROM orders WHERE mode = %s", (resolved,))
                fill_count = count("SELECT COUNT(*) FROM trades WHERE mode = %s", (resolved,))
                funding_count = count("SELECT COUNT(*) FROM funding_settlements WHERE mode = %s", (resolved,))
                long_count = count(
                    "SELECT COUNT(*) FROM positioning_snapshots WHERE direction = 'LONG'"
                )
                short_count = count(
                    "SELECT COUNT(*) FROM positioning_snapshots WHERE direction = 'SHORT'"
                )
                long_building_count = count(
                    "SELECT COUNT(*) FROM positioning_snapshots WHERE state = 'LONG_BUILDING'"
                )
                short_building_count = count(
                    "SELECT COUNT(*) FROM positioning_snapshots WHERE state = 'SHORT_BUILDING'"
                )
                duplicate_trades = count(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT source_event_id FROM trades
                        WHERE source_event_id IS NOT NULL
                        GROUP BY source_event_id HAVING COUNT(*) > 1
                    ) duplicated
                    """
                )
                duplicate_funding = count(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT mode, symbol, settlement_timestamp
                        FROM funding_settlements
                        GROUP BY mode, symbol, settlement_timestamp
                        HAVING COUNT(*) > 1
                    ) duplicated
                    """
                )
                invalid_positions = count(
                    """
                    SELECT COUNT(*) FROM positions
                    WHERE quantity < 0
                       OR (quantity > 0 AND COALESCE(entry_price, 0) <= 0)
                       OR (quantity > 0 AND COALESCE(leverage, 0) <= 0)
                       OR COALESCE(position_side, 'FLAT') NOT IN ('LONG', 'SHORT', 'FLAT')
                    """
                )
                stale_open = count(
                    "SELECT COUNT(*) FROM orders WHERE status = 'OPEN' AND updated_at < NOW() - INTERVAL '1 hour'"
                )
                unsafe_open = count(
                    "SELECT COUNT(*) FROM orders WHERE status IN ('UNKNOWN', 'UNSAFE')"
                )
                cursor.execute(
                    """
                    SELECT wallet_balance, used_margin, available_balance, unrealized_pnl, margin_balance, payload
                    FROM balances
                    WHERE mode = %s AND asset = 'USDT'
                    """,
                    (resolved,),
                )
                balance = cursor.fetchone()
                impossible_balance = False
                impossible_equity = False
                invalid_margin = False
                accounting: dict[str, str] = {}
                if balance is not None:
                    wallet, used, available, unrealized = (
                        Decimal(str(balance[0] or 0)),
                        Decimal(str(balance[1] or 0)),
                        Decimal(str(balance[2] or 0)),
                        Decimal(str(balance[3] or 0)),
                    )
                    stored_equity = Decimal(str(balance[4])) if balance[4] is not None else wallet + unrealized
                    payload = balance[5] if isinstance(balance[5], dict) else {}
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
                        WHERE status IN ('OPEN', 'UNRESOLVED')
                        GROUP BY symbol, market HAVING COUNT(*) > 1
                    ) split_rows
                    """
                ) > 0
                restart_recovery = (
                    not episode_split
                    and duplicate_trades == 0
                    and duplicate_funding == 0
                )
        return {
            "database_healthy": True,
            "observation_count": observation_count,
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
            "stale_open": stale_open,
            "unsafe_open": unsafe_open,
            "accounting": accounting,
            "restart_recovery": restart_recovery,
            "episode_split": episode_split,
        }

    def record_runtime_gate_status(self, gate: str, status: str, *, detail: dict[str, Any] | None = None) -> UUID:
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
        payload = {"gate": normalized_gate, "status": normalized_status, **(detail or {})}
        return self.record_system_event(
            event_type="RUNTIME_GATE_EVIDENCE",
            severity="INFO" if normalized_status != "FAILED" else "CRITICAL",
            message=f"runtime gate {normalized_gate} is {normalized_status}",
            payload=payload,
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
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT ON ((payload->>'gate')) payload->>'gate', payload->>'status'
                    FROM system_events
                    WHERE event_type = 'RUNTIME_GATE_EVIDENCE'
                      AND payload->>'gate' IN ('observation', 'paper', 'shadow', 'testnet', 'realtime_30m', 'realtime_2h', 'realtime_6h', 'realtime_24h')
                    ORDER BY (payload->>'gate'), event_at DESC
                    """
                )
                for gate, status in cursor.fetchall():
                    if gate in values and status:
                        values[gate] = str(status).upper()
        return values

    def runtime_gate_evidence(self) -> dict[str, dict[str, Any]]:
        """Return latest persisted status and verification timestamp per gate."""
        import psycopg2

        values: dict[str, dict[str, Any]] = {}
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT ON ((payload->>'gate'))
                           payload->>'gate', payload->>'status', event_at, payload
                    FROM system_events
                    WHERE event_type = 'RUNTIME_GATE_EVIDENCE'
                      AND payload->>'gate' IN ('observation', 'paper', 'shadow', 'testnet', 'realtime_30m', 'realtime_2h', 'realtime_6h', 'realtime_24h')
                    ORDER BY (payload->>'gate'), event_at DESC
                    """
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
                        }
        return values

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
        mode = self._mode((payload or {}).get("mode"))
        market = str((payload or {}).get("market", "FUTURES")).upper()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO risk_events(
                        event_id, intent_id, decision, reason, mode, market, event_at, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB))
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
        mode = self._mode((payload or {}).get("mode"))
        market = str((payload or {}).get("market", "FUTURES")).upper()
        with psycopg2.connect(self.dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO system_events(
                        event_id, event_type, severity, message, mode, market, event_at, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB))
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
                    ),
                )
                row = cursor.fetchone()
                return UUID(str(row[0]))
