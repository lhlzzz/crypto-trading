"""Pure legacy and capital-positioning decision core for bian.

The engine consumes normalized observations only. It never parses exchange
payloads, calls a broker, evaluates risk, or writes to storage.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from statistics import fmean
from typing import Any, Literal, Mapping
from uuid import UUID, NAMESPACE_URL, uuid5

from trade_intent import TradeIntent

PositioningState = Literal[
    "NEUTRAL", "LONG_BUILDING", "SHORT_BUILDING", "LONG_UNWIND",
    "SHORT_COVERING", "ABSORPTION_LONG", "ABSORPTION_SHORT",
    "EXHAUSTION_LONG", "EXHAUSTION_SHORT", "FORCED_DELEVERAGING",
    "TRANSITION", "CONFLICTED", "UNKNOWN",
]
Direction = Literal["LONG", "SHORT", "FLAT"]
MarketRegime = Literal[
    "RISK_ON", "RISK_OFF", "TRENDING_UP", "TRENDING_DOWN",
    "HIGH_VOL", "LOW_VOL", "NEUTRAL",
]


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _sign(value: Decimal | None, threshold: Decimal = Decimal("0")) -> int:
    if value is None:
        return 0
    return 1 if value > threshold else -1 if value < -threshold else 0


def _timestamps_consistent(
    source_timestamps: Mapping[str, Mapping[str, Any]], now: datetime
) -> bool:
    """Verify normalized timestamp/latency provenance before a decision."""
    if not source_timestamps:
        return False
    for timestamps in source_timestamps.values():
        try:
            source = _aware(datetime.fromisoformat(str(timestamps["source_timestamp"])))
            received = _aware(datetime.fromisoformat(str(timestamps["received_timestamp"])))
            declared_latency = int(timestamps["latency_ms"])
        except (KeyError, TypeError, ValueError):
            return False
        actual_latency = int((received - source).total_seconds() * 1000)
        if (
            actual_latency < 0
            or declared_latency != actual_latency
            or source > _aware(now)
            or received > _aware(now)
        ):
            return False
    return True


@dataclass(frozen=True)
class MarketDataEnvelope:
    """Normalized exchange observation with explicit clock provenance."""

    source: str
    market: Literal["SPOT", "FUTURES"]
    symbol: str
    event_type: str
    source_timestamp: datetime
    received_timestamp: datetime
    latency_ms: int
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        source = _aware(self.source_timestamp)
        received = _aware(self.received_timestamp)
        latency = int((received - source).total_seconds() * 1000)
        if latency < 0:
            raise ValueError("received_timestamp cannot precede source_timestamp")
        if self.latency_ms != latency:
            raise ValueError("latency_ms must equal timestamp delta")
        if not self.symbol.strip():
            raise ValueError("symbol is required")
        object.__setattr__(self, "source_timestamp", source)
        object.__setattr__(self, "received_timestamp", received)
        object.__setattr__(self, "symbol", self.symbol.upper().strip())

    @classmethod
    def create(cls, **values: Any) -> "MarketDataEnvelope":
        source = _aware(values["source_timestamp"])
        received = _aware(values["received_timestamp"])
        values["source_timestamp"] = source
        values["received_timestamp"] = received
        values["latency_ms"] = int((received - source).total_seconds() * 1000)
        return cls(**values)


@dataclass(frozen=True)
class SourceFreshness:
    source: str
    source_timestamp: datetime
    received_timestamp: datetime
    max_age_sec: int
    now: datetime

    @property
    def age_sec(self) -> int:
        return max(0, int((_aware(self.now) - _aware(self.source_timestamp)).total_seconds()))

    @property
    def fresh(self) -> bool:
        return (
            _aware(self.received_timestamp) <= _aware(self.now)
            and _aware(self.source_timestamp) <= _aware(self.now)
            and self.age_sec <= max(1, self.max_age_sec)
        )


@dataclass(frozen=True)
class EvidenceVector:
    price: int = 0
    spot_flow: int = 0
    cvd: int = 0
    oi: int = 0
    funding: int = 0
    taker: int = 0
    orderbook: int = 0
    liquidation: int = 0
    basis: int = 0
    relative_strength: int = 0
    market_regime: int = 0
    quality: Decimal = Decimal("0")
    freshness: Decimal = Decimal("0")


@dataclass(frozen=True)
class PositioningWeights:
    price: Decimal = Decimal("1")
    spot_flow: Decimal = Decimal("2")
    cvd: Decimal = Decimal("2")
    oi: Decimal = Decimal("1")
    funding: Decimal = Decimal("0.5")
    taker: Decimal = Decimal("2")
    orderbook: Decimal = Decimal("1")
    liquidation: Decimal = Decimal("1")
    basis: Decimal = Decimal("0.5")
    relative_strength: Decimal = Decimal("1")
    market_regime: Decimal = Decimal("1")


@dataclass(frozen=True)
class PositioningDecision:
    symbol: str
    timestamp: datetime
    direction: Direction
    state: PositioningState
    transition: str
    previous_state: PositioningState | None
    transition_strength: Decimal
    confidence: Decimal
    long_score: Decimal
    short_score: Decimal
    crowding_score: Decimal
    liquidity_score: Decimal
    data_quality_score: Decimal
    reason_codes: tuple[str, ...]
    evidence: EvidenceVector
    evidence_snapshot_id: UUID
    market_regime: MarketRegime
    source_timestamps: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    input_features: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timestamp": _aware(self.timestamp).isoformat(),
            "direction": self.direction,
            "state": self.state,
            "transition": self.transition,
            "previous_state": self.previous_state,
            "transition_strength": str(self.transition_strength),
            "confidence": str(self.confidence),
            "long_score": str(self.long_score),
            "short_score": str(self.short_score),
            "crowding_score": str(self.crowding_score),
            "liquidity_score": str(self.liquidity_score),
            "data_quality_score": str(self.data_quality_score),
            "reason_codes": list(self.reason_codes),
            "evidence": self.evidence.__dict__,
            "evidence_snapshot_id": str(self.evidence_snapshot_id),
            "market_regime": self.market_regime,
            "source_timestamps": dict(self.source_timestamps),
            "input_features": dict(self.input_features),
        }


@dataclass(frozen=True)
class MarketFrame:
    symbol: str
    closes: tuple[Decimal, ...]
    captured_at: datetime
    bid_price: Decimal | None = None
    ask_price: Decimal | None = None
    quote_volume: Decimal | None = None
    volume: Decimal | None = None
    last_price: Decimal | None = None
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    spot_buy_volume: Decimal | None = None
    spot_sell_volume: Decimal | None = None
    net_spot_flow: Decimal | None = None
    cvd: Decimal | None = None
    cvd_change: Decimal | None = None
    cvd_1m: Decimal | None = None
    cvd_3m: Decimal | None = None
    cvd_5m: Decimal | None = None
    cvd_15m: Decimal | None = None
    cvd_1h: Decimal | None = None
    cvd_30m: Decimal | None = None
    cvd_acceleration: Decimal | None = None
    price_cvd_divergence: bool | None = None
    volume_ratio_1m: Decimal | None = None
    volume_ratio_5m: Decimal | None = None
    volume_ratio_15m: Decimal | None = None
    volume_zscore: Decimal | None = None
    taker_buy_volume: Decimal | None = None
    taker_sell_volume: Decimal | None = None
    taker_buy_volume_30m: Decimal | None = None
    taker_sell_volume_30m: Decimal | None = None
    oi: Decimal | None = None
    oi_change: Decimal | None = None
    oi_change_1m: Decimal | None = None
    oi_change_3m: Decimal | None = None
    oi_change_5m: Decimal | None = None
    oi_change_15m: Decimal | None = None
    oi_change_1h: Decimal | None = None
    oi_change_30m: Decimal | None = None
    funding_rate: Decimal | None = None
    funding_change: Decimal | None = None
    funding_percentile: Decimal | None = None
    funding_zscore: Decimal | None = None
    basis_bps: Decimal | None = None
    global_long_short_ratio: Decimal | None = None
    top_trader_long_short_ratio: Decimal | None = None
    short_liquidation_notional: Decimal | None = None
    long_liquidation_notional: Decimal | None = None
    liquidation_acceleration: Decimal | None = None
    bid_depth_5: Decimal | None = None
    ask_depth_5: Decimal | None = None
    bid_depth_10: Decimal | None = None
    ask_depth_10: Decimal | None = None
    bid_depth_20: Decimal | None = None
    ask_depth_20: Decimal | None = None
    spread_bps: Decimal | None = None
    depth_10bps: Decimal | None = None
    depth_25bps: Decimal | None = None
    depth_50bps: Decimal | None = None
    price_impact_buy: Decimal | None = None
    price_impact_sell: Decimal | None = None
    liquidity_added: Decimal | None = None
    liquidity_removed: Decimal | None = None
    relative_strength: Decimal | None = None
    relative_strength_1m: Decimal | None = None
    relative_strength_5m: Decimal | None = None
    relative_strength_15m: Decimal | None = None
    relative_strength_1h: Decimal | None = None
    breadth_score: Decimal | None = None
    advance_decline_ratio: Decimal | None = None
    market_regime: MarketRegime = "NEUTRAL"
    freshness: tuple[SourceFreshness, ...] = ()
    data_quality_score: Decimal | None = None
    source_timestamps: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_evidence_snapshot(
        cls,
        payload: Mapping[str, Any],
        *,
        source_ttl_sec: int = 900,
    ) -> "MarketFrame":
        """Rebuild a decision frame from persisted normalized evidence only."""
        captured_at = _aware(datetime.fromisoformat(str(payload["timestamp"])))
        inputs = dict(payload.get("input_features") or {})
        source_timestamps = dict(payload.get("source_timestamps") or {})
        decimal_fields = {
            "bid_price", "ask_price", "quote_volume", "volume",
            "spot_buy_volume", "spot_sell_volume", "net_spot_flow", "cvd",
            "cvd_change", "cvd_1m", "cvd_3m", "cvd_5m", "cvd_15m",
            "cvd_1h", "cvd_30m", "cvd_acceleration", "volume_ratio_1m",
            "volume_ratio_5m", "volume_ratio_15m", "volume_zscore",
            "taker_buy_volume", "taker_sell_volume",
            "taker_buy_volume_30m", "taker_sell_volume_30m",
            "oi", "oi_change", "last_price", "mark_price", "index_price",
            "oi_change_1m", "oi_change_3m", "oi_change_5m",
            "oi_change_15m", "oi_change_1h", "oi_change_30m",
            "funding_rate", "funding_change", "funding_percentile", "funding_zscore",
            "basis_bps", "global_long_short_ratio", "top_trader_long_short_ratio",
            "short_liquidation_notional", "long_liquidation_notional",
            "liquidation_acceleration", "bid_depth_5", "ask_depth_5",
            "bid_depth_10", "ask_depth_10", "bid_depth_20", "ask_depth_20",
            "spread_bps", "depth_10bps", "depth_25bps", "depth_50bps",
            "price_impact_buy", "price_impact_sell", "liquidity_added",
            "liquidity_removed", "relative_strength", "relative_strength_1m",
            "relative_strength_5m", "relative_strength_15m", "relative_strength_1h",
            "breadth_score", "advance_decline_ratio", "data_quality_score",
        }
        values = {
            name: Decimal(str(value))
            for name in decimal_fields
            if (value := inputs.get(name)) is not None
        }
        if inputs.get("price_cvd_divergence") is not None:
            values["price_cvd_divergence"] = bool(inputs["price_cvd_divergence"])
        closes = tuple(Decimal(str(value)) for value in inputs.get("closes", ()))
        if not closes:
            raise ValueError("evidence snapshot requires closes")
        freshness: list[SourceFreshness] = []
        for source, timestamps in source_timestamps.items():
            freshness.append(
                SourceFreshness(
                    source=source,
                    source_timestamp=_aware(datetime.fromisoformat(
                        str(timestamps["source_timestamp"])
                    )),
                    received_timestamp=_aware(datetime.fromisoformat(
                        str(timestamps["received_timestamp"])
                    )),
                    max_age_sec=max(1, source_ttl_sec),
                    now=captured_at,
                )
            )
        market_regime = str(inputs.get("market_regime", "NEUTRAL"))
        valid_regimes = {
            "RISK_ON", "RISK_OFF", "TRENDING_UP", "TRENDING_DOWN",
            "HIGH_VOL", "LOW_VOL", "NEUTRAL",
        }
        if market_regime not in valid_regimes:
            raise ValueError("evidence snapshot has an invalid market regime")
        return cls(
            symbol=str(payload["symbol"]),
            closes=closes,
            captured_at=captured_at,
            market_regime=market_regime,  # type: ignore[arg-type]
            freshness=tuple(freshness),
            source_timestamps=source_timestamps,
            **values,
        )


@dataclass(frozen=True)
class StrategyConfig:
    fast_window: int = 5
    slow_window: int = 20
    minimum_confidence: Decimal = Decimal("0.6")
    strategy_version: str = "momentum-sma-1"
    order_quote_usdt: Decimal = Decimal("25")
    positioning_strategy_version: str = "positioning-v1"
    positioning_decision_enabled: bool = False
    minimum_positioning_confidence: Decimal = Decimal("0.6")
    minimum_positioning_edge: Decimal = Decimal("0.15")
    minimum_transition_strength: Decimal = Decimal("0.15")
    minimum_data_quality: Decimal = Decimal("0.8")
    maximum_crowding: Decimal = Decimal("0.9")
    minimum_liquidity_score: Decimal = Decimal("0.4")
    source_ttl_sec: int = 900
    positioning_weights: PositioningWeights = field(default_factory=PositioningWeights)

    @classmethod
    def from_env(cls) -> "StrategyConfig":
        import os

        def decimal(name: str, default: Decimal) -> Decimal:
            try:
                return Decimal(os.environ.get(name, str(default)))
            except Exception:
                return default

        return cls(
            strategy_version=os.environ.get("LEGACY_STRATEGY_VERSION", "momentum-sma-1"),
            positioning_strategy_version=os.environ.get(
                "POSITIONING_STRATEGY_VERSION", "positioning-v1"
            ),
            positioning_decision_enabled=os.environ.get(
                "POSITIONING_DECISION_ENABLED", "false"
            ).strip().lower() in {"1", "true", "yes", "on"},
            minimum_positioning_confidence=decimal(
                "POSITIONING_MIN_CONFIDENCE", Decimal("0.6")
            ),
            minimum_positioning_edge=decimal(
                "POSITIONING_MIN_EDGE", Decimal("0.15")
            ),
            minimum_transition_strength=decimal(
                "POSITIONING_MIN_TRANSITION_STRENGTH", Decimal("0.15")
            ),
            minimum_data_quality=decimal(
                "POSITIONING_MIN_DATA_QUALITY", Decimal("0.8")
            ),
            maximum_crowding=decimal(
                "POSITIONING_MAX_CROWDING", Decimal("0.9")
            ),
            minimum_liquidity_score=decimal(
                "POSITIONING_MIN_LIQUIDITY", Decimal("0.4")
            ),
        )

    def __post_init__(self) -> None:
        if self.fast_window < 2 or self.slow_window <= self.fast_window:
            raise ValueError("slow_window must be greater than fast_window >= 2")
        if not Decimal("0") <= self.minimum_confidence <= Decimal("1"):
            raise ValueError("minimum_confidence must be between 0 and 1")
        if self.order_quote_usdt <= 0:
            raise ValueError("order_quote_usdt must be positive")
        for value in (
            self.minimum_positioning_confidence,
            self.minimum_positioning_edge,
            self.minimum_transition_strength,
            self.minimum_data_quality,
            self.maximum_crowding,
            self.minimum_liquidity_score,
        ):
            if not Decimal("0") <= value <= Decimal("1"):
                raise ValueError("positioning thresholds must be between 0 and 1")
        if self.source_ttl_sec < 1:
            raise ValueError("source_ttl_sec must be positive")
        if not self.positioning_strategy_version.strip():
            raise ValueError("positioning_strategy_version is required")


@dataclass(frozen=True)
class Signal:
    symbol: str
    side: str
    confidence: Decimal
    reason: str
    price: Decimal
    captured_at: datetime


class StrategyEngine:
    """Single owner for legacy SMA and deterministic positioning decisions."""

    def __init__(self, config: StrategyConfig | None = None) -> None:
        self.config = config or StrategyConfig()
        self._previous_states: dict[str, PositioningState] = {}

    def evaluate(self, frame: MarketFrame) -> TradeIntent | None:
        if not self.config.positioning_decision_enabled:
            return self._legacy_intent(frame)
        return self._intent_from_positioning(self.positioning_decision(frame))

    def _legacy_intent(self, frame: MarketFrame) -> TradeIntent | None:
        signal = self.signal(frame)
        if signal is None:
            return None
        return TradeIntent(
            symbol=signal.symbol,
            side=signal.side,
            order_type="MARKET",
            quote_quantity=self.config.order_quote_usdt,
            confidence=signal.confidence,
            reason=signal.reason,
            strategy_version=self.config.strategy_version,
            created_at=signal.captured_at,
        )

    def evaluate_shadow(
        self,
        frame: MarketFrame,
        *,
        positioning: PositioningDecision | None = None,
    ) -> dict[str, Any]:
        """Return both decisions without changing the execution path."""
        legacy = self.signal(frame)
        positioning = positioning or self.positioning_decision(frame)
        legacy_direction: Direction = "FLAT" if legacy is None else (
            "LONG" if legacy.side == "BUY" else "SHORT"
        )
        return {
            "symbol": frame.symbol.upper(),
            "timestamp": _aware(frame.captured_at),
            "legacy_decision": legacy_direction,
            "positioning_decision": positioning,
            "agreement": legacy_direction == positioning.direction,
            "why_different": None if legacy_direction == positioning.direction else (
                f"legacy={legacy_direction}; positioning={positioning.direction}"
            ),
        }

    def positioning_decision(
        self,
        frame: MarketFrame,
        *,
        now: datetime | None = None,
        previous_state: PositioningState | None = None,
    ) -> PositioningDecision:
        timestamp = _aware(frame.captured_at)
        evaluation_now = _aware(now or timestamp)
        if timestamp > evaluation_now:
            return self._unknown_decision(frame, timestamp, "FUTURE_DATA")
        evidence, quality, reasons = self._evidence(frame, evaluation_now)
        long_score, short_score = self._scores(evidence)
        crowding = self._crowding_score(frame)
        liquidity = self._liquidity_score(frame)
        state = self._state(frame, evidence, crowding, quality)
        prior = previous_state or self._previous_states.get(frame.symbol.upper())
        transition = f"{prior}->{state}" if prior and prior != state else "NONE"
        edge = long_score - short_score
        transition_strength = min(Decimal("1"), abs(edge) * quality)
        confidence = min(
            Decimal("1"),
            abs(edge) * Decimal("1.5") * quality * (
                Decimal("1") - crowding * Decimal("0.35")
            ),
        )
        direction: Direction = "FLAT"
        passes = (
            confidence >= self.config.minimum_positioning_confidence
            and transition_strength >= self.config.minimum_transition_strength
            and quality >= self.config.minimum_data_quality
            and liquidity >= self.config.minimum_liquidity_score
            and crowding <= self.config.maximum_crowding
        )
        if state == "LONG_BUILDING" and passes:
            if edge >= self.config.minimum_positioning_edge:
                direction = "LONG"
            else:
                reasons.append("STATE_EDGE_CONFLICT")
        elif state == "SHORT_BUILDING" and passes:
            if edge <= -self.config.minimum_positioning_edge:
                direction = "SHORT"
            else:
                reasons.append("STATE_EDGE_CONFLICT")
        elif state not in {"UNKNOWN", "CONFLICTED", "FORCED_DELEVERAGING"}:
            reasons.append("ENTRY_STATE_NOT_CONFIRMED")
        if state == "UNKNOWN":
            reasons.append("DATA_UNKNOWN")
        if state == "CONFLICTED":
            reasons.append("EVIDENCE_CONFLICT")
        if crowding > self.config.maximum_crowding:
            reasons.append("CROWDING_HIGH")
        if liquidity < self.config.minimum_liquidity_score:
            reasons.append("LIQUIDITY_LOW")
        if quality < self.config.minimum_data_quality:
            reasons.append("DATA_QUALITY_LOW")
        if transition_strength < self.config.minimum_transition_strength:
            reasons.append("TRANSITION_STRENGTH_LOW")
        self._previous_states[frame.symbol.upper()] = state
        snapshot_id = self._snapshot_id(frame, timestamp, evidence)
        return PositioningDecision(
            symbol=frame.symbol.upper(),
            timestamp=timestamp,
            direction=direction,
            state=state,
            transition=transition,
            previous_state=prior,
            transition_strength=transition_strength,
            confidence=confidence,
            long_score=long_score,
            short_score=short_score,
            crowding_score=crowding,
            liquidity_score=liquidity,
            data_quality_score=quality,
            reason_codes=tuple(dict.fromkeys(reasons)),
            evidence=evidence,
            evidence_snapshot_id=snapshot_id,
            market_regime=frame.market_regime,
            source_timestamps=frame.source_timestamps,
            input_features=self._input_features(frame),
        )

    def _unknown_decision(
        self, frame: MarketFrame, timestamp: datetime, reason: str
    ) -> PositioningDecision:
        snapshot_id = self._snapshot_id(frame, timestamp, EvidenceVector())
        return PositioningDecision(
            symbol=frame.symbol.upper(), timestamp=timestamp, direction="FLAT",
            state="UNKNOWN", transition="NONE", previous_state=None,
            transition_strength=Decimal("0"), confidence=Decimal("0"),
            long_score=Decimal("0"), short_score=Decimal("0"),
            crowding_score=Decimal("0"), liquidity_score=Decimal("0"),
            data_quality_score=Decimal("0"), reason_codes=(reason,),
            evidence=EvidenceVector(), evidence_snapshot_id=snapshot_id,
            market_regime=frame.market_regime,
            source_timestamps=frame.source_timestamps,
            input_features=self._input_features(frame),
        )

    def _snapshot_id(
        self,
        frame: MarketFrame,
        timestamp: datetime,
        evidence: EvidenceVector,
    ) -> UUID:
        """Bind a deterministic snapshot id to the actual decision inputs."""
        payload = {
            "symbol": frame.symbol.upper(),
            "timestamp": _aware(timestamp).isoformat(),
            "strategy_version": self.config.positioning_strategy_version,
            "evidence": evidence.__dict__,
            "inputs": self._input_features(frame),
            "source_timestamps": frame.source_timestamps,
        }
        encoded = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
        return uuid5(NAMESPACE_URL, f"bian:evidence:{encoded}")

    @staticmethod
    def _input_features(frame: MarketFrame) -> dict[str, Any]:
        """Persist normalized inputs, never raw exchange payloads, for replay."""
        names = (
            "closes", "bid_price", "ask_price", "quote_volume", "volume",
            "spot_buy_volume", "spot_sell_volume", "net_spot_flow", "cvd",
            "cvd_change", "cvd_1m", "cvd_3m", "cvd_5m", "cvd_15m",
            "cvd_1h", "cvd_30m", "cvd_acceleration", "price_cvd_divergence",
            "volume_ratio_1m", "volume_ratio_5m", "volume_ratio_15m",
            "volume_zscore", "taker_buy_volume", "taker_sell_volume",
            "taker_buy_volume_30m", "taker_sell_volume_30m",
            "oi", "oi_change", "last_price", "mark_price", "index_price",
            "oi_change_1m", "oi_change_3m", "oi_change_5m",
            "oi_change_15m", "oi_change_1h", "oi_change_30m",
            "funding_rate", "funding_change", "funding_percentile", "funding_zscore",
            "basis_bps", "global_long_short_ratio", "top_trader_long_short_ratio",
            "short_liquidation_notional", "long_liquidation_notional",
            "liquidation_acceleration", "bid_depth_5", "ask_depth_5",
            "bid_depth_10", "ask_depth_10", "bid_depth_20", "ask_depth_20",
            "spread_bps", "depth_10bps", "depth_25bps", "depth_50bps",
            "price_impact_buy", "price_impact_sell", "liquidity_added",
            "liquidity_removed", "relative_strength", "breadth_score",
            "relative_strength_1m", "relative_strength_5m",
            "relative_strength_15m", "relative_strength_1h",
            "advance_decline_ratio",
            "market_regime", "data_quality_score",
        )
        return {name: getattr(frame, name) for name in names}

    def _intent_from_positioning(self, decision: PositioningDecision) -> TradeIntent | None:
        required_direction = {
            "LONG_BUILDING": "LONG",
            "SHORT_BUILDING": "SHORT",
        }.get(decision.state)
        if (
            decision.direction == "FLAT"
            or decision.direction != required_direction
            or decision.transition_strength < self.config.minimum_transition_strength
        ):
            return None
        return TradeIntent(
            symbol=decision.symbol,
            side="BUY" if decision.direction == "LONG" else "SELL",
            order_type="MARKET",
            quote_quantity=self.config.order_quote_usdt,
            confidence=decision.confidence,
            reason="; ".join(decision.reason_codes) or decision.state,
            strategy_version=self.config.positioning_strategy_version,
            created_at=decision.timestamp,
            direction=decision.direction,
            positioning_state=decision.state,
            transition=decision.transition,
            long_score=decision.long_score,
            short_score=decision.short_score,
            crowding_score=decision.crowding_score,
            liquidity_score=decision.liquidity_score,
            data_quality_score=decision.data_quality_score,
            reason_codes=decision.reason_codes,
            evidence_snapshot_id=decision.evidence_snapshot_id,
            market_regime=decision.market_regime,
        )

    def _evidence(
        self, frame: MarketFrame, now: datetime
    ) -> tuple[EvidenceVector, Decimal, list[str]]:
        reasons: list[str] = []
        if not frame.freshness:
            return EvidenceVector(), Decimal("0"), ["MISSING_FRESHNESS"]
        fresh_items = [
            _aware(item.received_timestamp) <= now
            and _aware(item.source_timestamp) <= now
            and (now - _aware(item.source_timestamp)).total_seconds() <= max(1, item.max_age_sec)
            for item in frame.freshness
        ]
        freshness = Decimal(sum(fresh_items)) / Decimal(len(fresh_items))
        if not all(fresh_items):
            reasons.append("STALE_DATA")
        timestamp_consistent = _timestamps_consistent(frame.source_timestamps, now)
        if not timestamp_consistent:
            reasons.append("TIMESTAMP_INCONSISTENT")
        required_features = (
            frame.net_spot_flow, frame.cvd_change, frame.oi_change,
            frame.funding_rate, frame.taker_buy_volume, frame.taker_sell_volume,
            frame.spread_bps, frame.depth_25bps,
        )
        if any(value is None for value in required_features):
            reasons.append("MISSING_REQUIRED_POSITIONING_SOURCE")
        required_timestamp_sources = (
            "spot_trade",
            "futures_open_interest",
            "futures_funding",
            "futures_taker_flow",
            "spot_book_ticker",
            "spot_orderbook",
        )
        timestamp_provenance_complete = all(
            source in frame.source_timestamps
            for source in required_timestamp_sources
        )
        if not timestamp_provenance_complete:
            reasons.append("MISSING_TIMESTAMP_PROVENANCE")
        supplied = sum(value is not None for value in (
            frame.spot_buy_volume, frame.spot_sell_volume, frame.net_spot_flow,
            frame.cvd_change, frame.oi_change, frame.funding_rate,
            frame.taker_buy_volume, frame.taker_sell_volume, frame.spread_bps,
            frame.depth_25bps,
        ))
        completeness = Decimal(supplied) / Decimal("10")
        quality = min(
            Decimal("1"), freshness * (Decimal("0.5") + completeness / Decimal("2"))
        )
        if frame.data_quality_score is not None:
            quality = min(quality, max(Decimal("0"), frame.data_quality_score))
        if frame.price_cvd_divergence:
            # Price and aggressive-flow direction disagree. Keep the raw
            # evidence for audit, but do not let a partially aligned subset
            # manufacture a directional trade.
            quality *= Decimal("0.85")
            reasons.append("PRICE_CVD_DIVERGENCE")
        if not all(fresh_items):
            quality = min(quality, self.config.minimum_data_quality - Decimal("0.01"))
        if not timestamp_consistent:
            quality = min(quality, self.config.minimum_data_quality - Decimal("0.01"))
        if any(value is None for value in required_features):
            quality = min(quality, self.config.minimum_data_quality - Decimal("0.01"))
        if not timestamp_provenance_complete:
            quality = min(quality, self.config.minimum_data_quality - Decimal("0.01"))
        price_signal = _sign(self._price_change(frame))
        flow_signal = _sign(frame.net_spot_flow)
        cvd_signal = _sign(frame.cvd_change)
        oi_signal = _sign(frame.oi_change)
        taker_signal = _sign(self._taker_flow(frame))
        funding_signal = _sign(frame.funding_rate)
        basis_signal = _sign(frame.basis_bps)
        orderbook_signal = _sign(self._orderbook_imbalance(frame))
        liquidation_signal = _sign(
            (frame.short_liquidation_notional or Decimal("0"))
            - (frame.long_liquidation_notional or Decimal("0"))
        )
        relative_signal = _sign(frame.relative_strength)
        regime_signal = (
            1 if frame.market_regime in {"RISK_ON", "TRENDING_UP"}
            else -1 if frame.market_regime in {"RISK_OFF", "TRENDING_DOWN"}
            else 0
        )
        if flow_signal > 0:
            reasons.append("SPOT_BUYING")
        elif flow_signal < 0:
            reasons.append("SPOT_SELLING")
        if cvd_signal > 0:
            reasons.append("POSITIVE_CVD")
        elif cvd_signal < 0:
            reasons.append("NEGATIVE_CVD")
        if frame.volume_ratio_5m is not None and frame.volume_ratio_5m >= Decimal("2"):
            reasons.append("VOLUME_EXCEPTIONAL")
        if oi_signal > 0:
            reasons.append("OI_EXPANSION")
        elif oi_signal < 0:
            reasons.append("OI_CONTRACTION")
        if taker_signal > 0:
            reasons.append("TAKER_BUY")
        elif taker_signal < 0:
            reasons.append("TAKER_SELL")
        return EvidenceVector(
            price=price_signal, spot_flow=flow_signal, cvd=cvd_signal, oi=oi_signal,
            funding=funding_signal, taker=taker_signal, orderbook=orderbook_signal,
            liquidation=liquidation_signal, relative_strength=relative_signal,
            basis=basis_signal, market_regime=regime_signal,
            quality=quality, freshness=freshness,
        ), quality, reasons

    def _scores(self, evidence: EvidenceVector) -> tuple[Decimal, Decimal]:
        weights = self.config.positioning_weights
        values = (
            (evidence.price, weights.price), (evidence.spot_flow, weights.spot_flow),
            (evidence.cvd, weights.cvd), (evidence.oi, weights.oi),
            (evidence.taker, weights.taker), (evidence.orderbook, weights.orderbook),
            (evidence.liquidation, weights.liquidation),
            (evidence.relative_strength, weights.relative_strength),
            (evidence.market_regime, weights.market_regime),
        )
        total = sum(weight for _, weight in values)
        return (
            sum(weight for value, weight in values if value > 0) / total,
            sum(weight for value, weight in values if value < 0) / total,
        )

    def _state(
        self, frame: MarketFrame, evidence: EvidenceVector,
        crowding: Decimal, quality: Decimal,
    ) -> PositioningState:
        if quality < self.config.minimum_data_quality:
            return "UNKNOWN"
        if (
            frame.liquidation_acceleration is not None
            and frame.liquidation_acceleration >= Decimal("2")
        ):
            return "FORCED_DELEVERAGING"
        price, oi = evidence.price, evidence.oi
        flow, taker, cvd = evidence.spot_flow, evidence.taker, evidence.cvd
        if frame.price_cvd_divergence and price and cvd and price != cvd:
            return "CONFLICTED"
        positive = sum(value > 0 for value in (price, flow, cvd, taker))
        negative = sum(value < 0 for value in (price, flow, cvd, taker))
        if positive >= 2 and negative >= 2:
            return "CONFLICTED"
        if price > 0 and oi < 0:
            return "SHORT_COVERING"
        if price < 0 and oi < 0:
            return "LONG_UNWIND"
        if price > 0 and oi > 0 and flow > 0 and taker > 0:
            if crowding > self.config.maximum_crowding and (
                cvd <= 0
                or (frame.cvd_acceleration is not None and frame.cvd_acceleration < 0)
            ):
                return "EXHAUSTION_LONG"
            return "LONG_BUILDING"
        if price < 0 and oi > 0 and flow < 0 and taker < 0:
            if crowding > self.config.maximum_crowding and (
                cvd >= 0
                or (frame.cvd_acceleration is not None and frame.cvd_acceleration > 0)
            ):
                return "EXHAUSTION_SHORT"
            return "SHORT_BUILDING"
        if (
            frame.price_impact_sell is not None
            and frame.price_impact_sell <= Decimal("0.001")
            and (frame.volume_ratio_5m or Decimal("0")) >= Decimal("2")
            and flow < 0 and taker < 0
        ):
            return "ABSORPTION_LONG"
        if (
            frame.price_impact_buy is not None
            and frame.price_impact_buy <= Decimal("0.001")
            and (frame.volume_ratio_5m or Decimal("0")) >= Decimal("2")
            and flow > 0 and taker > 0
        ):
            return "ABSORPTION_SHORT"
        return "NEUTRAL"

    @staticmethod
    def _price_change(frame: MarketFrame) -> Decimal | None:
        if len(frame.closes) < 2 or frame.closes[-2] <= 0:
            return None
        return (frame.closes[-1] - frame.closes[-2]) / frame.closes[-2]

    @staticmethod
    def _taker_flow(frame: MarketFrame) -> Decimal | None:
        if frame.taker_buy_volume is None or frame.taker_sell_volume is None:
            return None
        return frame.taker_buy_volume - frame.taker_sell_volume

    @staticmethod
    def _orderbook_imbalance(frame: MarketFrame) -> Decimal | None:
        bid, ask = frame.bid_depth_10, frame.ask_depth_10
        if bid is None or ask is None or bid + ask == 0:
            return None
        return (bid - ask) / (bid + ask)

    @staticmethod
    def _crowding_score(frame: MarketFrame) -> Decimal:
        signals: list[Decimal] = []
        if frame.funding_rate is not None:
            signals.append(min(Decimal("1"), abs(frame.funding_rate) / Decimal("0.001")))
        if frame.basis_bps is not None:
            signals.append(min(Decimal("1"), abs(frame.basis_bps) / Decimal("50")))
        if frame.oi_change is not None:
            signals.append(min(Decimal("1"), abs(frame.oi_change) / Decimal("0.05")))
        for ratio in (
            frame.global_long_short_ratio,
            frame.top_trader_long_short_ratio,
        ):
            if ratio is not None:
                signals.append(min(Decimal("1"), abs(ratio - Decimal("1"))))
        return min(Decimal("1"), sum(signals) / Decimal(len(signals))) if signals else Decimal("0")

    @staticmethod
    def _liquidity_score(frame: MarketFrame) -> Decimal:
        if frame.spread_bps is None or frame.depth_25bps is None:
            return Decimal("0")
        score = Decimal("1")
        if frame.spread_bps is not None:
            score -= min(Decimal("0.6"), max(Decimal("0"), frame.spread_bps) / Decimal("100"))
        if frame.depth_25bps is not None and frame.depth_25bps <= 0:
            return Decimal("0")
        return max(Decimal("0"), score)

    def signal(self, frame: MarketFrame) -> Signal | None:
        closes = tuple(Decimal(value) for value in frame.closes)
        if len(closes) < self.config.slow_window:
            return None
        if any(value <= 0 for value in closes):
            return None
        fast = Decimal(str(fmean(closes[-self.config.fast_window :])))
        slow = Decimal(str(fmean(closes[-self.config.slow_window :])))
        price = closes[-1]
        spread = abs(fast - slow) / slow if slow else Decimal("0")
        confidence = min(Decimal("1"), Decimal("0.5") + spread * Decimal("10"))
        if confidence < self.config.minimum_confidence:
            return None
        if fast > slow and price >= fast:
            return Signal(
                symbol=frame.symbol.upper(), side="BUY", confidence=confidence,
                reason="fast SMA above slow SMA with price confirmation", price=price,
                captured_at=_aware(frame.captured_at),
            )
        if fast < slow and price <= fast:
            return Signal(
                symbol=frame.symbol.upper(), side="SELL", confidence=confidence,
                reason="fast SMA below slow SMA with price confirmation", price=price,
                captured_at=_aware(frame.captured_at),
            )
        return None
