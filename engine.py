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
    "CONFLICTED", "UNKNOWN",
]
Direction = Literal["LONG", "SHORT", "FLAT"]
Action = Literal["OPEN", "REDUCE", "CLOSE"]
MemeRiskTier = Literal["TRADEABLE", "REDUCED", "OBSERVE", "BLOCK"]
MarketRegime = Literal[
    "RISK_ON", "RISK_OFF", "TRENDING_UP", "TRENDING_DOWN",
    "HIGH_VOL", "LOW_VOL", "NEUTRAL",
]


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _map_position_action(
    current: Direction,
    desired: Direction,
    state: str,
) -> tuple[Direction, Action] | None:
    if current == "FLAT":
        if state == "LONG_BUILDING" and desired == "LONG":
            return "LONG", "OPEN"
        if state == "SHORT_BUILDING" and desired == "SHORT":
            return "SHORT", "OPEN"
        return None
    if current == "LONG":
        if state == "LONG_BUILDING" and desired == "LONG":
            return None
        if state == "EXHAUSTION_LONG":
            return "LONG", "REDUCE"
        if state in {
            "LONG_UNWIND", "SHORT_COVERING", "FORCED_DELEVERAGING",
            "SHORT_BUILDING",
        }:
            return "LONG", "CLOSE"
        return None
    if current == "SHORT":
        if state == "SHORT_BUILDING" and desired == "SHORT":
            return None
        if state == "EXHAUSTION_SHORT":
            return "SHORT", "REDUCE"
        if state in {
            "SHORT_COVERING", "LONG_UNWIND", "FORCED_DELEVERAGING",
            "LONG_BUILDING",
        }:
            return "SHORT", "CLOSE"
        return None
    return None


CORE_FUTURES_SOURCES = frozenset(
    {
        "futures_open_interest",
        "futures_funding",
        "futures_trade_flow",
        "futures_taker_ratio",
        "futures_mark_price",
        "futures_index_price",
        "futures_liquidation",
        "futures_orderbook",
        "futures_book_ticker",
        "futures_force_order",
    }
)
AUXILIARY_SPOT_SOURCES = frozenset(
    {
        "spot_trade",
        "spot_book_ticker",
        "spot_orderbook",
        "spot",
        "binance_spot_klines",
    }
)


def _is_auxiliary_spot_source(source: str) -> bool:
    name = source.lower()
    return name in AUXILIARY_SPOT_SOURCES or name.startswith("spot")


def _is_core_futures_source(source: str) -> bool:
    name = source.lower()
    if _is_auxiliary_spot_source(name):
        return False
    return name in CORE_FUTURES_SOURCES or name.startswith("futures")


def _sign(value: Decimal | None, threshold: Decimal = Decimal("0")) -> int | None:
    if value is None:
        return None
    return 1 if value > threshold else -1 if value < -threshold else 0


def _positive(value: int | None) -> bool:
    return value is not None and value > 0


def _negative(value: int | None) -> bool:
    return value is not None and value < 0


def _timestamps_consistent(
    source_timestamps: Mapping[str, Mapping[str, Any]], now: datetime
) -> tuple[bool, bool]:
    """Return (core_ok, aux_ok) timestamp/latency provenance."""
    if not source_timestamps:
        return False, False
    core_ok = True
    aux_ok = True
    saw_core = False
    for source_name, timestamps in source_timestamps.items():
        try:
            source_ts = _aware(datetime.fromisoformat(str(timestamps["source_timestamp"])))
            received = _aware(datetime.fromisoformat(str(timestamps["received_timestamp"])))
            declared_latency = int(timestamps["latency_ms"])
        except (KeyError, TypeError, ValueError):
            if _is_core_futures_source(str(source_name)):
                core_ok = False
            else:
                aux_ok = False
            continue
        actual_latency = int((received - source_ts).total_seconds() * 1000)
        consistent = not (
            actual_latency < 0
            or declared_latency != actual_latency
            or source_ts > _aware(now)
            or received > _aware(now)
        )
        if _is_core_futures_source(str(source_name)):
            saw_core = True
            core_ok = core_ok and consistent
        elif not consistent:
            aux_ok = False
    return (core_ok and saw_core), aux_ok


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
    price: int | None = None
    futures_trade_flow: int | None = None
    cvd: int | None = None
    oi: int | None = None
    funding: int | None = None
    taker: int | None = None
    orderbook: int | None = None
    liquidation: int | None = None
    basis: int | None = None
    relative_strength: int | None = None
    market_regime: int | None = None
    quality: Decimal = Decimal("0")
    freshness: Decimal = Decimal("0")


@dataclass(frozen=True)
class PositioningWeights:
    price: Decimal = Decimal("1")
    futures_trade_flow: Decimal = Decimal("2")
    cvd: Decimal = Decimal("2")
    oi: Decimal = Decimal("1")
    taker: Decimal = Decimal("2")
    orderbook: Decimal = Decimal("1")
    liquidation: Decimal = Decimal("1")
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
    directional_strength: Decimal
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
            "directional_strength": str(self.directional_strength),
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
class CurrentPosition:
    direction: Direction = "FLAT"
    quantity: Decimal = Decimal("0")
    entry_price: Decimal | None = None
    leverage: Decimal = Decimal("1")
    meme_risk_tier: MemeRiskTier = "TRADEABLE"


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
    futures_buy_volume: Decimal | None = None
    futures_sell_volume: Decimal | None = None
    futures_trade_flow: Decimal | None = None
    futures_buy_notional: Decimal | None = None
    futures_sell_notional: Decimal | None = None
    futures_delta_notional: Decimal | None = None
    notional_cvd: Decimal | None = None
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
    funding_timestamp: datetime | None = None
    funding_settlement_timestamp: datetime | None = None
    funding_change: Decimal | None = None
    funding_percentile: Decimal | None = None
    funding_zscore: Decimal | None = None
    basis_bps: Decimal | None = None
    global_long_short_ratio: Decimal | None = None
    top_trader_long_short_ratio: Decimal | None = None
    taker_ratio: Decimal | None = None
    observed_short_liquidation_notional: Decimal | None = None
    observed_long_liquidation_notional: Decimal | None = None
    observed_liquidation_notional: Decimal | None = None
    liquidation_acceleration: Decimal | None = None
    liquidation_observed: bool | None = None
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
    # Universe qualification is required before a frame can create an intent.
    meme_risk_tier: MemeRiskTier = "OBSERVE"
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
            "spot_buy_volume", "spot_sell_volume", "net_spot_flow",
            "futures_buy_volume", "futures_sell_volume", "futures_trade_flow", "cvd",
            "futures_buy_notional", "futures_sell_notional", "futures_delta_notional",
            "notional_cvd",
            "cvd_change", "cvd_1m", "cvd_3m", "cvd_5m", "cvd_15m",
            "cvd_1h", "cvd_30m", "cvd_acceleration", "volume_ratio_1m",
            "volume_ratio_5m", "volume_ratio_15m", "volume_zscore",
            "taker_buy_volume", "taker_sell_volume",
            "taker_buy_volume_30m", "taker_sell_volume_30m",
            "oi", "oi_change", "last_price", "mark_price", "index_price",
            "oi_change_1m", "oi_change_3m", "oi_change_5m",
            "oi_change_15m", "oi_change_1h", "oi_change_30m",
            "funding_rate", "funding_timestamp", "funding_settlement_timestamp",
            "funding_change", "funding_percentile", "funding_zscore",
            "taker_ratio",
            "basis_bps", "global_long_short_ratio", "top_trader_long_short_ratio",
            "observed_short_liquidation_notional",
            "observed_long_liquidation_notional",
            "observed_liquidation_notional",
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
        for name in ("funding_timestamp", "funding_settlement_timestamp"):
            if inputs.get(name) is not None:
                values[name] = _aware(datetime.fromisoformat(str(inputs[name])))
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
            meme_risk_tier=str(inputs.get("meme_risk_tier", "OBSERVE")),  # type: ignore[arg-type]
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
    default_leverage: Decimal = Decimal("1")
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
            positioning_strategy_version=os.environ.get("POSITIONING_STRATEGY_VERSION", "positioning-v1"),
            default_leverage=decimal("DEFAULT_LEVERAGE", Decimal("1")),
            positioning_decision_enabled=os.environ.get(
                "POSITIONING_DECISION_ENABLED", "false"
            ).strip().lower() in {"1", "true", "yes", "on"},
            minimum_positioning_confidence=decimal(
                "MIN_POSITIONING_CONFIDENCE", Decimal("0.6")
            ),
            minimum_positioning_edge=Decimal("0.15"),
            minimum_transition_strength=Decimal("0.15"),
            minimum_data_quality=decimal(
                "MIN_DATA_QUALITY", Decimal("0.8")
            ),
            maximum_crowding=decimal(
                "MAX_CROWDING", Decimal("0.9")
            ),
            minimum_liquidity_score=decimal(
                "MIN_LIQUIDITY_SCORE", Decimal("0.4")
            ),
        )

    def __post_init__(self) -> None:
        if self.fast_window < 2 or self.slow_window <= self.fast_window:
            raise ValueError("slow_window must be greater than fast_window >= 2")
        if not Decimal("0") <= self.minimum_confidence <= Decimal("1"):
            raise ValueError("minimum_confidence must be between 0 and 1")
        if self.order_quote_usdt <= 0:
            raise ValueError("order_quote_usdt must be positive")
        if self.default_leverage <= 0:
            raise ValueError("default_leverage must be positive")
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

    def evaluate(
        self,
        frame: MarketFrame,
        *,
        current_position: CurrentPosition | None = None,
    ) -> TradeIntent | None:
        current = current_position or CurrentPosition()
        if (
            current.meme_risk_tier in {"BLOCK", "OBSERVE"}
            or frame.meme_risk_tier in {"BLOCK", "OBSERVE"}
        ):
            return None
        if not self.config.positioning_decision_enabled:
            return self._legacy_intent(frame, current_position=current)
        return self._intent_from_positioning(
            self.positioning_decision(frame),
            frame,
            current_position=current,
        )

    def _legacy_intent(
        self,
        frame: MarketFrame,
        current_position: CurrentPosition | None = None,
    ) -> TradeIntent | None:
        signal = self.signal(frame)
        if signal is None:
            return None
        desired: Direction = "LONG" if signal.side == "BUY" else "SHORT"
        return self._intent_for_action(
            symbol=signal.symbol,
            desired=desired,
            state="LONG_BUILDING" if desired == "LONG" else "SHORT_BUILDING",
            frame=frame,
            current=current_position or CurrentPosition(),
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
        directional_strength = min(Decimal("1"), abs(edge) * quality)
        transition_strength = self._transition_strength(
            prior,
            state,
            long_score=long_score,
            short_score=short_score,
        )
        confidence = min(
            Decimal("1"),
            abs(edge) * Decimal("1.5") * quality * (
                Decimal("1") - crowding * Decimal("0.35")
            ),
        )
        direction: Direction = "FLAT"
        passes = (
            confidence >= self.config.minimum_positioning_confidence
            and directional_strength >= self.config.minimum_transition_strength
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
        if (
            prior is not None
            and prior != state
            and transition_strength < self.config.minimum_transition_strength
        ):
            reasons.append("STATE_CHANGE_STRENGTH_LOW")
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
            directional_strength=directional_strength,
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
            transition_strength=Decimal("0"), directional_strength=Decimal("0"),
            confidence=Decimal("0"),
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
            "spot_buy_volume", "spot_sell_volume", "net_spot_flow",
            "futures_buy_volume", "futures_sell_volume", "futures_trade_flow", "cvd",
            "futures_buy_notional", "futures_sell_notional", "futures_delta_notional",
            "notional_cvd",
            "cvd_change", "cvd_1m", "cvd_3m", "cvd_5m", "cvd_15m",
            "cvd_1h", "cvd_30m", "cvd_acceleration", "price_cvd_divergence",
            "volume_ratio_1m", "volume_ratio_5m", "volume_ratio_15m",
            "volume_zscore", "taker_buy_volume", "taker_sell_volume",
            "taker_buy_volume_30m", "taker_sell_volume_30m",
            "oi", "oi_change", "last_price", "mark_price", "index_price",
            "oi_change_1m", "oi_change_3m", "oi_change_5m",
            "oi_change_15m", "oi_change_1h", "oi_change_30m",
            "funding_rate", "funding_timestamp", "funding_settlement_timestamp",
            "funding_change", "funding_percentile", "funding_zscore",
            "taker_ratio",
            "basis_bps", "global_long_short_ratio", "top_trader_long_short_ratio",
            "observed_short_liquidation_notional",
            "observed_long_liquidation_notional",
            "observed_liquidation_notional",
            "liquidation_acceleration", "bid_depth_5", "ask_depth_5",
            "liquidation_observed",
            "bid_depth_10", "ask_depth_10", "bid_depth_20", "ask_depth_20",
            "spread_bps", "depth_10bps", "depth_25bps", "depth_50bps",
            "price_impact_buy", "price_impact_sell", "liquidity_added",
            "liquidity_removed", "relative_strength", "breadth_score",
            "relative_strength_1m", "relative_strength_5m",
            "relative_strength_15m", "relative_strength_1h",
            "advance_decline_ratio",
            "market_regime", "meme_risk_tier", "data_quality_score",
        )
        return {name: getattr(frame, name) for name in names}

    def _intent_from_positioning(
        self,
        decision: PositioningDecision,
        frame: MarketFrame,
        current_position: CurrentPosition | None = None,
    ) -> TradeIntent | None:
        current = current_position or CurrentPosition()
        if (
            current.meme_risk_tier in {"BLOCK", "OBSERVE"}
            or frame.meme_risk_tier in {"BLOCK", "OBSERVE"}
        ):
            return None
        return self._intent_for_action(
            symbol=decision.symbol,
            desired=decision.direction,
            state=decision.state,
            frame=frame,
            current=current,
            confidence=decision.confidence,
            reason="; ".join(decision.reason_codes) or decision.state,
            strategy_version=self.config.positioning_strategy_version,
            created_at=decision.timestamp,
            positioning=decision,
        )

    def _intent_for_action(
        self,
        *,
        symbol: str,
        desired: Direction,
        state: str,
        frame: MarketFrame,
        current: CurrentPosition,
        confidence: Decimal,
        reason: str,
        strategy_version: str,
        created_at,
        positioning: PositioningDecision | None = None,
    ) -> TradeIntent | None:
        mapped = _map_position_action(current.direction, desired, state)
        if mapped is None:
            return None
        direction, action = mapped
        price = frame.mark_price or frame.last_price or frame.closes[-1]
        if price <= 0:
            return None
        if action == "OPEN":
            quantity = self.config.order_quote_usdt / price
            reduce_only = False
        elif action == "REDUCE":
            quantity = current.quantity / Decimal("2")
            reduce_only = True
        else:
            quantity = current.quantity
            reduce_only = True
        if quantity <= 0:
            return None
        values = {
            "symbol": symbol,
            "direction": direction,
            "action": action,
            "reduce_only": reduce_only,
            "leverage": self.config.default_leverage,
            "order_type": "MARKET",
            "quantity": quantity,
            "confidence": confidence,
            "reason": reason,
            "strategy_version": strategy_version,
            "created_at": created_at,
            "meme_risk_tier": frame.meme_risk_tier,
        }
        if positioning is not None:
            values.update(
                positioning_state=positioning.state,
                previous_state=positioning.previous_state,
                transition=positioning.transition,
                long_score=positioning.long_score,
                short_score=positioning.short_score,
                crowding_score=positioning.crowding_score,
                liquidity_score=positioning.liquidity_score,
                data_quality_score=positioning.data_quality_score,
                directional_strength=positioning.directional_strength,
                transition_strength=positioning.transition_strength,
                reason_codes=positioning.reason_codes,
                evidence_snapshot_id=positioning.evidence_snapshot_id,
                market_regime=positioning.market_regime,
            )
        return TradeIntent(**values)


    def _evidence(
        self, frame: MarketFrame, now: datetime
    ) -> tuple[EvidenceVector, Decimal, list[str]]:
        reasons: list[str] = []
        if not frame.freshness:
            return EvidenceVector(), Decimal("0"), ["MISSING_FRESHNESS"]

        def _item_fresh(item: SourceFreshness) -> bool:
            return (
                _aware(item.received_timestamp) <= now
                and _aware(item.source_timestamp) <= now
                and (now - _aware(item.source_timestamp)).total_seconds() <= max(1, item.max_age_sec)
            )

        core_freshness_items = [
            item for item in frame.freshness if _is_core_futures_source(item.source)
        ]
        aux_freshness_items = [
            item for item in frame.freshness if _is_auxiliary_spot_source(item.source)
        ]
        core_fresh_flags = [_item_fresh(item) for item in core_freshness_items]
        aux_fresh_flags = [_item_fresh(item) for item in aux_freshness_items]
        if core_fresh_flags:
            freshness = Decimal(sum(core_fresh_flags)) / Decimal(len(core_fresh_flags))
            if not all(core_fresh_flags):
                reasons.append("STALE_DATA")
        else:
            freshness = Decimal("1")
        if aux_fresh_flags and not all(aux_fresh_flags):
            reasons.append("AUXILIARY_SPOT_STALE")
        core_ok, aux_ok = _timestamps_consistent(frame.source_timestamps, now)
        if not core_ok:
            reasons.append("TIMESTAMP_INCONSISTENT")
        elif not aux_ok:
            reasons.append("AUXILIARY_SPOT_TIMESTAMP_INCONSISTENT")
        core_features = (
            frame.futures_trade_flow,
            frame.oi_change, frame.funding_rate,
            frame.taker_buy_volume, frame.taker_sell_volume,
            frame.spread_bps, frame.depth_25bps,
        )
        if any(value is None for value in core_features):
            reasons.append("MISSING_CORE_FUTURES_EVIDENCE")
        auxiliary_features = (
            frame.net_spot_flow, frame.cvd_change,
            frame.spot_buy_volume, frame.spot_sell_volume,
        )
        if any(value is None for value in auxiliary_features):
            reasons.append("AUXILIARY_SPOT_MISSING")
        timestamp_provenance_complete = all(
            source in frame.source_timestamps
            for source in (
                "futures_open_interest",
                "futures_funding",
                "futures_taker_ratio",
            )
        ) and bool(
            {"futures_trade", "futures_trade_flow"}.intersection(
                frame.source_timestamps
            )
        )
        if not timestamp_provenance_complete:
            reasons.append("MISSING_TIMESTAMP_PROVENANCE")
        supplied = sum(value is not None for value in core_features)
        completeness = Decimal(supplied) / Decimal(len(core_features))
        quality = min(
            Decimal("1"), freshness * (Decimal("0.5") + completeness / Decimal("2"))
        )
        if frame.data_quality_score is not None:
            quality = min(quality, max(Decimal("0"), frame.data_quality_score))
        if frame.price_cvd_divergence and frame.cvd_change is not None:
            # Price and aggressive-flow direction disagree. Keep the raw
            # evidence for audit, but do not let a partially aligned subset
            # manufacture a directional trade.
            quality *= Decimal("0.85")
            reasons.append("PRICE_CVD_DIVERGENCE")
        if core_fresh_flags and not all(core_fresh_flags):
            quality = min(quality, self.config.minimum_data_quality - Decimal("0.01"))
        if not core_ok:
            quality = min(quality, self.config.minimum_data_quality - Decimal("0.01"))
        if any(value is None for value in core_features):
            quality = min(quality, self.config.minimum_data_quality - Decimal("0.01"))
        if not timestamp_provenance_complete:
            quality = min(quality, self.config.minimum_data_quality - Decimal("0.01"))
        price_signal = _sign(self._price_change(frame))
        flow_source = "FUTURES"
        flow_signal = _sign(frame.futures_trade_flow)
        cvd_signal = _sign(frame.cvd_change)
        oi_signal = _sign(frame.oi_change)
        taker_signal = _sign(
            frame.taker_ratio - Decimal("1")
            if frame.taker_ratio is not None
            else self._taker_flow(frame)
        )
        funding_signal = _sign(frame.funding_rate)
        basis_signal = _sign(frame.basis_bps)
        orderbook_signal = _sign(self._orderbook_imbalance(frame))
        liquidation_signal = _sign(
            (frame.observed_short_liquidation_notional or Decimal("0"))
            - (frame.observed_long_liquidation_notional or Decimal("0"))
        )
        relative_signal = _sign(frame.relative_strength)
        regime_signal = (
            1 if frame.market_regime in {"RISK_ON", "TRENDING_UP"}
            else -1 if frame.market_regime in {"RISK_OFF", "TRENDING_DOWN"}
            else 0
        )
        if _positive(flow_signal):
            reasons.append(f"{flow_source}_BUYING")
        elif _negative(flow_signal):
            reasons.append(f"{flow_source}_SELLING")
        if _positive(cvd_signal):
            reasons.append("POSITIVE_CVD")
        elif _negative(cvd_signal):
            reasons.append("NEGATIVE_CVD")
        if frame.volume_ratio_5m is not None and frame.volume_ratio_5m >= Decimal("2"):
            reasons.append("VOLUME_EXCEPTIONAL")
        if _positive(oi_signal):
            reasons.append("OI_EXPANSION")
        elif _negative(oi_signal):
            reasons.append("OI_CONTRACTION")
        if _positive(taker_signal):
            reasons.append("TAKER_BUY")
        elif _negative(taker_signal):
            reasons.append("TAKER_SELL")
        return EvidenceVector(
            price=price_signal, futures_trade_flow=flow_signal, cvd=cvd_signal, oi=oi_signal,
            funding=funding_signal, taker=taker_signal, orderbook=orderbook_signal,
            liquidation=liquidation_signal, relative_strength=relative_signal,
            basis=basis_signal, market_regime=regime_signal,
            quality=quality, freshness=freshness,
        ), quality, reasons

    def _scores(self, evidence: EvidenceVector) -> tuple[Decimal, Decimal]:
        weights = self.config.positioning_weights
        values = (
            (evidence.price, weights.price),
            (evidence.futures_trade_flow, weights.futures_trade_flow),
            (evidence.cvd, weights.cvd), (evidence.oi, weights.oi),
            (evidence.taker, weights.taker), (evidence.orderbook, weights.orderbook),
            (evidence.liquidation, weights.liquidation),
            (evidence.relative_strength, weights.relative_strength),
            (evidence.market_regime, weights.market_regime),
        )
        present = [(value, weight) for value, weight in values if value is not None]
        total = sum(weight for _, weight in present) or Decimal("1")
        return (
            sum(weight for value, weight in present if value > 0) / total,
            sum(weight for value, weight in present if value < 0) / total,
        )

    @staticmethod
    def _transition_strength(
        previous: PositioningState | None,
        current: PositioningState,
        *,
        long_score: Decimal,
        short_score: Decimal,
    ) -> Decimal:
        """Measure state-change magnitude continuously in the [0, 1] range."""
        if previous is None or previous == current:
            return Decimal("0")
        edge = abs(long_score - short_score)
        long_states = {
            "LONG_BUILDING", "SHORT_COVERING", "ABSORPTION_LONG", "EXHAUSTION_LONG",
        }
        short_states = {
            "SHORT_BUILDING", "LONG_UNWIND", "ABSORPTION_SHORT", "EXHAUSTION_SHORT",
        }
        previous_bias = 1 if previous in long_states else -1 if previous in short_states else 0
        current_bias = 1 if current in long_states else -1 if current in short_states else 0
        if previous_bias and current_bias and previous_bias != current_bias:
            state_delta = Decimal("0.50")
        elif previous_bias != current_bias:
            state_delta = Decimal("0.15")
        elif previous in {"LONG_BUILDING", "SHORT_BUILDING"} and current in {
            "LONG_UNWIND", "SHORT_COVERING", "EXHAUSTION_LONG", "EXHAUSTION_SHORT",
        }:
            state_delta = Decimal("0.35")
        elif current in {"LONG_UNWIND", "SHORT_COVERING", "EXHAUSTION_LONG", "EXHAUSTION_SHORT"}:
            state_delta = Decimal("0.25")
        else:
            state_delta = Decimal("0.15")
        return min(Decimal("1"), edge * Decimal("0.50") + state_delta)

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
        flow, taker, cvd = evidence.futures_trade_flow, evidence.taker, evidence.cvd
        if frame.price_cvd_divergence and price is not None and cvd is not None and price != cvd:
            return "CONFLICTED"
        positive = sum(_positive(value) for value in (price, flow, cvd, taker))
        negative = sum(_negative(value) for value in (price, flow, cvd, taker))
        if positive >= 2 and negative >= 2:
            return "CONFLICTED"
        if _positive(price) and _negative(oi):
            return "SHORT_COVERING"
        if _negative(price) and _negative(oi):
            return "LONG_UNWIND"
        if _positive(price) and _positive(oi) and _positive(taker):
            if crowding > self.config.maximum_crowding and (
                cvd is not None and cvd <= 0
                or (frame.cvd_acceleration is not None and frame.cvd_acceleration < 0)
            ):
                return "EXHAUSTION_LONG"
            return "LONG_BUILDING"
        if _negative(price) and _positive(oi) and _negative(taker):
            if crowding > self.config.maximum_crowding and (
                cvd is not None and cvd >= 0
                or (frame.cvd_acceleration is not None and frame.cvd_acceleration > 0)
            ):
                return "EXHAUSTION_SHORT"
            return "SHORT_BUILDING"
        if (
            frame.price_impact_sell is not None
            and frame.price_impact_sell <= Decimal("0.001")
            and (frame.volume_ratio_5m or Decimal("0")) >= Decimal("2")
            and _negative(flow) and _negative(taker)
        ):
            return "ABSORPTION_LONG"
        if (
            frame.price_impact_buy is not None
            and frame.price_impact_buy <= Decimal("0.001")
            and (frame.volume_ratio_5m or Decimal("0")) >= Decimal("2")
            and _positive(flow) and _positive(taker)
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
