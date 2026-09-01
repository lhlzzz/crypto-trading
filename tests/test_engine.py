from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from engine import (
    CurrentPosition,
    EvidenceSufficiency,
    MarketDataEnvelope,
    MarketFrame,
    SourceFreshness,
    StrategyConfig,
    StrategyEngine,
    _map_position_action,
)


def _frame(closes: list[str]) -> MarketFrame:
    return MarketFrame(
        symbol="btcusdt",
        closes=tuple(Decimal(value) for value in closes),
        captured_at=datetime.now(timezone.utc),
        meme_risk_tier="TRADEABLE",
    )


def test_engine_returns_no_intent_until_slow_window_is_ready() -> None:
    engine = StrategyEngine(StrategyConfig(fast_window=3, slow_window=5))

    assert engine.evaluate(_frame(["1", "2", "3", "4"])) is None


def test_research_sma_signal_does_not_create_a_production_intent() -> None:
    engine = StrategyEngine(
        StrategyConfig(
            fast_window=3,
            slow_window=5,
            minimum_confidence=Decimal("0.5"),
            order_quote_usdt=Decimal("25"),
            positioning_decision_enabled=False,
            legacy_execution_enabled=True,
        )
    )
    frame = _frame(["1", "1", "1", "1", "1", "2", "3"])
    signal = engine.signal(frame)

    assert signal is not None
    assert signal.side == "BUY"
    assert "SMA" in signal.reason
    assert engine.evaluate(frame) is None
    assert not hasattr(engine, "_legacy_intent")


def test_engine_does_not_create_intent_without_tradeable_meme_evidence() -> None:
    frame = MarketFrame(
        symbol="BTCUSDT",
        closes=(Decimal("1"), Decimal("1"), Decimal("1"), Decimal("2"), Decimal("3")),
        captured_at=datetime.now(timezone.utc),
    )

    assert StrategyEngine(
        StrategyConfig(fast_window=2, slow_window=4, minimum_confidence=Decimal("0.5"))
    ).evaluate(frame) is None


def test_positioning_open_requires_positive_meme_membership() -> None:
    frame = _positioning_frame()
    engine = StrategyEngine(
        StrategyConfig(positioning_decision_enabled=True)
    )

    assert engine.evaluate(frame) is None
    assert engine.evaluate(
        MarketFrame(**{**frame.__dict__, "is_meme": True})
    ) is not None


def test_engine_does_not_call_broker_or_risk() -> None:
    source = open("engine.py", encoding="utf-8").read()

    assert "PrivateClient" not in source
    assert "create_order" not in source
    assert "RiskGate" not in source


def _positioning_frame(**updates: object) -> MarketFrame:
    captured = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
    source_timestamps = {
        source: {
            "source_timestamp": captured.isoformat(),
            "received_timestamp": captured.isoformat(),
            "latency_ms": 0,
        }
        for source in (
            "spot_trade",
            "futures_open_interest",
            "futures_funding",
            "futures_trade_flow",
            "futures_taker_ratio",
            "spot_book_ticker",
            "spot_orderbook",
        )
    }
    values: dict[str, object] = {
        "symbol": "BTCUSDT",
        "closes": (Decimal("100"), Decimal("101")),
        "captured_at": captured,
        "spot_buy_volume": Decimal("12"),
        "spot_sell_volume": Decimal("4"),
        "net_spot_flow": Decimal("8"),
        "futures_trade_flow": Decimal("8"),
        "cvd_change": Decimal("8"),
        "taker_buy_volume": Decimal("10"),
        "taker_sell_volume": Decimal("3"),
        "oi_change": Decimal("0.03"),
        "funding_rate": Decimal("0.0001"),
        "spread_bps": Decimal("2"),
        "depth_25bps": Decimal("100"),
        "market_regime": "RISK_ON",
        "meme_risk_tier": "TRADEABLE",
        "freshness": (SourceFreshness("spot", captured, captured, 900, captured),),
        "source_timestamps": source_timestamps,
    }
    values.update(updates)
    return MarketFrame(**values)  # type: ignore[arg-type]


def test_market_data_envelope_calculates_latency() -> None:
    source = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
    received = source.replace(second=1)
    envelope = MarketDataEnvelope.create(
        source="binance", market="SPOT", symbol="btcusdt", event_type="TRADE",
        source_timestamp=source, received_timestamp=received, payload={"p": "1"},
    )
    assert envelope.symbol == "BTCUSDT"
    assert envelope.latency_ms == 1000


def test_evidence_sufficiency_derives_missing_required_inputs() -> None:
    sufficiency = EvidenceSufficiency(
        required=("A", "B", "C"),
        available=("A", "B"),
        missing=(),
    )

    assert sufficiency.sufficient is False
    assert sufficiency.missing == ("C",)


def test_source_freshness_rejects_mismatched_latency() -> None:
    captured = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
    freshness = SourceFreshness(
        "futures_trade_flow",
        captured,
        captured.replace(second=1),
        60,
        captured.replace(second=1),
        latency_ms=0,
    )

    assert freshness.fresh is False


def test_positioning_building_is_explainable_but_shadow_only_by_default() -> None:
    frame = _positioning_frame()
    decision = StrategyEngine().positioning_decision(frame)
    assert decision.state == "LONG_BUILDING"
    assert decision.direction == "LONG"
    assert "FUTURES_BUYING" in decision.reason_codes
    assert StrategyEngine().evaluate(frame) is None


def test_stale_positioning_data_fails_closed() -> None:
    captured = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
    stale = SourceFreshness(
        "futures_open_interest", captured, captured, 1, captured.replace(hour=13)
    )
    decision = StrategyEngine().positioning_decision(
        _positioning_frame(freshness=(stale,)), now=captured.replace(hour=13)
    )
    assert decision.state == "UNKNOWN"
    assert decision.direction == "FLAT"


def test_stale_spot_confirmation_does_not_fail_futures() -> None:
    captured = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
    stale_spot = SourceFreshness("spot_trade", captured, captured, 1, captured.replace(hour=13))
    decision = StrategyEngine().positioning_decision(
        _positioning_frame(freshness=(stale_spot,)), now=captured.replace(hour=13)
    )
    assert decision.state == "LONG_BUILDING"
    assert "AUXILIARY_SPOT_STALE" in decision.reason_codes


def test_one_stale_required_source_fails_closed_even_when_others_are_fresh() -> None:
    captured = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
    fresh = SourceFreshness("spot_trade", captured, captured, 900, captured)
    stale = SourceFreshness(
        "futures_taker", captured, captured, 1, captured.replace(hour=13)
    )

    decision = StrategyEngine().positioning_decision(
        _positioning_frame(freshness=(fresh, stale)),
        now=captured.replace(hour=13),
    )

    assert decision.state == "UNKNOWN"
    assert decision.direction == "FLAT"


def test_evidence_sufficiency_reports_stale_and_unsafe_critical_inputs() -> None:
    captured = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
    frame = _positioning_frame(
        oi_change=Decimal("0.03"),
        evidence_status={"orderbook": "UNSAFE"},
        freshness=(
            SourceFreshness(
                "futures_open_interest",
                captured.replace(hour=11),
                captured.replace(hour=11),
                1,
                captured.replace(hour=13),
            ),
        ),
    )

    decision = StrategyEngine().positioning_decision(
        frame, now=captured.replace(hour=13)
    )

    assert decision.evidence_sufficiency.sufficient is False
    assert "oi" in decision.evidence_sufficiency.stale
    assert "orderbook" in decision.evidence_sufficiency.unsafe
    assert decision.direction == "FLAT"
    assert "POSITIONING_EVIDENCE_INSUFFICIENT" in decision.reason_codes


def test_unsafe_orderbook_cannot_be_entry_evidence_when_other_inputs_are_fresh() -> None:
    decision = StrategyEngine().positioning_decision(
        _positioning_frame(evidence_status={"orderbook": "UNSAFE"})
    )

    assert decision.state == "UNKNOWN"
    assert decision.direction == "FLAT"
    assert decision.evidence_sufficiency.sufficient is False


def test_missing_timestamp_provenance_fails_closed() -> None:
    decision = StrategyEngine().positioning_decision(
        _positioning_frame(source_timestamps={})
    )

    assert decision.state == "UNKNOWN"
    assert decision.direction == "FLAT"
    assert "MISSING_TIMESTAMP_PROVENANCE" in decision.reason_codes


def test_conflicting_flow_fails_closed() -> None:
    decision = StrategyEngine().positioning_decision(
        _positioning_frame(
            net_spot_flow=Decimal("-8"),
            futures_trade_flow=Decimal("-8"),
            cvd_change=Decimal("-8"),
        )
    )
    assert decision.state == "CONFLICTED"
    assert decision.direction == "FLAT"


def test_price_cvd_divergence_is_a_conflicted_flat_decision() -> None:
    decision = StrategyEngine().positioning_decision(
        _positioning_frame(
            cvd_change=Decimal("-8"),
            price_cvd_divergence=True,
        )
    )

    assert decision.state == "CONFLICTED"
    assert decision.direction == "FLAT"
    assert "PRICE_CVD_DIVERGENCE" in decision.reason_codes


def test_absorption_requires_exceptional_flow_and_low_price_impact() -> None:
    decision = StrategyEngine().positioning_decision(
        _positioning_frame(
            closes=(Decimal("100"), Decimal("100")),
            net_spot_flow=Decimal("-8"),
            futures_trade_flow=Decimal("-8"),
            cvd_change=Decimal("-8"),
            taker_buy_volume=Decimal("3"),
            taker_sell_volume=Decimal("10"),
            volume_ratio_5m=Decimal("2.1"),
            price_impact_sell=Decimal("0.0005"),
        )
    )

    assert decision.state == "ABSORPTION_LONG"
    assert decision.direction == "FLAT"


def test_long_unwind_cannot_open_a_new_long_intent() -> None:
    engine = StrategyEngine(StrategyConfig(positioning_decision_enabled=True))
    frame = _positioning_frame(
        closes=(Decimal("101"), Decimal("100")),
        oi_change=Decimal("-0.03"),
    )

    decision = engine.positioning_decision(frame)

    assert decision.state == "LONG_UNWIND"
    assert decision.direction == "FLAT"
    assert "ENTRY_STATE_NOT_CONFIRMED" in decision.reason_codes
    assert engine.evaluate(frame) is None


def test_short_covering_cannot_open_a_new_long_intent() -> None:
    decision = StrategyEngine().positioning_decision(
        _positioning_frame(oi_change=Decimal("-0.03"))
    )

    assert decision.state == "SHORT_COVERING"
    assert decision.direction == "FLAT"
    assert "ENTRY_STATE_NOT_CONFIRMED" in decision.reason_codes


def test_short_building_is_directionally_symmetric_with_long_building() -> None:
    decision = StrategyEngine().positioning_decision(
        _positioning_frame(
            closes=(Decimal("101"), Decimal("100")),
            net_spot_flow=Decimal("-8"),
            futures_trade_flow=Decimal("-8"),
            cvd_change=Decimal("-8"),
            taker_buy_volume=Decimal("3"),
            taker_sell_volume=Decimal("10"),
            oi_change=Decimal("0.03"),
            market_regime="RISK_OFF",
        )
    )

    assert decision.state == "SHORT_BUILDING"
    assert decision.direction == "SHORT"


def test_directional_and_transition_strength_are_split() -> None:
    engine = StrategyEngine(StrategyConfig(positioning_decision_enabled=True))
    frame = _positioning_frame()
    decision = engine.positioning_decision(frame)
    assert decision.state == "LONG_BUILDING"
    assert decision.direction == "LONG"
    assert decision.transition_strength == Decimal("0")
    assert decision.directional_strength > Decimal("0")
    assert decision.directional_strength != decision.transition_strength


def test_positioning_decision_has_no_process_local_episode_state() -> None:
    from dataclasses import replace

    engine = StrategyEngine()
    first_frame = _positioning_frame()
    first = engine.positioning_decision(first_frame)
    second = engine.positioning_decision(
        replace(
            first_frame,
            captured_at=first_frame.captured_at.replace(minute=1),
        )
    )

    assert first.episode_id is None
    assert second.episode_id is None
    assert StrategyEngine.episode_direction(first.state) == "LONG"
    assert "_episodes" not in engine.__dict__


def test_positioning_episode_direction_follows_market_interpretation() -> None:
    from dataclasses import replace

    engine = StrategyEngine()
    first_frame = _positioning_frame()
    first = engine.positioning_decision(first_frame)
    second = engine.positioning_decision(
        replace(
            first_frame,
            captured_at=first_frame.captured_at.replace(minute=1),
            closes=(Decimal("101"), Decimal("100")),
            net_spot_flow=Decimal("-8"),
            futures_trade_flow=Decimal("-8"),
            cvd_change=Decimal("-8"),
            taker_buy_volume=Decimal("3"),
            taker_sell_volume=Decimal("10"),
            oi_change=Decimal("0.03"),
            market_regime="RISK_OFF",
        )
    )

    assert StrategyEngine.episode_direction(first.state) == "LONG"
    assert StrategyEngine.episode_direction(second.state) == "SHORT"


def test_transition_strength_reflects_state_delta() -> None:
    from engine import StrategyEngine

    building_to_unwind = StrategyEngine._transition_strength(
        "LONG_BUILDING",
        "LONG_UNWIND",
        long_score=Decimal("0.4"),
        short_score=Decimal("0.2"),
    )
    neutral_to_building = StrategyEngine._transition_strength(
        "NEUTRAL",
        "LONG_BUILDING",
        long_score=Decimal("0.4"),
        short_score=Decimal("0.2"),
    )

    assert building_to_unwind > neutral_to_building


def test_engine_maps_flat_long_building_to_open_long() -> None:
    assert _map_position_action("FLAT", "LONG", "LONG_BUILDING") == ("LONG", "OPEN")
    assert _map_position_action("LONG", "LONG", "LONG_BUILDING") is None
    assert _map_position_action("LONG", "FLAT", "EXHAUSTION_LONG") == ("LONG", "REDUCE")
    assert _map_position_action("LONG", "SHORT", "SHORT_BUILDING") == ("LONG", "CLOSE")
    assert _map_position_action("FLAT", "SHORT", "SHORT_BUILDING") == ("SHORT", "OPEN")
    assert _map_position_action("SHORT", "LONG", "LONG_BUILDING") == ("SHORT", "CLOSE")


def test_unknown_and_conflicted_never_close_existing_positions() -> None:
    for state in ("UNKNOWN", "CONFLICTED"):
        assert _map_position_action("LONG", "FLAT", state) is None
        assert _map_position_action("SHORT", "FLAT", state) is None


def test_stale_and_future_data_never_create_close_intents() -> None:
    engine = StrategyEngine(StrategyConfig(positioning_decision_enabled=True))
    frame = _positioning_frame()
    stale = _positioning_frame(
        freshness=(
            SourceFreshness(
                "futures_trade_flow",
                frame.captured_at,
                frame.captured_at,
                1,
                frame.captured_at.replace(hour=13),
            ),
        ),
    )
    assert engine.evaluate(stale, current_position=CurrentPosition(direction="LONG", quantity=Decimal("1"))) is None
    future_decision = engine.positioning_decision(
        frame,
        now=frame.captured_at.replace(hour=11),
    )
    assert future_decision.state == "UNKNOWN"
    assert engine._intent_from_positioning(
        future_decision,
        frame,
        current_position=CurrentPosition(direction="SHORT", quantity=Decimal("1")),
    ) is None


def test_engine_does_not_repeat_open_when_already_long() -> None:
    engine = StrategyEngine(StrategyConfig(positioning_decision_enabled=True))
    frame = _positioning_frame()
    intent = engine.evaluate(
        frame,
        current_position=CurrentPosition(direction="LONG", quantity=Decimal("0.1")),
    )
    assert intent is None


def test_positioning_snapshot_id_is_replay_deterministic() -> None:
    frame = _positioning_frame()
    first = StrategyEngine().positioning_decision(frame)
    second = StrategyEngine().positioning_decision(frame)
    assert first.as_dict() == second.as_dict()


def test_positioning_decision_preserves_normalized_input_provenance() -> None:
    captured = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
    frame = _positioning_frame(
        source_timestamps={
            "spot_trade": {
                "source_timestamp": captured.isoformat(),
                "received_timestamp": captured.isoformat(),
                "latency_ms": 0,
            }
        }
    )

    decision = StrategyEngine().positioning_decision(frame)

    assert decision.source_timestamps["spot_trade"]["latency_ms"] == 0
    assert decision.input_features["net_spot_flow"] == Decimal("8")
    assert decision.as_dict()["source_timestamps"] == dict(frame.source_timestamps)


def test_future_data_fails_closed() -> None:
    frame = _positioning_frame()
    decision = StrategyEngine().positioning_decision(
        frame, now=frame.captured_at.replace(hour=11)
    )
    assert decision.state == "UNKNOWN"
    assert decision.direction == "FLAT"


def test_received_after_decision_fails_closed() -> None:
    frame = _positioning_frame(
        freshness=(
            SourceFreshness(
                "futures_trade_flow",
                datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc),
                datetime(2026, 8, 26, 12, 0, 1, tzinfo=timezone.utc),
                900,
                datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc),
            ),
        )
    )

    decision = StrategyEngine().positioning_decision(frame, now=frame.captured_at)

    assert decision.state == "UNKNOWN"
    assert decision.direction == "FLAT"


def test_inconsistent_declared_latency_fails_closed() -> None:
    captured = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
    frame = _positioning_frame(
        source_timestamps={
            "futures_funding": {
                "source_timestamp": captured.isoformat(),
                "received_timestamp": captured.replace(second=1).isoformat(),
                "latency_ms": 0,
            }
        }
    )

    decision = StrategyEngine().positioning_decision(frame)

    assert decision.state == "UNKNOWN"
    assert decision.direction == "FLAT"
    assert "TIMESTAMP_INCONSISTENT" in decision.reason_codes


def test_market_frame_rebuilds_from_normalized_evidence_snapshot() -> None:
    captured = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
    frame = _positioning_frame(
        source_timestamps={
            "spot_trade": {
                "source_timestamp": captured.isoformat(),
                "received_timestamp": captured.isoformat(),
                "latency_ms": 0,
            }
        }
    )
    expected = StrategyEngine().positioning_decision(frame)

    rebuilt = MarketFrame.from_evidence_snapshot(expected.as_dict())
    actual = StrategyEngine().positioning_decision(rebuilt)

    assert actual.as_dict() == expected.as_dict()


def test_missing_core_futures_evidence_cannot_create_directional_intent() -> None:
    engine = StrategyEngine(StrategyConfig(positioning_decision_enabled=True))
    frame = _positioning_frame(oi_change=None, funding_rate=None, taker_buy_volume=None, taker_sell_volume=None)
    decision = engine.positioning_decision(frame)
    assert decision.state == "UNKNOWN"
    assert decision.direction == "FLAT"
    assert engine.evaluate(frame) is None


def test_missing_spot_confirmation_still_allows_futures_long_building() -> None:
    frame = _positioning_frame(
        net_spot_flow=None,
        cvd_change=None,
        spot_buy_volume=None,
        spot_sell_volume=None,
    )
    decision = StrategyEngine().positioning_decision(frame)
    assert decision.state == "LONG_BUILDING"
    assert decision.direction == "LONG"
    assert "AUXILIARY_SPOT_MISSING" in decision.reason_codes


def test_unknown_cannot_create_executable_intent() -> None:
    engine = StrategyEngine(StrategyConfig(positioning_decision_enabled=True))
    frame = _positioning_frame(source_timestamps={})
    assert engine.positioning_decision(frame).state == "UNKNOWN"
    assert engine.evaluate(frame) is None


def test_missing_sign_is_not_neutral() -> None:
    from engine import _sign
    assert _sign(None) is None
    assert _sign(Decimal("0")) == 0
