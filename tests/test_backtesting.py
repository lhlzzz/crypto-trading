from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from backtesting import BacktestResult, evaluate_alpha_gate, replay_positioning_frames, run_backtest
from engine import MarketFrame, SourceFreshness, StrategyConfig, StrategyEngine


def test_backtesting_uses_strategy_engine_and_returns_metrics() -> None:
    timestamps = [
        datetime(2026, 8, 1, tzinfo=timezone.utc) + timedelta(minutes=index)
        for index in range(12)
    ]
    result = run_backtest(
        [100, 101, 102, 101, 99, 98, 100, 103, 105, 104, 103, 106],
        timestamps=timestamps,
        initial_cash=Decimal("1000"),
        engine=StrategyEngine(
            StrategyConfig(
                fast_window=2,
                slow_window=4,
                minimum_confidence=Decimal("0.5"),
            )
        ),
    )

    assert result.initial_cash == Decimal("1000")
    assert result.final_value > 0
    assert result.max_drawdown >= 0
    assert result.fees >= 0
    assert result.slippage >= 0
    assert result.loss_rate >= 0
    assert result.profit_factor >= 0
    assert result.average_loss <= 0
    assert result.exposure >= 0
    assert result.average_holding_time_minutes >= 0


def test_backtest_frames_are_timestamp_safe_and_prefix_only() -> None:
    seen: list[tuple[int, Decimal]] = []

    class PrefixOnlyStrategy:
        def evaluate(self, frame):
            seen.append((len(frame.closes), frame.closes[-1]))
            return None

    result = run_backtest(
        [100, 101, 102, 103],
        timestamps=[
            datetime(2026, 8, 1, tzinfo=timezone.utc) + timedelta(minutes=index)
            for index in range(4)
        ],
        engine=PrefixOnlyStrategy(),
    )

    assert result.total_trades == 0
    assert [size for size, _ in seen] == [1, 2, 3, 4]
    assert [price for _, price in seen] == [
        Decimal("100"),
        Decimal("101"),
        Decimal("102"),
        Decimal("103"),
    ]


def _positioning_frame(timestamp: datetime, before: str, current: str) -> MarketFrame:
    source = {
        name: {
            "source_timestamp": timestamp.isoformat(),
            "received_timestamp": timestamp.isoformat(),
            "latency_ms": 0,
        }
        for name in (
            "spot_trade",
            "futures_open_interest",
            "futures_funding",
            "futures_trade_flow",
            "futures_taker_ratio",
            "futures_mark_price",
            "futures_orderbook",
            "futures_book_ticker",
            "futures_mark_price",
            "futures_orderbook",
            "futures_book_ticker",
            "spot_book_ticker",
            "spot_orderbook",
        )
    }
    return MarketFrame(
        symbol="BTCUSDT",
        closes=(Decimal(before), Decimal(current)),
        captured_at=timestamp,
        bid_price=Decimal(current) - Decimal("0.1"),
        ask_price=Decimal(current) + Decimal("0.1"),
        last_price=Decimal(current),
        mark_price=Decimal(current),
        index_price=Decimal(current),
        spot_buy_volume=Decimal("12"),
        spot_sell_volume=Decimal("4"),
        net_spot_flow=Decimal("8"),
        futures_trade_flow=Decimal("8"),
        cvd_change=Decimal("8"),
        taker_buy_volume=Decimal("10"),
        taker_sell_volume=Decimal("3"),
        oi_change=Decimal("0.03"),
        funding_rate=Decimal("0.0001"),
        spread_bps=Decimal("2"),
        depth_25bps=Decimal("100"),
        funding_timestamp=timestamp,
        funding_settlement_timestamp=timestamp,
        market_regime="RISK_ON",
        meme_risk_tier="TRADEABLE",
        evidence_status={"orderbook": "VALID"},
        is_meme=True,
        freshness=(SourceFreshness("spot", timestamp, timestamp, 900, timestamp),),
        source_timestamps=source,
    )


def test_positioning_replay_is_deterministic_and_attributes_after_decision() -> None:
    start = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
    frames = [
        _positioning_frame(start, "100", "101"),
        _positioning_frame(start + timedelta(minutes=1), "101", "102"),
        _positioning_frame(start + timedelta(minutes=2), "102", "104"),
    ]

    first = replay_positioning_frames(frames, horizons_seconds=(60, 120))
    second = replay_positioning_frames(frames, horizons_seconds=(60, 120))

    assert first == second
    assert first.records[0].positioning.direction == "LONG"
    assert first.attribution["positioning"][60].samples == 2
    assert first.by_state["LONG_BUILDING"][60].average_return > 0


def test_positioning_replay_rejects_non_chronological_frames() -> None:
    start = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
    later = _positioning_frame(start + timedelta(minutes=1), "100", "101")
    earlier = _positioning_frame(start, "99", "100")

    try:
        replay_positioning_frames([later, earlier])
    except ValueError as exc:
        assert "chronological" in str(exc)
    else:
        raise AssertionError("expected chronological replay validation")


def test_backtest_frames_use_futures_paper_execution_contract() -> None:
    start = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
    frames = [
        _positioning_frame(start, "100", "100"),
        _positioning_frame(start + timedelta(minutes=1), "100", "125"),
    ]

    result = run_backtest([], frames=frames, symbol="BTCUSDT")

    assert result.net_return != result.gross_return
    assert result.fees > 0
    assert result.slippage >= 0
    assert result.funding != 0


def test_alpha_gate_requires_sufficient_chronological_oos_sample() -> None:
    start = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
    result = evaluate_alpha_gate(
        [_positioning_frame(start + timedelta(minutes=i), str(100 + i), str(101 + i)) for i in range(8)],
        min_samples=5,
    )
    assert result.status == "INSUFFICIENT_SAMPLE"
    assert result.out_of_sample_samples >= 0


def test_episode_deduplication() -> None:
    start = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
    replay = replay_positioning_frames(
        [
            _positioning_frame(start + timedelta(minutes=index), str(100 + index), str(101 + index))
            for index in range(6)
        ],
        horizons_seconds=(60,),
    )

    assert replay.attribution["positioning"][60].samples > replay.independent_episodes
    assert replay.independent_episodes == 1
    assert replay.episode_trade_metrics[60].samples == 1


def test_alpha_gate_true_oos_and_does_not_tune_oos(monkeypatch) -> None:
    start = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
    frames = [
        _positioning_frame(start + timedelta(minutes=index), str(100 + index), str(101 + index))
        for index in range(16)
    ]
    seen = []

    def fake_backtest(*args, **kwargs):
        seen.append((kwargs["frames"], kwargs["fee_rate"], kwargs["slippage_bps"], kwargs["engine"].config))
        return BacktestResult(
            symbol="BTCUSDT", initial_cash=Decimal("1000"), final_value=Decimal("1010"),
            total_return=Decimal("0.01"), max_drawdown=Decimal("0.01"), total_trades=1,
            win_rate=Decimal("1"), loss_rate=Decimal("0"), profit_factor=Decimal("2"),
            expectancy=Decimal("0.01"), average_win=Decimal("1"), average_loss=Decimal("0"),
            sharpe=Decimal("1"), sortino=Decimal("1"), exposure=Decimal("0"),
            average_holding_time_minutes=Decimal("1"), fees=Decimal("0"), slippage=Decimal("0"),
        )

    monkeypatch.setattr("backtesting.run_backtest", fake_backtest)
    result = evaluate_alpha_gate(frames, min_samples=1)

    assert result.status == "ALPHA_NOT_SUPPORTED"
    assert result.reason == "DIRECTIONAL_SAMPLE_INSUFFICIENT"
    assert result.train_samples == 8
    assert result.validation_samples == 4
    assert result.oos_samples == result.oos_metrics["independent_episodes"]
    assert result.strategy_version == "positioning-v1"
    assert result.parameter_version == result.config_hash
    assert len(result.config_hash) == 64
    payload = result.as_dict()
    assert payload["oos_samples_semantics"] == "independent_episodes"
    assert payload["oos_frame_count"] == 4
    assert payload["oos_independent_episodes"] == result.oos_metrics["independent_episodes"]
    assert payload["frozen_strategy"] is True
    assert payload["model_training_completed"] is False
    assert payload["chronological_train"] is True
    assert len(seen) == 2
    assert seen[0][0] == frames[12:]
    assert [frame.captured_at for frame in seen[1][0]] == [
        frame.captured_at for frame in frames[12:]
    ]
    assert seen[0][3] == seen[1][3]


def test_alpha_cost_stress_rejects_strategy(monkeypatch) -> None:
    start = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
    frames = [
        _positioning_frame(start + timedelta(minutes=index), str(100 + index), str(101 + index))
        for index in range(16)
    ]

    def fake_backtest(*args, **kwargs):
        stressed = kwargs["fee_rate"] > Decimal("0.001")
        expectancy = Decimal("-0.01") if stressed else Decimal("0.01")
        return BacktestResult(
            symbol="BTCUSDT", initial_cash=Decimal("1000"), final_value=Decimal("1000"),
            total_return=Decimal("0"), max_drawdown=Decimal("0.01"), total_trades=1,
            win_rate=Decimal("1"), loss_rate=Decimal("0"), profit_factor=Decimal("2"),
            expectancy=expectancy, average_win=Decimal("1"), average_loss=Decimal("0"),
            sharpe=Decimal("1"), sortino=Decimal("1"), exposure=Decimal("0"),
            average_holding_time_minutes=Decimal("1"), fees=Decimal("0"), slippage=Decimal("0"),
        )

    monkeypatch.setattr("backtesting.run_backtest", fake_backtest)
    result = evaluate_alpha_gate(frames, min_samples=1)

    assert result.status == "ALPHA_NOT_SUPPORTED"
    assert result.oos_metrics["cost_stress"]["net_expectancy"] == "-0.01"


def test_future_data_injection_does_not_change_replay() -> None:
    start = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
    frames = [
        _positioning_frame(start + timedelta(minutes=index), str(100 + index), str(101 + index))
        for index in range(8)
    ]
    baseline = replay_positioning_frames(frames)
    future = _positioning_frame(start + timedelta(days=1), "200", "201")
    injected = replay_positioning_frames([*frames, future])
    assert [record.positioning.direction for record in baseline.records] == [
        record.positioning.direction for record in injected.records[:-1]
    ]
    assert [record.positioning.state for record in baseline.records] == [
        record.positioning.state for record in injected.records[:-1]
    ]
