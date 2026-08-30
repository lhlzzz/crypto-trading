from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from backtesting import replay_positioning_frames, run_backtest
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
        def signal(self, frame):
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
            "spot_book_ticker",
            "spot_orderbook",
        )
    }
    return MarketFrame(
        symbol="BTCUSDT",
        closes=(Decimal(before), Decimal(current)),
        captured_at=timestamp,
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
        market_regime="RISK_ON",
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
        _positioning_frame(start, "100", "101"),
        _positioning_frame(start + timedelta(minutes=1), "101", "102"),
    ]

    result = run_backtest([], frames=frames, symbol="BTCUSDT")

    assert result.net_return != result.gross_return
    assert result.fees > 0
    assert result.slippage >= 0
    assert result.funding != 0
