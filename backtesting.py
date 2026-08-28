"""Research-only backtesting entry point using the single strategy engine."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterable, Mapping

from engine import Direction, MarketFrame, PositioningDecision, StrategyConfig, StrategyEngine


@dataclass(frozen=True)
class BacktestResult:
    symbol: str
    initial_cash: Decimal
    final_value: Decimal
    total_return: Decimal
    max_drawdown: Decimal
    total_trades: int
    win_rate: Decimal
    loss_rate: Decimal
    profit_factor: Decimal
    expectancy: Decimal
    average_win: Decimal
    average_loss: Decimal
    sharpe: Decimal
    sortino: Decimal
    exposure: Decimal
    average_holding_time_minutes: Decimal
    fees: Decimal
    slippage: Decimal

    def as_dict(self) -> dict[str, str | int]:
        return {
            "symbol": self.symbol,
            "initial_cash": str(self.initial_cash),
            "final_value": str(self.final_value),
            "total_return": str(self.total_return),
            "max_drawdown": str(self.max_drawdown),
            "total_trades": self.total_trades,
            "win_rate": str(self.win_rate),
            "loss_rate": str(self.loss_rate),
            "profit_factor": str(self.profit_factor),
            "expectancy": str(self.expectancy),
            "average_win": str(self.average_win),
            "average_loss": str(self.average_loss),
            "sharpe": str(self.sharpe),
            "sortino": str(self.sortino),
            "exposure": str(self.exposure),
            "average_holding_time_minutes": str(self.average_holding_time_minutes),
            "fees": str(self.fees),
            "slippage": str(self.slippage),
        }


@dataclass(frozen=True)
class ForwardReturnMetrics:
    samples: int
    win_rate: Decimal
    expectancy: Decimal
    average_return: Decimal
    mfe: Decimal
    mae: Decimal
    max_drawdown: Decimal
    profit_factor: Decimal
    sharpe: Decimal
    sortino: Decimal
    average_holding_time_minutes: Decimal

    def as_dict(self) -> dict[str, str | int]:
        return {
            "samples": self.samples,
            "win_rate": str(self.win_rate),
            "expectancy": str(self.expectancy),
            "average_return": str(self.average_return),
            "mfe": str(self.mfe),
            "mae": str(self.mae),
            "max_drawdown": str(self.max_drawdown),
            "profit_factor": str(self.profit_factor),
            "sharpe": str(self.sharpe),
            "sortino": str(self.sortino),
            "average_holding_time_minutes": str(self.average_holding_time_minutes),
        }


@dataclass(frozen=True)
class PositioningReplayRecord:
    timestamp: datetime
    symbol: str
    positioning: PositioningDecision
    legacy_direction: Direction
    momentum_direction: Direction


@dataclass(frozen=True)
class PositioningReplayResult:
    records: tuple[PositioningReplayRecord, ...]
    attribution: Mapping[str, Mapping[int, ForwardReturnMetrics]]
    by_state: Mapping[str, Mapping[int, ForwardReturnMetrics]]
    by_transition: Mapping[str, Mapping[int, ForwardReturnMetrics]]

    def as_dict(self) -> dict[str, object]:
        def render(groups: Mapping[str, Mapping[int, ForwardReturnMetrics]]) -> dict[str, object]:
            return {
                name: {str(horizon): metrics.as_dict() for horizon, metrics in values.items()}
                for name, values in groups.items()
            }
        return {
            "records": len(self.records),
            "attribution": render(self.attribution),
            "by_state": render(self.by_state),
            "by_transition": render(self.by_transition),
        }


def _direction_from_signal(signal: object | None) -> Direction:
    side = getattr(signal, "side", None)
    if side == "BUY":
        return "LONG"
    if side == "SELL":
        return "SHORT"
    return "FLAT"


def _forward_metrics(
    observations: list[tuple[Decimal, Decimal, Decimal, Decimal]],
) -> ForwardReturnMetrics:
    if not observations:
        return ForwardReturnMetrics(
            samples=0, win_rate=Decimal("0"), expectancy=Decimal("0"),
            average_return=Decimal("0"), mfe=Decimal("0"), mae=Decimal("0"),
            max_drawdown=Decimal("0"), profit_factor=Decimal("0"),
            sharpe=Decimal("0"), sortino=Decimal("0"),
            average_holding_time_minutes=Decimal("0"),
        )
    returns = [item[0] for item in observations]
    count = Decimal(len(returns))
    mean = sum(returns, Decimal("0")) / count
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value < 0]
    variance = sum((value - mean) ** 2 for value in returns) / count
    stddev = variance.sqrt() if variance > 0 else Decimal("0")
    downside = [value for value in returns if value < 0]
    downside_variance = (
        sum(value ** 2 for value in downside) / Decimal(len(downside))
        if downside else Decimal("0")
    )
    downside_stddev = downside_variance.sqrt() if downside_variance > 0 else Decimal("0")
    equity = Decimal("1")
    peak = equity
    max_drawdown = Decimal("0")
    for value in returns:
        equity *= Decimal("1") + value
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - equity) / peak)
    gross_profit = sum(wins, Decimal("0"))
    gross_loss = abs(sum(losses, Decimal("0")))
    return ForwardReturnMetrics(
        samples=len(returns),
        win_rate=Decimal(len(wins)) / count,
        expectancy=mean,
        average_return=mean,
        mfe=sum((item[1] for item in observations), Decimal("0")) / count,
        mae=sum((item[2] for item in observations), Decimal("0")) / count,
        max_drawdown=max_drawdown,
        profit_factor=gross_profit / gross_loss if gross_loss else Decimal("0"),
        sharpe=mean / stddev if stddev else Decimal("0"),
        sortino=mean / downside_stddev if downside_stddev else Decimal("0"),
        average_holding_time_minutes=(
            sum((item[3] for item in observations), Decimal("0")) / count
        ),
    )


def replay_positioning_frames(
    frames: Iterable[MarketFrame],
    *,
    engine: StrategyEngine | None = None,
    horizons_seconds: tuple[int, ...] = (60, 180, 300, 900, 1800, 3600),
) -> PositioningReplayResult:
    """Replay normalized historical frames and attribute only afterward.

    Each decision sees exactly its own frame and timestamps. Forward prices are
    read only after the decision has been materialized for research metrics.
    """
    ordered = list(frames)
    for previous, current in zip(ordered, ordered[1:]):
        if current.captured_at < previous.captured_at:
            raise ValueError("replay frames must be chronological")
    config = engine.config if engine is not None else StrategyConfig()
    replay_engine = StrategyEngine(config)
    records: list[PositioningReplayRecord] = []
    previous_state = None
    for frame in ordered:
        decision = replay_engine.positioning_decision(
            frame,
            now=frame.captured_at,
            previous_state=previous_state,
        )
        previous_state = decision.state
        legacy = _direction_from_signal(replay_engine.signal(frame))
        momentum: Direction = "FLAT"
        if len(frame.closes) >= 2:
            momentum = "LONG" if frame.closes[-1] > frame.closes[-2] else (
                "SHORT" if frame.closes[-1] < frame.closes[-2] else "FLAT"
            )
        records.append(
            PositioningReplayRecord(
                timestamp=frame.captured_at,
                symbol=frame.symbol.upper(),
                positioning=decision,
                legacy_direction=legacy,
                momentum_direction=momentum,
            )
        )

    by_strategy: dict[str, dict[int, list[tuple[Decimal, Decimal, Decimal, Decimal]]]] = {
        "positioning": {}, "legacy_sma": {}, "simple_momentum": {},
    }
    by_state: dict[str, dict[int, list[tuple[Decimal, Decimal, Decimal, Decimal]]]] = {}
    by_transition: dict[str, dict[int, list[tuple[Decimal, Decimal, Decimal, Decimal]]]] = {}
    for index, record in enumerate(records):
        entry = ordered[index].closes[-1]
        if entry <= 0:
            continue
        directions = {
            "positioning": record.positioning.direction,
            "legacy_sma": record.legacy_direction,
            "simple_momentum": record.momentum_direction,
        }
        for horizon in horizons_seconds:
            target = record.timestamp.timestamp() + max(1, horizon)
            exit_index = next(
                (
                    candidate for candidate in range(index + 1, len(records))
                    if records[candidate].timestamp.timestamp() >= target
                ),
                None,
            )
            if exit_index is None:
                continue
            prices = [frame.closes[-1] for frame in ordered[index + 1:exit_index + 1]]
            holding = Decimal(str(
                (records[exit_index].timestamp - record.timestamp).total_seconds() / 60
            ))
            for strategy_name, direction in directions.items():
                if direction == "FLAT":
                    continue
                multiplier = Decimal("1") if direction == "LONG" else Decimal("-1")
                path = [multiplier * (price / entry - Decimal("1")) for price in prices]
                observation = (path[-1], max(path), min(path), holding)
                by_strategy[strategy_name].setdefault(horizon, []).append(observation)
                if strategy_name == "positioning":
                    by_state.setdefault(record.positioning.state, {}).setdefault(
                        horizon, []
                    ).append(observation)
                    if record.positioning.transition != "NONE":
                        by_transition.setdefault(record.positioning.transition, {}).setdefault(
                            horizon, []
                        ).append(observation)

    def summarize(
        groups: Mapping[str, Mapping[int, list[tuple[Decimal, Decimal, Decimal, Decimal]]]],
    ) -> dict[str, dict[int, ForwardReturnMetrics]]:
        return {
            name: {horizon: _forward_metrics(values) for horizon, values in horizons.items()}
            for name, horizons in groups.items()
        }

    return PositioningReplayResult(
        records=tuple(records),
        attribution=summarize(by_strategy),
        by_state=summarize(by_state),
        by_transition=summarize(by_transition),
    )


def run_backtest(
    closes: Iterable[Decimal | str | float],
    *,
    symbol: str = "BTCUSDT",
    initial_cash: Decimal = Decimal("1000"),
    fee_rate: Decimal = Decimal("0.001"),
    slippage_bps: Decimal = Decimal("5"),
    engine: StrategyEngine | None = None,
) -> BacktestResult:
    """Run research only; no database, broker, or order side effects."""

    try:
        import pandas as pd
        import vectorbt as vbt
    except ImportError as exc:
        raise RuntimeError("vectorbt is required for backtesting") from exc

    values = [Decimal(str(value)) for value in closes]
    if len(values) < 2 or any(value <= 0 for value in values):
        raise ValueError("backtesting requires at least two positive closes")
    strategy = engine or StrategyEngine()
    index = pd.date_range(
        end=datetime.now(timezone.utc), periods=len(values), freq="min"
    )
    entries: list[bool] = []
    exits: list[bool] = []
    for position in range(len(values)):
        frame = MarketFrame(
            symbol=symbol,
            closes=tuple(values[: position + 1]),
            captured_at=index[position].to_pydatetime(),
        )
        signal = strategy.signal(frame)
        entries.append(signal is not None and signal.side == "BUY")
        exits.append(signal is not None and signal.side == "SELL")

    close_series = pd.Series([float(value) for value in values], index=index)
    portfolio = vbt.Portfolio.from_signals(
        close_series,
        entries=pd.Series(entries, index=index),
        exits=pd.Series(exits, index=index),
        init_cash=float(initial_cash),
        fees=float(fee_rate),
        slippage=float(slippage_bps / Decimal("10000")),
        freq="1min",
    )
    no_slippage = vbt.Portfolio.from_signals(
        close_series,
        entries=pd.Series(entries, index=index),
        exits=pd.Series(exits, index=index),
        init_cash=float(initial_cash),
        fees=float(fee_rate),
        slippage=0.0,
        freq="1min",
    )
    stats = portfolio.stats()
    final_value = Decimal(str(portfolio.value().iloc[-1]))
    total_return = final_value / initial_cash - Decimal("1")
    max_drawdown = abs(Decimal(str(stats.get("Max Drawdown [%]", 0))) / Decimal("100"))
    trades = int(stats.get("Total Trades", 0))
    raw_win_rate = Decimal(str(stats.get("Win Rate [%]", 0)))
    win_rate = (
        Decimal("0")
        if not raw_win_rate.is_finite()
        else raw_win_rate / Decimal("100")
    )
    raw_loss_rate = Decimal(str(stats.get("Loss Rate [%]", 0)))
    loss_rate = (
        Decimal("0")
        if not raw_loss_rate.is_finite()
        else raw_loss_rate / Decimal("100")
    )

    def metric(name: str, *, percent: bool = False) -> Decimal:
        value = Decimal(str(stats.get(name, 0)))
        if not value.is_finite():
            return Decimal("0")
        return value / Decimal("100") if percent else value

    records = portfolio.trades.records
    if len(records) == 0:
        average_holding_minutes = Decimal("0")
    else:
        durations = [
            Decimal(str(int(row.exit_idx) - int(row.entry_idx)))
            for row in records.itertuples(index=False)
        ]
        average_holding_minutes = sum(durations, Decimal("0")) / Decimal(str(len(durations)))
    return BacktestResult(
        symbol=symbol,
        initial_cash=initial_cash,
        final_value=final_value,
        total_return=total_return,
        max_drawdown=max_drawdown,
        total_trades=trades,
        win_rate=win_rate,
        loss_rate=loss_rate,
        profit_factor=metric("Profit Factor"),
        expectancy=metric("Expectancy"),
        average_win=metric("Avg Winning Trade [%]", percent=True),
        average_loss=metric("Avg Losing Trade [%]", percent=True),
        sharpe=metric("Sharpe Ratio"),
        sortino=metric("Sortino Ratio"),
        exposure=metric("Max Gross Exposure [%]", percent=True),
        average_holding_time_minutes=average_holding_minutes,
        fees=Decimal(str(stats.get("Total Fees Paid", 0))),
        slippage=abs(
            Decimal(str(no_slippage.value().iloc[-1])) - final_value
        ),
    )
