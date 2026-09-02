"""Research-only backtesting entry point using the single strategy engine."""
from __future__ import annotations

import os

from dataclasses import asdict, dataclass, replace
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from statistics import fmean
from typing import Any, Iterable, Mapping
from uuid import UUID, uuid4

from engine import Direction, MarketFrame, PositioningDecision, StrategyConfig, StrategyEngine
from execution import ExecutionConfig, MarketSnapshot, PaperExecutor
from risk import RiskGate, RiskLimits
from trade_intent import TradeIntent


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
    funding: Decimal = Decimal("0")
    gross_return: Decimal = Decimal("0")
    net_return: Decimal = Decimal("0")
    mfe: Decimal = Decimal("0")
    mae: Decimal = Decimal("0")

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
            "funding": str(self.funding),
            "gross_return": str(self.gross_return),
            "net_return": str(self.net_return),
            "mfe": str(self.mfe),
            "mae": str(self.mae),
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
    episode_trade_metrics: Mapping[int, ForwardReturnMetrics]
    independent_episodes: int

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
            "observation_samples": sum(
                metrics.samples
                for metrics in self.attribution.get("positioning", {}).values()
            ),
            "independent_episodes": self.independent_episodes,
            "episode_trade_metrics": {
                str(horizon): metrics.as_dict()
                for horizon, metrics in self.episode_trade_metrics.items()
            },
        }


@dataclass(frozen=True)
class AlphaGateResult:
    """Evidence-backed outcome for a chronological train/validation/OOS split."""

    status: str
    train_samples: int
    validation_samples: int
    oos_samples: int
    train_metrics: Mapping[str, Any]
    validation_metrics: Mapping[str, Any]
    oos_metrics: Mapping[str, Any]
    strategy_version: str
    parameter_version: str
    config_hash: str
    reason: str
    code_commit: str = "UNKNOWN"
    universe_version: str = ""
    long_episodes: int = 0
    short_episodes: int = 0

    @property
    def out_of_sample_samples(self) -> int:
        return self.oos_samples

    @property
    def out_of_sample_expectancy(self) -> Decimal:
        return Decimal(str(self.oos_metrics.get("expectancy", "0")))

    @property
    def strategy_config_hash(self) -> str:
        return self.config_hash

    def as_dict(self) -> dict[str, str | int]:
        return {
            "status": self.status,
            "train_samples": self.train_samples,
            "validation_samples": self.validation_samples,
            "oos_samples": self.oos_samples,
            "out_of_sample_samples": self.oos_samples,
            "train_metrics": dict(self.train_metrics),
            "validation_metrics": dict(self.validation_metrics),
            "oos_metrics": dict(self.oos_metrics),
            "strategy_version": self.strategy_version,
            "parameter_version": self.parameter_version,
            "config_hash": self.config_hash,
            "strategy_config_hash": self.config_hash,
            "reason": self.reason,
            "code_commit": self.code_commit,
            "universe_version": self.universe_version,
            "long_episodes": self.long_episodes,
            "short_episodes": self.short_episodes,
        }


class _BacktestStore:
    """Ephemeral research ledger consumed by the shared PaperExecutor.

    This store never persists or talks to an exchange. Trading facts remain
    owned by TradingStore in runtime paths; the ledger only lets a backtest
    exercise the exact paper execution contract without a database.
    """

    def __init__(self, initial_cash: Decimal) -> None:
        self.orders: dict[UUID, dict[str, Any]] = {}
        self.positions: dict[str, dict[str, Any]] = {}
        self.balances: dict[str, dict[str, Any]] = {}
        self.trades: list[dict[str, Any]] = []
        self.funding_entries: list[Decimal] = []
        self.events: list[dict[str, Any]] = []
        self.halted = False
        self.upsert_balance(
            "USDT",
            free=initial_cash,
            locked=Decimal("0"),
            wallet_balance=initial_cash,
            available_balance=initial_cash,
            margin_balance=initial_cash,
            used_margin=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            mode="paper",
            payload={"source": "backtest"},
        )

    def initialize(self) -> None:
        return None

    def is_halted(self) -> bool:
        return self.halted

    def set_halt(self, halted: bool, *, reason: str, source: str) -> None:
        self.halted = halted
        self.events.append({"event_type": "HALT", "reason": reason, "source": source})

    def record_system_event(self, **fields: Any) -> UUID:
        self.events.append(fields)
        return uuid4()

    def record_risk_event(self, **fields: Any) -> UUID:
        self.events.append(fields)
        return uuid4()

    def record_intent(self, intent: TradeIntent, *, status: str = "CREATED") -> None:
        del intent, status

    def create_order(self, intent: TradeIntent, *, mode: str, status: str, **fields: Any) -> UUID:
        order_id = uuid4()
        self.orders[order_id] = {
            "order_id": order_id,
            "intent_id": intent.id,
            "symbol": intent.symbol,
            "client_order_id": intent.client_order_id,
            "quantity": intent.quantity,
            "price": intent.price,
            "executed_quantity": Decimal("0"),
            "status": status,
            "mode": mode,
            "order_type": intent.order_type,
            "position_side": intent.direction,
            "position_action": intent.action,
            "reduce_only": intent.reduce_only,
            "leverage": intent.leverage,
            "intent": intent,
            **fields,
        }
        return order_id

    def update_order(self, order_id: UUID, *, status: str, **fields: Any) -> None:
        self.orders[order_id].update({"status": status, **fields})

    def append_order_event(self, order_id: UUID, **fields: Any) -> None:
        self.events.append({"order_id": order_id, **fields})

    def get_order(self, order_id: UUID) -> dict[str, Any] | None:
        return self.orders.get(order_id)

    def get_order_by_client_order_id(self, client_order_id: str) -> dict[str, Any] | None:
        return next(
            (row for row in self.orders.values() if row["client_order_id"] == client_order_id),
            None,
        )

    def list_open_local_orders(self) -> list[dict[str, Any]]:
        return [
            row for row in self.orders.values()
            if row["status"] in {
                "CREATED", "RISK_APPROVED", "SUBMITTED", "ACKNOWLEDGED",
                "PARTIALLY_FILLED", "UNKNOWN",
            }
        ]

    def record_trade(self, order_id: UUID, **fields: Any) -> UUID:
        trade_id = uuid4()
        self.trades.append({"trade_id": trade_id, "order_id": order_id, **fields})
        return trade_id

    def upsert_position(self, symbol: str, **fields: Any) -> None:
        self.positions[symbol.upper()] = {"symbol": symbol.upper(), **fields}

    def get_position(self, symbol: str, **_: Any) -> dict[str, Any] | None:
        return self.positions.get(symbol.upper())

    def upsert_balance(self, asset: str, **fields: Any) -> None:
        self.balances[asset.upper()] = {"asset": asset.upper(), **fields}

    def get_balance(self, asset: str) -> dict[str, Any] | None:
        return self.balances.get(asset.upper())


def _metric_from_returns(values: list[Decimal]) -> tuple[Decimal, Decimal, Decimal]:
    if not values:
        return Decimal("0"), Decimal("0"), Decimal("0")
    mean = sum(values, Decimal("0")) / Decimal(len(values))
    variance = sum((value - mean) ** 2 for value in values) / Decimal(len(values))
    stddev = variance.sqrt() if variance > 0 else Decimal("0")
    downside = [value for value in values if value < 0]
    downside_stddev = (
        (sum(value * value for value in downside) / Decimal(len(downside))).sqrt()
        if downside else Decimal("0")
    )
    return (
        mean / stddev if stddev else Decimal("0"),
        mean / downside_stddev if downside_stddev else Decimal("0"),
        mean,
    )


def _research_sma_direction(frame: MarketFrame, config: StrategyConfig) -> Direction:
    """Research-only SMA baseline. Never used as a production owner."""
    closes = tuple(Decimal(value) for value in frame.closes)
    if len(closes) < config.slow_window:
        return "FLAT"
    if any(value <= 0 for value in closes):
        return "FLAT"
    fast = Decimal(str(fmean(closes[-config.fast_window :])))
    slow = Decimal(str(fmean(closes[-config.slow_window :])))
    price = closes[-1]
    if fast > slow and price >= fast:
        return "LONG"
    if fast < slow and price <= fast:
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
        legacy = _research_sma_direction(frame, replay_engine.config)
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

    # Collapse consecutive observations in the same directional episode. The
    # raw forward observations remain available for diagnostics, but episode
    # metrics count one trade opportunity per continuous positioning regime.
    episode_groups: dict[int, list[tuple[Decimal, Decimal, Decimal, Decimal]]] = {}
    episode_starts: list[int] = []
    previous_direction: Direction = "FLAT"
    for index, record in enumerate(records):
        direction = record.positioning.direction
        if direction != "FLAT" and direction != previous_direction:
            episode_starts.append(index)
        previous_direction = direction

    for episode_number, index in enumerate(episode_starts):
        record = records[index]
        entry = ordered[index].closes[-1]
        if entry <= 0:
            continue
        episode_end = (
            episode_starts[episode_number + 1]
            if episode_number + 1 < len(episode_starts)
            else len(records)
        )
        for horizon in horizons_seconds:
            target = record.timestamp.timestamp() + max(1, horizon)
            horizon_exit = next(
                (
                    candidate for candidate in range(index + 1, len(records))
                    if records[candidate].timestamp.timestamp() >= target
                ),
                None,
            )
            exit_index = min(
                horizon_exit if horizon_exit is not None else episode_end - 1,
                episode_end - 1,
            )
            if exit_index <= index:
                continue
            prices = [frame.closes[-1] for frame in ordered[index + 1:exit_index + 1]]
            if not prices:
                continue
            multiplier = Decimal("1") if record.positioning.direction == "LONG" else Decimal("-1")
            path = [multiplier * (price / entry - Decimal("1")) for price in prices]
            holding = Decimal(str(
                (records[exit_index].timestamp - record.timestamp).total_seconds() / 60
            ))
            episode_groups.setdefault(horizon, []).append(
                (path[-1], max(path), min(path), holding)
            )

    episode_metrics = {
        horizon: _forward_metrics(values)
        for horizon, values in episode_groups.items()
    }
    return PositioningReplayResult(
        records=tuple(records),
        attribution=summarize(by_strategy),
        by_state=summarize(by_state),
        by_transition=summarize(by_transition),
        episode_trade_metrics=episode_metrics,
        independent_episodes=len(episode_starts),
    )


def run_backtest(
    closes: Iterable[Decimal | str | float],
    *,
    timestamps: Iterable[datetime] | None = None,
    frames: Iterable[MarketFrame] | None = None,
    symbol: str = "BTCUSDT",
    initial_cash: Decimal = Decimal("1000"),
    fee_rate: Decimal = Decimal("0.001"),
    slippage_bps: Decimal = Decimal("5"),
    engine: StrategyEngine | None = None,
) -> BacktestResult:
    """Replay historical Futures evidence through the shared paper contract.

    ``frames`` is the preferred input. The close-only form remains available
    for the SMA research baseline, but it intentionally produces no
    positioning order because missing Futures evidence fails closed.
    """
    if initial_cash <= 0:
        raise ValueError("initial_cash must be positive")
    if frames is not None:
        historical_frames = list(frames)
        if not historical_frames:
            raise ValueError("frames must contain at least one historical frame")
        values = [Decimal(str(frame.last_price or frame.closes[-1])) for frame in historical_frames]
        historical_times = [frame.captured_at for frame in historical_frames]
        if any(frame.symbol.upper() != symbol.upper() for frame in historical_frames):
            raise ValueError("all frames must use the requested symbol")
    else:
        values = [Decimal(str(value)) for value in closes]
        if len(values) < 2 or any(value <= 0 for value in values):
            raise ValueError("backtesting requires at least two positive closes")
        if timestamps is None:
            raise ValueError("backtesting requires historical observation timestamps")
        historical_times = list(timestamps)
        if len(historical_times) != len(values):
            raise ValueError("timestamps must align one-to-one with closes")
        historical_frames = [
            MarketFrame(
                symbol=symbol,
                closes=tuple(values[: position + 1]),
                captured_at=historical_times[position],
            )
            for position in range(len(values))
        ]
    if any(timestamp.tzinfo is None for timestamp in historical_times):
        raise ValueError("historical timestamps must be timezone-aware")
    if any(current < previous for previous, current in zip(historical_times, historical_times[1:])):
        raise ValueError("historical timestamps must be chronological")

    strategy = engine or StrategyEngine()
    store = _BacktestStore(initial_cash)
    executor = PaperExecutor(
        store=store,
        config=ExecutionConfig(
            mode="paper",
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
            default_leverage=(
                strategy.config.default_leverage
                if isinstance(strategy, StrategyEngine)
                else Decimal("1")
            ),
        ),
    )
    risk_gate = RiskGate(limits=RiskLimits.from_env())
    from paper_runner import _risk_context

    previous_state = None
    equity_curve: list[Decimal] = []
    holding_minutes: list[Decimal] = []
    mfe_values: list[Decimal] = []
    mae_values: list[Decimal] = []
    active_start: datetime | None = None
    active_direction: str | None = None
    active_entry = Decimal("0")
    active_mfe = Decimal("0")
    active_mae = Decimal("0")
    max_exposure = Decimal("0")

    for frame in historical_frames:
        if hasattr(strategy, "evaluate") and not isinstance(strategy, StrategyEngine):
            strategy.evaluate(frame)
        decision = None
        intent = None
        if isinstance(strategy, StrategyEngine):
            decision = strategy.positioning_decision(
                frame,
                now=frame.captured_at,
                previous_state=previous_state,
            )
            previous_state = decision.state
            current = store.get_position(symbol)
            from engine import CurrentPosition

            current_position = CurrentPosition(
                direction=str((current or {}).get("position_side") or "FLAT"),  # type: ignore[arg-type]
                quantity=Decimal(str((current or {}).get("quantity") or "0")),
                entry_price=(
                    Decimal(str((current or {}).get("entry_price")))
                    if (current or {}).get("entry_price") is not None else None
                ),
            )
            intent = strategy._intent_from_positioning(
                decision, frame, current_position=current_position
            )
        market = MarketSnapshot(
            last_price=values[len(equity_curve)],
            mark_price=frame.mark_price,
            index_price=frame.index_price,
            bid_price=frame.bid_price,
            ask_price=frame.ask_price,
            available_liquidity_notional_usdt=(
                frame.depth_25bps * frame.mark_price
                if frame.depth_25bps is not None and frame.mark_price is not None
                else None
            ),
            funding_rate=frame.funding_rate,
            funding_timestamp=frame.funding_timestamp,
            settlement_timestamp=frame.funding_settlement_timestamp,
            current_timestamp=frame.captured_at,
        )
        current_before = store.get_position(symbol)
        if current_before and Decimal(str(current_before.get("quantity") or "0")) > 0:
            if active_start is None:
                active_start = frame.captured_at
                active_direction = str(current_before.get("position_side") or "LONG")
                active_entry = Decimal(str(current_before.get("entry_price") or "0"))
            if active_entry > 0:
                excursion = (
                    (market.mark_price - active_entry) / active_entry
                    if active_direction == "LONG"
                    else (active_entry - market.mark_price) / active_entry
                )
                active_mfe = max(active_mfe, excursion)
                active_mae = min(active_mae, excursion)
        if intent is not None and not store.is_halted():
            context = _risk_context(
                intent,
                market,
                account_snapshot=executor.account_snapshot(),
                mode="paper",
                paper_executor=executor,
            )
            risk_decision = risk_gate.evaluate(intent, context)
            if risk_decision.executable_intent is not None:
                executor.submit(intent, risk_decision, market=market)
        if (
            frame.funding_rate is not None
            and frame.funding_settlement_timestamp is not None
            and not store.is_halted()
        ):
            executor.apply_funding(symbol, market)
        if not store.is_halted() and market.mark_price is not None:
            executor.mark_to_market(symbol, market.mark_price)
        current_after = store.get_position(symbol)
        after_quantity = Decimal(str((current_after or {}).get("quantity") or "0"))
        if after_quantity > 0 and active_start is None:
            active_start = frame.captured_at
            active_direction = str((current_after or {}).get("position_side") or "LONG")
            active_entry = Decimal(str((current_after or {}).get("entry_price") or "0"))
        if after_quantity > 0 and active_entry > 0:
            excursion = (
                (market.mark_price - active_entry) / active_entry
                if active_direction == "LONG"
                else (active_entry - market.mark_price) / active_entry
            )
            active_mfe = max(active_mfe, excursion)
            active_mae = min(active_mae, excursion)
            max_exposure = max(
                max_exposure,
                after_quantity * market.mark_price / initial_cash,
            )
        elif active_start is not None:
            holding_minutes.append(
                Decimal(str((frame.captured_at - active_start).total_seconds() / 60))
            )
            mfe_values.append(active_mfe)
            mae_values.append(active_mae)
            active_start = None
            active_direction = None
            active_entry = Decimal("0")
            active_mfe = Decimal("0")
            active_mae = Decimal("0")
        equity_curve.append(executor.account_state()["equity"])

    final_value = equity_curve[-1] if equity_curve else initial_cash
    net_return = final_value / initial_cash - Decimal("1")
    fees = sum(
        (Decimal(str(trade.get("fee") or "0")) for trade in store.trades),
        Decimal("0"),
    )
    slippage = sum(
        (
            abs(Decimal(str((trade.get("payload") or {}).get("slippage") or "0")))
            for trade in store.trades
        ),
        Decimal("0"),
    )
    funding = -executor.account_state()["funding_pnl"]
    gross_return = (final_value + fees + slippage + funding) / initial_cash - Decimal("1")
    returns = [
        (current / previous - Decimal("1"))
        for previous, current in zip(equity_curve, equity_curve[1:])
        if previous > 0
    ]
    sharpe, sortino, period_expectancy = _metric_from_returns(returns)
    peak = initial_cash
    max_drawdown = Decimal("0")
    for value in equity_curve:
        peak = max(peak, value)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - value) / peak)
    closing_trades = [
        trade for trade in store.trades
        if (trade.get("payload") or {}).get("action") in {"REDUCE", "CLOSE"}
    ]
    outcomes = [Decimal(str(trade.get("realized_pnl") or "0")) for trade in closing_trades]
    wins = [value for value in outcomes if value > 0]
    losses = [value for value in outcomes if value < 0]
    gross_profit = sum(wins, Decimal("0"))
    gross_loss = abs(sum(losses, Decimal("0")))
    average_win = sum(wins, Decimal("0")) / Decimal(len(wins)) if wins else Decimal("0")
    average_loss = sum(losses, Decimal("0")) / Decimal(len(losses)) if losses else Decimal("0")
    expectancy = (
        sum(outcomes, Decimal("0")) / Decimal(len(outcomes)) / initial_cash
        if outcomes else period_expectancy
    )
    return BacktestResult(
        symbol=symbol.upper(),
        initial_cash=initial_cash,
        final_value=final_value,
        total_return=net_return,
        max_drawdown=max_drawdown,
        total_trades=len(store.trades),
        win_rate=Decimal(len(wins)) / Decimal(len(outcomes)) if outcomes else Decimal("0"),
        loss_rate=Decimal(len(losses)) / Decimal(len(outcomes)) if outcomes else Decimal("0"),
        profit_factor=gross_profit / gross_loss if gross_loss else Decimal("0"),
        expectancy=expectancy,
        average_win=average_win,
        average_loss=average_loss,
        sharpe=sharpe,
        sortino=sortino,
        exposure=max_exposure,
        average_holding_time_minutes=(
            sum(holding_minutes, Decimal("0")) / Decimal(len(holding_minutes))
            if holding_minutes else Decimal("0")
        ),
        fees=fees,
        slippage=slippage,
        funding=funding,
        gross_return=gross_return,
        net_return=net_return,
        mfe=(sum(mfe_values, Decimal("0")) / Decimal(len(mfe_values)) if mfe_values else Decimal("0")),
        mae=(sum(mae_values, Decimal("0")) / Decimal(len(mae_values)) if mae_values else Decimal("0")),
    )


def evaluate_alpha_gate(
    frames: Iterable[MarketFrame],
    *,
    min_samples: int = 30,
    train_fraction: Decimal = Decimal("0.5"),
    validation_fraction: Decimal = Decimal("0.25"),
    horizon_seconds: int = 300,
    minimum_expectancy: Decimal = Decimal("0"),
    maximum_drawdown: Decimal = Decimal("1"),
    minimum_profit_factor: Decimal = Decimal("0"),
    minimum_cost_stress: Decimal = Decimal("0"),
    strategy: StrategyEngine | None = None,
) -> AlphaGateResult:
    """Evaluate a frozen strategy through chronological train/validation/OOS.

    The strategy configuration is hashed before OOS evaluation. OOS only
    replays that frozen configuration; it never selects thresholds or a more
    profitable candidate.
    """
    frozen_strategy = strategy or StrategyEngine()
    config = frozen_strategy.config
    strategy_version = config.positioning_strategy_version
    config_payload = json.dumps(asdict(config), sort_keys=True, default=str)
    config_hash = hashlib.sha256(config_payload.encode("utf-8")).hexdigest()
    parameter_version = config_hash

    def result(
        status: str,
        train: list[MarketFrame],
        validation: list[MarketFrame],
        oos: list[MarketFrame],
        reason: str,
        *,
        oos_metrics: Mapping[str, Any] | None = None,
        train_metrics: Mapping[str, Any] | None = None,
        validation_metrics: Mapping[str, Any] | None = None,
    ) -> AlphaGateResult:
        resolved_oos_samples = (
            int(oos_metrics["independent_episodes"])
            if oos_metrics is not None and "independent_episodes" in oos_metrics
            else len(oos)
        )
        metrics = dict(oos_metrics or {})
        return AlphaGateResult(
            status=status,
            train_samples=len(train),
            validation_samples=len(validation),
            oos_samples=resolved_oos_samples,
            train_metrics=train_metrics or {},
            validation_metrics=validation_metrics or {},
            oos_metrics=metrics,
            strategy_version=strategy_version,
            parameter_version=parameter_version,
            config_hash=config_hash,
            reason=reason,
            code_commit=os.environ.get("GIT_COMMIT", "UNKNOWN"),
            long_episodes=int(metrics.get("long_episodes") or 0),
            short_episodes=int(metrics.get("short_episodes") or 0),
        )

    ordered = list(frames)
    if not ordered:
        return result("INSUFFICIENT_SAMPLE", [], [], [], "no historical frames")
    if not Decimal("0") < train_fraction < Decimal("1"):
        raise ValueError("train_fraction must be between 0 and 1")
    if not Decimal("0") < validation_fraction < Decimal("1"):
        raise ValueError("validation_fraction must be between 0 and 1")
    if train_fraction + validation_fraction >= Decimal("1"):
        raise ValueError("train and validation fractions must leave OOS data")
    train_end = max(1, int(Decimal(len(ordered)) * train_fraction))
    validation_end = max(train_end + 1, int(Decimal(len(ordered)) * (train_fraction + validation_fraction)))
    validation_end = min(len(ordered) - 1, validation_end)
    train = ordered[:train_end]
    validation = ordered[train_end:validation_end]
    out_of_sample = ordered[validation_end:]
    if min(len(train), len(validation), len(out_of_sample)) < 2:
        return result("INSUFFICIENT_SAMPLE", train, validation, out_of_sample,
                      "each chronological split needs observations")

    train_replay = replay_positioning_frames(train, engine=frozen_strategy, horizons_seconds=(horizon_seconds,))
    validation_replay = replay_positioning_frames(validation, engine=frozen_strategy, horizons_seconds=(horizon_seconds,))
    oos_replay = replay_positioning_frames(out_of_sample, engine=frozen_strategy, horizons_seconds=(horizon_seconds,))
    train_forward = train_replay.attribution.get("positioning", {}).get(horizon_seconds)
    validation_forward = validation_replay.attribution.get("positioning", {}).get(horizon_seconds)
    oos_forward = oos_replay.attribution.get("positioning", {}).get(horizon_seconds)
    train_metrics = train_forward.as_dict() if train_forward else {}
    validation_metrics = validation_forward.as_dict() if validation_forward else {}
    observation_samples = oos_forward.samples if oos_forward else 0
    episode_samples = oos_replay.independent_episodes
    long_episodes = 0
    short_episodes = 0
    previous_direction = "FLAT"
    for record in oos_replay.records:
        direction = record.positioning.direction
        if direction != "FLAT" and direction != previous_direction:
            if direction == "LONG":
                long_episodes += 1
            elif direction == "SHORT":
                short_episodes += 1
        previous_direction = direction
    if episode_samples < min_samples:
        return result(
            "INSUFFICIENT_SAMPLE", train, validation, out_of_sample,
            f"independent OOS episodes {episode_samples} below minimum {min_samples}",
            train_metrics=train_metrics,
            validation_metrics=validation_metrics,
            oos_metrics={
                **(oos_forward.as_dict() if oos_forward else {}),
                "observation_samples": observation_samples,
                "independent_episodes": episode_samples,
                "long_episodes": long_episodes,
                "short_episodes": short_episodes,
            },
        )
    try:
        baseline = run_backtest(
            [],
            frames=out_of_sample,
            fee_rate=Decimal("0.001"),
            slippage_bps=Decimal("5"),
            engine=frozen_strategy,
        )
        stressed_frames = [
            replace(frame, funding_rate=(frame.funding_rate * Decimal("1.5"))
                    if frame.funding_rate is not None else None)
            for frame in out_of_sample
        ]
        stressed = run_backtest(
            [],
            frames=stressed_frames,
            fee_rate=Decimal("0.001") * Decimal("1.25"),
            slippage_bps=Decimal("5") * Decimal("1.50"),
            engine=frozen_strategy,
        )
    except (ValueError, RuntimeError):
        return result(
            "ALPHA_NOT_SUPPORTED", train, validation, out_of_sample,
            "cost-adjusted OOS execution could not be evaluated",
            train_metrics=train_metrics,
            validation_metrics=validation_metrics,
            oos_metrics={
                "observation_samples": observation_samples,
                "independent_episodes": episode_samples,
                "long_episodes": long_episodes,
                "short_episodes": short_episodes,
            },
        )
    oos_metrics = {
        **baseline.as_dict(),
        "observation_samples": observation_samples,
        "independent_episodes": episode_samples,
        "cost_stress": {
            "fee_multiplier": "1.25",
            "slippage_multiplier": "1.50",
            "funding_multiplier": "1.50",
            "net_expectancy": str(stressed.expectancy),
            "net_return": str(stressed.net_return),
            "max_drawdown": str(stressed.max_drawdown),
            "profit_factor": str(stressed.profit_factor),
        },
        "long_episodes": long_episodes,
        "short_episodes": short_episodes,
    }
    supported = all(
        (
            baseline.expectancy >= minimum_expectancy,
            baseline.max_drawdown <= maximum_drawdown,
            baseline.profit_factor >= minimum_profit_factor,
            stressed.expectancy >= minimum_cost_stress,
        )
    )
    return result(
        "ALPHA_SUPPORTED" if supported and min(long_episodes, short_episodes) > 0 else "ALPHA_NOT_SUPPORTED",
        train, validation, out_of_sample,
        "frozen OOS metrics and cost stress passed"
        if supported and min(long_episodes, short_episodes) > 0
        else (
            "DIRECTIONAL_SAMPLE_INSUFFICIENT"
            if supported and min(long_episodes, short_episodes) == 0
            else "frozen OOS metrics or cost stress failed"
        ),
        train_metrics=train_metrics,
        validation_metrics=validation_metrics,
        oos_metrics=oos_metrics,
    )
