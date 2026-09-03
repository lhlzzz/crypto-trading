# bian - Binance Trading System

`bian` owns Binance public market data and its isolated trading lifecycle.

- Database: `postgresql://bian:bian@localhost:5446/bian`
- Frontend: Financial OS `/dashboard/bian`
- Modes: `paper`, `testnet`, `live`

Never read Polymarket data, add wallet/withdrawal access, or import code from
another bot workspace. `bian_api.py` is read-only. `execution.py` is the only
order-submission owner. `database.py` remains schema/connection owner only.
`engine.py` only creates TradeIntent objects. `SpotPublicClient`/`PublicClient`
and `FuturesPublicClient` are read-only. `FuturesPrivateClient` is the only authenticated USD-M order/account adapter. Spot `PrivateClient` is removed.
Paper cannot construct `FuturesPrivateClient`. Paper, Testnet, and Live share
the same Executor interface, and Live requires configuration plus explicit
startup confirmation. Testnet and Live credentials are separate mode-specific
environment secrets; validation-only signal injection is Paper-only and
disabled by default.

Capital Positioning is observation and shadow-only until its explicit runtime
acceptance gates pass. `POSITIONING_DECISION_ENABLED` defaults to `false`;
stale, sequence-gapped, incomplete, or timestamp-inconsistent market data
must produce `UNKNOWN`/`FLAT`, never a synthetic trade signal. Futures market
data remains public observation until later paper/testnet/live phases. Spot is
confirmation only; production execution market is Binance USD-M Futures.
`scripts/bian_market.py observe` is the single public positioning-observation
owner, and `paper_runner.py --shadow-forever` records comparisons without
calling Risk, Execution, TradeIntent creation, or order submission.

`runtime_gate.py` is the canonical readiness owner. Its `live_allowed` result
must remain false until the externally verified observation, shadow, Testnet,
reconciliation, data-health, and human-confirmation gates are complete.

Current validation status on 2026-09-03, commit `e5f673fe743a76eaff6ed10028388bc866751fa8`:
code checks are `402` pytest tests passed, `compileall` PASS, `git diff --check`
PASS. PostgreSQL schema/status remains healthy. `LIVE_ALLOWED=false`.
`TESTNET=BLOCKED_BY_EXTERNAL_CREDENTIALS`. UserStream failure blocks OPEN;
REDUCE/CLOSE remains available under current risk/reconciliation rules.
Gate evidence is current-session scoped. `exchange_trade_id` is persisted.
Positions are isolated by `(mode, market, symbol)`. Validation episodes are
session-scoped. Startup recomputes the canonical gate after reconciliation.
Canonical symbols come from `trading_symbols_for_mode()`. Freeze-path and
runtime-safety files changed, so prior realtime evidence remains `EXPIRED`
and must not be reused: `REALTIME_30M=EXPIRED`, `REALTIME_2H=EXPIRED`,
`REALTIME_6H=NOT_STARTED`, `REALTIME_24H=NOT_STARTED`. `PAPER_24H=NOT_STARTED`,
`SHADOW_7D=NOT_STARTED`, `ALPHA=INSUFFICIENT_SAMPLE` until a later persisted
OOS session exists, `LIVE_PREFLIGHT` is live-mode only and still blocked,
`LIVE=BLOCKED`. Do not treat historical 30M/2H PASS as current.
