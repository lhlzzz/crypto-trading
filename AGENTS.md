# bian - Binance Trading System

`bian` owns Binance public market data and its isolated trading lifecycle.

- Database: `postgresql://bian:bian@localhost:5446/bian`
- Frontend: Financial OS `/dashboard/bian`
- Modes: `paper`, `testnet`, `live`

Never read Polymarket data, add wallet/withdrawal access, or import code from
another bot workspace. `bian_api.py` is read-only. `execution.py` is the only
order-submission owner. `database.py` remains schema/connection owner only.
`engine.py` only creates TradeIntent objects. `FuturesPublicClient` is
read-only. `FuturesPrivateClient` is the only authenticated USD-M
order/account adapter. Spot public and private clients are removed from the
runtime path. Paper cannot construct `FuturesPrivateClient`. Paper, Testnet,
and Live share the same Executor interface, and Live requires configuration
plus explicit startup confirmation. Testnet and Live credentials are separate
mode-specific environment secrets; validation-only signal injection is
Paper-only and disabled by default.

Capital Positioning is observation and shadow-only until its explicit runtime
acceptance gates pass. `POSITIONING_DECISION_ENABLED` defaults to `false`;
stale, sequence-gapped, incomplete, or timestamp-inconsistent market data
must produce `UNKNOWN`/`FLAT`, never a synthetic trade signal. Futures market
data remains public observation until later paper/testnet/live phases. Spot
historical data may exist in PostgreSQL for research; production execution
market is Binance USD-M Futures.
`scripts/bian_market.py observe` is the single public positioning-observation
owner, and `paper_runner.py --shadow-forever` records comparisons without
calling Risk, Execution, TradeIntent creation, or order submission.

`runtime_gate.py` is the canonical readiness owner. Its `live_allowed` result
must remain false until the externally verified observation, shadow, Testnet,
reconciliation, data-health, and human-confirmation gates are complete.
The gate is re-evaluated every trading cycle; a startup snapshot cannot be
reused forever. HALT, DEGRADED, RECONCILIATION_FAILED, USERSTREAM_FAILED,
DATA_STALE, and RISK_FAILED block new OPEN. Stale, UNKNOWN, UNSAFE, or
MISSING evidence cannot produce a strategy CLOSE; emergency flatten remains
a separate semantic.

Production execution market is Binance USD-M Futures. First-phase universe is
only `BTCUSDT`, `ETHUSDT`, and `BNBUSDT` via `trading_symbols_for_mode()`.
Any other live symbol is REJECT/HALT. Mixed env such as `BTCUSDT,DOGEUSDT`
fails the gate instead of silently running BTC only. Universe expansion is a
new release, not a config change. Spot is historical/research only and has no
private trading. Web3/DEX/Spot private trading is out of this workspace.
Store trading-state calls require explicit `mode=`; implicit `BIAN_MODE`
fallback is banned. Live CREATE, CANCEL, and CANCEL_ALL all require
`authorize_live_order_mutation`.

Current validation status on 2026-09-04 after major-futures hardening:
`456` pytest tests passed, `compileall` PASS, `git diff --check` PASS.
Do not reuse the prior 449-test count.
Prior 409-test evidence and commit
`d1fd55dde97cab5cf7f0ce22475ca46205119b63` are expired and must not be reused.
`LIVE_ALLOWED=false`. `TESTNET=BLOCKED_BY_EXTERNAL_CREDENTIALS`.
UserStream failure blocks OPEN. Gate evidence is current-session scoped.
`exchange_trade_id` is persisted. Positions are isolated by
`(mode, market, symbol)`. Unknown Binance exchange trades HALT. FILLED local
orders remain visible to trade recovery. Reconciler queries use explicit
`mode=self.mode`. Freeze-path and runtime-safety files changed, so prior
realtime evidence remains `EXPIRED`: `REALTIME_30M=EXPIRED`,
`REALTIME_2H=EXPIRED`, `REALTIME_6H=NOT_STARTED`, `REALTIME_24H=NOT_STARTED`.
`PAPER_24H=NOT_STARTED`, `SHADOW_7D=NOT_STARTED`,
`ALPHA=INSUFFICIENT_SAMPLE` until a later persisted OOS session exists,
`LIVE_PREFLIGHT` is live-mode only and still blocked, `LIVE=BLOCKED`.
Do not treat historical 30M/2H PASS as current.
