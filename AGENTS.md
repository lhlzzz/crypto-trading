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
