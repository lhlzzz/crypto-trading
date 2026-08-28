# State

The `bian` bot owns Binance public market snapshots and the trading schema in
the `bian` PostgreSQL database on port `5446`. Its read-only FastAPI contract
runs on port `8001` and is consumed by Financial OS; `bian` remains the sole
database owner.

Validated phases now include the database schema, official REST adapter,
single `engine.py`, TradeIntent, risk gate, shared Paper/Binance Executor,
reconciliation, isolated User Data Stream, VectorBT research boundary, and
read-only trading summary API. The latest validation is 118 tests passed.

Paper supports fee, slippage, latency, partial fills, resting LIMIT orders,
expiry, balances, positions, mark-to-market PnL, and equity accounting. A
Paper injection run on August 25, 2026 produced one simulated filled order;
this is not a 24-hour stability result. Testnet/live startup uses the same
runner path, but private exchange credentials and external network behavior
still require environment-backed integration validation.

Adapter safety boundaries:

- `BIAN_MODE=paper` hard-blocks `PrivateClient`.
- `BIAN_MODE=testnet` uses Binance Testnet only.
- `BIAN_MODE=live` requires `LIVE_TRADING_ENABLED=true` before private access.
- Live also requires `LIVE_CONFIRMATION_TOKEN` and a matching explicit startup
  confirmation through `start_live.sh`.
- API credentials are read only from environment variables.
- Testnet and Live use separate mode-specific credential names and endpoints.
- `TEST_ONLY_SIGNAL_INJECTION` is disabled by default and hard-blocked outside
  non-production Paper validation.
- SDK exceptions are translated into BIAN adapter exceptions.
- User Data Stream events are observations; REST reconciliation remains the
  final account/order authority.
- `bian_api.py` exposes only GET trading status, orders, trades, positions,
  balances, risk, summary, and system events.

Capital Positioning implementation snapshot on August 26, 2026:

- `engine.py` remains the single strategy owner. It now exposes deterministic
  `MarketDataEnvelope`, `MarketFrame`, positioning evidence, state/transition,
  scores, fail-closed freshness checks, and `evaluate_shadow()`.
- `POSITIONING_DECISION_ENABLED` defaults to `false`; legacy SMA remains the
  only paper/testnet/live execution input. Futures are observation-only.
- Spot trade-flow/CVD uses persisted events only when both exchange time and
  local receive time precede the decision. Candidate Spot streams include
  bookTicker plus snapshot/diff L2 observation; a sequence gap discards the
  local book until a fresh snapshot arrives.
- Futures public observation covers mark/index, OI, funding, taker and public
  long/short statistics, plus observation-only public force orders. It has no
  Futures execution capability.
- Persisted-event aggregation produces 1m, 3m, 5m, 15m, and 1h CVD; CVD
  acceleration/divergence; rolling volume ratios/z-score; timestamp-bounded
  OI changes; observed directional price impact; and native 5m, 15m, and 1h
  Futures taker and public long/short observations. Unsupported periods are
  left absent rather than synthesized.
- `paper_runner.py` builds positioning `MarketFrame` data from timestamp-bound
  persisted aggregates, then stores normalized feature provenance and the
  legacy-versus-positioning shadow comparison. Missing required flow, OI,
  funding, taker, or liquidity inputs produce `UNKNOWN`/`FLAT`.
- PostgreSQL remains the single schema owner and now includes flow,
  positioning, evidence, and liquidation facts. `bian_api.py` exposes only
  read-only positioning endpoints.
- Real public collection completed on August 26, 2026; the bian database was
  schema-complete and fresh at validation time. One Futures observation run
  wrote six public observation events through the configured proxy egress.
- Understand-Anything graph was refreshed on August 26, 2026 at 20:48 UTC:
  42 source files, 358 nodes, 528 edges, 5 layers, 6 tour steps; graph
  validation passed.

Production validation snapshot on August 26, 2026:

- 118 tests passed after multi-window flow, price-impact, Futures-period, and
  continuous-observer coverage was added. A fresh public Spot/Futures smoke
  completed at 20:22 UTC
  and produced `TRADE`, `BOOK_TICKER`, `ORDERBOOK`, `MARK_INDEX_FUNDING`,
  `OPEN_INTEREST`, and `TAKER_FLOW` observations with explicit latency.
  Full runtime-duration gates still must be completed before a readiness claim.
- `scripts/bian_market.py observe` is the single public observation owner for
  Tier 1 scanner refresh plus Tier 2 candidate streams. It defaults to five
  scanner candidates, keeps BTC/ETH benchmarks, and starts no private client
  or trading path. Its smoke used one candidate plus the benchmarks.
- `paper_runner.py --shadow-only` recorded a real legacy-versus-positioning
  comparison at 20:30 UTC. It returned `UNKNOWN`/`FLAT` because the bounded
  observer had ended and a required Futures source was stale; this confirms
  the system fails closed rather than manufacturing a Shadow direction.
- Raw-event storage measurement now reports actual observation spans and
  dailyized estimates by event type. The current short sample annualizes
  `TRADE` to roughly 72k events/day and roughly 54 MB/day; it is evidence to
  continue measuring, not authority to set a fixed retention period yet.
- The Universe Scanner now persists timestamped 24-hour breadth, candidate
  selection, and a deterministic regime label. `TradingStore` derives 1m,
  5m, 15m, and 1h relative strength versus BTC, ETH, and the stored market
  basket without raw ticks entering `MarketFrame`.
- Normalized evidence snapshots can reconstruct deterministic `MarketFrame`
  replay. Research attribution compares Positioning, legacy SMA, and simple
  momentum only after decisions are materialized. Storage measurement is
  available before any explicit raw-event retention deletion.
- The standalone historical `FUNDING` event was stale by its settlement time,
  while the current mark/index funding observation remained fresh for the
  positioning source contract. This is surfaced as diagnostics and does not
  relax the positioning freshness gate.
- Paper injection smoke order filled through Risk and Paper Execution; the
  PostgreSQL equity identity matched cash plus marked position value.
- API health and read-only trading endpoints returned 200; POST order returned
  405. Live guard cases remained blocked.
- A 24-hour Paper run, Testnet credentials/order lifecycle, WebSocket
  reconnect, and Live account validation were not executed and remain
  blockers for LIVE readiness.
- The continuous public observer and pure Shadow loop were restarted after a
  regression fix for sparse historical liquidation freshness. Fresh Spot and
  Futures observations continued to persist; Shadow recorded conflict-safe
  `FLAT` decisions, and the post-restart audit found zero TradeIntents and
  zero orders. The 1-hour, 6-hour, 24-hour, and seven-day elapsed gates remain
  incomplete.
- The direct `python scripts/bian_market.py observe` process now resolves the
  root-level `engine.py` decision contract before stream persistence. On
  August 26, 2026, the observer recovered from this import-path failure and
  completed consecutive `stream_trade`, `rest_24h`, and
  `futures_observation` batches with explicit event timestamps and latency.
  Stream persistence failures now retain an exception-class failure code and
  log the traceback without persisting exception text or secrets.
- Runtime API and replay validation on August 26, 2026 confirmed every
  `/api/positioning/*` GET endpoint returns `200`, while POST to the
  positioning status endpoint returns `405`. A direct Shadow cycle produced
  `CONFLICTED`/`FLAT` and zero recent order events. Current replay attribution
  has 54 frames but only four directional Positioning samples, so its apparent
  results are diagnostic only and do not meet the statistical acceptance gate.
- Local order-book recovery now explicitly obtains a Binance REST depth
  snapshot on initialization or a `U/u` gap, synchronizes buffered diffs, and
  persists the REST snapshot origin and update ID on each later diff event.
  Runtime observation on August 26, 2026 confirmed that provenance on live
  BTCUSDT order-book events. The public Futures adapter now uses the existing
  bounded retry/backoff configuration; a transient Futures timeout remains a
  classified collection failure and cannot create a positioning decision.
- The Tier 1 scanner now records the ticker receive timestamp only after the
  public 24-hour response returns. This preserves the causality check against
  Binance `closeTime` without treating ordinary response latency as future
  data. After the restart on August 26, 2026, consecutive `rest_24h`,
  `futures_observation`, and `stream_trade` batches completed successfully.
