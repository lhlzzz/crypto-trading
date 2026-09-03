# State

The `bian` bot owns Binance public market snapshots and the trading schema in
the `bian` PostgreSQL database on port `5446`. Its read-only FastAPI contract
runs on port `8001` and is consumed by Financial OS; `bian` remains the sole
database owner.

Current target: Binance USD-M Meme Futures capital positioning. Legacy SMA is
research baseline only; Capital Positioning remains disabled by default until
shadow, paper, testnet, and recovery gates pass.
`POSITIONING_DECISION_ENABLED` defaults to `false`.

## Owners

- `engine.py`: single strategy owner. Creates TradeIntent only.
- `risk.py`: single risk owner.
- `execution.py`: only order-submission owner. Paper and Binance share
  `Executor`.
- `binance_client.py`: `PublicClient` (Spot public), `FuturesPublicClient`
  (USD-M public, no order methods), `FuturesPrivateClient` (USD-M signed REST).
  Alias: `SpotPublicClient`. Spot private client is removed.
- `scripts/bian_market.py`: single public observation owner.
- `scripts/database.py`: schema/connection owner.
- `trading_store.py`: trading-fact persistence.
- `bian_api.py`: GET-only.
- `paper_runner.py`: paper loop and shadow comparison. Shadow does not call
  Risk, Execution, or order submission.

No `engine_v2.py`, `futures_engine.py`, `positioning_engine.py`, or second
executor family exists.

## Historical Phase A audit (2026-08-28)

Validated from `d75a795` with a clean tree. 118 tests passed, `compileall`
succeeded, and PostgreSQL reported `missing_tables=[]`. Collection is stale:
last successful `stream_trade` was 2026-08-27T17:04:38Z, age about 20 hours.

Already present and kept:

- Canonical positioning states including `SHORT_COVERING`, `LONG_BUILDING`,
  `LONG_UNWIND`, `SHORT_BUILDING`, absorption, exhaustion, and
  `FORCED_DELEVERAGING`.
- `test_short_covering_cannot_open_a_new_long_intent` and symmetric short
  tests. Intent creation requires `LONG_BUILDING` or `SHORT_BUILDING`.
- Timestamped envelopes, fail-closed freshness, evidence snapshots, and
  deterministic replay.
- Futures public REST: mark/index/funding, OI, native `5m`/`15m`/`1h` taker
  and long/short ratios, public force-order stream. Phase B added native
  `30m` periods, distinct last/mark/index, OI 30m change, and funding
  percentile/z-score from persisted samples.
- Spot trade flow/CVD, Spot bookTicker, Spot snapshot+diff L2 with gap
  resync.
- Shadow comparison and read-only `/api/positioning/*` endpoints.

The following was the historical gap list at that audit point. It is retained
for audit history, not as the current implementation state:

1. Adapter namespaces are `SpotPublicClient`, `FuturesPublicClient`, and
   `FuturesPrivateClient`. TradeIntent is futures-native: direction, action,
   reduce_only, leverage, isolated, one-way, quantity-only.
2. Paper accounting is isolated futures margin: mark-price PnL, funding,
   fees, slippage, partial fill, and mark-price liquidation HALT.
3. Risk validates margin, leverage, liquidation buffer, reverse-position,
   reduce-only, and meme `BLOCK`/`OBSERVE`/`REDUCED`/`TRADEABLE`.
4. Universe ranking is Spot 24h quote-volume/price-change. There is no
   `MEME_ALLOWLIST` / `MEME_BLOCKLIST` or `TRADEABLE`/`REDUCED`/`OBSERVE`/
   `BLOCK` quality tier.
5. Positioning liquidity evidence is Spot depth (`_spot_depth_snapshot`,
   Cryptofeed Spot diff). Futures book (`pu`) is not the production book.
6. `MarketFrame` still lacks `MEME_RISK_ON` / `MEME_RISK_OFF`. Last/mark/index
   and 30m CVD/OI/taker windows are now present. `1m`/`3m` taker ratios stay
   absent because Binance does not publish them.
7. `directional_strength` is current-view magnitude. `transition_strength` is
   previous-to-current state change and is 0 when state does not change.
8. The historical implementation had a dedicated transition state. The
   production contract treats transition as a field, not a state.
9. `.env.example` has `BIAN_MARKET=FUTURES`, one-way/isolated mode,
   `MAX_LEVERAGE`, `MAX_MARGIN_USDT`, and `MIN_LIQUIDATION_BUFFER_PERCENT`.
10. API has positioning candidates, not `/api/meme/universe` or
    `/api/meme/candidates`.

Safety boundaries that remain in force:

- `BIAN_MODE=paper` hard-blocks `FuturesPrivateClient`.
- Testnet private REST is `https://testnet.binancefuture.com`; Live is
  `https://fapi.binance.com`. Credentials stay mode-specific.
- Live requires `LIVE_TRADING_ENABLED=true`, `LIVE_CONFIRMATION_TOKEN`, and
  `start_live.sh` confirmation.
- `TEST_ONLY_SIGNAL_INJECTION` is disabled by default and Paper-only.
- User Data Stream is USD-M `ACCOUNT_UPDATE` / `ORDER_TRADE_UPDATE`.
  REST reconciliation is account authority and HALTs on mismatch.
- Positioning remains shadow-only unless `POSITIONING_DECISION_ENABLED=true`.
  Paper `create_order` is local paper only, never `FuturesPrivateClient`.

Elapsed gates still open: 1 hour, 6 hours, 24 hours of continuous public
observation, seven-day shadow with directional samples, Testnet USD-M
lifecycle, and Live. T33 is not skipped; it is required again by Phase I.

## Phase B (2026-08-28)

120 tests passed. `FuturesPublicClient.get_ticker_price` and native `30m`
taker/long-short collection are live. A public smoke on BTCUSDT returned
distinct last/mark/index and a native 30m taker row. No Futures private
order methods were added in Phase B. Schema needed no new tables; `LAST_PRICE` uses
`market_flow_events`. Collection remains stale until the observer restarts.
Codebase-memory graph after Phase B: 1319 nodes, 3317 edges.

## Legacy Spot assumption audit (2026-08-29)

| Item | Verdict |
|---|---|
| Spot SDK public klines/ticker/depth/exchangeInfo | KEEP as `SpotPublicClient` |
| `FuturesPublicClient` public REST + 30m periods | KEEP |
| Spot `PrivateClient` + `binance-sdk-spot` `new_order` | DELETE |
| `https://testnet.binance.vision` private REST | REPLACE with USD-M Futures testnet |
| Legacy base-inventory close/short semantics | DELETED |
| Paper cash inventory market buys | REPLACED with futures margin paper |
| User stream `executionReport` Spot WS | REPLACED with ACCOUNT_UPDATE / ORDER_TRADE_UPDATE |
| SMA as production path | REPLACE with positioning; SMA = research baseline |
| `transition_strength = abs(edge)*quality` | REPLACED; directional vs transition split |
| Dedicated transition state | DELETE as a state later |
| Meme allowlist / TRADEABLE tiers | CREATE later |
| Futures book `pu` | CREATE later (old T36) |

## Adapter refactor (2026-08-29)

T49/T50 landed before old T36 orderbook. Signed USD-M REST uses stdlib HMAC.
Paper cannot construct `FuturesPrivateClient`. Testnet/Live `BinanceExecutor`
no longer sends Spot quote-order quantity fields. TradeIntent is
OPEN/REDUCE/CLOSE.
Live remains HARD BLOCKED until elapsed observation, 7-day shadow, Testnet
lifecycle, data health, and human confirmation all pass.

## Futures contract cutover (2026-08-30)

TradeIntent, Risk, Paper, User Stream, Reconciliation, and TradingStore now
use futures position semantics. Spot remains public confirmation only.
Legacy quote-order quantity is migrated out of the active schema. SMA stays a
research baseline until positioning gates pass.

## Verification (2026-08-31)

- `260` tests pass; `compileall` and `git diff --check` pass.
- Futures order writes are single-attempt; transport uncertainty records
  `UNKNOWN` and reconciliation queries the exact client order ID.
- listenKey create/keepalive/close use API-key-only transport.
- Futures public streams provide trade, bookTicker, depth, mark/index/funding,
  and observed force-order events with provenance; Spot is auxiliary only.
- `runtime_gate.py` is the canonical readiness result and `live_allowed` is
  hard false in this release.
- Backtest normalized frames now use PositioningDecision, TradeIntent,
  RiskGate, and PaperExecutor semantics in an ephemeral research ledger.
- Paper wallet, margin, realized PnL, funding, fees, slippage, and liquidation
  are accounted by `PaperExecutor`; liquidation flattens and halts.
- Testnet is not runtime-verified because credentials are unavailable;
  status is `TESTNET_BLOCKED_BY_EXTERNAL_CREDENTIALS`.
- AlphaGate now reports frozen strategy version/config hash, train,
  validation, OOS, independent episode, baseline, and stressed-cost metrics.
- Paper funding settles once per persisted settlement timestamp and records a
  `FUNDING_SETTLED` accounting event.
- Runtime data health requires every configured Futures source, timestamp
  integrity, freshness, and latency limits; readiness responses include the
  verification timestamp and age.
- Balances are isolated by composite `(mode, asset)` identity and UserStream
  `ACCOUNT_UPDATE` observations are explicitly marked `PARTIAL`.
- Public Futures observation now uses the shared `BIAN_HTTP_PROXY` opener with
  bounded retry jitter and sanitized operation/host diagnostics. The
  `FuturesPublicClient` public path uses that same opener.
- Orderbook `GAP` and `UNSAFE` evidence is preserved instead of being replaced
  by a later fresh observation; `UNSAFE` is excluded from source health.
- Meme caps are separate: symbol, portfolio, and directional exposure. The
  old combined `MAX_MEME_NOTIONAL_USDT` alias is removed.
- Direct collector execution is covered, including root imports and Decimal
  JSON payload serialization.
- Positioning decisions now carry episode identity, lifecycle state, start/end
  timestamps, and episode transitions. Evidence persistence stores the full
  normalized input set, sufficiency/data quality, and Meme classification.
- Runtime gate and exchange reconciliation consume the canonical
  `FuturesAccountSnapshot`; User Stream records proxy mode, latency, disconnect,
  and reconnect health and passes the same unavailable state to reconciliation.

## Runtime Gate Status (2026-08-31)

- `CODE_READY=true`.
- `REAL_DATA_READY=false`: Binance public REST/WebSocket transport is
  externally unstable (`SSL: UNEXPECTED_EOF_WHILE_READING`, connection reset).
- `PAPER_READY=false`, `SHADOW_READY=false`, `TESTNET_READY=false`,
  `ALPHA_SUPPORTED` is not established, and `LIVE_PREFLIGHT_READY=false`.
- Testnet is `BLOCKED_BY_EXTERNAL_CREDENTIALS`; no credentials were present.
- `LIVE=BLOCKED` and `POSITIONING_DECISION_ENABLED=false` remain unchanged.

## Runtime Validation (2026-09-02)

- 30M realtime `PASSED` at that date. That evidence is no longer current.
- Production mark/index/funding liveness is `MARK_INDEX_FUNDING`. Dedicated
  markPrice is `NOT_REQUIRED` and is not a second subscribed source.
- Validation is a persisted session in `runtime_validation_report.json` with
  stage states `NOT_STARTED`/`RUNNING`/`PASSED`/`FAILED`/`BLOCKED`/`EXPIRED`.

## Current verification (2026-09-03)

Final commit: `d1fd55dde97cab5cf7f0ce22475ca46205119b63`.
Base: `c35379a38f8d6defd613ac66e4618f7553cc8867`.

This round closed remaining exchange-trade reconciliation gaps. `409` pytest
tests passed. `compileall` PASS. `git diff --check` PASS.
`LIVE_ALLOWED=false`. `TESTNET=BLOCKED_BY_EXTERNAL_CREDENTIALS`.

P0/P1 results:

- UserStream failure blocks OPEN; REDUCE/CLOSE remains available.
- Testnet lifecycle evidence is session/order/symbol/exchange-id bound.
- `trades.exchange_trade_id` is a first-class Binance trade id.
- Runtime gate evidence is current-session scoped; no session => NOT_STARTED.
- DB/report reconciliation is fail-closed.
- Startup order is initialize -> preflight -> reconcile -> recompute gate.
- Positions are isolated by `(mode, market, symbol)` with legacy backfill.
- Validation episodes are session-isolated.
- Live preflight constructs TradingStore + FuturesPrivateClient; missing
  credentials are BLOCKED, not PASS.
- Canonical symbols: `trading_symbols_for_mode(mode)`.
- Unknown Binance exchange trades HALT; they are never ignored.
- Trade recovery includes FILLED canonical orders, not only pending rows.
- Reconciler queries pass explicit `mode=self.mode` and do not follow `BIAN_MODE`.

Realtime/paper/shadow evidence is not current:

- `REALTIME_30M=EXPIRED`
- `REALTIME_2H=EXPIRED`
- `REALTIME_6H=NOT_STARTED`
- `REALTIME_24H=NOT_STARTED`
- `PAPER_24H=NOT_STARTED`
- `SHADOW_7D=NOT_STARTED`
- `ALPHA=INSUFFICIENT_SAMPLE`
- `TESTNET=BLOCKED_BY_EXTERNAL_CREDENTIALS`
- `LIVE_PREFLIGHT` evaluates `mode=live` and remains blocked
- `LIVE=BLOCKED`

Do not reuse historical 30M/2H PASS. `live_allowed` stays false until
observation, shadow, Testnet, reconciliation, data-health, and
human-confirmation gates complete.
