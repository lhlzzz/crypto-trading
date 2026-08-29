# State

The `bian` bot owns Binance public market snapshots and the trading schema in
the `bian` PostgreSQL database on port `5446`. Its read-only FastAPI contract
runs on port `8001` and is consumed by Financial OS; `bian` remains the sole
database owner.

Product target after the 2026-08-28 V4 addendum: Binance USD-M Meme Futures
capital positioning. Legacy SMA remains the only executable strategy until
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

## Phase A audit (2026-08-28)

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

Gaps versus the Meme Futures production pack:

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
8. `PositioningState` still includes `TRANSITION` as a state literal. The
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
| `available_base_quantity`, SELL-as-close, base-balance short | DELETED |
| Paper cash inventory / quote_quantity market buys | REPLACED with futures margin paper |
| User stream `executionReport` Spot WS | REPLACED with ACCOUNT_UPDATE / ORDER_TRADE_UPDATE |
| SMA as production path | REPLACE with positioning; SMA = research baseline |
| `transition_strength = abs(edge)*quality` | REPLACED; directional vs transition split |
| `PositioningState` includes `TRANSITION` | DELETE as a state later |
| Meme allowlist / TRADEABLE tiers | CREATE later |
| Futures book `pu` | CREATE later (old T36) |

## Adapter refactor (2026-08-29)

T49/T50 landed before old T36 orderbook. Signed USD-M REST uses stdlib HMAC.
Paper cannot construct `FuturesPrivateClient`. Testnet/Live `BinanceExecutor`
no longer sends Spot `quoteOrderQty`. TradeIntent is OPEN/REDUCE/CLOSE.
Live remains HARD BLOCKED until elapsed observation, 7-day shadow, Testnet
lifecycle, data health, and human confirmation all pass.

## Futures contract cutover (2026-08-30)

TradeIntent, Risk, Paper, User Stream, Reconciliation, and TradingStore now
use futures position semantics. Spot remains public confirmation only.
`quote_quantity` is migrated out of the active schema. SMA stays a research
baseline until positioning gates pass.
