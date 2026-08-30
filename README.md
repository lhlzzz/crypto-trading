# bian

Independent Binance market-data and trading-system foundation.

Repository name: `crypto trading`

Public market endpoints remain owned by `scripts/bian_market.py`. PostgreSQL
is the only bian data authority, `bian_api.py` is read-only, and Financial OS
owns the browser UI. The canonical trading path is:
public Futures evidence -> `engine.py` -> TradeIntent -> `risk.py` ->
`execution.py` -> Paper/BinanceExecutor -> FuturesPrivateClient, with REST
reconciliation and audit events around it. `runtime_gate.py` is the single
readiness result and never creates intents or submits orders.

```bash
docker-compose -f docker-compose.db.yml up -d
uv venv --python 3.12
uv pip install --python .venv/bin/python -r requirements.txt
.venv/bin/python scripts/database.py ensure
.venv/bin/python scripts/bian_market.py collect
.venv/bin/python scripts/database.py status
bash start_api.sh
```

The Binance mode is configuration-only:

- `BIAN_MODE=paper`: no private Binance client and no real orders.
- `BIAN_MODE=testnet`: `FuturesPrivateClient` targets USD-M Futures Testnet.
- `BIAN_MODE=live`: USD-M Futures production also requires
  `LIVE_TRADING_ENABLED=true`, `LIVE_CONFIRMATION_TOKEN`, and the explicit
  confirmation performed by `start_live.sh`.

Testnet credentials use `BIAN_TESTNET_API_KEY` and
`BIAN_TESTNET_API_SECRET`; Live credentials use the separate
`BIAN_LIVE_API_KEY` and `BIAN_LIVE_API_SECRET`. Secrets are only read from
secret configuration, never stored in the database or returned by the API.

Production execution market is Binance USD-M USDT-margined perpetual. Spot is
public confirmation only; `PrivateClient` cannot place Spot orders.

The read-only API exposes `/health` and
`/api/os/front-data`. Financial OS proxies that contract through
`BIAN_API_BASE_URL` and displays it at
`http://localhost:3000/dashboard/bian`.

`python3.12 scripts/bian_market.py stream` collects public Futures trades,
bookTicker, depth, mark/index/funding, and observed force-order events. It has
no authenticated feed configuration and does not support orders, wallets, or
private APIs. Spot remains auxiliary confirmation only.

Capital Positioning remains shadow-only. The collector derives 24-hour
cross-sectional breadth, a bounded Tier 1 candidate universe, and a
deterministic regime label; candidate trade/depth streams remain the only
Tier 2 observation path. `paper_runner.py --attribution` replays persisted
normalized evidence and compares positioning against legacy SMA and simple
momentum without creating intents or orders. Measure raw event volume before
selecting a retention period with `scripts/bian_market.py measure-storage`;
`prune-raw-events` requires an explicit retention age.

Use `scripts/bian_market.py observe` for continuous public-only observation.
It refreshes the Tier 1 scanner and Futures REST context, then maintains Spot
trade/book/depth plus Futures force-order streams only for the selected
candidates. `POSITIONING_OBSERVER_CANDIDATE_LIMIT` defaults to five; it is
separate from the broader scanner display limit to bound WebSocket, CPU, and
API load. The command does not construct TradeIntent objects or invoke Risk,
Execution, Testnet, or Live trading.

Run `paper_runner.py --shadow-forever --symbols BTCUSDT` alongside the public
observer to persist legacy-versus-positioning decisions. This pure Shadow loop
only writes positioning/evidence snapshots and decision-trace events; it does
not create signals, TradeIntent objects, risk decisions, or orders.

Run the validated paper path with `bash start_paper_trading.sh`. Testnet uses
`bash start_testnet.sh`; live uses `BIAN_MODE=live` and
`bash start_live.sh`. The live guard exits before creating a private client
unless all required conditions pass.

Backtesting is research-only. With normalized historical `MarketFrame` input,
`backtesting.py` runs the same positioning, TradeIntent, RiskGate, and
PaperExecutor contract in an ephemeral ledger; close-only SMA input remains a
baseline and fails closed when Futures evidence is absent.

Live remains blocked until the persisted observation, shadow, Testnet,
reconciliation, data-health, risk, Meme, and explicit operator gates pass.
Testnet requires separate credentials and a real account preflight; without
them its lifecycle is `TESTNET_BLOCKED_BY_EXTERNAL_CREDENTIALS`.

The API contract and release identity default to `2026-08-15` and can be
overridden independently with `BIAN_OPERATOR_CONTRACT_VERSION` and
`BIAN_RELEASE_VERSION`.
