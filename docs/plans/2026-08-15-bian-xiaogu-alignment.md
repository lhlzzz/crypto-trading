# BIAN Trading System Upgrade

**Goal:** Upgrade bian from a public Binance market-data service into one
auditable trading system with a single strategy core and configuration-only
paper, Testnet, and guarded live modes.
**Constraints:** `bian_market.py` remains the public market-data owner;
`bian_api.py` remains read-only; `database.py` remains schema/connection owner;
`engine.py` only produces `TradeIntent`; `execution.py` is the only order
submission owner; public and private Binance clients remain separate.
**Out of scope:** Wallet/withdrawal access, Polymarket data, copied trading
frameworks, and parallel strategy or execution implementations.

## Must-Haves

- MH1: The bian frontend receives a stable read-only API contract. A:I1
- MH2: Database schema, reachability, and provenance are observable. A:I1
- MH3: Runtime configuration and version identity are explicit and isolated.
  A:I1
- MH4: Tests and local runtime checks prove the affected paths. A:I2

## V2.0 Execution Addendum

The original T1-T9 records preserve the completed public-data and Financial OS
baseline. The remaining work is tracked below in dependency order and must not
advance past a failed verification gate.

- Phase 1: database and Binance adapter contract audit
- Phase 2: TradeIntent and risk contract audit
- Phase 3: paper execution lifecycle and accounting parity
- Phase 4: engine-to-execution orchestration boundary
- Phase 5: reconciliation and restart recovery
- Phase 6: isolated User Data Stream
- Phase 7: backtesting/performance research boundary
- Phase 8: read-only Financial OS trading views
- Phase 9: Testnet runner and lifecycle checks
- Phase 10: live guard and emergency-stop controls
- Phase 11: production readiness review; live remains operator-gated

### Task 1: Map current contracts and establish the bian runtime baseline A:I1
- [ ] Inspect bian collector, database scripts, tests, and Financial OS bian
  adapter/page; record the existing ownership and gaps in the implementation.
- [ ] Add only the minimum bian runtime metadata/configuration files needed by
  the existing repository conventions.
- [ ] Verification: run the existing bian tests and compile checks before
  changing behavior; capture the baseline result.

### Task 2: Add the bian read API and database status boundary A:I1
- [ ] Implement a FastAPI read-only service around the existing collector and
  PostgreSQL tables, with health and overview/front-data responses.
- [ ] Reuse the existing schema owner and DSN resolution; do not add a second
  persistence layer or duplicate collector.
- [ ] Expose collection timestamp, source URL, market snapshots, coverage,
  database status, and contract/version metadata.
- [ ] Verification: add focused API tests for healthy data, unavailable
  database, and empty-result behavior; run them with the bian test suite.

### Task 3: Align Financial OS bian integration A:I1
- [ ] Update the existing bian dashboard adapter/route to consume the stable
  bian read contract through the existing Financial OS integration pattern.
- [ ] Preserve the existing bian route and show explicit unavailable/error
  states without querying bian PostgreSQL from browser code.
- [ ] Verification: run the affected frontend typecheck/build and inspect the
  bian dashboard response against the API contract.

### Task 4: Add provenance, runtime, and version configuration A:I1
- [ ] Make bian environment variables, database target, service URL,
  contract version, and runtime mode explicit in existing configuration/docs.
- [ ] Add a compact provenance/knowledge surface using the existing bian
  coverage and raw-payload data rather than creating a parallel knowledge
  database.
- [ ] Define reproducible version identity and repository status checks without
  rewriting unrelated repository history.
- [ ] Verification: exercise configuration parsing, schema status, provenance
  fields, and version output in tests or deterministic CLI checks.

### Task 5: Validate the end-to-end bian path A:I2
- [ ] Run focused backend tests, compile checks, and Financial OS checks for
  touched paths.
- [ ] Start the local API when dependencies are available and verify health and
  front-data responses; otherwise record the exact blocked dependency.
- [ ] Review the final diff for scope creep, duplicate owners, forbidden data
  domains, and untracked generated artifacts.
- [ ] Verification: record test commands/results and a final repository health
  summary.

### Task 6: Add a public-only real-time collection path A:I1
- [ ] Run bian on Python 3.12 and add Cryptofeed only for Binance public trade
  channels; do not configure authenticated feeds, trading, wallets, or orders.
- [ ] Keep PostgreSQL and the existing collector as the sole persistence and
  ingestion owners; the stream writes through the existing persistence path.
- [ ] Verification: test stream configuration and its public-only callback
  boundary without opening an exchange connection.

### Task 7: Make collection results correct and diagnosable A:I1
- [ ] Preserve source numeric precision with Decimal, record collection runs,
  make repeated persistence idempotent, and expose the latest successful and
  failed collection attempt.
- [ ] Treat database authentication and query failures as degraded status
  responses without exposing DSN credentials.
- [ ] Verification: test precision, retries, idempotency SQL, and database
  failure behavior.

### Task 8: Expose freshness and verify the actual service identity A:I2
- [ ] Add bounded collection freshness to health and front-data responses.
- [ ] Make the launch script verify that an occupied port is the bian service
  rather than any arbitrary listener.
- [ ] Verification: run Python 3.12 tests, shell syntax checks, and local
  health/start-script reproducers.

### Task 9: Provision bian's isolated PostgreSQL endpoint A:I2
- [ ] Add the minimal reproducible PostgreSQL lifecycle configuration for
  bian's existing `localhost:5446` DSN.
- [ ] Start only the dedicated bian database, initialize the schema, collect
  one public snapshot, and verify API health/front-data against it.
- [ ] Verification: confirm the container health, database status, stored run,
  and API response without accessing another bot database.

## Production Validation Addendum

The implementation ledger above records architecture delivery. The following
validation tasks are separate gates. A task may only be marked verified after
the corresponding runtime or test evidence exists. External credentials and
elapsed-time requirements must remain explicitly blocked when unavailable.

### Task 21: Add isolated paper validation controls
- [ ] Record `NO_SIGNAL` as an auditable system event without changing engine
  behavior.
- [ ] Add `TEST_ONLY_SIGNAL_INJECTION`, restricted to paper and non-production
  environments, and keep it inside the runner boundary.
- [ ] Verification: injection is off by default, hard-blocked outside paper,
  and still traverses risk and execution.

### Task 22: Validate paper order lifecycle and accounting
- [ ] Cover market/limit, buy/sell, partial fill, cancel, expiry, and reject
  outcomes using the shared executor interface.
- [ ] Verify balances, fees, realized/unrealized PnL, and equity identities.
- [ ] Verification: deterministic lifecycle and accounting tests pass.

### Task 23: Validate paper restart and idempotency
- [ ] Recreate executor state from persisted orders and verify open-order
  recovery, terminal-state preservation, and no duplicate trade/fee effects.
- [ ] Verification: restart/recovery and duplicate-request tests pass.

### Task 24: Validate risk and transport failure behavior
- [ ] Exercise every configured risk limit and adapter timeout, rate-limit,
  server-error, and connection-failure translation.
- [ ] Verification: ALLOW/DENY/HALT and fail-closed behavior are evidenced.

### Task 25: Validate strategy research boundaries
- [ ] Add the required research metrics and timestamp-safety checks without a
  second strategy implementation.
- [ ] Verification: backtest metrics are deterministic and no future candle is
  visible to each engine frame.

### Task 26: Execute available runtime gates
- [x] Run full tests, schema ensure/status, fresh public collection, API health,
  shell/compile checks, and refresh the code graph.
- [x] Record 24-hour paper, Testnet, and Live gates as verified only when they
  actually run with the required external conditions.
- [x] Verification: readiness report lists exact evidence and blockers.

### Validation Result

As of August 25, 2026, the local validation gates pass with 71 tests. The
available Paper smoke and accounting checks pass, but the required 24-hour
Paper run, Testnet credential/order/reconnect checks, and Live account checks
remain unverified because the required elapsed runtime and external
credentials were not available in this validation session. LIVE is therefore
not approved.

## Capital Positioning V3 Addendum

This work extends the single `engine.py` in shadow mode. It does not create a
second strategy or execution path. `POSITIONING_DECISION_ENABLED` remains
`false` until the runtime acceptance gates below have evidence.

### Task 27: Restore timestamped market observations
- [x] Normalize public Spot/Futures observations with source time, receive
  time, and latency; stale or incomplete inputs fail closed in positioning.
- [x] Verification: envelope, stale-data, and no-future-input tests pass.

### Task 28: Persist and aggregate Spot trade flow
- [x] Persist short-retention trade facts and calculate CVD/net aggressive flow
  only from events received by the decision time.
- [x] Verification: delayed-event and persisted metadata aggregation tests pass.

### Task 29: Add candidate Spot liquidity observation
- [x] Subscribe candidate symbols to public bookTicker and L2 depth; local
  depth output is discarded on a sequence gap and rebuilt from a snapshot.
- [x] Verification: snapshot, contiguous update, feature, and gap tests pass.
- [ ] Runtime gate: prove uninterrupted collection for 1, 6, and 24 hours.

### Task 30: Add Futures observation-only context
- [x] Observe mark/index, OI, funding, taker flow, public long/short statistics,
  and force-order liquidations without Futures execution.
- [x] Verification: public adapter and force-order normalization tests pass.

### Task 31: Assemble the normalized MarketFrame
- [x] Read persisted observations as-of the decision timestamp and build only
  aggregated `MarketFrame` features plus source provenance.
- [x] Verification: no-lookahead event filtering and evidence provenance tests pass.

### Task 32: Keep positioning decisions in shadow mode
- [x] Record legacy versus positioning comparison and evidence snapshots while
  legacy SMA remains the only execution input by default.
- [x] Verification: positioning tests and runner guards pass.

### Task 33: Complete remaining research and runtime gates
- [x] Add market breadth, relative-strength, and regime aggregation to the
  Universe Scanner.
- [x] Add deterministic historical `MarketFrame` replay and forward-return
  attribution against legacy SMA and simple momentum.
- [ ] Complete seven days of fresh shadow evidence, then Paper attribution.
- [ ] Complete Testnet lifecycle observation and explicit Live readiness review.
