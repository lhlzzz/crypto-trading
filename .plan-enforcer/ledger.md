<!-- source: docs/plans/2026-08-15-bian-xiaogu-alignment.md -->
<!-- tier: structural -->
<!-- created: 2026-08-15T00:00:00+08:00 -->

## Scoreboard

 69 total  |  0 done  |  60 verified  |  0 skipped  |  2 blocked  |  7 remaining
 Drift: 5  |  Last reconcile: T69 |  Tier: structural

## Task Ledger

| ID | Task | Status | Evidence | Chain | Notes |
|---|---|---|---|---|---|
| T1 | Map current contracts and establish runtime baseline | verified | pytest 2 passed | A:I1,V1 | Baseline captured |
| T2 | Add bian read API and database status boundary | verified | 5 API tests passed | A:I1,V2 | Read-only contract added |
| T3 | Align Financial OS bian integration | verified | Financial OS tsc passed | A:I1,V3 | Next route proxies API |
| T4 | Add provenance, runtime, and version configuration | verified | config and compile checks pass | A:I1,V4 | Explicit release/contract versions |
| T5 | Validate the end-to-end bian path | verified | 5 tests; tsc; API 200 | A:I2,V5 | Final payload boundary reviewed |
| T6 | Add public-only real-time collection path | verified | public trade feed test passes | D1,D2,V9 | Python 3.12, no auth channels |
| T7 | Make collection results correct and diagnosable | verified | Decimal, retries, DB errors pass | D1,V7 | Runs and idempotent SQL covered |
| T8 | Expose freshness and verify service identity | verified | API health and shell checks pass | D1,V8 | Service identity verified on :18001 |
| T9 | Provision bian isolated PostgreSQL endpoint | verified | DB healthy; API reads 5 markets | D3,D4,D5,D6,D7,V10 | Dedicated localhost:5446 service verified |
| T10 | Audit Phase 1-4 ownership boundaries | verified | 38 tests passed | V11 | Adapter/schema/engine/execution owners verified |
| T11 | Implement exchange rules and transport-safe adapter helpers | verified | adapter cache tests passed | V12 | Exchange filters parsed and cached |
| T12 | Implement isolated User Data Stream | verified | user stream tests passed | D8,V13 | SDK stream isolated; reconnect hook wired |
| T13 | Complete reconciliation and restart recovery | verified | 44 tests; schema ensure | V14 | Client-order lookup; restore or HALT |
| T14 | Add paper lifecycle parity and accounting coverage | verified | limit/expiry tests passed | V15 | Latency, limit pending, expiry, PnL paths |
| T15 | Add backtesting research boundary | verified | vectorbt test passed | V16 | One research path; no second live engine |
| T16 | Extend read-only Financial OS trading contract | verified | API summary/events tests passed | V17 | Mode, risk, PnL, events remain GET-only |
| T17 | Add Testnet runner and startup recovery gate | verified | shell/recovery guards passed | V18 | Reconcile before private trading loop |
| T18 | Add emergency stop and live guard | verified | live guard test passed | V19 | Token and explicit confirmation; hard block |
| T19 | Add recovery/safety/integration tests | verified | 50 tests passed | V20 | Restart, unknown, timeout, live-disabled cases |
| T20 | Final runtime, graph, and repository health review | verified | 50 tests; graph 526 nodes | V21 | API, DB, market, guards, owners checked |
| T21 | Add isolated paper validation controls | verified | 18 tests; injection guards pass | D9,V22 | NO_SIGNAL and test-only injection |
| T22 | Validate paper order lifecycle and accounting | verified | 18 tests; lifecycle/PnL pass | V22 | Deterministic lifecycle and accounting |
| T23 | Validate paper restart and idempotency | verified | restart/idempotency pass | V23 | Recovery and duplicate effects |
| T24 | Validate risk and transport failure behavior | verified | 34 focused tests pass | V24 | Risk limits and adapter failures |
| T25 | Validate strategy research boundaries | verified | backtest/lookahead tests pass | V25 | Metrics and timestamp safety |
| T26 | Execute available runtime gates | verified | 71 tests; API/DB; graph 564/1869 | D10,V26 | External gates explicitly unverified |
| T27 | Restore timestamped market observations | verified | 89 tests; fresh collection | D11,V27 | Envelopes and stale gate fail closed |
| T28 | Persist and aggregate Spot trade flow | verified | CVD/no-lookahead tests pass | D11,V27 | Received-time boundary enforced |
| T29 | Add candidate Spot liquidity observation | verified | real trade/book/depth smoke | D11,V27 | Runtime duration gate remains pending |
| T30 | Add Futures observation-only context | verified | fresh REST and force-order tests | D11,V27 | No Futures order path added |
| T31 | Assemble timestamp-aligned MarketFrame | verified | real frame and provenance | D11,V27 | Raw events stay out of engine |
| T32 | Run positioning in shadow mode | verified | API snapshot; no signal | D11,V27 | Legacy remains execution owner |
| T33 | Complete research and elapsed-time gates | pending | 104 tests; observer/Shadow restart and zero-order audit | D11,V28,V29,V30 | Duration, 7-day Shadow, Testnet remain |
| T34 | Phase A meme-futures architecture audit | verified | 118 tests; schema ok; compile ok | D12,A:I3,A:I4,A:I5,V31 | Owners intact; execution still Spot |
| T35 | Phase B Futures data completeness | verified | 120 tests; live 30m taker | D12,A:I3,V32 | No private Futures orders |
| T36 | Phase C Futures orderbook | verified | orderbook tests pass | D12,D14,V35 | Snapshot/buffer/gap/resync |
| T37 | Phase D Futures positioning features | verified | Futures feature tests pass | D12,V35 | Native windows, no synthesis |
| T38 | Phase E MarketFrame alignment | verified | frame replay tests pass | D12,V35 | Normalized evidence only |
| T39 | Phase F positioning state machine | verified | state safety tests pass | D12,A:I5,V35 | No dedicated transition state |
| T40 | Phase G transition semantics | verified | continuous strength tests pass | D12,A:I5,V35 | State delta magnitude |
| T41 | Phase H evidence and replay | verified | replay/backtest tests pass | D12,V35 | MISSING remains UNKNOWN |
| T42 | Phase I shadow duration gates | pending |  | D12,D13 | Includes T33 elapsed gates |
| T43 | Phase J Futures paper | verified | paper lifecycle tests pass | D12,A:I3,V36 | Margin/PnL/funding/liquidation |
| T44 | Phase K backtest/walk-forward | verified | 210 tests pass | D12,V38 | Futures PaperExecutor chain |
| T45 | Phase L Testnet Futures lifecycle | pending |  | D12,A:I3 | Separate testnet secrets |
| T46 | Phase M failure injection | verified | uncertainty tests pass | D12,V36 | UNKNOWN/HALT fail closed |
| T47 | Phase N production guards | verified | gate/API/owner checks | D12,A:I3,V36 | Futures-only guards |
| T48 | Phase O limited live | pending |  | D12 | Hard block until gates pass |
| T49 | Legacy Spot assumption audit | verified | STATE KEEP/REPLACE/DELETE table | D14,A:I6 | Adapter first |
| T50 | USD-M private adapter | verified | 124 tests; HMAC/400/tombstone | D14,A:I3,A:I6,V33 | HMAC stdlib; no quoteOrderQty |
| T51 | TradeIntent futures contract | verified | 18 intent tests pass | D15,V34 | quantity-only OPEN/REDUCE/CLOSE |
| T52 | Futures-native RiskGate | verified | risk tests cover margin/liq | D15,V34 | delete spot quote/base checks |
| T53 | Futures paper executor | verified | paper tests + inject fill | D15,T43,V34 | margin, funding, liquidation |
| T54 | paper_runner futures context | verified | cycle no_signal + fill | D15,V34 | position action + MarketSnapshot |
| T55 | Futures user stream | verified | stream unit tests pass | D15,V34 | ACCOUNT_UPDATE / ORDER_TRADE_UPDATE |
| T56 | Futures reconciliation | verified | mismatch HALT tests pass | D15,V34 | account/position/order truth |
| T57 | Store and schema futures fields | verified | ALTER + store writes | D15,V34 | modify existing tables |
| T58 | Engine TradeIntent generation | verified | OPEN/HOLD/CLOSE mapping | D15,V34 | current position + strength split |
| T59 | Replace obsolete spot tests | verified | 185 pytest passed | D15,V34 | intent/risk/paper/stream/recon |
| T60 | Delete Spot private leftovers | verified | PrivateClient removed | D15,V34 | migrate callers then drop alias |
| T61 | Live switches and paper smoke | verified | paper fill; live blocked | D15,V34 | HARD BLOCK until gates pass |
| T62 | Full pytest/compile/diff | verified | 210 pass; compile; diffcheck | D15,V38 | no commit without evidence |
| T63 | Restore and validate Futures realtime sources | verified | 76 tests; nine BTCUSDT sources FRESH | D16,V39,V40 | Real REST/stream evidence retained |
| T64 | Validate data, orderbook, flow, funding contracts | verified | 246 tests; compile; diff; schema ok | D16,V40,V41,V42,V44,V47 | Accounting, backtest, recon, stream covered |
| T65 | Run 30-minute Futures realtime smoke | blocked | real WS reset and REST disconnect | D16,D17,V43,V45,V46 | External transport unstable; no duration pass |
| T66 | Run 24-hour Futures paper validation | pending |  | D16 | Simulated execution on real market data |
| T67 | Run seven-day shadow validation | pending |  | D16 | Decision persistence; no execution |
| T68 | Run Testnet lifecycle validation | blocked | Separate credentials not present | D16 | BLOCKED_BY_EXTERNAL_CREDENTIALS |
| T69 | Final static validation, commit, and push | pending | 246 tests; compile; diff; push pending | D16,D17,V47 | Commit/push follows final worktree review |

## Decision Log

| ID | Task Ref | Decision | Reason |
|---|---|---|---|
| D1 | T6-T8 | Use Python 3.12 and Cryptofeed | User approved higher runtime; public-only boundary remains |
| D2 | T6 | Use trade instead of ticker stream | Ticker only exposes bid/ask, not last price |
| D3 | T9 | Add bian database compose file | No existing bian lifecycle configuration exists |
| D4 | T9 | Use docker-compose binary | Docker Compose plugin is unavailable |
| D5 | T9 | Use project Python 3.12 virtualenv | uv-managed Python cannot be modified in place |
| D6 | T9 | Declare Uvicorn CLI dependency | Uvicorn 0.52 imports click without installing it |
| D7 | T9 | Use uv pip install in setup command | It resolves Cryptofeed runtime dependencies |
| D8 | T12 | Use official SDK WebSocket API user-data stream | Installed SDK exposes signed user-data subscriptions |
| D9 | T21 | Keep test-only signal injection in paper_runner.py | It is validation control, not strategy logic or execution |
| D10 | T24 | Require mode-specific Testnet/Live credential names | Shared credentials cannot prove environment isolation |
| D11 | T27-T32 | Keep Positioning fail-closed and shadow-only | Data is incomplete until runtime gates pass |
| D12 | T34-T48 | Continue from d75a795; no rollback | User locked Meme Futures production constraints |
| D13 | T33,T42 | Do not skip elapsed-time gates | 1h/6h/24h/7d remain required evidence |
| D14 | T36,T49,T50 | Pivot: adapter/audit before orderbook | New pack requires Spot deletion and FuturesPrivateClient first |
| D15 | T51-T62 | Unplanned: targeted futures semantic reset | User locked TradeIntent/risk/paper/stream/recon rewrite; do not replan T1-T50 |
| D16 | T63-T69 | Continue existing readiness ledger | User supplied final validation pack; preserve external gates |
| D17 | T65 | Persist resync GAP before fresh book coalescing | Real WS reset exposed overwritten fault evidence |

## Reconciliation History

| Round | Tasks Checked | Gaps Found | Action Taken |
|---|---|---|---|
| R7 | T21-T25 | 0 | Paper controls, lifecycle, recovery, risk, transport, and research gates verified |
| R8 | T26 | External credentials and elapsed-time gates unavailable | Marked unverified; no LIVE approval |
| R9 | T27-T33 | 1 | T33 remains runtime/research gated |
| R10 | T27-T33 | 1 | Research verified; elapsed gates remain |
| R11 | T27-T33 | 1 | Feature smoke passed; elapsed gates remain |
| R12 | T27-T33 | 1 | Observer validated; elapsed gates remain |
| R13 | T27-T33 | 1 | Regression test, 104-test suite, restart, zero-order audit, and 358/528 graph refresh passed; elapsed gates remain |
| R14 | T1-T48 | 15 | T34 verified; T33 elapsed gates remain; T35-T48 pending; no duplicate owners |
| R15 | T1-T48 | 14 | T35 verified; native 30m + distinct last/mark/index; T33 remains |
| R16 | T1-T50 | 14 | T49/T50 verified; T36 deferred; T33 elapsed gates remain |
| R17 | T1-T62 | 14 | T51-T62 verified; T33 elapsed + T36-T48 remain; Testnet BLOCKED_BY_EXTERNAL_CREDENTIALS |
| R18 | T1-T62 | 4 | 210 tests; elapsed/Testnet/Live remain external |
| R19 | T63 | 0 | Futures source recovery has real DB evidence |
| R20 | T63-T65 | 0 | Contract suite verified; smoke restart follows symbol fix |
| R21 | T63-T65 | 0 | Real reset found lost GAP audit event; fixed and tested |
| R22 | T63-T69 | 1 | T65 remains runtime-blocked; later gates not advanced |
| R23 | T63-T69 | 2 | Fixed direct-script root import and Decimal JSON serialization; runtime gates remain |

## Reconciliation History

| Round | Tasks Checked | Gaps Found | Action Taken |
|---|---|---|---|
| R1 | T1-T5 | 0 | All tasks have verification evidence |
| R2 | T1-T5 | 0 | Removed unused import and bounded API payload |
| R3 | T1-T8 | 0 | All tasks have verification evidence |
| R4 | T1-T8 | 0 | Trade price semantics verified |
| R5 | T1-T9 | 0 | Database and API connection verified |
| R6 | T10-T19 | 0 | Tests, schema, market collect, API, and guards verified |
