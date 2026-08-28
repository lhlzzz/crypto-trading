# Next Action

Keep `POSITIONING_DECISION_ENABLED=false` and run the positioning observer in
shadow mode. The observer and pure Shadow loop are active after the historical
liquidation-freshness regression fix and the direct-script import-path repair;
the full suite has 118 passing tests. Local order books now carry explicit
REST snapshot provenance across diff events, and Futures public observation
uses the existing bounded retry/backoff transport configuration.
Breadth, relative strength, regime, deterministic replay, and research
attribution are implemented. Multi-window flow, price impact, and native
Futures-period observations remain covered by focused and full regression
tests.
Use `scripts/bian_market.py observe` for the continuing public-only data gate;
its Tier 2 candidate limit is independent from the broader scanner limit.
The next evidence gates are 1 hour, 6 hours, and 24 hours of public
Spot/Futures observation, followed by seven days of fresh Shadow snapshots
with meaningful forward-return samples. Storage measurement now reports
observed-span daily estimates; continue sampling before setting
`POSITIONING_RAW_RETENTION_DAYS`, and do not run raw pruning by default.
The current replay store contains 54 frames but only four non-flat Positioning
samples. Continue collecting before interpreting attribution or changing any
positioning score threshold.
Testnet remains observation-only until credentials, account lifecycle, and
reconciliation evidence exist. The refreshed interactive Understand-Anything
graph is served from the tokenized dashboard URL reported for the current
session.
