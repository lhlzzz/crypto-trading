# Align Bian With The A-Share System

**Goal:** Bring bian to the xiaogu engineering baseline for frontend
integration, read-only backend access, PostgreSQL lifecycle, provenance
metadata, runtime configuration, and version identity while preserving the
Binance-only public market-data boundary.
**Constraints:** Financial OS owns the browser UI; PostgreSQL is bian's only
data authority; no trading, wallet, Polymarket, or A-share strategy logic.
**Out of scope:** Rebuilding the shared Financial OS shell or copying xiaogu
signal-scoring and paper-trading features.

## Must-Haves

- MH1: The bian frontend receives a stable read-only API contract. A:I1
- MH2: Database schema, reachability, and provenance are observable. A:I1
- MH3: Runtime configuration and version identity are explicit and isolated.
  A:I1
- MH4: Tests and local runtime checks prove the affected paths. A:I2

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
