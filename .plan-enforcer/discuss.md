# Align Bian With The A-Share System

## Source Ask
> 对齐 A股的前端 后端 数据库 知识库 等等的系统配置 版本管理什么的

## Normalized Goal
Align bian with the existing xiaogu A-share system's engineering
boundaries and operational configuration across frontend integration,
read-only backend APIs, PostgreSQL ownership, knowledge/evidence handling,
runtime configuration, validation, and version management. Preserve bian as
Binance public market data only; do not import A-share strategy logic.

## Non-Negotiables
- NN1: Binance public observation remains read-only; Futures trading is gated
  by the canonical runtime result.
- NN2: No Polymarket data, order creation, wallet access, or trading paths.
- NN3: PostgreSQL remains the sole bian data authority.
- NN4: Financial OS remains the browser/UI owner.
- NN5: Modify existing bian and Financial OS integration points before adding
  parallel implementations.

## Hidden Contract Candidates
- HC1: The frontend must consume a stable read contract instead of querying
  PostgreSQL directly.
- HC2: Database lifecycle and schema health must be observable.
- HC3: Data source URLs, capture timestamps, raw payloads, and coverage status
  must remain diagnosable.
- HC4: Runtime configuration must be isolated to bian's own DSN and environment
  variables.
- HC5: Version identity must be explicit and reproducible even though bian
  currently has no local Git repository.

## Plausible Interpretations
- PI1: Align only configuration and operational conventions, leaving bian's
  current collector as the backend.
- PI2: Bring bian to feature parity with xiaogu's system skeleton: API
  contract, startup/runtime checks, database lifecycle, knowledge/evidence
  surfaces, frontend contract integration, tests, and version metadata.

## Chosen Interpretation
Plan against PI2, but only for the bian public-market-data domain. This is
the interpretation that makes "前端、后端、数据库、知识库、系统配置、版本管理"
an end-to-end request rather than a cosmetic configuration copy.

## Rejected / Forbidden Narrowings
- FN1: Do not only change README or environment variable names.
- FN2: Do not copy xiaogu's candidate scoring, paper-trading, or A-share
  research modules.
- FN3: Do not create a second bian API or second database owner.
- FN4: Do not silently initialize or rewrite repository history without
  defining the version-control boundary.

## In Scope
- bian backend read API and stable Financial OS-facing response contract.
- bian PostgreSQL schema/lifecycle/status configuration.
- Financial OS bian page and adapter alignment with the contract.
- bian knowledge/evidence metadata for public market-data provenance and
  coverage, using existing project conventions where they fit.
- tests, startup/health validation, environment examples, and version metadata.

## Out of Scope
- Trading, wallet, order, portfolio execution, or investment advice.
- A-share strategy or indicator logic.
- Polymarket integration.
- Replacing Financial OS's shared frontend architecture.

## Constraints
- Prefer existing files and conventions in xiaogu and Financial OS.
- Keep the change auditable and avoid parallel implementations.
- Validate each affected boundary with focused tests and runtime checks.

## Success Signals
- A bian read API exposes the data needed by `/dashboard/bian`.
- The frontend reads the API contract and renders unavailable states clearly.
- Database schema and reachability can be checked independently.
- Provenance and coverage are queryable and test-covered.
- Runtime configuration and version identity are explicit and isolated.

## Proof Requirements
- PR1: Focused bian backend/database tests pass.
- PR2: Financial OS bian checks/build pass for touched paths.
- PR3: A local health/data request demonstrates the contract.
- PR4: Git diff and repository status show no unrelated or parallel system.

## Draft Handoff
- phase shape hint: inspect contracts -> implement backend/database ->
  integrate frontend -> add provenance/version config -> validate runtime.
- planning red lines: preserve Binance-only read scope and keep PostgreSQL and
  Financial OS ownership boundaries explicit.
