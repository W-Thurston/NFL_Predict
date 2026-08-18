# Gridiron Edge Roadmap

## Document Ownership

| Document | Purpose |
|---|---|
| `ROADMAP.md` | Genuine future capabilities, strategic priorities, and current limitations |
| `PLAN.md` | Active implementation checklist and completed-unit record |
| `HANDOFF.md` | Current operating system, commands, artifacts, and recovery guidance |
| `DECISIONS.md` | Append-only architectural decisions and supersession history |
| `CHANGELOG.md` | Dated implementation history |

The canonical weekly prediction, product, API, frontend, and verification architecture is implemented. Future work should build on the persisted-event and explicitly selected weekly-product contracts rather than restore retired archive, fallback, or request-time behavior.

## Current Platform State

Gridiron Edge currently provides:

- canonical one-row Away/Home game modeling;
- independent Win and Total model families;
- unversioned champion artifacts and a persisted champion manifest;
- model-specific weekly availability inspection and policy selection;
- immutable `live` and `backfilled` forecast events;
- immutable schedule-complete weekly products with explicit current selection;
- independent Win, Spread, Total, and projected-score readiness;
- source-neutral current-market storage and explicit edge diagnostics;
- completed-week closeout against exact selected live forecast events;
- archive-driven historical evaluation and champion comparison;
- a read-only serialization API;
- a generated-contract React frontend;
- schedule-first game presentation and truthful missing-data states;
- betting ledger, bankroll, performance, and local BetSlip decision support;
- season and playoff simulation;
- Python and frontend quality boundaries.

The successful 2026 Week 1 rehearsal produced complete Win, Spread, Total, projected-score, and provenance coverage for all scheduled games while market readiness remained independently blocked. Forecast PNG and HTML publication succeeded without requiring market prices.

## Strategic Priorities

Prioritize work by value density and architectural fit:

1. Preserve truthful persisted-state boundaries before adding breadth.
2. Resolve supported external data sources before building interfaces that depend on them.
3. Improve predictive quality only through honest time-ordered evaluation.
4. Keep the API a serialization boundary.
5. Keep unavailable, blocked, and analytical-empty states explicit.
6. Add product surface area only when the underlying data contract is real.
7. Continue using files until concurrency, transactional integrity, or query complexity requires a database.

## Future Work

### Supported Market Provider and Multi-Book Shopping

**Status:** Active program, with Market Unit 26 as the only active bounded unit.

**Goal:** operate a truthful provider-aware, multi-book recommendation product
whose persisted evidence can be followed from selected forecasts and exact
pregame quotes through candidate issuance, policy evaluation, presentation,
optional local wager recording, closeout, CLV, and realized performance.

#### Implemented Platform

The supported current-market and recommendation foundations are implemented:

- The Odds API v4 is the supported current and upcoming NFL market provider.
- The canonical quote contract preserves provider, provider event, sportsbook,
  game, market, side, price, line, local fetch time, sportsbook update time,
  kickoff, and live state.
- Current snapshots and append-preserved season-and-week quote history have
  separate storage and operational semantics.
- Exact replay is idempotent, same-fetch conflicts are rejected, later
  observations remain distinct, and historical loading is deterministic.
- Leakage-safe historical boundaries preserve earliest observed and latest
  eligible non-live pregame evidence independently for each exact
  provider-aware market identity.
- Current ingestion, weekly collection planning, active-plan selection,
  single-shot due execution, immutable claims and results, quota safeguards,
  and partial-persistence reporting are implemented.
- The Raspberry Pi systemd worker deployment, installation, verification,
  secret handling, monitoring, rollback, and recovery assets are
  repository-owned and validated.
- Sportsbook-specific Moneyline, Spread, and Total offers remain independently
  traceable through edge calculation, diagnostics, API, CLI, CSV, Line
  Shopping, Game Detail, Available Edges, and Bet Slip staging.
- Line Shopping preserves every exact offer, classifies line and price quality,
  evaluates exact-offer expected value against the selected weekly product, and
  keeps model likelihood, analytical value, and recommendation semantics
  separate.
- Immutable pregame candidate issuance preserves the exact product, forecast,
  offer, model probability, expected value, evaluation time, state, and reason.
- Exact candidate and recorded-wager references can be closed against the
  latest eligible pregame observation from the same provider, provider event,
  sportsbook, game, market, and side.
- Moneyline price CLV, Spread point CLV, and Total point CLV are calculated only
  from complete validated closeout terms.
- Empirical Moneyline, Spread, and Total evaluation reports outcomes, closeout,
  CLV, observation depth, quote age, cohorts, and exact settled-wager return
  evidence without converting descriptive evidence into intuitive thresholds.
- Immutable recommendation governance, deterministic policy derivation,
  mandatory policy checks, Kelly sizing, bankroll and portfolio constraints,
  and immutable recommended-bet result persistence are implemented.
- Persisted recommendation states are serialized mechanically and presented in
  Line Shopping, Available Edges, Game Detail, and Bet Slip without request-time
  qualification or frontend decision calculation.
- A user may explicitly record a wager locally through one rollback-safe ledger
  and bankroll operation. Gridiron Edge does not place sportsbook wagers.
- Production-chain preflight validates exact selected-product, quote-history,
  collection-plan, candidate, policy, result, backend, frontend, collection
  execution, outcome, closeout, CLV, and performance evidence independently by
  market family.
- The repository-wide Ruff, Pyrefly, Python test, and frontend quality
  boundaries are restored and enforced.

#### Active Proof: Market Unit 26

The active unit is proving the complete production recommendation chain for one
real completed NFL week independently for Moneyline, Spread, and Total.

The real 2026 Week 1 rehearsal has already established:

- one explicitly selected 16-game weekly product with complete selected
  forecast provenance;
- a current 840-row multi-book quote snapshot across 16 games and nine
  sportsbooks;
- a canonical 1,680-row weekly quote ledger at two distinct UTC fetch
  timestamps;
- repeated depth of two for all 274 Moneyline, 282 Spread, and 284 Total exact
  historical identities;
- immutable candidate issuance across all 1,680 observations, containing 698
  candidates, 982 not-candidates, and zero unavailable rows;
- 228 Moneyline, 226 Spread, and 244 Total candidates;
- explicit immutable recommendation governance;
- one persisted family-specific recommendation policy whose three families are
  unavailable because required completed-outcome, closeout, and return evidence
  is not yet available;
- 698 persisted recommended-bet results, all explicitly unavailable and none
  qualified, recommended, failed, or conflicting;
- exact-offer API attachment and frontend rendering of Recommendation
  unavailable with expandable Policy evidence;
- strict preflight resolution of the exact candidate issuance, policy, and
  recommendation evaluation;
- shared postgame evidence assembly through existing outcome, closeout, CLV,
  historical-boundary, market-family evaluation, cleaned-game, and optional
  settled-wager owners.

The persisted rehearsal identities are maintained in the active `PLAN.md` unit.

#### Remaining Unit 26 Acceptance

The active unit remains open for evidence that cannot exist yet:

1. Execute due polls from the selected 2026 Week 1 collection plan through the
   repository-owned worker and preserve immutable claim and terminal-result
   receipts. The first planned poll is `2026-09-08T12:00:00Z`.
2. Refresh cleaned completed-game outcomes after Week 1 games finish and
   reconcile them to the exact selected weekly product and forecast events.
3. Close exact issued candidates against the latest eligible non-live quote
   observed strictly before kickoff from the same provider, provider event,
   sportsbook, game, market, and side.
4. Validate Moneyline price CLV, Spread point CLV, and Total point CLV
   independently. Evidence from one market family cannot satisfy another.
5. Evaluate realized performance only from uniquely attributed settled-wager
   evidence. No recorded wager is required; absent wager evidence must remain
   explicitly unavailable rather than zero.
6. Persist chronological production-chain assessments at explicit UTC
   timestamps and verify exact replay without reassessing mutable repository
   state.
7. Complete a final real-data frontend presentation review after outcome,
   closeout, and CLV evidence exists. Presentation cleanup must not change
   persisted recommendation semantics.

Market Unit 26 closes only after a real completed week satisfies independent
Moneyline, Spread, and Total proof or records an explicit evidence-backed
unavailable state for a component that cannot validly become available.

#### Genuine Follow-On Market Capabilities

The following remain future capabilities after Unit 26:

- supported provider historical backfill if operational collection does not
  produce sufficient depth for empirical calibration;
- empirically validated joint threshold-selection methods and later policy
  calibration from matured Moneyline, Spread, and Total evidence;
- line and price movement interpretation beyond validated observed boundaries;
- strategy backtesting with strict chronological evidence controls;
- arbitrage detection with exact executable book, line, price, timing, and stake
  constraints;
- middle detection with explicit paired-line and settlement semantics;
- consensus or cross-book reference policies that do not weaken exact-offer
  provenance;
- frontend density, layout, evidence-detail, and accessibility cleanup informed
  by the real recommendation and postgame evidence experience;
- optional API and frontend appliance hosting on the Raspberry Pi as a separate
  deployment decision.

Current snapshots and historical evidence remain separate workstreams sharing
one normalized quote contract. Forecast publication stays independent from
provider access. Missing market or recommendation evidence remains explicit and
does not invalidate the selected weekly prediction product.

Do not fabricate production prices, collapse provider quotes before
normalization, infer recommendation state from positive expected value, hide a
network fetch inside weekly prediction, treat manual ingestion as selected-plan
execution, or use the retired DraftKings adapter as a dependable recovery path.

### Model Ensemble

**Goal:** determine whether a time-ordered ensemble improves operational Win prediction enough to justify additional complexity.

Candidate approaches:

- Brier-weighted averaging;
- constrained blending;
- logistic stacking with time-ordered out-of-fold inputs;
- simple rank or probability averaging as a baseline.

Acceptance should require an honest historical comparison against the current champion, preserved calibration quality, complete upcoming-game feature coverage, deployable artifact metadata, availability inspection, and compatibility with the existing weekly policy and immutable event contracts.

An ensemble should register as another model identity. It must not compute dynamically in the API.

### Injury and News Data

**Goal:** add a reliable, timestamped source for player availability and material team news.

Required design work:

- choose a source and usage policy;
- preserve fetched-at and effective-at timestamps;
- distinguish reported, confirmed, and resolved status;
- map players and teams to canonical identities;
- define historical availability for honest evaluation;
- expose blocked or unavailable states when the source is incomplete.

This capability unlocks injury-aware game and prop presentation and is a prerequisite for credible personnel scenarios.

### Scenario Engine and Feature Attribution

**Goal:** answer bounded what-if and explanation questions without mutating production forecasts.

Potential scope:

- feature contribution or local explanation for persisted predictions;
- comparable historical games;
- controlled team-strength or player-availability adjustments;
- usage redistribution for player props;
- scenario-specific Win, Spread, Total, projected score, and edge calculations;
- explicit separation between persisted production output and hypothetical output.

Scenario computation should use an explicit request and response contract. It must not silently alter the selected weekly product or champion artifacts.

### Real-Time and Live Game Support

**Goal:** support in-game decision analysis.

Required foundations:

- live score, clock, down, distance, possession, and timeout state;
- timestamped live market data;
- a validated live win-probability model;
- live edge and hedge calculations;
- streaming or polling transport;
- strict freshness and stale-state presentation.

This remains lower priority than reliable pregame multi-book data and injury/news integration.

### Remaining API Batch-Artifact Boundaries

**Goal:** ensure every API endpoint serializes persisted artifacts rather than performing meaningful computation at request time.

Known candidate for verification:

- model-performance summaries should be confirmed as batch-produced artifacts; if still computed on request, add a batch writer and serialize its output.

For each candidate:

1. identify the current request-time computation;
2. define the persisted artifact schema and writer;
3. add freshness and provenance;
4. migrate loaders to read the artifact;
5. keep routes and serializers thin;
6. add parity tests before removing the old path.

Do not assume a listed historical deviation still exists. Verify it against current code before scheduling work.

### Frontend Product Enhancements

The core game-day and portfolio surfaces are functional. Remaining work should be pulled by real data availability and user value.

Potential enhancements:

- multi-book line-shopping views;
- injury and news presentation;
- scenario and explanation surfaces;
- line-movement and live-game charts;
- richer bankroll history and Kelly-adherence views;
- recorded-bet export and an explicitly designed recorded-bet write workflow;
- remaining table, layout, and accessibility polish;
- a real-data pending-state visual audit after all required backend artifacts are populated.

BetSlip remains a draft decision workspace. Any recorded-bet write workflow requires duplicate protection, bankroll transaction coupling, partial-failure semantics, and an explicit user action. It is not sportsbook execution.

### Model and Feature Research

Candidate research areas:

- offensive and defensive rating decomposition;
- coaching and coordinator effects;
- pace and neutral-situation tendencies;
- special-teams features;
- penalties, pressure, and situational efficiency;
- additional opponent-quality cohorts;
- richer prop distribution models;
- era-aware feature availability and imputation;
- calibrated uncertainty for ratings and projections.

Every new feature must preserve chronological construction, avoid leakage, and use empirical thresholds rather than arbitrary bins.

### Tooling and CI

Future tooling work:

- restore the intended repository-wide Pyrefly boundary using
  `uvx pyrefly check`;
- define the production, test, script, and exploratory-notebook type-check
  scope explicitly;
- correct repository and test import roots before treating missing test-fixture
  imports as source defects;
- triage configuration failures, production-source findings, shared fixture
  annotations, negative validation tests, Pandas inference limitations, and
  exploratory notebook diagnostics separately;
- establish and enforce a zero-error repository-wide baseline without
  suppressing genuine production defects;
- preserve focused Pyrefly checks during bounded implementation units while the
  repository-wide baseline is being restored;
- exercise `gridiron verify --strict` in a real CI surface;
- run the separate frontend lint, build, and test gates in CI;
- consider performance baselines if test or training runtime regresses;
- maintain generated OpenAPI and TypeScript contract checks;
- improve long-running composite resume diagnostics where needed;
- clamp current-season PBP requests to the maximum season published by the
  upstream source once that policy is defined;
- verify and repair any remaining baseline-report parser edge cases;
- review repository-wide lint exclusions only through dedicated,
  behavior-preserving work.

## Known Limitations

### Market data

The Odds API v4 client, parser, provider-aware quote contract, partitioned
historical observations, explicit ingest command, sportsbook-specific offer
evaluation, operational edge integration, frontend sportsbook preferences,
Line Shopping, Bet Slip quote identity, kickoff-aware collection planning,
single-shot execution, and active-plan selection are implemented.

A Raspberry Pi quote-collection worker is running through a systemd timer.
Repository-owned deployment assets, installation verification, monitoring,
recovery, and operational artifact synchronization remain active work.
Repeated real quote coverage, validated closeout and CLV, empirical
recommendation thresholds, recommendation product integration, and full
Moneyline, Spread, and Total production proof remain incomplete.

### Injury, news, and live state

There is no integrated injury/news feed or live-game state. Related API and frontend fields must remain explicitly blocked.

### Scenario and explanation

Feature attribution, comparable-game retrieval, and what-if propagation are not implemented.

### Current-season PBP cadence

The upstream source may not publish the current season immediately. Pipeline refresh can warn while continuing with available historical feature state. A future cleanup may clamp requests to the latest published season.

### Postgame timing

`post-week` requires completed outcomes. Running it before games finish correctly exits nonzero and lists missing outcomes.

### Markets versus predictions

A selected weekly product can be prediction-ready while market readiness is blocked. Missing market data means no current edge result; it does not invalidate forecasts.

### File-backed architecture

Files remain appropriate for the current single-user workflow. Revisit this only for real multi-user concurrency, transactional guarantees, or query requirements.

## Prioritization Guidance

The next major work should normally be chosen from:

1. supported market provider and multi-book shopping;
2. model ensemble research;
3. injury/news source;
4. scenario engine and explanations;
5. remaining API batch-artifact migrations;
6. frontend enhancements unlocked by real data;
7. real-time and live-game support.

Before starting a new work item:

- verify the gap still exists in current code;
- add it to `PLAN.md` as a bounded execution unit;
- record any locked architectural choice in `DECISIONS.md`;
- update `HANDOFF.md` only after behavior ships;
- record completion in `CHANGELOG.md`.
