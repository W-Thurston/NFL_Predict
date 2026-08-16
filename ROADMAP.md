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

**Status:** Active program

**Goal:** establish a dependable supported market-data provider and add
cross-book execution tooling without coupling forecast publication to market
availability.

Current state:

- market storage is source-neutral;
- the nflverse schedule adapter can populate current game-market context when
  source data is available;
- the DraftKings adapter is legacy and unreliable because anti-bot responses
  can block access;
- weekly prediction consumes an existing current snapshot and does not fetch
  external prices;
- missing markets soft-fail only edge generation.

Program sequence:

1. **Provider and contract selection [Complete].** The Odds API v4 selected;
   normalized quote, freshness, identity, configuration, and failure boundaries
   locked.
2. **Source-neutral quote migration [Complete].** Provider and sportsbook
   identity are separate, provider event and update provenance are preserved,
   and storage is multi-book safe.
3. **Current provider adapter [Complete].** Current and upcoming NFL moneyline,
   spread, and total quotes can be explicitly ingested from The Odds API into
   the normalized store.
4. **Operational integration [Complete].** Sportsbook-specific prices remain
  independently traceable through edge calculation, diagnostics, API, CLI,
  CSV, frontend selection, and Bet Slip staging while forecast publication
  remains independent from provider access.
5. **Real-data frontend integration and audit [Complete].** Dashboard, Game
  Detail, Available Edges, Model Edges, Settings, browser navigation,
  responsive presentation, sportsbook provenance, and Bet Slip staging were
  validated against the real current multi-book snapshot.
6. **Multi-book shopping [Complete].** The current Line Shopping product
  preserves every exact sportsbook quote, classifies line and price quality
  independently, evaluates each offer against the selected weekly product,
  exposes playable guidance and fair Moneyline prices, and provides persisted
  accessible visual guidance and detailed offer explanations. Arbitrage, middle
  detection, movement, and historical market evaluation remain planned as
  separate follow-on work.
7. **Market decision semantics [Complete].** Line Shopping now separates model
  likelihood from exact-offer value, labels positive expected value as a
  candidate rather than a recommendation, and provides independently persisted
  controls for each visual comparison layer. Recommended-bet qualification
  remains planned until an empirically validated edge, reliability, freshness,
  sizing, and exposure policy is implemented.
8. **Recommendation evidence foundation [Complete].** Established the evidence
  and diagnostic contracts required before a positive-EV candidate can become a
  qualified or recommended bet. The system now provides:
  - immutable qualification diagnostics that distinguish not-candidate,
    not-qualified, and qualification-unavailable states without prematurely
    assigning a recommendation;
  - deterministic provider-aware historical quote observations with exact
    replay idempotence, same-fetch conflict rejection, and explicit temporal
    coverage;
  - leakage-safe `earliest_observed` and `latest_eligible_pregame` boundaries
    that preserve provider, provider-event, sportsbook, game, market, and side
    identity;
  - explicit missing, conflicting, live, and post-kickoff boundary states;
  - recorded wager terms stored independently from immutable reference-offer
    provider, sportsbook, event, timestamps, kickoff, odds, and line evidence;
  - removal of unsupported development behavior that treated first or last
    stored quotes as opening, closing, or closing-line-value evidence.

  These contracts establish the provenance chain from an exact evaluated offer
  through historical market evidence to a recorded wager. They do not yet
  promote candidates into recommended bets, calculate validated CLV, or define
  empirical edge and exposure thresholds.
- **Recommended-bet qualification [Planned].** Complete the remaining evidence,
  analytical, and policy work that promotes a positive-EV candidate into a
  qualified opportunity or recommended bet.

  Completed foundations:

  - Exact reference-backed bet matching preserves provider, provider event,
    sportsbook, game, market, side, fetch timestamp, line, price, sportsbook
    update time, and kickoff evidence with explicit manual, missing, ambiguous,
    conflicting, and matched states.
  - Historical observations are stored in deterministic season-and-week
    partitions with exact-replay idempotence, same-fetch conflict rejection,
    canonical ordering, and explicit repeated-observation depth.
  - Leakage-safe boundaries preserve earliest-observed and latest-eligible
    non-live pregame observations for each exact provider-aware market identity.
  - Kickoff-aware weekly collection planning, atomic single-shot execution, and
    explicit active-plan selection are implemented.
  - A Raspberry Pi quote-collection worker has been deployed and validated
    manually; repository-owned installation, verification, documentation, and
    recovery remain the active implementation unit.

  Remaining sequence:

  1. Codify the deployed quote-collection worker as repository-owned systemd,
     installation, verification, secret-handling, monitoring, and recovery
     assets.
  2. Accumulate repeated exact-identity pregame quote observations through the
     selected weekly collection plans.
  3. Persist immutable pregame candidate issuance so later analysis evaluates
     only the offers and evidence available before kickoff.
  4. Implement validated same-provider, same-sportsbook closeout using the
     latest eligible pregame boundary. Populate closing fields and calculate
     price or point CLV only when the required evidence exists.
  5. Evaluate model reliability, realized outcomes, CLV, and performance across
     empirical EV cohorts independently for Moneyline, Spread, and Total.
     Derive thresholds from observed results rather than intuitive cutoffs.
  6. Define versioned quote-freshness, fractional-Kelly sizing, duplicate
     exposure, conflicting exposure, per-game concentration, portfolio
     concentration, and correlation policies.
  7. Promote an exact offer only when every mandatory evidence and policy check
     passes. Preserve explicit failed and unavailable reasons when
     recommendation qualification cannot be completed.

  The intermediate goal is satisfied when one exact current sportsbook offer
  can produce a persisted recommended-bet result with exact offer identity,
  selected-product and forecast provenance, evaluated timestamp, policy
  version, supporting checks, unavailable reasons, and suggested stake. The
  system does not place a sportsbook wager.

  Model-favorite status remains descriptive and is not a universal
  recommendation requirement. A positive-EV candidate remains necessary but
  insufficient for recommendation.

10. **Recommendation product integration [Planned].** Add the qualified and
  recommended states to the backend contract, Line Shopping, Available Edges,
  Bet Slip, and recorded-bet workflow only after the qualification policy is
  validated. Present recommendation profile, supporting evidence, unavailable
  checks, suggested stake, and provenance without placing sportsbook wagers.
11. **Derived market opportunities [Planned].** Build arbitrage and middle
  detection on the validated exact-offer comparison contract, preserving book,
  line, price, timing, and execution constraints. Keep these opportunities
  distinct from model-value recommendations.
12. **Market movement and historical evaluation [Planned].** Add append-only
  quote history, opening and closing definitions, leakage-safe pre-kickoff quote
  selection, line-movement and closing-line-value analysis, provider backfill,
  and coverage reporting. Use the historical evidence to validate and recalibrate
  recommendation thresholds rather than choosing intuitive cutoffs.

Current and historical market data are separate workstreams within this
program. The current-market workstream comes first because it unlocks immediate
weekly operation and frontend usability. Historical archive and evaluation
follow after the provider and normalized quote contract are stable.

- **Recurring quote acquisition [Implementation complete; deployment codification active].**
  Kickoff-aware weekly collection planning, bounded
  provider-credit allocation, versioned plan persistence, atomic single-shot
  execution, immutable claims and terminal results, quota-reserve safeguards,
  partial-persistence reporting, and explicit global active-plan selection are
  implemented.

  A Raspberry Pi 4 worker has been validated against the selected 2026 Week 1
  plan using a systemd oneshot service and five-minute timer. The worker runs
  from a 2 TB SSD over the proven stable USB 2 path, resolves the selected plan,
  generates an explicit UTC evaluation time, preserves the provider secret in a
  root-owned environment file, and returns `not_due` without provider access or
  execution artifacts before the first planned poll.

  The active implementation work is to make this deployment reproducible and
  repository-owned. It must preserve:

  - explicit plan generation and selection outside the timer;
  - no season or week inference;
  - no implicit retry or catch-up request;
  - one provider request at most per claimed due poll;
  - non-root service execution;
  - protected secret configuration;
  - systemd journal observability;
  - installation, verification, disablement, and recovery guidance;
  - explicit storage-health checks for the deployed Raspberry Pi worker.

  Full API and frontend appliance hosting on the Raspberry Pi remains a
  separate future deployment decision.

- **Moneyline, Spread, and Total production recommendation proof [Planned].**
  Confirm the complete production chain independently for all three game-market
  families:

  1. canonical schedule and selected weekly prediction product;
  2. repeated exact timestamped multi-book quote observations;
  3. immutable pregame candidate issuance;
  4. versioned qualification, freshness, sizing, and exposure policy;
  5. persisted recommended or explicitly unavailable result;
  6. backend serialization and frontend presentation;
  7. explicit optional recorded-bet action with duplicate protection and
     bankroll transaction semantics;
  8. completed outcome and same-source, same-sportsbook closeout;
  9. Moneyline price CLV or Spread and Total point CLV;
  10. realized performance and a reproducible end-to-end audit trail.

  Moneyline, Spread, and Total require separate empirical acceptance. A policy
  validated for one family must not be assumed valid for another. Production
  confirmation requires a real-week rehearsal proving identity, chronology,
  evidence, recommendation state, presentation, optional recording, closeout,
  and performance without request-time model computation or fabricated market
  evidence.

Current-market scope:

- documented supported API and secret/configuration boundary;
- sportsbook-level current and upcoming moneyline, spread, and total quotes;
- provider, book, event, market, outcome, line, price, and fetch provenance;
- canonical game identity resolution and unmatched-event diagnostics;
- source-neutral snapshot validation and atomic replacement;
- freshness, staleness, partial coverage, malformed response, rate-limit, and
  provider-failure states;
- `verify-week`, unified edge service, API, Dashboard, Games, Game Detail, and
  BetSlip integration.

Historical-market foundations implemented:

- append-only timestamped provider-aware quote observations;
- deterministic season-and-week partitioning;
- exact-replay idempotence and same-fetch conflict rejection;
- explicit temporal coverage and repeated-observation depth;
- leakage-safe earliest-observed and latest-eligible-pregame boundaries;
- exact reference-backed bet matching;
- preservation of missing, conflicting, live, and post-kickoff states.

Historical-market work remaining:

- sufficient repeated real pregame coverage;
- immutable pregame candidate issuance;
- validated same-source, same-sportsbook closeout;
- price and point CLV;
- line and price movement interpretation;
- empirical market-family cohort analysis;
- recommendation-policy calibration;
- supported provider historical backfill if operational collection does not
  provide sufficient coverage;
- strategy backtesting and consensus policies.

The initial normalized quote contract must support both current snapshots and a
future historical archive, but historical ingestion and evaluation are not
acceptance requirements for the first current-provider implementation.

Do not fabricate production prices, collapse provider quotes to one book before
normalization, hide a network fetch inside `weekly-predict`, or treat the legacy
DraftKings adapter as a dependable recovery path.

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
  `uvx pyrefly check .`;
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
