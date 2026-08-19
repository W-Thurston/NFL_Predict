# Market Unit 26 Plan

**Status:** Active, calendar-gated production proof.

**Canonical root documents:** `PLAN.md` and `ROADMAP.md` are available for other active work.

**Historical snapshot:** `docs/archive/market-program-through-unit-26/PLAN.md`.

This document owns the active execution checklist, completed implementation evidence, persisted rehearsal identities, validation record, and final acceptance conditions for Market Unit 26. Update this file whenever Unit 26 evidence changes.

---

### Market Unit 26: Prove the Full Production Recommendation Chain

#### Goal

Prove the complete production recommendation chain for one real completed NFL
week independently for Moneyline, Spread, and Total.

Each market-family proof must begin with one explicitly selected weekly product
and exact forecast provenance, continue through repeated timestamped pregame
quote observations, immutable candidate issuance, versioned qualification and
recommendation policy evaluation, and persisted recommendation or explicit
unavailability, and remain visible through backend serialization and frontend
presentation.

Where an explicit wager is recorded, the proof must preserve duplicate
protection, ledger provenance, and bankroll transaction semantics without
placing a sportsbook wager.

Each proof must finish with a completed game outcome, validated same-provider
and same-sportsbook closeout evidence, market-appropriate closing-line value,
realized performance, and one reproducible chronological audit record.

Moneyline, Spread, and Total require separate empirical acceptance. Evidence
for one market family cannot satisfy another.

#### Implemented Evidence Chain

The real 2026 Week 1 rehearsal now proves the complete pregame and
recommendation middle chain for Moneyline, Spread, and Total.

The explicitly selected weekly product and its immutable forecast provenance
resolve successfully for all 16 scheduled games. The current quote snapshot is
available, and the canonical weekly quote ledger contains 1,680 observations at
two distinct UTC fetch timestamps. Every exact historical identity has repeated
observation depth: 274 Moneyline identities, 282 Spread identities, and 284
Total identities each have two distinct fetch timestamps.

Explicit candidate issuance evaluated every canonical historical observation
before kickoff and persisted one immutable artifact with 698 candidates, 982
not-candidates, and zero unavailable rows. Candidate counts are independent by
market family: 228 Moneyline, 226 Spread, and 244 Total.

Explicit recommendation governance is persisted under a deterministic
content-derived identity. A recommendation policy was derived from the exact
candidate issuance, governed inputs, historical boundaries, current outcome
availability, and available closeout and return evidence. Moneyline, Spread,
and Total each remain explicitly unavailable because required completed-outcome,
closeout, and return evidence does not yet exist.

The exact issuance was evaluated against the exact persisted policy. All 698
candidate rows received immutable recommended-bet results. Every result is
truthfully unavailable, with no qualified, recommended, failed, or conflicting
results and no actionable stake.

The product now separates an immediate model-recommended bet from the stricter
governed qualification result. The backend selects one model-recommended
direction and one globally preferred exact offer for every available game and
market. Moneyline follows the higher modeled win probability. Spread and Total
select the exact wager with the strongest modeled cover probability. Stable
line, price, and exact-offer ordering resolve remaining ties.

The real Week 1 response contains 48 model recommendations across 16 games and
three market families, with exactly one recommendation per game and market and
zero duplicate game-market selections. Line Shopping renders the recommended
exact offer as one green cell. Recommendation badges, repeated lifecycle labels,
persisted suggested-stake callouts, and Policy evidence disclosures were removed
from the primary betting surfaces.

Sportsbook preferences do not change the backend-recommended direction. The
backend marks every exact offer on that direction, while the frontend selects
one best visible offer after applying the user's sportsbook filter. If the
globally preferred sportsbook is deselected, the green highlight moves to the
best remaining offer on the same side. If no selected sportsbook offers that
side, the interface shows no green recommendation rather than switching to the
opposing side.

The immutable governed policy and recommended-bet result artifacts remain a
separate qualification, sizing, exposure, and audit boundary. Their current
Week 1 results remain unavailable because mature empirical outcome, closeout,
CLV, and return evidence does not yet support an active threshold-selection
method. That governed unavailability no longer prevents the model from providing
an immediate pregame recommendation.

Production-chain preflight now validates candidate issuance, recommendation
policy, and recommendation evaluation through strict artifact readers and exact
identity relationships. It reports malformed evidence as invalid and ambiguous
matching evidence as conflicting rather than selecting the latest file by
recency.

Collection-execution readiness now uses the explicitly selected collection plan,
the existing due-state evaluator, and immutable claim and result receipts. The
two manual August 18 quote ingestions establish historical observation depth but
do not count as execution of the selected plan.

Postgame proof assembly is wired once per preflight assessment through the
existing selected-product outcome closeout, exact candidate market closeout,
market-specific CLV, historical-boundary, market-family evaluation, cleaned-game,
and optional settled-wager owners. Before the earliest kickoff, those owners are
short-circuited and all postgame states remain not yet eligible.

#### Persisted Rehearsal Identities

- Candidate issuance:
  `278d60da4e2dc089ff7eb973620f49050f83de336034cbff0c8c1a097401ccff`
- Recommendation governance:
  `56757db59c2d04a55eb3f980299699403fdc982e4fe7ff4963f0898112f4824e`
- Recommendation policy:
  `9e2cc3363656366eae76ec0935f01ff201ce9c9784e2736936fd0af9ab0ab024`
- Recommended-bet evaluation:
  `8301fb74e1eaa10437376ff3b616aaa1efc3477944d1a8da0df94abd55de073c`
- Persisted production-chain checkpoint:
  `acf50214f67aed1833e38f998685c3bde4f8f5489a3771f1e50adc319bb887fb`

#### Current Preflight State

The following components are available independently for Moneyline, Spread, and
Total:

- selected weekly product;
- exact forecast provenance;
- current quote snapshot;
- repeated canonical quote history;
- explicitly selected collection plan;
- exact immutable candidate issuance;
- exact persisted recommendation policy;
- exact persisted recommendation evaluation and governed results;
- one backend-owned model-recommended direction and exact offer per game-market;
- 48 real Week 1 model recommendations with no duplicate game-market selection;
- selected-sportsbook fallback within the same recommended direction;
- generated API and frontend contracts for exact-offer and recommended-side evidence;
- one-green-cell Line Shopping presentation with Recommended bet as the only
  default highlight;
- governed policy evidence retained separately from primary presentation.

The following components remain not yet eligible:

- selected-plan collection execution, whose first scheduled poll is
  `2026-09-08T12:00:00Z`;
- completed game outcomes;
- latest-eligible pregame market closeout;
- market-specific CLV;
- realized performance.

Recorded-wager evidence remains unavailable and optional. Gridiron Edge records
wagers locally only through explicit user action and does not place sportsbook
wagers.

#### Remaining Implementation and Operational Work

1. Allow the repository-owned quote-collection worker to execute the selected
   Week 1 plan and persist immutable claim and terminal-result receipts for due
   polls.
2. Reassess collection execution from those exact receipts without treating
   manual quote ingestion as planned execution.
3. Refresh cleaned completed-game outcomes after Week 1 games finish and verify
   selected-product outcome reconciliation.
4. Evaluate every exact candidate reference against the canonical quote ledger
   using latest-eligible, non-live, strictly pre-kickoff evidence from the same
   provider, provider event, sportsbook, game, market, and side identity.
5. Verify Moneyline price CLV, Spread point CLV, and Total point CLV independently
   with no cross-family substitution.
6. Evaluate realized performance only from uniquely attributed settled-wager
   evidence. Preserve unavailable return evidence when no wager was recorded.
7. Persist one or more postgame production-chain assessments at explicit UTC
   timestamps and verify exact replay without repository reassessment.
8. Complete the final frontend review after real postgame evidence is available.
   Presentation cleanup may improve density and readability but must not change
   persisted recommendation semantics.
9. Close Market Unit 26 only after one real completed week satisfies independent
   Moneyline, Spread, and Total acceptance or records an explicit evidence-backed
   unavailable state for a component that cannot validly become available.

#### Tests and Real-Data Validation to Date

- Repository Ruff, Pyrefly, focused unit, integration, API, and frontend gates
  pass for the implemented candidate, governance, policy, recommendation,
  preflight, collection-execution, and postgame-assembly boundaries.
- Two live The Odds API ingestions returned 840 quotes each across 16 games and
  nine sportsbooks and produced a 1,680-row canonical Week 1 history partition.
- Repeated-history validation confirmed two timestamps for every current exact
  Moneyline, Spread, and Total identity.
- The real candidate issuance evaluated all 1,680 historical observations and
  persisted 698 candidates with zero unavailable rows.
- The real policy preserved unavailable family states because completed
  outcomes, closeouts, and returns are not yet available.
- The real recommendation evaluation persisted 698 unavailable results and zero
  recommended results.
- The real Line Shopping response produced 48 immediate model recommendations:
one for each of 16 games across Moneyline, Spread, and Total, with a maximum of
one recommendation per game-market and zero duplicate game-market selections.
- Real-data verification confirmed that Moneyline recommendation direction
follows the higher win probability, including Seattle over New England, the
Rams over San Francisco, and Pittsburgh over Atlanta in the current Week 1
product.
- Frontend verification confirmed that Recommended bet is the only highlight
enabled by default in a clean browser state, one green exact-offer cell presents
the model recommendation, and +EV, best-line, best-price, and model-favorite
layers remain independently optional.
- Sportsbook-filter coverage confirmed that deselecting the globally preferred
book moves the green recommendation to the best remaining offer on the same
backend-recommended side without manufacturing an opposing recommendation.
- Recommendation badges, Policy evidence dropdowns, and primary-surface
persisted suggested-stake callouts remain absent while the underlying governed
audit contracts remain available.
- Exact production-chain verification reads persisted assessments without
  reassessing mutable repository evidence.
- The selected collection plan contains 34 planned polls across six kickoff
  groups, begins at `2026-09-08T12:00:00Z`, and remains not yet eligible at the
  current rehearsal timestamp.

#### Acceptance

Market Unit 26 remains active.

The pregame and recommendation middle chain is implemented and exercised with
real 2026 Week 1 market data. The product now provides one immediate
backend-owned model recommendation per available game-market before kickoff,
selects one best visible exact offer within the user's sportsbook preferences,
and presents it as one green cell without badges or policy disclosures.

The stricter governed policy chain remains persisted under exact immutable
identities, strictly revalidated by production-chain preflight, and separate
from the immediate model recommendation. Its current unavailable state is valid
audit evidence and does not suppress pregame model usefulness.

Final acceptance remains blocked only by future selected-plan execution and
postgame evidence. The unit is complete when a real completed NFL week provides
independent Moneyline, Spread, and Total proof through completed outcomes,
validated exact-identity closeouts, correct market-specific CLV, realized
performance when settled-wager evidence exists, and a persisted chronological
production-chain assessment.
