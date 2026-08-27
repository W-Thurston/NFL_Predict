# ROADMAP.md — Workstream 3 (First Complete Vertical Decision Slice)

## Sequencing decision (why this workstream opened before Workstream 2 fully closed)

Workstream 2 (Analytical Claims) closed all seven originally-planned
units (persistence hardening 1-3; identity-evolution 4; capability
protocol 5; attribution ownership 6; cleanup 7). Per the program-level
exit criterion, WS2 is not fully closed: two capabilities from Unit 5's
protocol remain open per D35/D36 — forward-impact discoverability
(confirmed absent everywhere; no mechanism selected) and general
validity/invalidation (Unit 4 delivered one confirmed mechanism for one
field on one artifact; a hypothetical second case was tested against real
source and explicitly rejected as insufficient evidence to generalize
from).

**Decision:** Workstream 3 opened anyway, on the reasoning that building
either mechanism speculatively — without a concrete consumer demonstrating
real need — would contradict D35/D36's own rationale, and that WS3's own
vertical slice was the most plausible source of the missing evidence.
This was not a decision to abandon the two open items; they were carried
forward explicitly, to be resolved if and when WS3's inspection or
implementation surfaced real, concrete evidence of need.

## Workstream 3 — First complete vertical decision slice

**Status: BOUNDARY INSPECTION COMPLETE. Implementation not yet started.**

**Goal:** exercise all ten of VISION.md's first-slice proof obligations
for one market.

**Entry conditions:** Workstreams 1-2 exit met (WS1 closed; WS2 closed
per units, two capabilities carried forward per the sequencing decision
above). First market: game spread (LOCKED); moneyline is fallback only if
spread-specific complexity blocks the architectural proof.

**Must prove — the ten obligations (verbatim from VISION.md):**
1. A mutable source observation is preserved without overwrite.
2. A time-valid analytical claim consumes an exact source version.
3. An estimated output includes honest uncertainty or limitation.
4. A market price remains separate from the prediction.
5. An analytical edge is derived without automatically becoming a
   recommendation.
6. Recommendation policy can recommend or abstain.
7. Portfolio policy can allocate zero despite an eligible recommendation.
8. A later observation can supersede or invalidate downstream artifacts.
9. The original decision remains reproducible.
10. Realized outcome and decision-quality evaluation remain separate.

**Portfolio scope note (locked):** the first implementation proves only
the semantic boundary (an eligible recommendation can receive an
explained zero allocation) — not an advanced correlation model.

**Exit criterion:** every one of the ten obligations is demonstrated end
to end for the chosen market (game spread).

**Method:** per CONTEXT_SWITCH_PLAYBOOK.md Switch B — five-boundary
dual-model inspection (Claude leads, ChatGPT reviews a single canonical
FINDINGS.md). All five boundaries closed. Every named candidate owner
from Boundary 1's inventory was confirmed (Reuse), found to need change
(Adapt), or confirmed absent/empty (Absent/Retire) by direct source
reading — no classification rests on inventory presence alone.

## Boundary inspection outcome (full detail in FINDINGS.md, compressed summary in HANDOFF.md)

All ten obligations carry an explicit disposition. **Four obligations
have a confirmed component requiring implementation work before this
workstream can honestly claim its exit criterion is met:**
- Obligation 6 — Blocked at the production-composition layer (no
  bankroll reaches the governed writer); Adapt at every inspected
  presentation surface.
- Obligation 7 — Blocked entirely (no eligible recommendation has ever
  been produced to test the zero-allocation branch against).
- Obligation 8 — Partial (one field, one artifact); requires a
  dedicated unit to test the general case, or this exit criterion must
  be amended by explicit program-level decision.
- Obligation 10 — Confirmed Absent for its decision-quality half.

## Implementation units (dependency-ordered by proof, not by file or urgency alone)

### Unit 1 — Reproducible decision-time bankroll evidence

**Goal:** build a truthful `BankrollBasis` from operational bankroll
history, or persist an immutable decision-time snapshot; supply it to the
governed recommendation writer (`evaluate_recommendation_issuance`'s sole
production caller); preserve its cutoff, source identity, and
reproducibility.

**Why first:** unlocks the positive recommendation branch. Obligation 7
cannot be tested without an eligible recommendation to allocate against.
Presentation work (Unit 3) needs real recommended/abstained/zero-
allocation states to display, not synthetic ones.

**Definition of done does NOT claim obligation 7 is complete** — only
that bankroll evidence is truthful and reproducible, the writer accepts
it, both absent- and supplied-bankroll paths are tested, the positive
branch becomes reachable, and existing abstention semantics remain
intact. No post-cutoff bankroll transaction may leak into the evidence.
The resulting recommended result must preserve bankroll source identity
and observation time.

May fold in, only if naturally touched: the hardcoded policy schema
literal; the stale "Unit 24" loader docstring.

### Unit 2 — Eligible recommendation and explained-zero portfolio proof

**Goal:** using bankroll evidence from Unit 1, produce two valid,
pinned-evidence cases: (1) an eligible recommendation with positive
allocation, and (2) an eligible recommendation with zero allocation and a
machine-readable explanation. The zero must be a genuine portfolio-policy
outcome after recommendation eligibility — not achieved by passing
`bankroll=0` or by making the recommendation itself ineligible.

**Relationship to Unit 1:** may be combined only if the resulting unit
stays bounded and acceptance tests can clearly separate recommendation
eligibility from portfolio allocation. Default to keeping them separate.

### Unit 3 — Governed recommendation presentation and action separation

**Goal:** update the composed decision surface to explicitly distinguish
analytical candidate, qualified opportunity, failed qualification,
insufficient/conflicting evidence, governed recommendation, manual
wager, and wager-based-on-governed-recommendation — at every presentation
surface confirmed to participate in the path.

**Confirmed scope:** `GameDetail.tsx` (`RecCell`, `ModelLeanCallout`,
`WhyLink` subject selection); `EdgesTable.tsx` ("Policy State" column,
currently a literal empty `<div>`); `BetLegCard.tsx` ("Persisted policy
result" section, currently an empty, accessibility-labeled `<section>`;
`ModelSection`'s analytics panel, currently sourced from
`leg.edgeAnalytics` rather than `leg.persistedRecommendation`); existing
`recommendationPresentation.ts` (reused, not rebuilt); bet-leg creation
and tests. May include the `recordWager.ts` `result_id` completeness fix.

**Must preserve unchanged:** `SlipPanel.tsx`'s recording confirmation —
confirmed sound (branches on `leg.persistedRecommendation`, shows exact
persisted state or explicit absence, requires `window.confirm()`).

### Unit 4 — Decision-quality evaluation contract and first spread evaluation

**Goal:** define the minimum persisted evaluation needed to assess one
recommendation decision separately from model correctness and realized
wager outcome. First implementation should answer: was the decision
evaluated using its exact persisted policy; were mandatory checks
passing/unavailable/conflicting; was recommendation or abstention
consistent with those checks; was sizing consistent with recorded
bankroll/portfolio evidence; can evaluation remain meaningful before the
game result is known; after the result is known, does realized outcome
remain a separate field/artifact.

Avoid building a broad analytics framework prematurely — prove the
minimum contract for one game-spread case first.

May swap order with Unit 3 if implementation convenience favors it.

### Unit 5 — Spread-slice supersession and invalidation proof

**Goal:** using one real or controlled later quote, demonstrate: the
original as-known decision remains reproducible; the later quote changes
the latest-current interpretation; affected downstream artifacts receive
an explicit, artifact-owned status or recomputation outcome; the
difference can be explained without rewriting the original.

This is the proper place to test D35 against a concrete second case. Do
not build a forward index or generalized invalidation mechanism
speculatively — if this unit reveals a genuine need to query all
downstream dependents, *that* becomes the evidence required to revisit
D36, not an assumption made ahead of it. If, after this unit, obligation
8 still cannot be honestly marked satisfied, that must be surfaced as an
explicit program-level exit-criterion question — not silently absorbed.

### Unit 6 — Scoped ownership and naming cleanup

Fold in wherever not already naturally resolved by Units 1-5: remove
`prediction_availability.py` (after import-safety verification); rename
model-side "recommended" language in `visibleRecommendedOffer.ts`;
tighten `recordWager.ts` completeness; remove any remaining "Unit 24"
wording; replace the policy schema literal if not already fixed; decide
whether renaming `market/recommendations.py` is worth its import churn
(optional, evidence-driven, not required to prove the vertical slice).

### Unit 7 — End-to-end ten-obligation proof matrix

**Goal:** verify one real game-spread case against all ten obligations,
not merely trust accumulated unit tests. Produce a durable proof matrix:
for each obligation, the artifact/response proving it, its exact ID/path,
evidence cutoff, relevant method/policy identity, verification
command/test, expected state, and whether the proof is historical or
latest-current.

This is the unit that allows Workstream 3 to close honestly against its
program-level exit criterion.

## Explicitly deferred (not resolved by this workstream, carried forward)

- D36 (forward-impact discoverability) — remains deferred unless Unit 5
  surfaces concrete evidence of need.
- Complete five-level transparency across the full composed application
  (only the recommendation-specific slice is in scope for Unit 3).
- `field-status/`'s remaining 9 of 11 files — explicitly scoped out by
  owner decision during Boundary 5, not required for closure.
