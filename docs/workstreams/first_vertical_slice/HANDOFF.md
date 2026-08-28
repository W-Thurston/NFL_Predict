# HANDOFF.md — Workstream 3 (First Complete Vertical Decision Slice)

> Compressed rehydration only. This file restates the program-level goal
> and the current honest state against it — it does not re-derive
> evidence. Full evidence lives in FINDINGS.md; full program authority
> lives in the root ROADMAP.md, VISION.md, and CONSTITUTION.md.

### Status: IMPLEMENTATION IN PROGRESS. Units 1-3 of 7 closed. Unit 4 (decision-quality evaluation) not yet started.

### The one thing a new thread must never lose: WHY this workstream exists

**Goal:** exercise all ten of VISION.md's first-slice proof obligations
for one market.

**Exit criterion:** every one of the ten obligations is demonstrated end
to end for the chosen market (game spread, locked; moneyline fallback
only).

### Why this workstream opened before Workstream 2 fully closed

WS2 closed all seven planned units but carries two capabilities forward
(D35: general validity/invalidation beyond one field; D36: forward-impact
discoverability, no mechanism selected). Boundary inspection (closed)
found no new evidence for either. Obligation 8 (later observation can
supersede/invalidate downstream artifacts) requires its own dedicated
implementation unit (Unit 5) — it is not being carried forward
indefinitely. D36 remains correctly deferred pending a concrete
forward-impact consumer need, which has not appeared.

### Boundary inspection (closed; full detail in FINDINGS.md)

All five boundaries closed. All ten obligations carry an explicit,
source-confirmed disposition. Four obligations (6, 7, 8, 10) were
confirmed to have a component genuinely missing, not merely
under-evidenced — this became the implementation roadmap below.

### Implementation units closed so far

**Unit 1 — Reproducible decision-time bankroll evidence.** Added
`betting/bankroll.py::bankroll_snapshot_as_of`, a cutoff-scoped,
content-identified bankroll evidence derivation, wired into
`cli/production_chain.py::evaluate_recommendations_cmd` (the sole
production caller of `evaluate_recommendation_issuance`). Hardened
`_write_txn_log` to atomic publication. Confirmed reproducible for a
fixed ledger state; does not claim immutable historical reproduction
independent of ledger concurrency-safety (deferred, no confirmed need).
Two full review rounds; three real bugs caught only by test execution
(wall-clock-dependent test timing, an invalid Settings test double, a
test fixture with the wrong market string). Closed with all gates green.

**Unit 2 — Separate recommendation eligibility from portfolio
allocation.** The existing evaluator coupled eligibility to allocation
amount (`eligible = (...) and sizing.actionable_stake is not None`),
making "eligible recommendation, zero allocation" structurally
unrepresentable. Added `PortfolioAllocationState`/`Reason`/`Result` as an
independent axis; restructured `evaluate_recommendation_candidate` into
explicit Stage 1 (recommendation eligibility, frozen once established)
and Stage 2 (portfolio allocation) phases. Bumped
`RECOMMENDED_BET_RESULT_SCHEMA_VERSION` 2→3 (clean-sheet replace, per
established precedent). Two design options were considered and rejected
before implementation (reusing `QUALIFIED_OPPORTUNITY` as the zero-proof;
manufacturing zero via `minimum_actionable_stake=0`) — both would have
preserved the coupling rather than fixing it. A full review round after
initial implementation found and required four additional corrections
(sizing.actionable_stake was silently overloaded to carry a zero-
allocation amount; the evidence-gate conflated real policy rejections
with genuine evidence gaps; validation checked amounts but not reasons/
cross-consistency; the new axis was never projected through the API).
Closed with all gates green, including a live-server discovery (stale
schema-2 data correctly rejected by the strict decoder — resolved by
regenerating, not migrating).

**Unit 3 — Governed recommendation presentation and action separation.**
Corrected `recommendationPresentation.ts` to consume `decision_state` and
`allocation` alongside `result_state` (the function predated Unit 2's
schema change and had a real, live correctness gap: `result_state`
alone conflated genuine insufficient-evidence with an eligible
recommendation whose allocation simply hadn't been evaluated, and
conflated positive allocation with genuine zero allocation). Wired the
correction through `GameDetail.tsx`, `EdgesTable.tsx`, and
`BetLegCard.tsx`. The action boundary (add-to-slip label, `WhyLink`
subject selection, `BetLegCard`'s sub-heading) is gated on **positive
completed allocation specifically**, not on mere persisted-result
presence — an earlier draft used presence alone and could have labeled a
failed, zero-allocated, or allocation-pending result as an executable
recommendation. A second review round, during verification, found and
required this correction plus a **retroactive Unit 2 backend defect**:
`evaluate_recommendation_candidate`'s evidence-gate had grouped real
policy rejections (exact-duplicate, opposing-position) with genuine
evidence gaps, making two `PortfolioAllocationReason` enum values
unreachable dead code. Fixed as part of this unit, not deferred, since it
was required to make the presented reasons truthful. **Scope decision:**
manual-wager execution mode (follow vs. override) is presentation-only in
this unit — a manually-staged leg retains `persistedRecommendation`
identically to a governed-staged leg; no new field records which action
label was used. Closed with all gates green, including 5 new
component-level tests covering all five recommendation states.

### Governing decisions relevant to this workstream

All of Workstream 1 and Workstream 2 (D27, D30-D37) remain in force.
None were reopened. No new numbered `DECISIONS.md` entry was required by
Units 1-3 — each closed as a direct, evidence-driven correction of an
already-locked contract, not a new architectural choice; if this changes
in a future unit, record it there, not here.

### Process note (carried here per reviewer guidance, not duplicated in each unit's PLAN closure)

Prefer complete, self-contained replacement blocks over prose-described
edits for any change spanning more than a few contiguous lines,
especially for TSX/JSX. A described transform ("replace the return block
with...") has twice produced unusable output in this workstream — once
as a request for content that was never actually pasted into the visible
response, once as a genuine unbalanced-brace syntax error from an
ambiguous edit boundary. Paste-and-confirm the complete result; verify
brace/paren balance against the full current file before running gates.

### Remaining implementation sequence (unchanged from original sequencing; Units 1-3 closed)

4. Decision-quality evaluation contract and first spread evaluation —
   **next.** Confirmed absent by boundary inspection (exhaustive search,
   not merely unfound): no consumer anywhere evaluates a persisted
   recommendation decision against its decision-time policy and evidence,
   as distinct from prediction accuracy or realized wager outcome.
5. Spread-slice supersession and invalidation proof — tests D35 against
   a second real case; closes obligation 8 or forces an explicit
   program-level exit-criterion decision. Do not build a forward-impact
   index unless this unit reveals a concrete need.
6. Scoped ownership and naming cleanup — the bounded low-priority items
   named in FINDINGS.md Boundary 5 (schema-literal replacements, stale
   docstrings, naming risks), folded in wherever a unit naturally touches
   the file, plus the manual-wager execution-mode follow-up from Unit 3
   if the program requires it recorded.
7. End-to-end ten-obligation proof matrix using real, verified artifacts
   — the unit that allows Workstream 3 to close honestly against its
   exit criterion.

### Updated obligation status (deltas from boundary-inspection baseline; full table in FINDINGS.md)

- **Obligation 6** (recommend or abstain): domain logic — Reuse
  (unchanged). Production composition — **no longer Blocked**: Unit 1
  unlocked the positive branch; Unit 2 made eligibility genuinely
  independent of allocation outcome. Presentation — **no longer Adapt**:
  Unit 3 corrected every confirmed-scope surface.
- **Obligation 7** (portfolio can allocate zero despite eligible
  recommendation): **no longer Blocked** — Unit 2 proved this directly
  (a real eligible-recommendation-with-zero-allocation case, from genuine
  capacity exhaustion, confirmed by test).
- **Obligations 8, 10**: unchanged: Partial and Absent respectively,
  pending Units 5 and 4.

### Reading order for a new thread (per AI_BOOTSTRAP.md)

Root `HANDOFF.md` → this file → root `PLAN.md` → root `DECISIONS.md` →
(only if evidence-level detail is needed) `FINDINGS.md`.
