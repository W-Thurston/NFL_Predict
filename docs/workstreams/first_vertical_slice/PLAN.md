# PLAN.md — Active implementation unit (Workstream 3)

Exactly ONE active unit. Future units live in ROADMAP.md, not here.

### Unit — Governed recommendation presentation and action separation

#### Completed

Corrected `recommendationPresentation.ts` to consume `decision_state` and
`allocation` alongside `result_state`, distinguishing persisted-result
presence, recommendation eligibility, and positive completed allocation
as three separate facts. Wired the correction through every confirmed-
scope surface (`GameDetail.tsx`'s `RecCell`/`ModelLeanCallout`/`WhyLink`
selection, `EdgesTable.tsx`'s Policy State column, `BetLegCard.tsx`'s
persisted-policy section and `ModelSection` addition). The action
boundary in `GameDetail.tsx` gates the add-to-slip label and the
`BetLegCard.tsx` `ModelSection` sub-heading on positive completed
allocation specifically (`hasPositiveAllocation`), not on mere persisted-
result presence.

The unit primarily owns presentation and action separation. During
verification, it corrected one directly related Unit 2 backend
allocation-classification defect required to make the presented
duplicate and opposing-position reasons reachable and truthful:
`evaluate_recommendation_candidate`'s Stage 2 evidence-gate had grouped
`exact_duplicate`/`opposing_position` (real, completed policy rejections)
together with genuine evidence-unavailability checks, making
`PortfolioAllocationReason.EXACT_DUPLICATE_FOUND`/`OPPOSING_POSITION_FOUND`
unreachable dead enum values. Corrected the evidence-gate to test only
genuine evidence availability; a real duplicate or opposing-position
rejection now produces a completed `ZERO_ALLOCATION` with its specific
reason.

Scope decision: manual-wager execution mode is presentation-only in this
unit. A leg staged via "Add as manual wager" retains
`persistedRecommendation` as reference evidence identically to a leg
staged via "Add recommendation to bet slip" — no new field distinguishes
follow-versus-override at the data layer. This unit closes correct
labeling and gating at the point of staging; it does not persist whether
the user followed or overrode a governed allocation. A follow-up unit is
required to close that distinction if the program requires it recorded.

#### Goal
Update the composed decision surface to explicitly distinguish analytical
candidate, qualified opportunity, failed qualification, insufficient/
conflicting evidence, governed recommendation, manual wager, and
wager-based-on-governed-recommendation — at every presentation surface
confirmed to participate in the path.

#### Files Changed
- `frontend/src/components/recommendations/recommendationPresentation.ts` —
  corrected state logic; added `isRecommendationEligible`,
  `hasPositiveAllocation`, `formatAllocationReason`,
  `recommendationToneColor`, exported `assertNever`; exhaustiveness guards
  on both switches.
- `frontend/src/screens/GameDetail.tsx` — `RecCell`, `ModelLeanCallout`,
  `WhyLink` subject selection.
- `frontend/src/components/betslip/EdgesTable.tsx` — Policy State column.
- `frontend/src/components/betslip/BetLegCard.tsx` — persisted-policy
  section; `ModelSection` governed-recommendation sub-section with
  eligibility-gated heading.
- `frontend/src/utils/betLegs.ts` — `parsePersistedRecommendation`
  validates `allocation`'s presence (shallow structural guard; state/
  reason/amount content validation remains backend-authoritative, not
  independently re-validated client-side).
- `frontend/src/components/recommendations/recommendationPresentation.test.ts`,
  `frontend/src/screens/GameDetail.test.tsx` — corrected and expanded.
- `src/gridiron_edge/market/recommendation_policy.py` —
  `PortfolioAllocationReason` gains `EXACT_DUPLICATE_FOUND`/
  `OPPOSING_POSITION_FOUND`; `evaluate_recommendation_candidate`'s Stage 2
  evidence-gate corrected.
- `tests/unit/market/test_recommendation_policy_evaluation.py` — one
  consolidated test each for the duplicate and opposing-position cases,
  replacing the prior pair of overlapping/stale tests.
- `api-schema.json`, `frontend/src/api/schema.ts` — regenerated.

#### Tests
Backend: `uv run ruff check . --fix && uvx pyrefly check && uv run pytest -m "unit and not slow"`
— green. Frontend: `pnpm lint && pnpm build && pnpm test:run` — green,
487 tests, including 5 new `GameDetail` component tests covering
candidate/ineligible/pending/zero/positive-allocation states and the
action-label gating for each, plus expanded `recommendationPresentation`
helper tests for eligibility/allocation distinctions.

#### Acceptance
Every confirmed-scope presentation surface reads the actual persisted
recommendation/allocation state, verified at the component level, not
only via helper-function tests. The action boundary gates on positive
completed allocation specifically, confirmed by test for all five
recommendation states (none, failed, pending, zero, positive) — a
persisted result that is failed, insufficient, conflicting, allocation-
pending, or zero-allocated is never labeled or rendered as an executable
governed recommendation. `BetLegCard.tsx`'s `ModelSection` sub-heading is
similarly eligibility-gated, not presence-gated. The retroactive backend
correction makes `PortfolioAllocationReason.EXACT_DUPLICATE_FOUND`/
`OPPOSING_POSITION_FOUND` live, reachable, tested outcomes. Manual-wager
execution mode remains presentation-only in this unit, per the scope
decision above; a follow-up unit is required to persist the follow-versus-
override distinction if needed. The unit is implemented, its design and
subsequent correction rounds verified against real tool execution, and
closed.
