## PLAN.md — Active implementation unit (Workstream 3)

Exactly ONE active unit. Future units live in ROADMAP.md, not here.

### Unit — Scoped ownership and naming cleanup

#### Completed

Removed the empty, unreferenced
`models/game_prediction/prediction_availability.py`; retained the actual
owners in `prediction_policy.py` and `availability.py`.

Renamed the analytical edge-report module from
`market/recommendations.py` to `market/edge_report.py` and migrated all
checked-in imports, exports, tests, and schema documentation. Renamed the
Line Shopping visible-offer helper and local presentation vocabulary
from governed-recommendation language to model-selected terminology
while preserving generated backend wire-field names.

Tightened `recordWager.ts` so recommendation provenance is emitted only
when result, evaluation, candidate-reference, and policy identities are
all nonempty; incomplete provenance emits all four identities as null.

Removed the two remaining live "Unit 24" docstrings while retaining
historical planning and decision references. Replaced decision-quality's
generic positive policy-schema check with exact
`RECOMMENDATION_POLICY_SCHEMA_VERSION` compatibility.

#### Goal

Fold in wherever not already naturally resolved by Units 1-5: remove
`prediction_availability.py` after import-safety verification; correct
model-side recommendation vocabulary; tighten wager-recording
completeness; remove remaining live "Unit 24" wording; replace the policy
schema literal; and decide the optional analytical module rename from
current evidence.

#### Files Added/Removed/Changed

- Removed
  `src/gridiron_edge/models/game_prediction/prediction_availability.py`.
- Renamed `src/gridiron_edge/market/recommendations.py` to
  `src/gridiron_edge/market/edge_report.py` and renamed its owning test.
- Renamed `visibleRecommendedOffer.ts` and its test to
  `visibleModelSelectedOffer.ts`; updated Line Shopping, game-card, CSS,
  and test consumers.
- Changed `frontend/src/components/betslip/recordWager.ts` and its tests.
- Changed `src/gridiron_edge/market/decision_quality.py` and its store
  tests.
- Changed live loader/fixture docstrings and migrated edge-report imports,
  exports, and generated schema references.

#### Tests

Focused backend tests:

`uv run pytest ...`

201 passed.

Focused frontend tests:

`npm --prefix frontend test -- --run ...`

33 passed.

Full backend quality gates:

`uv run ruff check . --fix && uvx pyrefly check && uv run pytest -m "unit and not slow"`

[record actual result]

Full frontend quality gates:

`npm --prefix frontend test -- --run && npm --prefix frontend run build`

[record actual result]

#### Acceptance

The dead prediction-availability module is removed with zero consumers
remaining. Analytical edge-report and Line Shopping ownership use
lasting terminology distinct from governed recommendations. Wager
recording cannot emit a partial recommendation identity chain. Live
development-era "Unit 24" wording is absent from source and tests.
Decision-quality accepts only the recommendation-policy schema it
actually understands. Generated contracts and all checked-in consumers
use the renamed owners. Focused and full backend/frontend gates pass.
