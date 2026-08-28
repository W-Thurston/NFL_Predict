# PLAN.md — Active implementation unit (Workstream 3)

Exactly ONE active unit. Future units live in ROADMAP.md, not here.

### Unit — Decision-quality evaluation contract and first spread evaluation

#### Completed
- Extracted one shared realized-outcome grader
  (`market/candidate_outcome.py`) and migrated all consumers, including
  one discovered only by a pre-edit repository search
  (`evaluation/historical_backtest.py`). The grader now explicitly
  rejects an unsupported market rather than silently defaulting to total
  grading.
- Added immutable decision-quality evaluation and persistence contracts
  (`market/decision_quality.py`, `market/decision_quality_store.py`).
- Added canonical parent-manifest validation
  (`validate_recommended_bet_evaluation`) shared by both
  `recommended_bet_result_store.py` and `decision_quality.py` — the
  store's prior private duplicate is removed; both the write and read
  paths now call the single public owner, which itself validates every
  child result and all parent identity-bearing fields.
- Added policy, candidate, and optional allocation-replay consistency
  checks, each resolving and validating its referenced artifact directly
  rather than trusting a claimed identifier.
- Preserved realized outcome as an identity-bearing but
  decision-status-independent field; a locked design choice
  (`evaluated_at` is included in decision-quality identity, since each
  evaluation call is a distinct event) is stated explicitly rather than
  left implicit.
- Added a canonical game-spread proof (real computed parent-manifest
  identity, not a placeholder) and strict store tests, including
  temporary-write and hard-link failure paths.
- Corrected `grade_candidate_outcome` to validate the market/side
  contract before inspecting scores, so an invalid row is rejected
  regardless of whether outcome evidence happens to be available yet.
  `historical_backtest.py`'s incidental lint-driven simplifications
  (redundant cast/wrapper removal) were retained alongside its import
  update, without behavioral change.

#### Goal
Define the minimum persisted evaluation needed to assess one
recommendation decision separately from model correctness and realized
wager outcome, for one canonical game-spread case.

#### Files Changed
- `src/gridiron_edge/market/candidate_outcome.py` — new.
- `src/gridiron_edge/market/market_family_evaluation.py`,
  `src/gridiron_edge/evaluation/historical_backtest.py` — consume the
  shared grader.
- `src/gridiron_edge/market/recommended_bet_result.py` — new
  `validate_recommended_bet_evaluation`, new `_require_digest`.
- `src/gridiron_edge/market/recommended_bet_result_store.py` — write and
  read paths migrated to the public parent-evaluation validator; private
  duplicate removed.
- `src/gridiron_edge/market/decision_quality.py` — new; canonical
  identity ownership consolidated into `decision_quality_evaluation_id`,
  which accepts the evaluation object directly (no caller independently
  constructs the identity payload).
- `src/gridiron_edge/market/decision_quality_store.py` — new.
- `tests/unit/market/test_candidate_outcome.py`,
  `tests/unit/market/test_decision_quality.py`,
  `tests/unit/market/test_decision_quality_store.py` — new.
- `tests/unit/market/test_market_family_evaluation.py` — import updated.

#### Tests
`uv run ruff check . --fix && uvx pyrefly check && uv run pytest -m "unit and not slow"`
— green.

#### Acceptance
One shared public grader owns all market grading and rejects unsupported
markets explicitly. A persisted, schema-versioned decision-quality
evaluation resolves its target result from a genuinely, fully validated
parent manifest (every child result validated, every parent identity
field checked), not a caller-supplied or partially-checked string. The
evaluator validates its own constructed artifact before returning it.
Canonical decision-quality identity has exactly one owner, which every
caller — the evaluator, the validator, and the test fixture — uses
identically. A real, monkeypatched replay-disagreement seam is proven to
flip the public evaluator's overall status to `INCONSISTENT`, honestly
described as a seam test rather than an independently-valid second domain
replay. Missing original allocation evidence does not lower an otherwise-
consistent conclusion. The canonical case uses a real, internally valid
game-spread candidate with a genuinely computed parent-manifest identity.
Persistence failure paths (hard-link and temporary-write) are both
covered. The unit is implemented, validated against real game-spread
evidence, and closed with all focused and full quality gates passing.
