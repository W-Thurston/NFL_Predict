# PLAN.md — Active implementation unit

Exactly ONE active unit. Future units live in ROADMAP.md, not here.

### Unit — Small API and documentation cleanup

#### Completed

Three independent items, all confirmed against fresh source before any
edit (per this workstream's established discipline; item 3 was
deliberately blocked mid-unit pending fresh reads of
`recommendation_policy.py` and `betting/ledger.py` rather than proceeding
from a months-old Boundary 2 characterization).

**Item 1 — Empty-`/portfolio/splits` D18 violation, fixed.**
`get_portfolio_splits`'s empty-ledger branch now calls
`serialize_splits(pd.DataFrame(), dimension)` instead of directly
constructing `PortfolioSplits(...)`, restoring the
`_meta.field_status`/`NO_SPLIT_DATA` metadata the direct-construction path
was silently omitting. Confirmed against the canonical `DECISIONS.md` D18
text; no new decision required, since D18 already governs this rule.

**Item 2 — `api/serializers/portfolio.py` D19 mis-citation, corrected.**
Module docstring no longer attributes its no-I/O, already-loaded-DataFrames
behavior to D19 (which governs `api/loaders.py`'s settings-threading, a
different concern). The factual behavioral description is retained without
a citation, per the locked disposition from Boundary 5's reconciliation —
no new decision drafted, since re-citing D18 was explicitly rejected as
governing a different specific rule.

**Item 3 — Development-era "Unit 22"/"Unit 24" phase-naming, cleaned up.**
Seven confirmed instances (not six — `log_bet`'s own `Args:` docstring
block carried two separate "Unit 24" mentions, previously undercounted),
each reviewed individually and replaced with accurate domain language, not
a blind find-and-replace:
- `recommendation_policy.py` module docstring and
  `empirical_evidence_fingerprint`'s docstring: "Market Unit 22"/"Unit 22
  evidence" → "empirical market-family evidence"/"empirical evidence."
- `betting/ledger.py::_validate_recommendation_provenance`'s docstring,
  plus two lines in `log_bet`'s `Args:` block: "Unit 24 recommendation
  identities"/"Unit 24 result identity"/"Unit 24 evaluation identity" →
  "recommendation identities"/"recommendation result identity"/
  "recommendation evaluation identity."
- `api/schemas/portfolio.py::RecordBetRequest`'s docstring: "Unit 24
  identity chain" → "recommendation identity chain."
- `api/schemas/recommendations.py::RecommendationPresentation`'s
  docstring: "Unit 24 result" → "recommendation result."
- `test_recommended_bet_result.py`'s test function name itself (not just a
  docstring — the fifth confirmed instance was a function name):
  `test_unit24_module_has_no_request_or_mutation_dependency` →
  `test_market_module_has_no_request_or_mutation_dependency`. Test body
  unchanged; only the name, since it should describe what the test
  verifies (the module's import boundary), not when it was written.

**One stray artifact caught and removed during review**, not part of the
locked design: a copy/paste path-header comment
(`# src/gridiron_edge/market/recommendation_policy.py`) had been carried
into the real diff from how the file was pasted into the design
conversation. Removed before commit, consistent with this project's
established convention against unnecessary source-path comments (first
flagged during Unit 3's review).

**One consequence surfaced and resolved, not anticipated in the original
plan:** Item 1's route-behavior change (the empty-splits response now
includes real `_meta.field_status` metadata it previously omitted) altered
the live OpenAPI contract, exactly as Unit 4's API-exposure change did.
`api-schema.json` and the frontend's generated `schema.ts` were
regenerated to match.

#### Goal

Three confirmed, independent, low-risk items: fix the empty-`/portfolio/splits`
D18 violation; correct `api/serializers/portfolio.py`'s D19 mis-citation;
clean up development-era phase-naming, reviewed per-instance.

#### Files Changed

- `src/gridiron_edge/api/routes/portfolio.py` — `get_portfolio_splits`'s
  empty branch now delegates to `serialize_splits`; added `import pandas
  as pd`.
- `src/gridiron_edge/api/serializers/portfolio.py` — module docstring's
  D19 citation removed.
- `src/gridiron_edge/api/schemas/portfolio.py` — `RecordBetRequest`
  docstring cleaned up.
- `src/gridiron_edge/api/schemas/recommendations.py` —
  `RecommendationPresentation` docstring cleaned up.
- `src/gridiron_edge/betting/ledger.py` —
  `_validate_recommendation_provenance` docstring and `log_bet`'s `Args:`
  block cleaned up (three instances in this file, not one).
- `src/gridiron_edge/market/recommendation_policy.py` — module docstring
  and `empirical_evidence_fingerprint` docstring cleaned up.
- `tests/unit/market/test_recommended_bet_result.py` — one test function
  renamed.
- `api-schema.json`, `frontend/src/api/schema.ts` — regenerated to reflect
  item 1's response-shape change.

#### Tests

`uv run ruff check . --fix && uvx pyrefly check && uv run pytest -m "unit and not slow"`
passed; all tests green, including
`tests/unit/cli/test_api.py::TestExportSchema::test_checked_in_schema_matches_application`
after regeneration. `frontend`: `pnpm gen:api && pnpm build` passed.

#### Acceptance

All three items are confirmed fixed against fresh, current source — item 3
was explicitly held blocked mid-unit rather than proceeding from a
remembered characterization of `recommendation_policy.py`/`betting/ledger.py`,
consistent with the correction discipline established in Units 2 and 4.
Seven phase-naming instances are corrected, not six, following direct
recount rather than the original estimate. No new `DECISIONS.md` entries
were required for any of the three items — items 1 and 2 apply
already-locked decisions (D18, D19/D20 boundary); item 3 is documentation/
naming only, with no behavioral change. This closes Workstream 2's
originally-planned seven-unit sequence.
