# PLAN.md — Active implementation unit

> Exactly ONE active unit. Future units (Unit 2–4) live in `ROADMAP.md`, not here.
> Derived from the locked Boundary 8 consolidation (FINDINGS rev 8). Governed by
> CONSTITUTION → VISION → ROADMAP → DECISIONS (**D28, D29**).

## Repository basis
- Inspected code commit: `<EXISTING_CODE_SHA>`
- Inspection snapshot: `WS1-SNAPSHOT-001`
- Findings revision: 8
- Context package revision: `WS1-REV-8`
- Working tree at inspection: clean
- Context package commit: identify through Git history for this file

### Unit — Candidate reference exact over canonical observation identity

#### Completed

The cross-artifact candidate reference (`candidate_issuance_row_id`) now hashes the
complete canonical observation identity, including `sportsbook_updated_at` and
`is_live`, which were previously omitted. The reference is therefore injective over
the canonical observation identity represented by `CandidateIssuanceRow`: two
canonically-distinct observations can no longer collapse to the same reference. The
external reference shape (`issuance_id:sha256`) and issuance scope are unchanged, and
every downstream consumer that re-derives the reference from an issuance row
(recommendation policy resolution, recommended-bet result resolution and validation,
and market closeout) remains consistent without modification because those rows
already carry both fields.

#### Goal

Guarantee that the immutable candidate reference used across recommendation policy,
recommended-bet results, and market closeout uniquely identifies exactly one
canonical observation, preserving the integrity of the exact evidence chain.

#### Files Added/Removed/Changed

Added:
- None.

Changed:
- `src/gridiron_edge/market/candidate_issuance.py` - `candidate_issuance_row_id`
  identity payload now includes `sportsbook_updated_at` (null-safe ISO-8601 or null)
  and `is_live`, making the reference exact over the complete canonical observation
  identity. Docstring updated to describe the complete-identity contract.
- `tests/unit/market/test_candidate_issuance.py` - Added focused injectivity
  coverage: references differ when two otherwise-identical rows differ only in
  `sportsbook_updated_at`, only in `is_live`, or in the presence of
  `sportsbook_updated_at`; added a shared canonical-row helper and clarified the
  stable-and-exact test name.

Removed:
- None.

#### Tests

- `uv run ruff check . --fix && uvx pyrefly check && uv run pytest -m "unit and not slow"`
  passed; all tests green.
- Focused suites exercising the cross-artifact re-derivation end to end:
  `tests/unit/market/test_candidate_issuance.py`,
  `test_candidate_issuance_evaluation.py`,
  `test_recommendation_policy_evaluation.py`,
  `test_recommended_bet_result.py`, and `test_market_closeout.py` all pass. The
  result and closeout suites re-derive the reference from reconstructed rows, so a
  green run confirms the four consumers agree under the new hash.
- Persisted-artifact check (WoW #8): `rg` located embedded `candidate_reference_id`
  values only under `data/output/recommended_bet_results/`. That directory is
  git-ignored development-state evidence, not a committed artifact; it implements the
  previous reference contract and regenerates through the production-chain commands.
  No committed artifact embeds or is addressed by the reference (candidate issuance
  files are addressed by `issuance_id`, which is unchanged). No repository artifact
  required regeneration for this commit.

#### Acceptance

The candidate reference is issuance-scoped and injective over the canonical
observation identity; canonically-distinct rows differing only in
`sportsbook_updated_at` or `is_live` now produce distinct references. Downstream
resolution ("resolve to exactly one") and result validation remain valid because the
reconstructed rows carry both fields. The external `issuance_id:sha256` format is
preserved. Quality gates and all tests pass. No committed artifact depends on the
prior reference. The unit is implemented, validated, documented, and ready for
downstream use.
