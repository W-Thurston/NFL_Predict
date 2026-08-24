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

## Unit 1 — Point-in-time quote evidence retrieval

### Completed

A single owned cutoff-visible quote-evidence operation, `as_known_at(observations,
cutoff)`, now returns the canonical quote observations whose system-known time
(`fetched_at`) is at or before an inclusive, UTC-validated decision cutoff. It
validates and normalizes input through the canonical quote contract
(`validate_quote_rows`), never mutates the input, returns a fresh
canonically-ordered frame, and returns the canonical empty quote frame when nothing
is visible. Visibility (`fetched_at <= cutoff`) is kept strictly separate from
downstream pregame eligibility (`is_live is False and fetched_at < commence_time`);
the two are never fused. The production `issue-candidates` command now routes the
loaded ledger through this operation before candidate issuance, so no observation
learned after the declared evaluation time can enter an issuance. The observed
history selector is reused by composition and is unchanged.

### Goal

Guarantee point-in-time correctness of quote evidence at the decision boundary: a
backdated evaluation can only consider observations that were system-known by the
declared cutoff, without weakening the canonical quote contract and without
conflating system visibility with pregame eligibility.

### Files Added/Removed/Changed

Added:
- `src/gridiron_edge/ingest/odds/as_known.py` - Cutoff-visible quote-evidence
  operation (`as_known_at`) and `CutoffError`; validates canonical rows, applies the
  inclusive UTC `fetched_at` boundary, returns a fresh canonically-ordered frame or
  the canonical empty quote frame.
- `tests/unit/ingest/odds/test_as_known.py` - Focused coverage of the cutoff
  operation: inclusive boundary, exclusion, canonical empty results, non-mutation,
  schema preservation, deterministic ordering, UTC-cutoff rejection, incomplete-schema
  rejection, and naive-observation-timestamp rejection.
- `tests/unit/ingest/odds/test_as_known_composition.py` - Composition with the
  observed history selector: post-cutoff observations cannot affect counts,
  repeated-evidence flags, kickoff-conflict state, or latest-eligible selection;
  inclusive visibility with strict pregame eligibility; visible live rows retained
  but not selected.

Changed:
- `src/gridiron_edge/cli/production_chain.py` - `issue_candidates_cmd` applies the
  cutoff-visible operation to the loaded ledger before candidate issuance, closing the
  point-in-time visibility gap at the production boundary.
- `tests/unit/cli/test_production_chain_cli.py` - Replaced the prior assertion that
  the raw ledger passed straight through with canonical-fixture coverage proving
  post-cutoff observations are excluded and every visible observation is preserved;
  added coverage for a fully-post-cutoff ledger yielding a valid zero-row issuance;
  aligned the write-path fixture with the canonical empty quote frame the real loader
  returns.

Removed:
- None.

### Tests

- `uv run ruff check . --fix && uvx pyrefly check && uv run pytest -m "unit and not slow"`
  passed; all tests green.
- Focused suites: `tests/unit/ingest/odds/test_as_known.py`,
  `tests/unit/ingest/odds/test_as_known_composition.py`, and
  `tests/unit/cli/test_production_chain_cli.py` all pass.
- Real-artifact verification (read-only, checksum-guarded) against
  `data/odds/history/season=2026-2027/week=01/observations.parquet`:
  cutoff `2026-08-18 14:30:00 UTC` reduced 1,680 input observations to 840 visible
  observations, retaining only `fetched_at = 2026-08-18 14:23:18.347996 UTC`;
  canonical schema and ordering verified; source parquet SHA-256 unchanged.

### Acceptance

The cutoff-visible operation enforces the canonical quote contract, applies an
inclusive UTC `fetched_at` boundary distinct from pregame eligibility, is read-only
and deterministically ordered, and returns a first-class canonical empty result. The
production issuance path is point-in-time correct for a backdated evaluation time.
Real-ledger verification confirms correct filtering and source immutability. The
observed history selector remains unchanged and is reused by composition. Quality
gates and all tests pass. The unit is implemented, validated, documented, and ready
for downstream use.
