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

### Unit — Truthful quote-history coverage counts

#### Completed

The quote-history coverage diagnostic now reports a genuine pregame count.
`pregame_observation_count` was previously computed as `row_count - live_count` (a
non-live count that never compared `fetched_at` to `commence_time`), so a non-live
observation collected at or after kickoff was mis-reported as pregame. It now counts
only non-live observations with a known kickoff whose `fetched_at` is strictly before
`commence_time`. A new independent count,
`non_live_at_or_after_kickoff_observation_count`, reports non-live observations with a
known kickoff collected at or after it, so no evidence is hidden.
`live_observation_count` and `missing_commence_time_count` retain their prior
meanings; the four counts are independent diagnostics and may overlap (a live row with
a missing kickoff increments both). They are not a partition of the rows.

#### Goal

Ensure the coverage diagnostic's reported counts mean exactly what their names claim,
so genuine pregame temporal depth is not overstated by counting late or ambiguous
observations as pregame.

#### Files Added/Removed/Changed

Added:
- None.

Changed:
- `src/gridiron_edge/market/history_coverage.py` - `pregame_observation_count` now
  requires `is_live is False`, a known `commence_time`, and `fetched_at <
  commence_time`. Added `non_live_at_or_after_kickoff_observation_count` for non-live
  observations with a known kickoff collected at or after it. Class documentation
  states that pregame and at-or-after-kickoff counts classify non-live rows with a
  known kickoff, while `live_observation_count` and `missing_commence_time_count` are
  independent diagnostics that may overlap and do not partition the rows.
- `tests/unit/market/test_history_coverage.py` - Added coverage for genuine pregame
  vs at-or-after-kickoff classification (strict kickoff boundary), the live/
  missing-kickoff overlap diagnostic, and the empty-result value of the new field;
  extended the multi-source test with the new count.

Removed:
- None.

#### Tests

- `uv run ruff check . --fix && uvx pyrefly check && uv run pytest -m "unit and not slow"`
  passed; all tests green.
- Focused `tests/unit/market/test_history_coverage.py` passes, including: before
  kickoff is pregame; exactly at kickoff and after kickoff are non-live-at-or-after;
  missing kickoff is in neither temporal count; live observations are excluded from
  both temporal counts; and a live observation with a missing kickoff is reported by
  both the live and missing-kickoff diagnostics (explicit overlap).
- Consumer search (recorded per review): `rg -n
  'QuoteHistoryCoverage|pregame_observation_count|live_observation_count|missing_commence_time_count'
  src tests frontend` shows the field is referenced only within
  `history_coverage.py` and its test; no API, serializer, generated contract, or
  frontend consumes it, so no generated contract required regeneration (WoW #9).
- Real-artifact verification (read-only, checksum-guarded) over
  `data/odds/history/season=2026-2027/week=01/observations.parquet`:
  `row_count=1680`, `pregame=1680`, `non_live_at_or_after_kickoff=0`, `live=0`,
  `missing_commence=0`; source parquet SHA-256 unchanged. On this ledger every
  observation is genuinely pregame because the ingest parser excludes started and
  live events, so the corrected count equals `row_count` here (no regression). The
  correction's effect on non-live-at-or-after-kickoff and missing-kickoff rows is
  proven by the focused unit tests, since this real ledger contains no such rows.

#### Acceptance

The coverage diagnostic reports truthful counts: a non-live observation at or after
kickoff is no longer counted as pregame, and a row with an unknown kickoff is never
classified as pregame or at-or-after kickoff. The new count surfaces non-live
late observations explicitly. Independent live and missing-kickoff diagnostics retain
their meaning and are documented as possibly overlapping. Quality gates and all tests
pass; real-ledger counts verified with the source unchanged. No committed consumer
depends on a prior meaning of the field. The unit is implemented, validated,
documented, and ready for downstream use.
