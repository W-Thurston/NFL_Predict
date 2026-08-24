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

### Unit — Collection claim and receipt lifecycle robustness

#### Completed

A lost claim-creation race between two collection processes now resolves
gracefully: if `write_claim` loses the exclusive-create race, execution returns the
existing `CLAIMED` outcome instead of crashing on an uncaught `FileExistsError`.
Claim and result publication is now crash-atomic as well as create-only: both write
to a uniquely named temporary file beside the destination, then publish through
`os.link`, which raises `FileExistsError` on an existing destination and never
exposes a partially serialized file. An unexpected exception during ingestion after
a claim is created is now recorded as an explicit `UNEXPECTED_FAILURE` terminal
result rather than leaving the claim unresolved with no record. No retry, reclaim,
lease, or expiry of an already-stranded claim was introduced; the sole case that
remains unresolved (the terminal write itself failing) continues to surface as a
degraded verification state, unchanged.

#### Goal

Harden the quote-collection claim/receipt lifecycle against a lost creation race,
non-atomic partial writes, and unexpected post-claim failures, without altering the
existing no-automatic-retry posture for an already-stranded claim.

#### Files Added/Removed/Changed

Added:
- None.

Changed:
- `src/gridiron_edge/market/collection_execution.py` - `write_claim` is now called
  inside a `try/except FileExistsError` that returns the existing `CLAIMED`
  outcome on a lost race. The odds-specific failure branch and the unrecorded
  unexpected-failure gap are unified behind one `except Exception` and a new
  `_collection_failure_status` helper, which maps known ingestion failures to their
  existing statuses and any other exception to `UNEXPECTED_FAILURE`. The prior
  narrow failure-status helper and exception-union alias were removed in favor of
  this single, `Exception`-typed mapping.
- `src/gridiron_edge/market/collection_receipt_store.py` - `write_claim` and
  `write_result` now publish through a shared create-only, crash-atomic helper
  (temporary file, then `os.link` into place) instead of serializing directly into
  the final path. Added `CollectionExecutionStatus.UNEXPECTED_FAILURE`.
- `tests/unit/market/test_collection_execution.py` - Replaced a race test that
  pre-created the claim (and therefore never reached the new exception handler)
  with one that makes `write_claim` itself raise `FileExistsError`, proving the
  true lost-race path. Added coverage for an unexpected ingestion exception
  producing a persisted `UNEXPECTED_FAILURE` result without a repeated provider
  call. Corrected a pre-existing tautological assertion in the success-path test to
  assert the exact subsequent due-state.
- `tests/unit/market/test_collection_receipt_store.py` - Added coverage that an
  interrupted serialization leaves no partial destination file and no leaked
  temporary file.

Removed:
- None.

#### Tests

- `uv run ruff check . --fix && uvx pyrefly check && uv run pytest -m "unit and not slow"`
  passed; all tests green.
- Focused suites pass:
  `tests/unit/market/test_collection_execution.py`,
  `tests/unit/market/test_collection_receipt_store.py`, and
  `tests/unit/deployment/test_quote_collection_worker.py` (unresolved-claim
  degraded-verification behavior confirmed unchanged).
- Confirmed no remaining references to the removed exception-union alias or the
  prior narrow failure-status helper, and no type-suppression comment remains on
  the broadened error parameter.
- Confirmed the receipt-store integer deserialization conversions are unchanged
  from their prior runtime behavior.

#### Acceptance

A lost claim-creation race is proven to resolve as `CLAIMED` without invoking the
provider a second time. Claim and result files are proven to leave no partial
destination on an interrupted write and remain create-only. An unexpected
ingestion exception is proven to produce a persisted, explicit terminal result
rather than an unresolved claim, without repeating the provider call on
re-evaluation. No stale-claim retry, reclaim, or recovery behavior was introduced;
the one case that cannot be truthfully recorded (a failure in the terminal write
itself) is left exactly as before: an unresolved claim surfaced as degraded. Quality
gates and all tests pass. The unit is implemented, validated, documented, and ready
for downstream use.
