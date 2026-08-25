# PLAN.md — Active implementation unit (Workstream 2)

Exactly ONE active unit. Future units live in ROADMAP.md, not here.

### Unit — Immutable artifact publication hardening

#### Goal
Eliminate overwrite-capable publication and partial-final-file exposure
across the affected immutable JSON artifact writers by adopting the
verified create-only, atomically visible publication pattern already used
by `collection_receipt_store.py` (WS1 Unit 4), while preserving each store's
existing path, schema, canonical serialization, replay-equality, and
conflicting-replay behavior. **Mutable current pointers and the Parquet bet
ledger are excluded from this unit and handled by their own persistence
contracts (Unit 2, ROADMAP).**

#### Confirmed scope (from WS2 Boundary 8, no re-derivation needed)

| Store | Confirmed defect | Fix class |
|---|---|---|
| `collection_plan_store.py` | `temporary.replace(path)` — overwrite-capable, unguarded | Adopt `os.link` pattern **only for write paths classified as immutable artifacts** — see design question 1 |
| `production_chain_preflight_store.py` | exists-check → `temporary.replace(path)` — overwrite-capable | Adopt `os.link` pattern |
| `recommendation_governance_store.py` | exists-check → `temporary.replace(path)` — overwrite-capable | Adopt `os.link` pattern |
| `recommendation_policy_store.py` | `temporary.replace(path)` inside a dead `except FileExistsError` — overwrite-capable, plus dead code | Adopt `os.link` pattern; remove dead exception branch |
| `recommended_bet_result_store.py` | exists-check → `temporary.replace(path)` — overwrite-capable | Adopt `os.link` pattern (preserve the existing reflection-based codec) |
| `candidate_issuance_store.py` | `open("x")` direct-to-final-path — create-only but NOT atomic | Adopt `os.link` pattern (temp-file write, then link) |
| `collection_receipt_store.py` | None — confirmed correct (WS1 Unit 4) | Reference template; **not modified** |

**Explicitly excluded from this unit** (separate ROADMAP unit): `betting/ledger.py`
(mutable, whole-file-rewritten Parquet store — atomic rename alone does not
solve its concurrent-writer data-loss problem; requires its own design and
its own tested concurrency contract).

#### Owning boundaries to read before editing (WoW #1)
- `src/gridiron_edge/market/collection_receipt_store.py` — the exact,
  already-shipped correct pattern and its own test suite (mirror its
  verification approach per store).
- Each of the six affected stores, in full, at time of implementation —
  confirm current write function signatures and store-specific
  replay-equality logic (byte-comparison vs. reconstructed-object
  comparison) before changing the publish step.
- `collection_plan_store.py` specifically: **read every write function in
  this module before touching any of them** — do not assume "one file, one
  fix."

#### Design questions to resolve from source (not pre-decided)

1. **Classify every write path in `collection_plan_store.py` by persistence
   semantics before modifying anything**, using this taxonomy:
   - Immutable create-once artifact
   - Mutable current pointer
   - Idempotent replay artifact
   - Composite manifest
   - Operational state
   **Only "immutable create-once artifact" write paths adopt create-only
   linking in this unit.** A mutable current-selection pointer must retain
   its intentional atomic-replacement semantics — converting it to
   create-only would break the existing selection workflow, not fix a
   defect. Apply this same per-write-path classification discipline to the
   other five stores as a confirmation step, even though WS2's inspection
   found them uniformly immutable — do not assume that finding without a
   fresh look at the current source.
2. Does `os.link` require the temp file and final path to share a
   filesystem? Confirm the deployment target (single local filesystem) makes
   this a non-issue; note explicitly if any store's paths could cross a
   mount boundary.
3. Does adopting `os.link` change any externally observable behavior beyond
   crash-atomicity (file permissions, hardlink-count implications for any
   code that stats the file)? Mirror whatever `collection_receipt_store.py`'s
   own tests already verify.
4. Does `recommendation_policy_store.py`'s dead `except FileExistsError`
   branch have any test currently asserting on that (unreachable) path?
   Confirm before deleting.
5. Final `rg` sweep across `src/gridiron_edge/{market,betting}/*_store.py`
   for any `temporary.replace(path)` instance not already named, to confirm
   this unit's scope is complete.

#### Acceptance criteria and verification checks

1. **Every write path in every scoped module is classified by persistence
   semantics (per design question 1) before modification.** Create-only
   publication is applied only to write paths classified as immutable
   identity-addressed artifacts.
2. Each immutable write path publishes via `os.link` (temp-file completion,
   then create-only atomic link), matching `collection_receipt_store.py`'s
   pattern exactly.
3. Mutable current-pointer write paths (if any are found in
   `collection_plan_store.py`) are left unchanged, unless separately proven
   defective under their own intended replacement semantics (not in scope
   here if so — route to a new item, do not silently fix in this unit).
4. **Race-specific publication tests, not interruption-only tests**, prove
   the defect is closed. For each hardened store, cover:
   - Failure before temporary serialization completes.
   - Failure after the temporary file is complete but before publication
     (mid-write interruption — the original test class).
   - **Destination created by another writer immediately before
     publication** (the actual race regression test): a second writer's
     conflicting content must not be overwritten, and the attempting writer
     must raise the store's existing conflicting-replay error.
   - Existing identical destination (idempotent replay).
   - Existing conflicting destination (conflicting replay rejected).
   - Temporary-file cleanup after any failure path above.
5. **Replay-equality semantics preserved explicitly, per store, not
   generically:**
   - Byte-comparison stores (governance, recommended-result) retain
     byte-based exact replay.
   - Object-comparison stores (policy) retain domain-object replay
     equivalence.
   - Existing malformed-artifact rejection continues to fail through the
     same reader boundaries.
   - An existing-path replay does not rewrite the file.
6. `recommendation_policy_store.py`'s dead exception-handling code is
   removed, with a regression test confirming conflicting-replay rejection
   via the live code path.
7. `candidate_issuance_store.py` moves from create-only-but-not-atomic to
   create-only-and-atomic; its existing behavioral tests (filename/
   issuance-ID agreement, embedded-ID validation, deterministic row
   ordering) continue to pass unchanged.
8. `uv run ruff check . --fix && uvx pyrefly check && uv run pytest -m "unit and not slow"`
   passes.
9. The reflection-based codec in `recommended_bet_result_store.py` (WS2
   finding B3-7b) is preserved as-is — only its publish mechanism changes.
10. `collection_receipt_store.py` remains unchanged (verified reference).

#### Non-regression guardrails
Do not: touch `recorded_wager`'s cross-file compensating snapshot/restore
mechanism (a distinct, already-working responsibility); touch
`betting/ledger.py` (separate ROADMAP unit); change any store's replay-
equality comparison mechanism (byte vs. object) — only the publish step
changes; change `recommended_bet_result_store.py`'s codec; add any
identity-evolution, claim-contract, or attribution-ownership logic (later
units); convert a mutable current-pointer write path to create-only; add
workstream/unit/finding identifiers to source or test names.

#### Decision-entry policy
No new `DECISIONS.md` entry is expected for this unit — it adapts an
already-ratified local pattern (the receipt-store precedent) to additional
immutable stores, not a new architectural choice.

#### Commit boundary (locked now, not decided during implementation)
One coherent commit per store (six candidate commits: collection_plan,
preflight, governance, policy, recommended-result, candidate-issuance),
followed by one unit-closing commit containing the condensed PLAN.md
update, CHANGELOG entry, and any final cross-store cleanup. If a shared
helper is extracted (e.g., a common `_atomic_create_json`-style function
reused across stores), that extraction is its own preceding commit, before
any store migrates to use it.

#### Definition of done
Six affected immutable-artifact write paths (the seventh surveyed owner,
`collection_receipt_store.py`, remains unchanged as the verified reference)
publish through a create-only, atomically visible mechanism, with mutable
current-pointer write paths in `collection_plan_store.py` (if any) left
correctly unconverted. Race-specific and interruption-specific tests both
pass per store. Gates and all tests pass. PLAN.md condensed to the completed
form (Completed · Goal · Files Added/Removed/Changed · Tests · Acceptance);
CHANGELOG updated; committed per the locked commit boundary above.
