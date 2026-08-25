# PLAN.md — Active implementation unit

Exactly ONE active unit. Future units live in ROADMAP.md, not here.

### Unit — Bet-ledger atomic publication

#### Completed

`betting/ledger.py`'s `_write_ledger` now stages every complete ledger
rewrite to a colocated temporary file and publishes it via `os.replace`,
instead of calling `df.to_parquet(path, ...)` directly on the canonical
path. Direct-path writing was confirmed, by tracing pandas' pyarrow write
path and reproducing it empirically, to truncate the destination to zero
bytes synchronously before any row is serialized — meaning any interruption
during any write (not just the row being added) previously destroyed the
entire prior ledger, including every previously logged and settled bet.
Under the fix, an interruption at any point — during temp-file
serialization, or between serialization and the atomic rename — leaves the
prior valid ledger completely unchanged, whether or not a ledger already
existed. The module docstring was corrected from "append-only" (the physical
mechanism rewrites the complete file, and settlement mutates existing rows)
to an accurate description, which also explicitly discloses — rather than
implies away — that this unit provides atomically visible publication only
and does not coordinate overlapping writers.

This unit deliberately implements only "Guarantee A" (atomic publication).
"Guarantee B" (writer coordination — locking, optimistic concurrency, or an
enforced single-writer boundary) was explicitly scoped out after review
correction: an earlier draft of this unit claimed overlapping-writer safety
that its design did not provide, since `os.replace` makes each individual
publication atomic but does not make the full read-modify-write sequence
atomic across two overlapping callers. Guarantee B is recorded as a
distinct, unresolved open item for a future unit (see below), not silently
dropped.

#### Goal

Protect the canonical Parquet ledger from partial or destructive direct
serialization by staging each complete ledger update to a colocated
temporary file and atomically replacing the destination, so that process
interruption during serialization or publication preserves the previously
valid ledger. Preserve ledger schema, UUID bet identity, log/settlement
behavior, and `recording.py`'s cross-call compensation semantics exactly as
they are today.

#### Files Added/Removed/Changed

Added: None.

Changed:
- `src/gridiron_edge/betting/ledger.py` — `_write_ledger` now writes to a
  colocated temporary file and publishes via `os.replace` instead of writing
  directly to the final path. Module docstring corrected from "append-only"
  to an accurate mutable-ledger description that explicitly discloses the
  absence of writer coordination.
- `tests/unit/betting/test_ledger.py` — added `TestAtomicPublication` with
  four tests: temporary-serialization-failure preservation (existing
  ledger), pre-publication-failure preservation (existing ledger),
  first-write serialization-failure (no ledger yet — canonical path remains
  absent, not corrupt), and successful-write atomic replacement.

Removed: None.

#### Tests

`uv run ruff check . --fix && uvx pyrefly check && uv run pytest -m "unit and not slow"`
passed; all tests green. The four new tests prove the specific defect
closed (interruption at any stage never destroys or corrupts the prior
ledger); all pre-existing `log_bet`/`settle_bet`/`load_bets` and
`TestPersistedLedgerSchema` tests, and all of `recording.py`'s existing
compensating-rollback tests, pass unchanged in observable behavior.

#### Acceptance

Ledger writes are staged and published atomically; no reader can observe a
partial or corrupt ledger file, and no interruption at any point during a
write destroys previously valid ledger content, whether or not a ledger
existed before the write. Ledger schema, `bet_id` UUID identity, and all
existing behavioral contracts are unchanged. `recording.py` was not
modified. Writer coordination (Guarantee B) remains explicitly unresolved
and is recorded as future scope, not claimed by this unit's docstring, code,
or tests. No `DECISIONS.md` entry was required for Guarantee A; Guarantee B
will require its own design and decision entry when a future unit takes it
up. The unit is implemented, reviewed across two ChatGPT ratification
rounds (including a correction of a real Goal/design contradiction in the
first draft), validated, and ready for downstream use.
