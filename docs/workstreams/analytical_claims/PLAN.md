# PLAN.md — Active implementation unit

Exactly ONE active unit. Future units live in ROADMAP.md, not here.

### Unit — Bet-ledger writer coordination

#### Completed

`betting/ledger.py` gained a module-level `threading.RLock` (`_LEDGER_LOCK`)
held across the complete `_read_ledger → mutate → _write_ledger` sequence in
both `log_bet` and `settle_bet`. `betting/recording.py::record_wager` holds
the same lock across its full snapshot→ledger-write→bankroll-write→restore
sequence, so a rollback triggered by a failed bankroll write cannot discard
a different, concurrently-completed ledger mutation from another thread.
A reentrant lock (`RLock`, not `Lock`) was required because `record_wager`
calls `log_bet` internally while already holding the lock; a plain `Lock`
would deadlock on that reentry.

The mechanism was selected from confirmed real evidence, not assumption:
the owner's own local `rg`/`grep` output showed no existing locking utility
and no `DECISIONS.md` entry governing ledger concurrency; the owner directly
confirmed the CLI bet commands are never used and the API runs as a single
process (`uv run gridiron api serve --reload`). The actual, evidenced risk
is intra-process: `api/routes/portfolio.py::record_portfolio_bet` is a sync
`def`, which FastAPI/Starlette route through a thread pool (confirmed by
this session's own earlier seed-incident traceback showing
`run_in_threadpool` → `anyio.to_thread.run_sync`), so two near-simultaneous
requests genuinely execute as two threads of the one running process. A
`threading.RLock` is proportionate to that confirmed risk; a file-based or
cross-process lock, or optimistic-concurrency machinery, would have been
unjustified complexity with no evidence of the cross-process risk they'd
address.

The lock's boundary is stated explicitly in the module docstring, not left
implicit: it coordinates threads within one process only, and provides no
protection if the API is ever run with multiple worker processes, or if the
CLI bet commands are ever used alongside a running API instance.

#### Goal

Coordinate the complete `_read_ledger → mutate → _write_ledger` sequence in
`betting/ledger.py`, and `recording.py::record_wager`'s
snapshot→ledger-write→bankroll-write→restore sequence, so that two
overlapping callers within the running API process cannot silently lose one
another's write.

#### Files Added/Removed/Changed

Added: None.

Changed:
- `src/gridiron_edge/betting/ledger.py` — added `_LEDGER_LOCK` (a module-
  level `threading.RLock`); `log_bet` and `settle_bet` each hold it across
  their full read-modify-write sequence. Module docstring updated to state
  the thread-only coordination boundary explicitly.
- `src/gridiron_edge/betting/recording.py` — `record_wager` holds
  `_LEDGER_LOCK` (imported from `ledger.py`) across its complete
  snapshot/write/restore sequence.
- `tests/unit/betting/test_ledger.py` — added `TestWriterCoordination` with
  two deterministic two-thread race tests: concurrent `log_bet` calls both
  survive; a concurrent `log_bet` and `settle_bet` both survive. Both tests
  widen the critical section with a monkeypatched `_read_ledger` delay to
  force a genuine race window; both would fail without the lock.
- `tests/unit/betting/test_recording.py` — added a test proving a failed
  `record_wager` (triggering its rollback) does not discard a different,
  concurrently-completed ledger mutation from another thread, timed so the
  concurrent write is attempted exactly during `record_wager`'s critical
  section.

Removed: None.

#### Tests

`uv run ruff check . --fix && uvx pyrefly check && uv run pytest -m "unit and not slow"`
passed; all tests green. The three new race tests are genuine regression
proofs — each would fail without the lock in place, since the induced delay
inside the locked section creates a real window a competing thread could
otherwise write into. All pre-existing ledger, settlement, schema, atomic-
publication (Unit 2), and single-call compensating-rollback tests pass
unchanged.

#### Acceptance

Two overlapping threads logging bets, or a `log_bet` overlapping a
`settle_bet`, both survive without silently losing either write. A
`record_wager` rollback cannot discard a concurrent, independently-completed
ledger mutation. The coordination boundary (threads within one process
only) is stated explicitly in the module docstring, not silently assumed.
Units 1 and 2's behavior (immutable-artifact publication; ledger atomic
publication; schema; single-call rollback) is unchanged. A `DECISIONS.md`
entry records the chosen mechanism, its evidence basis, and its explicit
revisit trigger. The unit is implemented, validated, and ready for
downstream use, pending ChatGPT review of the diff before final closure.
