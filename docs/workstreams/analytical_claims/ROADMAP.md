# ROADMAP.md — Unit 2 correction (apply to the WS2 entry)

> Replace the existing "Unit 2 — Bet-ledger durability and writer
> coordination" description with the following two entries, reflecting what
> was actually designed and shipped, plus the explicit deferred item that
> review correction surfaced. Renumber all subsequent units by one (old
> Unit 3 → 4, old Unit 4 → 5, old Unit 5 → 6, old Unit 6 → 7).

**Unit 2 — Bet-ledger atomic publication.** *(Completed.)* Replaced
`betting/ledger.py`'s direct-to-final-path Parquet writes (confirmed to
truncate the destination immediately, destroying the entire prior ledger on
any interruption — not merely failing to add the new row) with a
temp-file-plus-`os.replace` publication sequence. Provides atomically
visible publication only; does not coordinate overlapping writers.

**Unit 3 — Bet-ledger writer coordination.** *(New — not started.)*
Serialize or conflict-detect the complete `_read_ledger → mutate →
_write_ledger` sequence, and — per source tracing during Unit 2 — the
`recording.py` snapshot→ledger-write→bankroll-write→restore sequence, so
overlapping callers cannot silently lose one another's updates. Requires an
explicit design decision (locking, optimistic generation/conflict detection,
or an enforced single-writer boundary), deterministic overlapping-writer
tests proving whichever contract is chosen, and — since no existing decision
governs this — a new `DECISIONS.md` entry. Not scoped further until a
concrete future unit takes it up; no mechanism is implied or preferred by
Unit 2's implementation.

**Unit 4 — Identity-evolution contract for candidate references.**
*(Renumbered from Unit 3.)* Unchanged from prior ROADMAP text.

**Unit 5 — Common claim capability protocol.** *(Renumbered from Unit 4.)*
Unchanged from prior ROADMAP text.

**Unit 6 — Attribution-operation ownership.** *(Renumbered from Unit 5.)*
Unchanged from prior ROADMAP text.

**Unit 7 — Small API and documentation cleanup.** *(Renumbered from Unit 6.)*
Unchanged from prior ROADMAP text.
