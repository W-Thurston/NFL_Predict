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

**Unit 3 — Bet-ledger writer coordination.** *(Completed.)* Added an
intra-process `threading.RLock` covering `betting/ledger.py`'s complete
read-modify-write sequence and `betting/recording.py::record_wager`'s
snapshot/write/restore sequence, so overlapping callers within the running
API process cannot silently lose one another's write. Mechanism selected
from confirmed evidence (no existing coordination utility; API runs as a
single process; CLI bet commands unused; sync route handlers execute via a
thread pool) — not cross-process locking or optimistic concurrency, neither
of which is justified by any evidenced risk. The lock's boundary (threads
within one process only) is explicit in the module docstring and in
`DECISIONS.md` D27. This closes the persistence-hardening arc (Units 1–3).

**Unit 4 — Identity-evolution contract for candidate references.**
*(Renumbered from Unit 3.)* Unchanged from prior ROADMAP text.

**Unit 5 — Common claim capability protocol.** *(Renumbered from Unit 4.)*
Unchanged from prior ROADMAP text.

**Unit 6 — Attribution-operation ownership.** *(Renumbered from Unit 5.)*
Unchanged from prior ROADMAP text.

**Unit 7 — Small API and documentation cleanup.** *(Renumbered from Unit 6.)*
Unchanged from prior ROADMAP text.
