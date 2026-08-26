# ROADMAP.md — Unit 2 correction (apply to the WS2 entry)

> Replace the existing "Unit 2 — Bet-ledger durability and writer
> coordination" description with the following two entries, reflecting what
> was actually designed and shipped, plus the explicit deferred item that
> review correction surfaced. Renumber all subsequent units by one (old
> Unit 3 → 4, old Unit 4 → 5, old Unit 5 → 6, old Unit 6 → 7).

**Unit 1 — Immutable artifact publication hardening.** *(Completed.)*
Harden the six affected immutable JSON persistence modules
(`collection_plan_store.py`, `production_chain_preflight_store.py`,
`recommendation_governance_store.py`, `recommendation_policy_store.py`,
`recommended_bet_result_store.py`, `candidate_issuance_store.py`) using the
verified create-only, atomically visible publication pattern already owned
by `collection_receipt_store.py` (WS1 Unit 4). Preserve each store's schema,
path, serialization, replay equality, and conflict behavior. **Classify
mixed modules such as `collection_plan_store.py` by write path, not by
file** — mutable current-selection pointers must not be converted to
create-only artifacts.

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

**Unit 4 — Common claim capability protocol.** [PENDING]
Formalizes the eleven-capability profile as a documented protocol/conformance profile — not a base
class. Generalizes from Unit 3's identity-evolution pattern.

**Unit 5 — Attribution-operation ownership.** [PENDING]
Separates and names the six confirmed reference operations as explicit, non-interchangeable capabilities
per Unit 4's contract. Corrects `_closeout_matches`'s current integrity
ambiguity via one of three named options (validate-then-compare /
attribution-only / expose both results separately).

**Unit 6 — Small API and documentation cleanup.** [PENDING]
Empty-`/portfolio/splits` serializer bypass (D18 violation, one-line fix); `api/serializers/portfolio.py`'s
incorrect D19 citation (remove, do not re-cite D18); development-era phase-
naming ("Unit 22"/"Unit 24," reviewed per-file, not blind substitution).

**Unit 7 — Small API and documentation cleanup.** *(Renumbered from Unit 6.)*
Unchanged from prior ROADMAP text.

### Explicitly deferred (not resolved by WS2 inspection; do not silently answer in any unit above)
`QualificationResult`'s disposition (Remove/Adapt/Retain); same-identity/
different-`created_at` write-conflict behavior; the missing-child composite-
manifest error model; the props family's relationship to this contract;
`suggested_stake`'s alias duplication; the resolver's scan-vs-index tradeoff;
forward-impact discoverability's physical mechanism.

### Method
Same dual-model, boundary-by-boundary inspection discipline as WS1 was used
for inspection; author/reviewer ratification required before any unit's
PLAN.md entry is treated as locked.
