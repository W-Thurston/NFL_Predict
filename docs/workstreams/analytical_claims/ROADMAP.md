# ROADMAP.md — Workstream 2 (Analytical Claims) entry (revised)

> Replace the existing WS2 entry with the following. Persistence hardening is
> split into two units (immutable JSON publication; mutable ledger
> durability/concurrency), per reviewer correction — everything after is
> renumbered accordingly.

## Analytical Claims (WS2) — [INSPECTION COMPLETE — IMPLEMENTATION SEQUENCED]

Per VISION.md's locked build order, Stage 2: establish the common claim and
lineage contract. Follows Quote Observation (WS1, complete). Inspection
(Boundaries 1–8) is closed; see
`docs/workstreams/analytical_claims/FINDINGS.md` for full evidence.

### Consolidated inspection verdict
The analytical-evidence substrate is substantially reusable: deterministic
domain identities, strict current-version validation, extensive backward
provenance, explicit unavailable/conflicting states, selective persistence,
and strong read-only API presentation are all confirmed working. The primary
gap is the absence of an explicit composition contract across independently-
owned domain artifacts, compounded by concrete, repeated persistence-
correctness defects that should be corrected before new artifacts depend on
these stores.

### Locked scope boundary
This workstream's evidentiary scope is the inspected game-market
recommendation chain (candidate issuance → recommendation policy →
recommended-bet result → API presentation). The props family
(`api/routes/props.py` + schemas/serializers) was never inspected and remains
explicitly outside this scope.

### Governing principle for the persistence units (locked by reviewer correction)
> Do not organize persistence work by file format alone. Organize it by
> ownership semantics: immutable identity-addressed publication, mutable
> current-pointer replacement, mutable operational state, and cross-store
> compensation are different contracts and require different correctness
> tests.

### Implementation units (in locked sequence — see PLAN.md for the active unit)

**Unit 1 — Immutable artifact publication hardening.** [COMPLETE]
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

**Unit 2 — Bet-ledger durability and writer coordination.** [PENDING]
Replace direct whole-file Parquet writes (`betting/ledger.py::_write_ledger`) with
atomically published complete-file replacement, and define an explicit,
tested writer-concurrency contract (locking, optimistic concurrency, or
documented single-writer enforcement — chosen and proven in this unit, not
preselected). Preserve ledger schema, UUID bet identity, log/settlement
behavior, and `recorded_wager`'s compensation semantics. Separated from Unit
1 because atomic rename alone does not address concurrent-writer data loss —
a distinct correctness problem requiring its own design and its own test
proof.

**Unit 3 — Identity-evolution contract for candidate references.** [PENDING]
Every persisted reference contract must gain an explicit evolution owner and
incompatibility policy — starting with `candidate_issuance_row_id` and its
consumers. The owner/mechanism is decided by this unit, not preselected by
inspection.

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
