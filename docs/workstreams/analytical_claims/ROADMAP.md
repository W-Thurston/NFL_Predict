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
of which is justified by any evidenced risk. The lock's boundary (threads within one process only) is explicit in the module docstring and in DECISIONS.md D30. This closes the persistence-hardening arc (Units 1–3).

**Unit 4 — Identity-evolution contract for candidate references.** *(Completed.)*
candidate_issuance_row_id gained an independently owned, dispatched derivation version; RecommendedBetResult schema incremented to 2; schema-1 development artifacts regenerated and deleted. DECISIONS.md D31. Closes the persistence-hardening-plus-first-identity-evolution arc.

**Unit 5 — Common claim capability protocol.** *(Completed.)* Defined the
eleven-capability profile (`CLAIM_CAPABILITY_PROTOCOL.md`) and the
per-artifact conformance matrix (`CLAIM_CONFORMANCE_MATRIX.md`) for durable
analytical claims. Zero production code changes. Recorded as `DECISIONS.md`
D32–D36.

**Capabilities confirmed strong across the substrate:** evidence versions
and backward lineage (Capability 5) — the strongest capability found,
present or domain-equivalent for every durable claim role.

**Capabilities confirmed genuinely open, carried forward explicitly (not
resolved by this unit):**
- **Forward-impact discoverability (Capability 10) — Absent for every
  durable claim.** No mechanism exists anywhere in the codebase.
  `production_chain_preflight.py` confirmed NOT to secretly satisfy this
  (it performs backward, scope-based resolution, not forward indexing).
  **This is the specific reason Workstream 2's own program-level exit
  criterion ("traceability can be followed both upstream and downstream")
  is not fully met by Unit 5's closure.** Five candidate mechanisms are
  named in D36; none selected. Revisit when a concrete use case
  demonstrates real need.
- **Validity and invalidation (Capability 9) — Partial at best.** Only
  `CandidateIssuance`/`RecommendedBetResult` have any implementation (Unit
  4's derivation-version dispatch, one field). `RecommendationPolicy` and
  `RecommendationGovernanceVersion` are Absent. Unit 4's mechanism is
  confirmed a precedent, not a generalized solution (D35) — a hypothetical
  second case (governance-fingerprint revision) was tested against real
  source and found not to be a confirmed analog.
- **`QualificationResult` and `MarketCloseoutResult`** carry genuine
  `Undecided` marks in the conformance matrix for several capabilities —
  not resolved by this unit, visible rather than hidden.

**Unit 6 — Attribution-operation ownership.** *(Completed.)* Formally
named and classified seven reference-attribution operations (corrected
from Boundary 4's original count of six —
`bet_reference_matching.py::match_bet_references` confirmed as a genuine
seventh operation), split into canonical authentication (digest-backed,
1:1) and structural attribution (group/aggregate, no digest) families.
Corrected `market_family_evaluation.py::_closeout_matches`'s confirmed
integrity ambiguity: it now re-derives the canonical reference via
`candidate_issuance_row_id` instead of checking a prefix plus redundant
individual fields. `_history_matches` and `_wager_return_for_row`
confirmed correct as-is — structurally different questions, no equivalent
gap. Recorded as `DECISIONS.md` D37. This resolves Unit 5's deferred
Capability 8 (attribution).

**Unit 7 — Small API and documentation cleanup.** *(Next — not started.)*
Unchanged from prior ROADMAP text: empty-`/portfolio/splits` serializer
bypass (D18 violation); `api/serializers/portfolio.py`'s incorrect D19
citation; development-era phase-naming ("Unit 22"/"Unit 24," reviewed
per-file, not blind substitution).

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

## Workstream 2 exit-criterion status after Unit 5 (for HANDOFF.md)

Per the program ROADMAP.md's nine-item exit criterion, restated against
Unit 5's eleven-capability protocol:

| Program item | Status after Unit 5 |
|---|---|
| Subject and kind | Documented as a capability; satisfied by domain-specific equivalents across inspected artifacts |
| Exact evidence versions | Strong; documented as capability 5, not universally uniform (aggregate fingerprints noted honestly) |
| Evidence cutoff | Documented as capability 4; mapped per-artifact, not treated as one universal field |
| Method identity | Documented as capability 6; mapped per-artifact |
| Uncertainty/limitation | Documented as capability 7; three confirmed domain shapes, no shared type (D33) |
| Backward lineage | Strongest capability; documented as capability 5 |
| **Downstream dependents** | **Still Absent. Not resolved by Unit 5. This is the specific open item blocking full exit-criterion satisfaction (D36).** |
| Invalidation contract | Documented as capability 9; Partial (one field, one artifact — Unit 4); mechanism generalization explicitly not attempted without real second-case evidence (D35) |
| Lifecycle state | Same status as invalidation contract — Partial, not general |

**Workstream 2 is not yet ready to close.** Two items remain fully open:
a forward-impact mechanism (Capability 10) and a general (not single-field)
validity/invalidation mechanism (Capability 9). Both are correctly deferred,
not silently dropped, pending real evidence of need in a future unit.

## Workstream 2 status after Unit 6

Of Unit 5's eleven-capability profile, Capability 8 (attribution) is now
resolved by this unit. Two capabilities remain deliberately open, carried
forward unchanged from Unit 5's own closure:
- **Forward-impact discoverability (Capability 10)** — Absent for every
  durable claim; no mechanism selected (D36).
- **Validity and invalidation (Capability 9)** — Partial; generalized only
  as a requirement, not a mechanism (D35).

Workstream 2 is not yet ready to close. Unit 7 (small cleanup) does not
address either open item.
