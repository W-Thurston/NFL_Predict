## ROADMAP.md — Workstream 3 (First Complete Vertical Decision Slice)

### Sequencing decision

Workstream 2 closed all seven originally planned units while carrying two
capabilities forward under D35 and D36: general validity or invalidation
beyond the one proven field and general forward-impact discoverability.
No mechanism had been selected because no concrete consumer had shown a
need for one.

Workstream 3 opened before those deferred capabilities were generalized.
Its vertical slice was the intended evidence-gathering path. The
workstream would implement only the bounded validity behavior required
by a real case and would revisit D36 only if a concrete need to discover
arbitrary downstream dependents appeared.

### Workstream 3 — First complete vertical decision slice

**Status: COMPLETE. All seven implementation units are closed. The exit
criterion is met for game spread.**

**Goal:** exercise all ten of VISION.md's first-slice proof obligations
for one market.

**Chosen market:** game spread.

**Entry conditions:** Workstream 1 closed. Workstream 2 closed its planned
units with D35 and D36 explicitly carried forward.

**Exit criterion:** every one of the ten obligations is demonstrated end
to end for the chosen market.

**Exit evidence:**

```text
docs/workstreams/first_vertical_slice/TEN_OBLIGATION_PROOF_MATRIX.md
tests/integration/market/test_spread_vertical_slice_proof.py
tests/fixtures/spread_vertical_slice.py
```

The final proof uses one controlled `2026_01_KC_LAC` home-spread subject
inside a temporary repository. Production writers, readers, validators,
identity owners, cutoff semantics, candidate issuance, recommendation
policy, portfolio allocation, and decision-quality evaluation produce
and verify the complete chain.

### Ten obligations and final disposition

- A mutable source observation is preserved without overwrite —
  **Proven**.
- A time-valid analytical claim consumes an exact source version —
  **Proven**.
- An estimated output includes honest uncertainty or limitation —
  **Proven**.
- A market price remains separate from the prediction — **Proven**.
- An analytical edge is derived without automatically becoming a
  recommendation — **Proven**.
- Recommendation policy can recommend or abstain — **Proven**.
- Portfolio policy can allocate zero despite an eligible recommendation
  — **Proven**.
- A later observation can supersede or invalidate downstream artifacts —
  **Proven** through a concrete spread recomputation outcome.
- The original decision remains reproducible — **Proven**.
- Realized outcome and decision-quality evaluation remain separate —
  **Proven**.

The exact artifacts, identities, cutoffs, methods, states, temporal
meanings, and verification tests are recorded in
`TEN_OBLIGATION_PROOF_MATRIX.md`.

### Boundary inspection outcome

All five boundaries closed before implementation. Every candidate owner
was confirmed by direct source reading as reusable, requiring adaptation,
absent, or intentionally retired.

Four obligations had a confirmed missing component:

- Obligation 6 lacked complete governed recommendation composition and
  truthful presentation.
- Obligation 7 could not represent an eligible recommendation with a
  completed zero allocation independently of eligibility.
- Obligation 8 required a second concrete supersession or invalidation
  case.
- Obligation 10 lacked a persisted decision-quality evaluation separate
  from outcome.

The implementation sequence below closed each gap.

### Completed implementation units

#### Unit 1 — Reproducible decision-time bankroll evidence

Added cutoff-scoped, content-identified bankroll evidence and supplied it
to the governed recommendation writer. Hardened transaction-log
publication with atomic replacement. Preserved the explicit bounded
limitation around ledger concurrency safety.

**Status: Closed.**

#### Unit 2 — Eligible recommendation and explained-zero portfolio proof

Separated recommendation eligibility from portfolio allocation. Added
stable allocation state, reason, and amount evidence. Proved an eligible
recommendation with positive allocation and an independently eligible
recommendation with a genuine capacity-exhausted zero allocation.

**Status: Closed.**

#### Unit 3 — Governed recommendation presentation and action separation

Updated composed recommendation presentation to distinguish decision and
allocation states truthfully. Gated governed action language on positive
completed allocation and corrected backend classification of completed
portfolio-policy rejections.

**Status: Closed.**

#### Unit 4 — Decision-quality evaluation contract and first spread evaluation

Added the shared candidate outcome grader and a persisted,
schema-versioned decision-quality evaluation. Decision quality validates
cross-artifact consistency and allocation replay separately from realized
outcome.

**Status: Closed.**

#### Unit 5 — Spread-slice supersession and invalidation proof

Added a controlled later spread quote in the same declared comparison
scope. T1 remains preserved and reproducible. T2 produces a separately
identified issuance whose latest same-scope row changes from a positive-
EV candidate to a negative-EV non-candidate. The downstream row owns the
explicit changed state and reason.

This concrete second case satisfies obligation 8 without requiring a
general forward-impact index.

**Status: Closed.**

#### Unit 6 — Scoped ownership and naming cleanup

Completed the bounded ownership and lasting-language cleanup required by
the workstream. Implementation-sequence naming was not retained as a
lasting production or test contract. Optional broad renaming and the
remaining owner-scoped `field-status` cleanup were not made closure
requirements.

**Status: Closed.**

#### Unit 7 — End-to-end ten-obligation proof matrix

Constructed one deterministic game-spread proof through real persistence
and evaluation boundaries. Persisted and verified:

- two source observations;
- one selected weekly product;
- the referenced forecast event;
- two candidate issuances;
- two recommendation policies;
- three recommendation evaluations and results;
- two decision-quality evaluations.

The integration test contains ten obligation tests plus one cross-cutting
canonical path and embedded-identity test. Focused execution passed all
11 tests. The established Python quality gates passed and all tests are
green.

**Status: Closed.**

### Workstream exit determination

**Workstream 3 is closed.**

All ten first-slice obligations are demonstrated end to end for the
chosen game-spread subject. The proof is reproducible from a clean
checkout because it constructs controlled source evidence and persists
all durable artifacts through checked-in production owners rather than
depending on local runtime artifacts.

### Explicitly deferred and out of scope

- **D36, general forward-impact discoverability:** remains deferred. The
  final proof validates its exact bounded relationships but does not
  discover every arbitrary downstream dependent. No concrete consumer
  requirement emerged that justified a general impact index.
- **Complete five-level transparency across the full composed
  application:** outside the recommendation-specific slice proved here.
- **Remaining owner-scoped `field-status` cleanup:** explicitly outside
  this workstream's closure requirements.
- **Advanced portfolio correlation modeling:** the workstream proves the
  semantic boundary and a genuine capacity-exhausted zero allocation,
  not an advanced model.
- **Other market families:** moneyline, total, props, and other markets
  are not included in this one-market exit proof.
