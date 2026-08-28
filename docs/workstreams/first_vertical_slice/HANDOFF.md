### HANDOFF.md — Workstream 3 (First Complete Vertical Decision Slice)

Compressed rehydration only. This file restates the program-level goal
and the current honest state against it. Full boundary evidence lives in
FINDINGS.md. The durable exit proof lives in
TEN_OBLIGATION_PROOF_MATRIX.md. Program authority remains in the root
ROADMAP.md, VISION.md, CONSTITUTION.md, and DECISIONS.md.

#### Status: COMPLETE. All seven implementation units are closed. All ten first-slice obligations are proven end to end for one controlled game-spread subject.

#### The one thing a new thread must never lose: why this workstream exists

**Goal:** exercise all ten of VISION.md's first-slice proof obligations
for one market.

**Exit criterion:** every one of the ten obligations is demonstrated end
to end for the chosen market.

The chosen market remained game spread. The durable proof uses one
controlled `2026_01_KC_LAC` home-spread subject and production writers,
readers, validators, identity owners, cutoff semantics, recommendation
evaluation, portfolio allocation, and decision-quality evaluation.

#### Exit determination

**Workstream 3 is complete.**

`TEN_OBLIGATION_PROOF_MATRIX.md` records the exact artifact or response,
canonical identity or path, evidence cutoff, method or policy identity,
expected and observed state, temporal meaning, and verification test for
each obligation.

The integration proof is owned by:

```text
tests/integration/market/test_spread_vertical_slice_proof.py
```

Its deterministic evidence builders are owned by:

```text
tests/fixtures/spread_vertical_slice.py
```

The proof contains ten obligation tests plus one cross-cutting canonical
path and embedded-identity test. Focused execution completed with all 11
tests passing. The established Python quality gates also passed and all
tests are green.

#### Why the proof is controlled and real-store-backed

A local runtime inspection found a strict-reader-valid current-product
spread lineage and a genuine policy abstention. The proposed product,
forecast, issuance, policy, recommendation-evaluation, result, and
current-selection artifacts were not Git-tracked. They were therefore
rejected as durable proof dependencies.

The final proof constructs a deterministic temporary repository from
checked-in fixtures. Source values are controlled, while every durable
artifact is written and read through its production persistence owner.
All domain states and identities are produced by production evaluators
and identity functions. The proof therefore reproduces in CI and a clean
checkout without depending on a developer's local `data/output` tree.

#### Boundary inspection

All five boundaries closed before implementation began. Every one of the
ten obligations received a source-confirmed disposition. Four
obligations had a genuinely missing component rather than merely missing
test evidence:

- Obligation 6 lacked complete production recommendation composition and
  truthful presentation across the governed action boundary.
- Obligation 7 could not represent an independently eligible
  recommendation with a completed zero allocation.
- Obligation 8 had only one narrow validity case and needed a second
  concrete supersession or recomputation proof.
- Obligation 10 lacked a persisted decision-quality evaluation separate
  from realized outcome.

The seven implementation units closed those gaps without introducing a
general downstream impact index.

#### Completed implementation sequence

**Unit 1 — Reproducible decision-time bankroll evidence.** Added a
cutoff-scoped, content-identified bankroll evidence derivation and wired
it into the governed recommendation writer. Hardened transaction-log
publication with atomic replacement. Preserved the bounded limitation
that this does not claim immutable historical reproduction independent
of ledger concurrency safety.

**Unit 2 — Eligible recommendation and explained-zero portfolio proof.**
Separated recommendation eligibility from portfolio allocation. Added
stable allocation state, reason, and amount evidence. Made positive
allocation, completed zero allocation, and genuinely unavailable
allocation evidence distinct. Bumped the recommended-bet result schema
to 3 under the project's clean-sheet replacement policy.

**Unit 3 — Governed recommendation presentation and action separation.**
Updated recommendation presentation to consume decision state and
allocation alongside result state. Gated governed action language on a
positive completed allocation rather than persisted-result presence.
Corrected the backend evidence gate so exact-duplicate and opposing-
position outcomes remain completed zero-allocation conclusions rather
than being misclassified as missing evidence.

**Unit 4 — Decision-quality evaluation contract and first spread
evaluation.** Added one shared candidate outcome grader and a persisted,
schema-versioned decision-quality evaluation. The evaluator verifies
result integrity, parent recommendation relationship, policy reference,
candidate reference, and optional allocation replay. Realized outcome is
identity-bearing but does not determine decision status.

**Unit 5 — Spread-slice supersession and invalidation proof.** Added a
second concrete validity case for game spread. A T1 quote produces a
positive-EV candidate. A later T2 quote in the same declared comparison
scope produces an explicit negative-EV non-candidate. Both source
observations and both issuance artifacts remain preserved, and the T1
decision remains reproducible after T2 exists. This satisfies obligation
8 without a speculative general forward-impact index.

**Unit 6 — Scoped ownership and naming cleanup.** Completed the bounded
ownership and lasting-language cleanup required before the final proof.
Temporary implementation-sequence naming was not allowed to become a
lasting production or test contract. Optional broad renaming and the
remaining out-of-scope `field-status` cleanup were not made prerequisites
for closure.

**Unit 7 — End-to-end ten-obligation proof matrix.** Added one controlled,
real-store-backed vertical-slice integration proof and the durable
matrix. The proof persists two candidate issuances, two recommendation
policies, three recommendation evaluations and results, and two
pre/post-outcome decision-quality evaluations. Canonical paths and
embedded identities are verified through public owners.

#### Final obligation status

1. Mutable source observation preserved without overwrite — **Proven**.
2. Time-valid analytical claim consumes an exact source version —
   **Proven**.
3. Estimated output includes uncertainty or limitation — **Proven**.
4. Market price remains separate from prediction — **Proven**.
5. Analytical edge does not automatically become a recommendation —
   **Proven**.
6. Recommendation policy can recommend or abstain — **Proven**.
7. Portfolio policy can allocate zero despite eligibility — **Proven**.
8. Later observation can supersede or invalidate downstream artifacts —
   **Proven** through an explicit row-owned recomputation outcome.
9. Original decision remains reproducible — **Proven**.
10. Realized outcome and decision-quality evaluation remain separate —
    **Proven**.

#### Governing decisions and carried-forward scope

All Workstream 1 and Workstream 2 decisions D27 and D30-D37 remain in
force. Workstream 3 did not reopen them.

D36, general forward-impact discoverability, remains deferred. Unit 5
and Unit 7 prove the exact bounded chain they construct, but they do not
implement discovery of every arbitrary downstream dependent. No
concrete consumer requirement emerged that justified a general impact
index.

The following also remain outside Workstream 3's closure claim:

- complete five-level transparency across the full composed application;
- the remaining out-of-scope `field-status` files;
- an advanced portfolio correlation model;
- generalized validity lifecycle machinery across all artifact families;
- proof for moneyline, total, props, or other market families.

#### Durable evidence and reproduction

Read in this order when evidence-level detail is needed:

```text
docs/workstreams/first_vertical_slice/TEN_OBLIGATION_PROOF_MATRIX.md
tests/integration/market/test_spread_vertical_slice_proof.py
tests/fixtures/spread_vertical_slice.py
```

Focused proof:

```bash
uv run pytest \
  tests/integration/market/test_spread_vertical_slice_proof.py \
  -v
```

Established Python quality gates:

```bash
uv run ruff check . --fix \
&& uvx pyrefly check \
&& uv run pytest -m "unit and not slow"
```

#### Reading order for a new thread

Root HANDOFF.md → this file → root PLAN.md → root DECISIONS.md →
TEN_OBLIGATION_PROOF_MATRIX.md → FINDINGS.md only if boundary-level
evidence is needed.
