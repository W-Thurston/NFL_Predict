## PLAN.md — Active implementation unit (Workstream 3)

Exactly ONE active unit. Future units live in ROADMAP.md, not here.

### Unit — End-to-end ten-obligation proof matrix

#### Goal

Verify one real game-spread case against all ten obligations, not merely
trust accumulated unit tests. Produce a durable proof matrix: for each
obligation, the artifact or response proving it, its exact ID or path,
evidence cutoff, relevant method or policy identity, verification
command or test, expected state, and whether the proof is historical or
latest-current.

This unit allows Workstream 3 to close only if every obligation is
demonstrated end to end for the same exact spread slice. Existing tests,
documents, and artifacts may provide evidence, but no obligation is
accepted solely because an earlier unit claimed it was complete.

#### Locked proof discipline

- Use one exact game-spread slice throughout the matrix. Record the
  season, week, game ID, provider, provider event ID, sportsbook, market,
  side, line, American price, quote observation time, evidence cutoff,
  product ID/run ID, issuance ID, candidate-reference ID, policy ID,
  recommendation-evaluation ID, result ID, and decision-quality
  evaluation ID wherever those values exist.
- Distinguish exact artifact identity from descriptive scope. A season,
  week, game, market, or sportsbook is not a substitute for a persisted
  content identity.
- Distinguish historical proof from latest-current proof explicitly.
  Never reconstruct a historical claim from newer repository state.
- Record the command that verifies every matrix row and the expected
  machine-readable result. A prose statement without a repeatable
  verification path is insufficient.
- Open and validate every referenced persisted artifact. File existence,
  directory presence, newest modification time, and copied identifiers
  are insufficient.
- Follow relationships through their owning public validators and
  resolvers. Do not duplicate identity payloads or perform string-only
  relationship checks in the proof.
- Use existing production owners wherever they satisfy an obligation.
  Add no new domain machinery merely to make the matrix visually
  complete.
- If an obligation cannot be proven, mark it OPEN with the exact missing
  artifact, status, relationship, or command. Do not reinterpret the
  obligation downward.
- D35 and D36 remain governed by their accepted decisions. This unit may
  surface a concrete revisit trigger, but must not declare either
  capability solved without an implemented and verified mechanism.
- Do not use unit-numbered production names, fixture names, IDs, or test
  names.

#### Required durable deliverables

1. A checked-in proof matrix under
   `docs/workstreams/first_vertical_slice/`.
2. One executable integration or end-to-end proof that validates the
   matrix's load-bearing relationships against real persisted artifacts
   or a controlled repository containing artifacts written through the
   real stores.
3. Exact commands for reproducing every proof row.
4. Updated `HANDOFF.md`, `ROADMAP.md`, and any conformance or obligation
   registry whose status changes.
5. A final explicit Workstream 3 exit determination:
   - CLOSED: all ten obligations proven for the exact slice; or
   - OPEN: named obligations remain unproven, with their exact missing
     evidence and next owner.

#### Status

**Canonical architecture locked: controlled real-store-backed spread
proof; implementation in progress.**

Local runtime inspection found a strict-reader-valid current-product
spread lineage and a genuine policy abstention, but none of the proposed
product, forecast, issuance, policy, recommendation-evaluation, or result
artifacts are tracked by Git. They therefore cannot be durable proof
dependencies in CI or a clean checkout.

The proof will construct one deterministic `2026_01_KC_LAC` home-spread
lineage in a temporary repository through production writers, readers,
validators, evaluators, identity owners, and cutoff semantics. The same
subject will be evaluated in explicitly named contexts for active-policy
recommendation, unavailable-policy abstention, eligible zero allocation,
later-observation recomputation, historical replay, and pre/post-outcome
decision quality.

Planned durable outputs:

- `tests/integration/market/test_spread_vertical_slice_proof.py`
- `docs/workstreams/first_vertical_slice/TEN
