# Gridiron Edge — Program Roadmap

> This is the **program outline**. It is a **capability and proof roadmap**, not a
> conventional feature backlog and not a technical specification. It converts the
> five dependency-ordered build stages from VISION.md into bounded workstreams
> with entry conditions, exit criteria, dependencies, exclusions, and unresolved
> decisions.
>
> Supersedes the interim PROJECT_OUTLINE.md (its content is absorbed here to avoid
> two artifacts with overlapping authority).

---

## Authority

- **CONSTITUTION.md** — purpose, audience, responsibility, non-goals.
- **VISION.md** — conceptual architecture and the six invariants.
- **ROADMAP.md** (this doc) — program scope, dependency order, workstream
  boundaries, and proof progression.
- **PLAN.md** — exactly one active implementation unit.
- **DECISIONS.md** — irreversible design choices as they are made.

Where this roadmap conflicts with CONSTITUTION.md or VISION.md, they win. This
document only sequences and bounds.

---

## Program objective

Prove that Gridiron Edge can **preserve mutable NFL evidence**, derive
**versioned, uncertainty-aware claims** from the exact evidence available at a
cutoff, convert those claims through **separate recommendation and portfolio
policies**, and **evaluate the process without rewriting history**.

Progress is measured by **proof outcomes**, never by a count of shipped features.

---

## Locked program decisions

- **First mutable observation source: sportsbook quotes. [LOCKED]**
  Rationale: quotes most sharply test the irreversible requirement (an uncaptured
  quote generally cannot be reconstructed later); their *observed* state is
  deterministic, isolating temporal identity and supersession before any model
  uncertainty is introduced; and the same preserved observations later feed market
  state, edge, recommendation eligibility, price-based invalidation, settlement,
  and evaluation.
- **First complete vertical market: game spread. [LOCKED]**
  Rationale: spread exercises **line and price as separate observations**, natural
  **market-move invalidation**, **push settlement**, and movement through
  **meaningful (key) numbers** — a more complete architectural proof for modest
  additional complexity. It tests proof obligations 4 (price separate from
  prediction) and 8 (supersession/invalidation) more honestly than moneyline,
  which tests them only thinly. Does not gate Workstream 1.
  - **Moneyline: fallback only** — used solely if spread-specific complexity
    (margin distribution, key numbers, pushes) blocks the architectural proof.
- **Minimum coherent experience:** a single market on a single game, carried end
  to end through the full evidence → decision → evaluation chain. Fantasy,
  multi-game portfolios, additional lenses, and finalized navigation are **out**.

---

## Minimum coherent experience (MCE)

> For a single market on a single game, the system captures mutable market and
> supporting observations without overwrite; produces a prediction with honest
> uncertainty; keeps the market price independent of the prediction; derives an
> analytical edge; runs recommendation policy (recommend **or** abstain); runs
> portfolio policy (allocate **or** explain zero); records a manually-entered
> execution status; and — after the game — evaluates prediction quality, decision
> quality, and result **separately**, while the original decision remains
> reproducible from as-known-at-decision-time evidence.

---

## Workstream 1 — Time-valid sportsbook observations *(foundational, irreversible)*

**Goal:** preserve changing sportsbook quotes with point-in-time identity and
without overwrite.

**Entry conditions:** VISION.md and CONSTITUTION.md locked; first observation
source locked (quotes). No dependency on the first-market decision.

**Must prove:**
- A quote observation has an unambiguous source and market identity.
- Two differing observations of the same market state can coexist.
- Effective, source-published (where available), system-known, and supersession
  semantics are not conflated.
- The system can answer *what version was available at cutoff T*.
- Conflicting, incomplete, or malformed observations remain explicit.
- No current-state replacement destroys historical evidence.

**Explicit exclusions:** edge; recommendation; "best"-quote selection; opening or
closing interpretation; CLV; model prediction; portfolio allocation; any frontend
market experience; consensus markets; provider-quality rankings; line-shopping.

**Scope boundary:** "quotes first" does **not** mean specializing observation
semantics around moneylines. Preserve market observations generically, without
implying all future markets share moneyline semantics.

**Exit criterion:** a real or controlled mutable quote sequence is preserved
across a change without overwrite, and an as-known-at query returns only the
version available at the requested cutoff.

---

## Workstream 2 — Analytical claim and traceability contract

**Goal:** establish the common conceptual behavior for consequential claims —
**without** a universal physical "god object."

**Entry conditions:** Workstream 1 exit met (there exist preserved, time-valid
observations for a claim to reference).

**Must prove — one claim identifies:** its subject and kind; its exact evidence
versions; its evidence cutoff; its method identity; its applicable uncertainty or
limitation; its backward lineage; its downstream dependents; its invalidation
contract; its lifecycle state.

**Exit criterion:** a claim can be reproduced from pinned evidence and method
identity, and its traceability can be followed both upstream and downstream.

---

## Workstream 3 — First complete vertical decision slice

**Goal:** exercise all ten of VISION.md's first-slice proof obligations for one
market.

**Entry conditions:** Workstreams 1–2 exit met. **First market: game spread
(LOCKED)**; moneyline is fallback only if spread-specific complexity blocks the
architectural proof.

**Likely sequence:**
```
quote observation
  + football evidence
  + derived feature
  + prediction with uncertainty
→ analytical edge
→ recommendation or abstention
→ portfolio allocation (including possible zero)
→ recorded execution state
→ settlement
→ separate outcome and decision-quality evaluation
```

**Must prove — the ten obligations (verbatim from VISION.md):**
1. A mutable source observation is preserved without overwrite.
2. A time-valid analytical claim consumes an exact source version.
3. An estimated output includes honest uncertainty or limitation.
4. A market price remains separate from the prediction.
5. An analytical edge is derived without automatically becoming a recommendation.
6. Recommendation policy can recommend or abstain.
7. Portfolio policy can allocate zero despite an eligible recommendation.
8. A later observation can supersede or invalidate downstream artifacts.
9. The original decision remains reproducible.
10. Realized outcome and decision-quality evaluation remain separate.

**Portfolio scope note:** the first implementation proves only the *semantic
boundary* (an eligible recommendation can receive an explained zero allocation) —
not an advanced correlation model.

**Exit criterion:** every one of the ten obligations is demonstrated end to end
for the chosen market.

---

## Workstream 4 — Reproduction, supersession, and invalidation

**Goal:** prove that later information updates *current* understanding without
corrupting *historical* truth.

**Entry conditions:** Workstream 3 exit met (there exists a complete decision case
to revisit).

**Must prove:**
- A later source observation supersedes an earlier version.
- A downstream claim is expired, challenged, superseded, or marked for
  recomputation as appropriate (not merely "flipped").
- The original claim and decision still reproduce.
- The latest-corrected view reflects the newer evidence.
- The difference view identifies what changed and which conclusions differ.
- Realized evidence evaluates but does not rewrite the original artifacts.

**Exit criterion:** one complete historical case can be viewed as-known-at the
original cutoff, as latest-corrected, and as an explicit difference between the
two.

---

## Workstream 5 — Additional lenses and product surfaces

**Goal:** extend the proven substrate without duplicating ownership or recomputing
shared truth.

**Entry conditions:** Workstreams 1–4 exit met (the foundation and one full slice
are proven).

**Candidate expansion areas (remain candidates until the foundation is proven):**
Football intelligence; scenario analysis; prediction observatory; market
intelligence; additional bet types; portfolio analysis; season-long fantasy;
model and decision-quality evaluation; broader transparent surfaces.

**Exit criterion:** deferred — defined per expansion once Workstream 4 closes.

---

## Deferred by design

DFS; public anonymous access; personalized accounts for other users; sportsbook
deep links / pre-filled slips; commercial positioning; full navigation
architecture; broad research tooling; whether fantasy is launch scope. Deferred
per CONSTITUTION.md §11 and VISION.md, and only while they do not alter the locked
audience, responsibility, and execution boundaries.

---

## What happens after this roadmap is locked

1. Inspect the current repository **specifically against Workstream 1**.
2. Identify the owning boundaries for quote observation, identity, persistence,
   and retrieval.
3. Classify existing work as **reuse · adapt · replace · retire · absent ·
   undecided** — reusable only if it can support separation of kinds, point-in-
   time correctness, estimated uncertainty, backward lineage, downstream impact,
   invalidation, explicit abstention, and separate evaluation dimensions.
4. Record irreversible findings in **DECISIONS.md**.
5. Create one active **PLAN.md** unit for the smallest coherent observation-
   preservation proof.
6. Design only the contracts required by that unit.
7. Implement, test, verify real artifacts, close the unit, and advance normally.

This gives the repository/ownership inspection you always run before building a
**concrete target** (Workstream 1) rather than an open-ended audit.

---

## Closed decisions (all project decisions needed to proceed are locked)

- **First mutable observation source** — sportsbook quotes. **[LOCKED]**
- **First complete vertical market** — game spread; moneyline fallback only.
  **[LOCKED]**
- **Fantasy in the minimum coherent experience** — no. **[LOCKED]**
- **Portfolio in the first slice** — prove the semantic boundary only (eligible
  recommendation → explained zero allocation); no advanced correlation model.
  **[LOCKED]**
- **Evidence-inspection depth for the first slice** — all five inspection levels
  must exist for the single slice. **[LOCKED]**
- **Initial product surface** — one composed vertical-slice experience, not full
  navigation. **[LOCKED]**

Record these in DECISIONS.md as the roadmap is adopted.
