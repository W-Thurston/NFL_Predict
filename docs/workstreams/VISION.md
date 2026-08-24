# Gridiron Edge — Product Vision (Canonical, LOCKED)

> Status: **LOCKED** by two-model dialectic (author → adversarial critique →
> reconciliation). Changes only by explicit amendment with recorded rationale,
> consequences, and rejected alternatives — never incidentally inside an
> implementation unit.
>
> This document defines *what the product is* and *what must always be true*. It
> deliberately ignores current implementation. Build order and unit planning live
> in ROADMAP.md / PLAN.md; irreversible decisions land in DECISIONS.md; product
> purpose and boundaries live in CONSTITUTION.md.

---

## North star

**Gridiron Edge is a versioned evidence and decision-support system for NFL
games. It preserves time-valid observations, turns them into uncertainty-aware
and reproducible analytical claims, and applies explicit betting, portfolio, and
fantasy policies without hiding uncertainty, abstention, invalidation, or
historical error.**

**Internal design sentence:** *Separate every kind of claim, preserve what was
knowable at the time, trace every consequential conclusion in both directions,
and evaluate the process independently of the result.*

The central product is neither picks nor projections. It is a **traceable chain**
from evidence → uncertain belief → disciplined decision → honest evaluation.

---

## The central object: the versioned analytical claim

The **versioned analytical claim** is the common **conceptual contract** of the
system. Every consequential statement is a claim. Edges, recommendations, fantasy
calls, and portfolio allocations *specialize or reference* that contract — they
are **not** forced into one universal physical schema. Conceptual spine;
domain-owned specializations.

The shared contract consists of capabilities such as:

- Identity and version
- Claim kind
- Subject (game, team, player, market, model, or portfolio)
- Evidence cutoff
- Inputs (exact upstream versioned artifacts)
- Method identity (definition, model, selector, or policy)
- Uncertainty or limitations, *when applicable*
- Lineage (backward and forward)
- Invalidation contract
- Lifecycle status (current, superseded, invalidated, settled, evaluated)

Everything beyond that is owned by the domain object. A recommendation adds
policy, eligibility, an exact executable quote, expiration, and responsible-use
constraints. A portfolio allocation adds portfolio state, joint-exposure
assumptions, allocation policy, accepted risk, and a zero-allocation explanation.

*Claims — not pages, models, or bets — are the unit of reuse.*

---

## The six invariants (canonical, LOCKED)

**1. Separation of kinds.**
A source observation, derived fact, interpretation, prediction, market price,
analytical edge, recommendation, portfolio allocation, execution, and realized
outcome are distinct objects with distinct ownership. No lens recomputes another
layer's meaning.

**2. Point-in-time correctness.**
Every conclusion uses only evidence versions available within its declared
cutoff. The platform preserves *when evidence applied* (effective time), *when the
system learned it* (system-known time), *when the source published it* (where
relevant), the *decision cutoff* (latest permissible system-known time for a
claim), and *when a version was superseded* — rather than overwriting history.

**3. Uncertainty is intrinsic to estimated claims.**
Every estimated claim carries an appropriate representation of uncertainty,
support, scenarios, or limitation. A point estimate is never presented as the
complete account of an uncertain outcome. Deterministic artifacts (a displayed
price, a scheduled kickoff, a push settlement, a policy rejection) carry
provenance and validity, not artificial distributions.

**4. Traceability and invalidation are structural.**
Every consequential claim identifies three things, carried *through* computation
rather than attached afterward:
- **Backward lineage** — which exact upstream evidence produced this claim.
- **Downstream impact** — which claims and decisions consumed this evidence.
- **Invalidation contract** — which future observations or state changes would
  *expire, challenge, supersede, or require recomputation of* this claim (not
  merely "flip" it — evidence may widen uncertainty or remove recommendation
  eligibility while leaving the analytical edge intact).

**5. Abstention is first-class.**
No conclusion, no edge, no recommendation, no allocation, stale evidence, missing
evidence, conflicting evidence, and out-of-support are explicit, explainable
outcomes — never empty states or system failures.

**6. Evaluation separates distinct questions.**
Descriptive accuracy, predictive quality, market performance, betting
profitability, portfolio outcomes, and decision quality are evaluated separately.
A favorable result does not validate a defective process; an unfavorable result
does not invalidate a sound one.

---

## Truth states and the model as an instrument

`Observed → Estimated → Decided → Realized`, with a learning loop from Realized
back to earlier states. **Realized truth may evaluate an earlier claim but must
never silently rewrite it.**

- **Observed** — a source recorded something at a time (not necessarily
  ultimately correct).
- **Estimated** — a method produced an uncertain interpretation.
- **Decided** — a versioned policy returned a decision.
- **Realized** — the real-world outcome and its evaluation.

**Instrument truth:** a model produces a *falsifiable belief about the future with
uncertainty* — never truth about the future itself. Football / market / data
truth concern the world; model truth concerns our instrument.

---

## Domain model: an evidence graph (DAG), not a linear pipeline

Market observations are **independent inputs**, not downstream of the model.
Features may serve football analysis even when no prediction is produced. Every
node is immutable or versioned; every edge names the exact upstream artifacts it
consumed.

```
Football events & conditions
        │
        ▼
Time-valid facts & feature state ───────────────┐
        │                                        │
        ▼                                        │
Predictions (+ uncertainty)                      │
        │                                        │
Market quotes & movement ───────────► Candidate edges
        │                                        │
        │                                        ▼
        │                                Decision policy → Recommendation
        │                                        │
        │                                        ▼
        │                                Portfolio policy → Allocation
        │                                        │
        │                                        ▼
        └──────────────────────────────► Execution → Settlement & evaluation
```

---

## Lenses over the shared substrate

Layers describe an artifact's **epistemic status**; lenses describe the
**question asked**. They are two axes, not a hierarchy — a lens may read observed,
estimated, and decision artifacts at once.

**Analytical lenses** (interpret; do not choose action): Football, Scenario,
Prediction, Market.

**Decision lenses** (convert evidence to action under explicit policy — not merely
"thin lenses," since action requires rules, thresholds, and responsibility):
Betting, Risk & Portfolio, Fantasy, (DFS — deferred).

The betting path produces **three distinct outputs**; a wager can pass the first
two and still receive zero of the third:

- **Analytical edge** — is there a discrepancy worth noting?
- **Recommendation** — worth acting on after uncertainty, price sensitivity,
  freshness, eligibility, and invalidation?
- **Portfolio allocation** — given everything already held, what exposure (if
  any) after correlation, concentration, drawdown, and risk-of-ruin?

Adversarial/operator concerns (vig, shading, limits, quote responsiveness,
sportsbook-specific settlement) fold into Market Intelligence and Betting
Decision — real, but not a top-level pillar.

---

## Assurance & research (platform-wide, not optional lenses)

Provenance, point-in-time validity, data quality, uncertainty integrity,
reproducibility, recommendation audit, execution audit, outcome & decision-quality
evaluation, responsible-use controls. Plus a **research loop** (internal,
deferred) that learns from the accumulated graph and proposes *new versions*
rather than retroactively changing history. Evaluation publishes the **complete
eligible universe** so the track record cannot be cherry-picked.

---

## Transparency, defined precisely

Five levels of inspection; a consequential output must not appear unless all
*applicable* levels exist: **Conclusion → Explanation → Quantitative context →
Method → Evidence & reproduction.** The user is never forced through all five, but
nothing important is hidden and every claim can be followed downward.

**Temporal transparency** requires three views: as-known-at-decision-time
(authoritative for reproduction/evaluation), latest-corrected-record (for
research/description), and a **difference view** (what changed, when, and which
downstream conclusions differ). The system never pretends later facts were
available earlier, nor hides that an original assumption proved materially wrong.

---

## Navigation ≠ architecture

Surfaces are composed views over the layers above; a Game page draws from nearly
every layer while owning none of them. Navigation does not define the
architecture.

---

## Build order (dictated by irreversibility)

1. **Preserve time-valid source observations** with point-in-time identity — the
   only thing that *cannot* be reconstructed later. Mutable evidence (quotes,
   injury reports, weather, depth charts, news-driven status, corrections) must be
   captured without overwrite before sophisticated conclusions are built over it.
2. **Establish the common claim and lineage contract.**
3. **Prove one complete vertical decision slice** (fact → feature → prediction →
   market → edge → recommendation → allocation → settlement → evaluation).
4. **Prove historical reproduction and invalidation.**
5. **Add new analytical and decision lenses and surfaces.**

Claims regenerate from preserved inputs and pinned methods. Missing historical
observations cannot be regenerated honestly.

### What the first vertical slice must prove

Chosen for *least unrelated complexity*, not commercial excitement. It must show:

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

The exact first market is deferred to product/technical planning.

---

## Deferred by design

Research lens (internal-first), DFS optimization (season-long fantasy precedes
it), physical schemas, APIs/selectors, storage technology, first vertical market
choice, model uncertainty representation, portfolio correlation methodology,
launch navigation, whether fantasy is launch scope, public-vs-internal evidence
depth, and migration/reuse of current Gridiron Edge components.

---

## Non-negotiable one-liners

- Facts, predictions, markets, and decisions are separate objects.
- Point-in-time correctness is mandatory.
- Estimated claims carry uncertainty; deterministic ones carry provenance.
- No-bet is a first-class decision.
- Every recommendation is reproducible and can expire, be superseded, or
  invalidated.
- Evaluation includes the complete eligible universe.
- Accuracy, profit, and decision quality are never treated as equivalent.
- The frontend never recreates analytical or recommendation logic owned elsewhere.
- The system explains where it does not know.
