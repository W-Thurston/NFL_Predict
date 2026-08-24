# Gridiron Edge — Product Constitution (Canonical, LOCKED)

> Status: **LOCKED** by two-model dialectic (author → adversarial critique →
> reconciliation). Altitude: product definition only — no data schemas, APIs,
> technology, or implementation phases. This document governs *what the product
> is for and what it must never do*. Changes only by explicit amendment with
> recorded rationale, consequences, and rejected alternatives.
>
> Companion: VISION.md (conceptual architecture). On questions of purpose, users,
> and boundaries this document wins; on questions of structure VISION.md wins.

---

## 1. Product thesis **[LOCKED]**

Gridiron Edge is a **transparent NFL intelligence and decision-support system**.
It preserves time-valid observations, turns them into uncertainty-aware and
reproducible analytical claims, and applies explicit betting, portfolio, and
fantasy policies without hiding uncertainty, abstention, invalidation, or
historical error.

Its central product is neither picks nor projections. It is a **traceable chain**
from evidence → uncertain belief → disciplined decision → honest evaluation.

**Product promise:** *Understand what is known, what is estimated, what is
uncertain, what action the evidence supports, and how well that process has
performed.*

**Differentiating promise:** *Every consequential output can be traced to the
evidence, assumptions, methods, market state, and policy that produced it — as
they existed at that time.*

---

## 2. Audience & responsibility **[LOCKED]**

**Primary user:** William, acting as NFL analyst, model researcher, betting
decision-maker, portfolio owner, and product evaluator.

**Secondary users:** invited peers acting as *read-only* readers of analysis,
reviewers of explanations/usability, evaluators of transparency, and sources of
feedback.

**Locked boundary:** *Gridiron Edge is primarily a personal NFL research and
decision-support instrument. It may expose read-only analytical views to invited
peers for evaluation and feedback, but it does not presently position its outputs
as individualized betting guidance for those viewers.*

**Not currently supported:** anonymous public users relying on Gridiron Edge for
their own wagering; user-specific bankroll or risk judgments for other people;
any audience that reasonably expects the system to monitor their financial
circumstances; commercial advisory or execution relationships.

**Responsibility posture:** build every consequential artifact to **public-grade
evidentiary rigor from the beginning** (point-in-time correctness, immutable
history, uncertainty, abstention, reproducibility) regardless of current
audience. **Epistemic honesty is a non-reducible duty for every audience;
additional duties arise as access, personalization, reliance, and execution
proximity increase** — not merely "with audience size."

**Expansion trigger:** broader publication or personalized use by others requires
a *new constitutional decision* covering audience responsibilities,
personalization boundaries, risk controls, language standards, and user-account
separation. Public expansion is an explicit governance event, never gradual scope
drift.

---

## 3. Jobs to be done **[LOCKED]**

Help the user: understand what is actually happening in football terms; see what
the system believes and how uncertain it is; understand what the market believes
and where it disagrees; learn when to trust the model and when not to; decide
whether a wager is worth acting on (including *no*); decide how much exposure to
take given everything already held; make fantasy roster decisions from the same
player outcomes; and review how the process has performed without cherry-picking.

---

## 4. Decisions the product explicitly supports **[LOCKED]**

Three separate outputs — passing the first two never implies the third:

1. **Analytical edge** — does a discrepancy worth noting exist?
2. **Recommendation** — does policy permit acting, after uncertainty, price
   sensitivity, freshness, eligibility, and invalidation?
3. **Portfolio allocation** — what exposure (if any) given everything already
   held, after correlation, concentration, drawdown, and risk-of-ruin?

Plus fantasy roster action, and **withholding** a decision — abstention is
supported, not a failure state.

**Portfolio scope [LOCKED]:** *analysis* (shared risk factors, concentration,
correlation assumptions, incremental exposure, drawdown scenarios, allocation
results, zero-allocation reasons) and *recordkeeping* (intended / recommended /
allocated / attempted / executed / rejected / settled — kept as distinct
statuses; a recommendation is never evidence a wager was placed) are supported
for the primary user.

---

## 5. Non-goals **[LOCKED]**

Gridiron Edge is **not**: a certainty engine; a system that converts every game
into a bet; a picks feed optimized for volume or excitement; a proof that past
success will continue; a substitute for user judgment; a system that reads model
confidence as guaranteed correctness or market disagreement as automatically
exploitable; a system that retroactively improves its record with corrected
information; a system that hides abstentions, invalidations, or unavailable
prices; a general-purpose financial-planning tool; or an automated wagering
agent.

**Execution boundary [LOCKED]:** *Gridiron Edge may identify the exact market,
outcome, line, price, provider, observation time, maximum acceptable price, and
invalidation state necessary for a user to independently locate and assess an
opportunity. It does not presently transmit, prepopulate, submit, or confirm a
wager.* Sportsbook deep links / pre-filled slips are **deferred** (product-
responsibility consequences, not mere presentation); **automated execution is a
standing non-goal** unless the entire responsibility model is reopened by
amendment.

*Wording caution:* the non-goal is **persuasive certainty and unsupported
simplification**, not visual emphasis itself. A valid recommendation must remain
visually discoverable — but always connected to price, time, uncertainty,
validity, and policy.

---

## 6. Responsible-use boundaries **[LOCKED]**

**Non-reducible duties (every audience, including a purely personal tool):** never
imply certainty; preserve losses and invalid decisions; separate recommendation
from execution; represent portfolio exposure honestly; do not optimize
presentation to encourage action; make stale or invalid states unmistakable; do
not claim a price is available without current evidence; never let a favorable
outcome erase a process defect.

**Duties that activate as others rely on the system:** clear product-scope
disclosure; no individualized risk claims without adequate context; user-specific
position/bankroll separation; stronger responsible-use controls; protection
against using another user's financial state; more deliberate recommendation
language; public methodology and record definitions; public-grade accessibility
and comprehension testing.

---

## 7. Epistemic vocabulary **[LOCKED]**

**Truth states — epistemic classifications, NOT a mandatory linear lifecycle:**
`Observed` (a source recorded it at a time — not necessarily correct) ·
`Estimated` (a method produced an uncertain interpretation) · `Decided` (a
versioned policy returned a decision) · `Realized` (the real outcome and its
evaluation). Not every artifact advances through all four — a quote may be
observed then superseded without becoming estimated; a diagnosis may be estimated
but never decided; a recommendation may be invalidated before any realization.
**Realized truth evaluates prior claims but never overwrites their original
versions.**

**Instrument truth:** a model produces a *falsifiable belief about the future with
uncertainty* — never truth about the future itself.

**Analytical lenses** (interpret): Football, Scenario, Prediction, Market.
**Decision lenses** (act under explicit policy): Betting, Risk & Portfolio,
Fantasy, (DFS — deferred).
**Assurance system** (platform-wide invariants): provenance, point-in-time
validity, data quality, uncertainty integrity, reproducibility, recommendation
audit, execution audit, outcome & decision-quality evaluation, responsible-use.

**Central object — the versioned Analytical Claim:** the common *conceptual
contract*, not a universal physical schema. Domain artifacts (edge,
recommendation, allocation, fantasy call) specialize or reference it. Claims —
not pages, models, or bets — are the unit of reuse.

---

## 8. The transparency promise, defined precisely **[LOCKED]**

Five levels of inspection; a consequential output must not appear unless all
*applicable* levels exist: **Conclusion → Explanation → Quantitative context →
Method → Evidence & reproduction.** Never forced through all five; nothing
important hidden; every claim followable downward.

**Temporal transparency — three views:** `as-known-at-decision-time`
(authoritative for reproduction/evaluation), `latest-corrected-record`
(research/description), and a **difference view** (what changed, when, and which
downstream conclusions differ).

**Scope of transparency [LOCKED]:** *Transparency requires that consequential
claims be understandable, challengeable, and auditable. It does NOT require
disclosure of private, licensed, security-sensitive, or user-specific
information. Any such limitation must be stated without obscuring the basis of the
claim.* Auditability of reasoning ≠ universal publication of underlying data.

---

## 9. Architectural invariants **[LOCKED — see VISION.md for canonical wording]**

(1) Separation of kinds · (2) Point-in-time correctness · (3) Uncertainty
intrinsic to *estimated* claims (deterministic artifacts carry provenance, not
distributions) · (4) Traceability & invalidation are structural (backward
lineage · downstream impact · invalidation contract) · (5) Abstention is
first-class · (6) Evaluation separates distinct questions. *Compute shared truth
once; interpret through explicit policies; preserve every decision as an
immutable historical claim.*

---

## 10. Product success criteria **[LOCKED — explicitly not P&L]**

Success is **decision-quality delivery and earned trust**, evaluated across five
dimensions:

1. **Integrity** — consequential historical outputs reproduce from the evidence
   and methods available at the time; superseded evidence retained; integrity
   failures surfaced.
2. **Epistemic honesty** — estimated claims carry uncertainty/limitation;
   out-of-support detected; missing/conflicting evidence produces explained
   abstention; invalidation and difference views preserved.
3. **Decision discipline** — edges don't auto-become recommendations;
   recommendations don't auto-become allocations; zero-allocations carry reasons;
   stale/unavailable quotes block valid recommendation states.
4. **Evaluative honesty** — complete eligible universe retained; predictive
   quality separated from realized return; original claims evaluated without
   retrospective rewriting; defects recorded as findings.
5. **Comprehension & utility** — an intended user can determine what is claimed,
   what isn't known, why, what would change it, whether the price is still usable,
   and why an edge did/didn't become a recommendation or allocation.

**Financial outcomes:** win rate, realized return, and similar measures are
**reported completely but never independently define success** — they are
interpreted alongside price, uncertainty, sample support, market movement, policy
compliance, and decision quality. Omitting them would itself be a transparency
failure.

**Calibration:** evaluated leakage-safely at the **broadest defensible level**,
with sample support and uncertainty shown. The platform must **not** make precise
segmented calibration claims where evidence cannot support them; calibration may
legitimately be in states of *sufficient / directional-only / insufficient /
unstable / not-applicable*. Calibration integrity is a **process guarantee**
(no leakage, defined population, shown support, documented binning, no overstated
subgroups, calibrators fit only on permissible history) — not a binary "the model
is calibrated" claim.

**Explicitly excluded as *defining* metrics:** ROI, units, win rate, hit rate,
engagement/volume — anything that would reward overconfidence or overbetting.

---

## 11. Remaining deferred choices (do not silently decide)

Fantasy as launch scope vs. later reader over the shared substrate; DFS
optimization; sportsbook deep links / pre-filled interfaces; public anonymous
access; personalized support and user-specific accounts for others; commercial
positioning; exact statistical success thresholds; the minimum coherent
experience; and what the product refuses to model at all vs. models-but-declines-
to-recommend. These may remain open **only while they do not alter the locked
audience, responsibility, and execution boundaries above.**

---

## 12. What changes only by amendment

The audience & execution boundaries (§2, §5), the six invariants (§9), the
epistemic vocabulary (§7), the transparency definition and its scope (§8), and
the "success ≠ P&L" stance (§10) are the constitutional core. They change only by
explicit amendment with recorded rationale, consequences, and rejected
alternatives — never incidentally inside an implementation unit.
