# HANDOFF.md — Workstream 3 (First Complete Vertical Decision Slice)

> Compressed rehydration only. This file restates the program-level goal
> and the current honest state against it — it does not re-derive
> evidence. Full evidence lives in FINDINGS.md; full program authority
> lives in the root ROADMAP.md, VISION.md, and CONSTITUTION.md.

### Status: BOUNDARY INSPECTION COMPLETE (Boundaries 1-5 closed). Ready for ROADMAP/PLAN construction. No implementation unit yet started.

### The one thing a new thread must never lose: WHY this workstream exists

**Goal:** exercise all ten of VISION.md's first-slice proof obligations
for one market.

**Exit criterion:** every one of the ten obligations is demonstrated end
to end for the chosen market (game spread, locked; moneyline fallback
only).

### Why this workstream opened before Workstream 2 fully closed

WS2 closed all seven planned units but carries two capabilities forward
(D35: general validity/invalidation beyond one field; D36: forward-impact
discoverability, no mechanism selected). WS3's own obligation 8 and its
real vertical-slice pipeline were expected to supply the first genuine
evidence needed to resolve them.

**Full boundary inspection is now complete. This is resolved as far as
inspection alone can resolve it, and no further:** no new concrete
evidence for a general invalidation mechanism or a forward-impact need
emerged during Boundaries 1-5. **Critically: this does NOT mean
obligation 8 can simply remain "Partial" indefinitely.** Workstream 3's
own exit criterion requires all ten obligations demonstrated end to end.
A dedicated implementation unit (Unit — spread-slice supersession and
invalidation proof, see below) is required to either close obligation 8
with a bounded, evidence-driven second test case, or this workstream's
exit criterion must be amended through an explicit program-level
decision — not silently accepted as unmet. D36 remains a separate,
correctly-deferred item (no concrete forward-impact consumer need has
appeared anywhere).

### Governing decisions relevant to this workstream

All of Workstream 1 and Workstream 2 (D27, D30-D37) remain in force.
None were reopened during this inspection.

### Boundary inspection summary — all five boundaries closed (full detail in FINDINGS.md)

**Boundary 1** named candidate owners for all ten obligations. **Boundary
2** traced the complete production chain end-to-end, confirmed
`market/recommendations.py` as the separate analytical-edge layer, and
surfaced the bankroll-composition gap. **Boundary 3** confirmed the
calibration/product-composition layers and the historical-backtest
report chain, sharpened the bankroll finding, and left decision-quality
evaluation open. **Boundary 4** confirmed decision-quality evaluation
Absent (exhaustive search, not merely unfound), confirmed the raw
prediction-generation layer, retired `prediction_availability.py`, and
found `GameDetail.tsx`'s presentation gap. **Boundary 5** produced the
full ten-obligation coverage table, recorded direct source evidence for
`EdgesTable`/`BetLegCard`/`SlipPanel` (extending the presentation-gap
finding — confirming the actual recording/confirmation step is sound,
but every decision-support display before it is not), and corrected
obligation 8's status from an unassigned carry-forward into a named,
required implementation item.

### Final obligation status (all ten; full detail and evidence in FINDINGS.md Boundary 5)

| # | Obligation | Status |
|---|---|---|
| 1 | Mutable observation preserved without overwrite | Reuse (cutoff-visibility); persistence owned by WS1 |
| 2 | Time-valid claim consumes exact source version | Reuse, full chain traced |
| 3 | Honest uncertainty/limitation | Reuse |
| 4 | Market price separate from prediction | Reuse |
| 5 | Edge derived without automatically becoming recommendation | Reuse, architecturally enforced |
| 6 | Recommendation policy can recommend or abstain | Reuse (domain, both branches); Blocked (production composition, positive branch — no bankroll); Adapt (every inspected presentation surface) |
| 7 | Portfolio can allocate zero despite eligible recommendation | Blocked — no eligible recommendation has ever been produced to test against |
| 8 | Later observation can supersede/invalidate downstream artifacts | Partial (WS2 D31, one field). **Requires a dedicated implementation unit — not a permanent carry-forward.** |
| 9 | Original decision remains reproducible | Reuse, for the inspected candidate/policy/result/evaluation/historical-report artifacts |
| 10 | Realized outcome vs. decision-quality evaluation remain separate | Reuse (realized-outcome/prediction-quality split); Absent (decision-quality half) — **requires a dedicated implementation unit.** |

**Three obligations (6's production branch, 7, 8, 10) each have a
confirmed-missing component requiring implementation work, not merely a
documented finding** — corrected from the prior revision, which
undercounted this by treating obligation 8 as already-resolved-enough
to carry forward indefinitely.

### Final Workstream 3 candidate implementation inventory

The inspection identified **two primary vertical-slice gaps, two missing
proof capabilities required by the exit criterion, and several bounded
cleanup items** — not a fixed count of "eight," which does not survive
correction and should not be treated as a durable fact.

**Primary vertical-slice gaps (high priority):**
1. Bankroll-composition gap — blocks obligation 6's positive branch and
   obligation 7 entirely.
2. Governed recommendation presentation across the composed decision
   surface (`GameDetail.tsx`, `EdgesTable`, `BetLegCard`) — `SlipPanel`'s
   final confirmation is sound and must be preserved unchanged.

**Missing proof capabilities (required for the exit criterion):**
3. Decision-quality evaluation for persisted recommendations.
4. Spread-slice supersession and invalidation proof (closes obligation 8
   or forces an explicit program-level exit-criterion decision).

**Bounded cleanup items (low priority):**
5. `cli/production_chain.py` hardcoded policy schema literal.
6. `resolve_recommended_bet_recording_evidence` stale "Unit 24" docstring.
7. `visibleRecommendedOffer.ts` naming/presentation risk.
8. `recordWager.ts` local completeness-check (`result_id`).
9. `prediction_availability.py` — Retire.
10. `market/recommendations.py` file path/naming.

### Recommended implementation sequence (dependency-ordered by proof, not file or urgency alone)

1. Reproducible decision-time bankroll evidence (unlocks the positive
   recommendation branch; nothing downstream can be tested without it).
2. Eligible-recommendation and explained-zero portfolio proof (bankroll
   alone doesn't prove obligation 7 — needs two real cases: positive
   allocation and zero allocation with a machine-readable explanation).
3. Governed recommendation presentation and action separation (needs
   real recommended/abstained states from units 1-2 to display
   correctly).
4. Decision-quality evaluation contract and first spread evaluation.
5. Spread-slice supersession and invalidation proof (tests D35 against a
   second real case; do not build a forward-impact index unless this
   unit reveals a concrete need — that would be the honest trigger for
   revisiting D36).
6. Scoped ownership and naming cleanup (items 5-10 above, folded in
   wherever a unit naturally touches the file).
7. End-to-end ten-obligation proof matrix using real, verified artifacts
   — the unit that actually allows Workstream 3 to close honestly.

Units 3 and 4 may be swapped if implementation convenience favors it.
Unit 1 must remain first.

### Immediate action items

1. Boundary inspection is complete. Do not re-open any boundary without
   new evidence.
2. Next: full Workstream 3 implementation ROADMAP.md reflecting the
   seven-unit sequence above.
3. Then: one active PLAN.md, scoped to Unit 1 only (reproducible
   decision-time bankroll evidence). Its definition of done must not
   claim obligation 7 is complete — only that bankroll evidence is
   truthful/reproducible, the writer accepts it, both absent- and
   supplied-bankroll paths are tested, the positive branch becomes
   reachable, and existing abstention semantics remain intact.

### Reading order for a new thread (per AI_BOOTSTRAP.md)

Root `HANDOFF.md` → this file → root `PLAN.md` → root `DECISIONS.md` →
(only if evidence-level detail is needed) `FINDINGS.md`.
