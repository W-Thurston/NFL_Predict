# Analytical Claims Workstream — HANDOFF
Status: **INSPECTION ACTIVE** (Boundary 1 not yet started)

> Compressed rehydration state for the Analytical Claims workstream (WS2). Full
> evidence is in `FINDINGS.md` (rev 0). This handoff links finding IDs; it does not
> restate their evidence. If this conflicts with a higher authority
> (CONSTITUTION → VISION → ROADMAP → DECISIONS → root PLAN), the higher authority
> controls. This is a supporting/rehydration document; it does not outrank the
> active `PLAN.md`.

## Authority
**Governs actions:** 1. `CONSTITUTION.md` · 2. `VISION.md` · 3. `ROADMAP.md` ·
4. `DECISIONS.md` · 5. root `PLAN.md`.
**Supporting context:** 6. root `HANDOFF.md` · 7. this workstream handoff ·
8. `FINDINGS.md`.

## Repository basis
- Inspection snapshot: `WS2-SNAPSHOT-001`
- Findings revision: 0
- Context package revision: `WS2-REV-0`

## Objective (from VISION's locked build order, no new scope)
Establish the common claim and lineage contract (VISION build-order Stage 2): the
versioned Analytical Claim as a conceptual contract — identity/version, claim kind,
subject, evidence cutoff, inputs, method identity, uncertainty/limitations,
backward+forward lineage, invalidation contract, lifecycle status — that domain
artifacts specialize or reference, not a universal physical schema.

## Inspection scope
**Included:** whether existing domain objects already behave as partial claim
specializations; where identity, versioning, and invalidation are handled today;
what the smallest shared contract would need to be for composition without a
god-object.
**Excluded:** redesigning WS1-hardened modules wholesale; new market/edge/
recommendation calculation logic; frontend claim presentation; portfolio/fantasy/
CLV interpretation; reopening D28/D29 without new evidence.

## Seed motivating case (see FINDINGS.md for full detail)
A real production incident: WS1 Unit 2 correctly changed the candidate-reference
identity contract; previously-persisted `RecommendedBetResult` artifacts then
failed re-validation on read (`ValueError: Recommended-bet candidate identity does
not match offer evidence`). Root cause confirmed, resolved via a three-step
production-chain regeneration (issue-candidates → derive-policy →
evaluate-recommendations). The incident is closed and resolved; it is recorded as
grounded evidence that existing claim-shaped objects lack explicit lineage,
contract-versioning, and invalidation semantics — the exact gap VISION's central
object addresses.

## Status
No boundary is closed yet. Boundary 1 (inventory & ownership) has not started.

## Do not reopen without new evidence
- Locked product/roadmap decisions (CONSTITUTION/VISION/ROADMAP).
- WS1's closed decisions (D28, D29) and closed units — this workstream inspects
  whether claim-shaped behavior should be *formalized*, not whether WS1's fixes
  were correct.

## Full evidence
- `docs/workstreams/analytical_claims/FINDINGS.md` (rev 0)
