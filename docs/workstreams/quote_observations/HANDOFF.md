# HANDOFF.md — Repository-wide execution state

> Where the repository stands now and where to resume. Points to the active
> workstream and the one active implementation unit. Method lives in
> `AI_BOOTSTRAP.md`; durable decisions in `DECISIONS.md`. This is a supporting
> context document — it does not outrank `DECISIONS.md` or the active `PLAN.md`.

## Repository basis
- Inspected code commit: `<EXISTING_CODE_SHA>`
- Context package revision: `WS1-REV-8`
- Working tree at inspection: clean
- Context package commit: identify through Git history for this file

## Current program position
- **Clean-sheet product definition COMPLETE and LOCKED:** `CONSTITUTION.md`,
  `VISION.md`, `ROADMAP.md`.
- **Active workstream:** Quote Observation (WS1) — first vertical-slice substrate.
- **WS1 inspection COMPLETE** (dual-model, 8 boundaries, FINDINGS rev 8):
  **the existing quote-observation substrate is fundamentally sound. No component
  requires wholesale replacement or retirement, but focused adaptations are required
  for point-in-time retrieval, candidate-reference identity, coverage semantics, and
  collection robustness.**

## Active implementation unit (see root `PLAN.md`)
**Point-in-time quote evidence retrieval (Unit 1).** Introduce one owned
cutoff-visible quote-evidence operation and route production candidate issuance
through it so no observation learned after the declared evaluation time can enter an
issuance. Basis: **D29** (`fetched_at` = system-known visibility). Acceptance: the
28 acceptance criteria and verification checks in `PLAN.md`.

## Where to resume
1. Read `AI_BOOTSTRAP.md` (method), then this file, then
   `docs/workstreams/quote_observations/HANDOFF.md`, then root `PLAN.md`, then
   `DECISIONS.md`.
2. Confirm commit + working-tree state.
3. Implement Unit 1 against its acceptance criteria; keep it the *only* active unit.

## Recently locked decisions
- **D29** — System-known visibility is governed by `fetched_at`.
- **D28** — Unresolved collection claims are not automatically retried.

## Queued (in `ROADMAP.md`, not active)
- Unit 2 — Candidate-reference identity hardening (F41)
- Unit 3 — Coverage diagnostic semantics (F33)
- Unit 4 — Collection claim & receipt robustness (F22/F26/F27)
- F20 store multi-writer safety — no unit until writer contract decided

## Workstream index
- `docs/workstreams/quote_observations/` — `FINDINGS.md` (rev 8), `HANDOFF.md`
