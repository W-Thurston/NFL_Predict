# HANDOFF.md — Workstream 2 (Analytical Claims)

> Compressed rehydration only. This file restates the program-level goal and
> the current honest state against it — it does not re-derive evidence. Full
> evidence lives in FINDINGS.md; full program authority lives in the root
> ROADMAP.md, VISION.md, and CONSTITUTION.md. If this file and FINDINGS.md
> ever disagree, treat that as a finding requiring reconciliation, not a
> reason to guess which is right.

## The one thing a new thread must never lose: WHY this workstream exists

Quoted verbatim from the root **ROADMAP.md**, Workstream 2 section (the
authoritative source):

> **Goal:** establish the common conceptual behavior for consequential
> claims — **without** a universal physical "god object."
>
> **Must prove — one claim identifies:** its subject and kind; its exact
> evidence versions; its evidence cutoff; its method identity; its
> applicable uncertainty or limitation; its backward lineage; its downstream
> dependents; its invalidation contract; its lifecycle state.
>
> **Exit criterion:** a claim can be reproduced from pinned evidence and
> method identity, and its traceability can be followed both upstream and
> downstream.

**Entry condition (met):** Workstream 1 closed; preserved, time-valid
observations exist for a claim to reference.

## Where WS2 actually stands against that exit criterion, right now

**Not yet met.** Progress is real but partial — stated plainly, not as a
failure.

### The nine identification requirements — current status

| Requirement | Status |
|---|---|
| Subject and kind | Partial — subject yes; no machine-readable "kind" discriminator across claim types |
| Exact evidence versions | Yes |
| Evidence cutoff | Yes |
| Method identity | Yes |
| Uncertainty/limitation | Partial — checks/states exist; no unified representation |
| Backward lineage | Yes — the artifact's strongest property |
| Downstream dependents | No — no forward-impact index exists anywhere |
| **Invalidation contract** | **Improved this session (Unit 4) — narrowly.** A recorded-vs-current derivation-version mismatch is now a distinct, typed outcome from ordinary evidence corruption, for one field (the candidate reference) on one artifact type. General invalidation (supersession, expiry, downstream recomputation triggers) remains absent. |
| **Lifecycle state** | **Unchanged — still partial.** `RecommendedBetResultState` is a decision-outcome enum, not a claim-validity lifecycle (current/superseded/invalidated). |

Two of nine improved narrowly this session; none are fully closed. This
remains the honest current distance to WS2's exit criterion.

## Units closed this workstream

- **Unit 1** (immutable artifact publication hardening) — closed. Fixed
  overwrite-capable publication in 5 of 6 affected stores via `os.link`.
- **Unit 2** (bet-ledger atomic publication) — closed. Fixed a defect where
  any write interruption destroyed the *entire* prior ledger via
  temp-file + `os.replace`.
- **Unit 3** (bet-ledger writer coordination) — closed. Added an
  intra-process `threading.RLock` (`DECISIONS.md` D27) after a design
  correction — an initial "documented but unenforced" approach was rejected
  during review as insufficient (absence of a coordination mechanism is
  not proof concurrency cannot occur).
- **Unit 4** (identity-evolution contract for candidate references) —
  closed. `candidate_issuance_row_id` gained an independently owned,
  dispatched version (`DECISIONS.md` D28); `RecommendedBetResult` schema
  incremented to 2; real production data (698 candidates, season
  2026-2027 week 1) was regenerated end-to-end under the new schema, and
  the old `schema=1/` tree deleted.

**Units 1–3 address none of the nine identification requirements** — they
are prerequisite infrastructure (don't lose data, don't lose it under
concurrency), correctly sequenced before claim-contract work per Boundary
8's reasoning, but not themselves exit-criterion progress. **Unit 4
addresses a narrow slice of two of the nine** (invalidation contract,
lifecycle state) for one field on one artifact type — not a general
solution.

## What Units 5–7 are meant to close

- **Unit 5** (common claim capability protocol) — **not yet designed in
  detail.** This is the unit that should most directly target the full
  nine-item list as a documented conformance profile, generalizing from
  Unit 4's identity-evolution pattern.
- **Unit 6** (attribution-operation ownership) — relates to backward
  lineage precision (the six named reference-attribution operations found
  in Boundary 4), not a new requirement.
- **Unit 7** (small API/documentation cleanup) — administrative; does not
  target the nine items.

**Open item, carried forward, not yet done:** before or during Unit 5, the
local `docs/workstreams/analytical_claims/ROADMAP.md`'s units 5–7 should be
explicitly re-checked, item-by-item, against the program ROADMAP.md's
nine-requirement list quoted above — confirming which unit (if any) closes
each requirement, so none are silently left unaddressed by the unit
sequence.

## Governing decisions relevant to this workstream

- **D27**: bet-ledger writer coordination uses an intra-process thread
  lock, not cross-process locking. Scoped to the confirmed single-process
  deployment; explicit revisit triggers if that changes.
- **D28**: candidate-reference derivation is independently versioned;
  `RecommendedBetResult` schema incremented to 2; schema-1 development
  artifacts regenerated and deleted. Full text in `DECISIONS.md`.

**Action required, still open from before Unit 4:** confirm D27 and D28 are
actually committed, verbatim, to the real repository-root `DECISIONS.md` —
not only described in prior conversation output. This handoff assumes they
are being committed as part of each unit's closing commit, per this
project's own convention (PLAN.md closure lands in the same commit as the
code); it does not independently re-verify the live file's contents.

## Two standing cautions for any future thread, earned this session

1. **This workstream's `HANDOFF.md` was found stale** (still reading
   "Boundary 1 not yet started" after all eight boundaries and three
   implementation units had closed) before this revision. If a future
   thread finds this file describes a state that contradicts `FINDINGS.md`,
   `PLAN.md`, or the real repository, **trust the more specific/recent
   artifact and treat the mismatch itself as a finding requiring a fix to
   this file** — do not silently reconcile by guessing which is right.
2. **The SharePoint mirror has been observed serving stale and/or corrupted
   content** for files recently modified by a closed unit (confirmed:
   `recommended_bet_result_store.py`'s mirror copy showed pre-Unit-1 code,
   and separately showed visibly mangled string interpolation in error
   messages, after Unit 1 had already changed that file). **Prefer direct,
   owner-pasted or owner-bundled local source over a mirror/index search
   result for any file a recent unit may have touched.** This is not a
   one-time incident; treat it as a standing property of the mirror
   pipeline until proven otherwise.

## Reading order for a new thread (per AI_BOOTSTRAP.md)

Root `HANDOFF.md` → this file → root `PLAN.md` → root `DECISIONS.md` → (only
if evidence-level detail is needed) `FINDINGS.md`.
