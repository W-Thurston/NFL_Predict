# Quote Observation Workstream — HANDOFF
Status: **WORKSTREAM COMPLETE**

> Compressed rehydration state for the Quote Observation workstream (WS1). Full
> evidence is in `FINDINGS.md` (rev 8). This handoff links finding IDs; it does not
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
- Inspected code commit: `<EXISTING_CODE_SHA>` (inspection); each of the four
  implementation units was gated and committed separately thereafter.
- Inspection snapshot: `WS1-SNAPSHOT-001` · Findings revision: 8
- Context package revision: `WS1-REV-8`

## Objective (from ROADMAP, no new scope)
Preserve mutable sportsbook quote observations with point-in-time identity so the
system can truthfully answer *what was known at a declared cutoff* — the substrate
for the first vertical decision slice. **Achieved.**

## Consolidated verdict (unchanged since Boundary 8)
The existing quote-observation substrate was fundamentally sound. No component
required wholesale replacement or retirement. Four focused adaptations closed every
verified gap the inspection found.

## All four units — COMPLETE

### Unit 1 — Point-in-time quote evidence retrieval (F11/F29/F40)
Introduced `as_known_at(observations, cutoff)`: an inclusive, UTC-validated,
schema-enforcing cutoff-visibility operation. Wired into `issue-candidates` before
candidate issuance, closing the **verified** production point-in-time leak (a
backdated `--evaluated-at` could previously admit observations fetched after the
cutoff). Visibility and pregame eligibility kept as separate composed predicates
(never `min(cutoff, kickoff)`). `history_boundaries` reused by composition, left
unmodified. Real Week-1 ledger verified: correct filtering, checksum unchanged.

### Unit 2 — Candidate reference exact over canonical observation identity (F41)
`candidate_issuance_row_id`'s hash now covers the complete canonical observation
identity (added `sportsbook_updated_at`, `is_live`), making the cross-artifact
reference injective. External `issuance_id:sha256` shape and issuance scope
unchanged; no consumer (recommendation policy, recommended-bet result, market
closeout) required modification, since each re-derives from issuance rows that
already carried both fields. No committed artifact depended on the prior reference
(embedded values existed only in git-ignored development output).

### Unit 3 — Truthful quote-history coverage counts (F33)
`pregame_observation_count` now requires `is_live is False`, a known
`commence_time`, and `fetched_at < commence_time` — genuine pregame evidence,
not a non-live count. Added independent `non_live_at_or_after_kickoff_observation_count`
so late evidence is surfaced rather than hidden. `live_observation_count` and
`missing_commence_time_count` retain prior meaning and are documented as
independent diagnostics that may overlap (do not partition the rows). Real Week-1
ledger: all 1,680 observations genuinely pregame (ingest excludes started/live
events); the corrected classification behavior is proven by focused unit tests.

### Unit 4 — Collection claim and receipt lifecycle robustness (F22/F26/F27)
A lost claim-creation race now resolves as the existing `CLAIMED` outcome instead
of an uncaught crash. Claim and result publication is crash-atomic as well as
create-only (temp file + `os.link`, never `Path.replace()`, which would have broken
the create-only guarantee). An unexpected post-claim ingestion exception is now
recorded as an explicit `UNEXPECTED_FAILURE` terminal result. No retry, reclaim,
lease, or expiry was introduced; the one case that cannot be truthfully recorded
(the terminal write itself failing) remains a degraded, surfaced, unresolved claim —
unchanged, per D28.

## Locked decisions (see `DECISIONS.md`)
- **D29** — System-known visibility is governed by `fetched_at` (inclusive `<=`).
- **D28** — Unresolved claims are not automatically retried; any future retry,
  lease, expiry, or reconciliation mechanism requires an explicit recovery-policy
  decision. **Held intact across all four units**, including Unit 4, which hardens
  detection and forward failure-handling without adding any recovery behavior.

No additional DECISIONS entries were required by Units 2, 3, or 4 — each corrects
or hardens an existing contract rather than establishing a new durable
architectural choice.

## What remains — deliberately NOT part of this workstream
- **F20 (store multi-writer safety)** — no implementation unit scheduled until the
  intended writer contract is decided. The store's read-modify-write append is safe
  today only because the repository-owned systemd deployment serializes ordinary
  timer-driven invocations; it is not safe against a second writer. Documented in
  ROADMAP as a deferred, undecided item — not a defect requiring immediate action.
- **F16 (provider-label/event stability)** — unresolved design question; no shared
  registry/normalization owner established or required yet.
- **F19 (descriptive/event-time trust boundary)** — documented trust question; no
  decision entry until a future unit proposes changing ownership or validation.
- **F11 residual (additional `effective_time` field)** — not needed to satisfy D29;
  revisit only if a concrete requirement emerges.

These are intentionally left open per the original Boundary 8 consolidation. WS1's
scope (quote observation, identity, persistence, supersession, cutoff retrieval,
downstream separation) is fully addressed; these four items are either genuinely
out of WS1's scope (F20 is a storage-strategy question, potentially a later
workstream) or explicitly deferred design questions that do not block downstream use
of the quote-observation substrate.

## Verified evidence (cumulative across all four units)
- All four units passed their focused test suites and the full
  `ruff + pyrefly + pytest -m "unit and not slow"` gate on every commit.
- Real Week-1 ledger verification (read-only, checksum-guarded) was performed for
  Units 1 and 3, directly exercising the corrected temporal-visibility and
  coverage-counting behavior against production data.
- Each unit was independently adversarially reviewed (author/reviewer two-model
  loop) before closure; each review surfaced at least one genuine correction
  (schema validation in Unit 1; evidence-strength and partition-claim corrections in
  Units 2 and 3; test-fidelity and typing-cleanup corrections in Unit 4), all
  resolved and re-verified before commit.

## Do not reopen without new evidence
- Locked product/roadmap decisions (CONSTITUTION/VISION/ROADMAP), D28, D29.
- All four units' verified contracts (this handoff's summaries above).
- Deliberately deferred questions (F16, F19, F20, residual F11).
- The "no REPLACE/RETIRE" verdict for the WS1 substrate.

## Full evidence
- `docs/workstreams/quote_observations/FINDINGS.md` (rev 8 — inspection) plus the
  four units' review handoffs and closure records (committed alongside each unit).
- `DECISIONS.md` (D28, D29).
- `ROADMAP.md` — WS1 marked complete; F20/F16/F19 remain as documented open items.

## Where to go next
WS1 is closed. The next workstream (per ROADMAP's program sequencing) is the next
vertical-slice capability — select and scope it the same way WS1 was: lock
CONSTITUTION/VISION/ROADMAP context, open a fresh `docs/workstreams/<name>/`
findings log, and run the same author/reviewer inspection discipline before any
implementation unit is activated.
