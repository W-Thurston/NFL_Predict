# Quote Observation Workstream — HANDOFF
Status: **INSPECTION CLOSED — IMPLEMENTATION READY**

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
- Inspected code commit: `<EXISTING_CODE_SHA>`
- Inspection snapshot: `WS1-SNAPSHOT-001`
- Findings revision: 8
- Context package revision: `WS1-REV-8`
- Working tree at inspection: clean (inspection was read-only)
- Context package commit: identify through Git history for this file
- Inspection method: dual-model (Claude author / M365 Copilot reviewer), 8 boundaries
- Test environment: `uv` / `pytest` (159 targeted WS1 tests) + real Week-1 parquet
- Source fidelity: mixed — SharePoint mirror (structural) + drag-and-drop attached
  source (full) + owner-run local tests/artifact. Any `LOCAL_VERIFICATION_REQUIRED`
  item is noted in FINDINGS.

> **Revalidation:** if any file relevant to F01–F41 changed after the inspected code
> commit, list it here with affected finding IDs and status = pending. (None known at
> package creation.)

## Objective (from ROADMAP, no new scope)
Preserve mutable sportsbook quote observations with point-in-time identity so the
system can truthfully answer *what was known at a declared cutoff* — the substrate
for the first vertical decision slice.

## Inspection scope
**Included:** quote observation, identity, persistence, supersession, cutoff
retrieval, downstream separation, tests & real artifacts.
**Excluded:** edge calculation, recommendation policy design, portfolio policy
design, spread modeling, CLV interpretation, frontend work.

## Consolidated verdict (Boundary 8)
**The existing quote-observation substrate is fundamentally sound. WS1 requires
focused contract adaptations and policy decisions, not a replacement odds system.**
**No component is classified REPLACE or RETIRE.**

## Reuse map (summary — see FINDINGS §Boundary 8 for the full table)
- **REUSE (verified):** quote schema/validation (F01/F10); fetch-conflict(7),
  exact-observation(11) and historical-series(6) identities + deterministic ordering
  (F06/F13/F31); logical-append/physical-atomic persistence (F03/F07); real
  depth-two ledger (F04 — see verified evidence below); malformed/conflict rejection
  (F08); separate atomic current snapshot (F09); source-neutral/market-generic (F10);
  `fetched_at`-once (F05); pregame-only exclusion (F12); price/line coexist-across /
  conflict-within (F14); market-family vs line/price (F15); provider/sportsbook
  separation (F16); game_id anchoring — Odds API adapter verified, others undecided
  (F17); `updated_at` exact-idempotency (F18); kickoff-relative selection (F28);
  explicit statuses + earliest-preserved (F30); interpretation-free selection/coverage
  (F32); same-poll duplicate-suppression + create-only receipt immutability (F21);
  explicit partial-persistence (F24). CLV/API separation (F37/F38) =
  VERIFIED_LOCAL_SOURCE (no CLV/API suite ran). Candidate issuance evaluates every
  supplied canonical row (F34) — but does NOT preserve the full source row and its
  reference is not exact over canonical identity (→ Unit 2).
- **ADAPT:** as-known-at-cutoff evidence view absent (F11/F29) + verified production
  cutoff leak (F35/F40) → **Unit 1**; reference non-injectivity (F41) → Unit 2;
  coverage misnomer (F33) → Unit 3; receipt crash-atomicity (F26), TOCTOU (F22),
  post-claim failure (F27) → Unit 4; store multi-writer safety (F20) → no unit until
  writer contract decided.
- **UNDECIDED (design questions, not decisions yet):** stale-claim recovery (F23 —
  present behavior locked by D28), provider-label/event stability (F16),
  descriptive/event-time trust boundary (F19), additional `effective_time` field
  (F11).

## Locked decisions (see `DECISIONS.md`)
- **D29** — System-known visibility is governed by `fetched_at`. `sportsbook_updated_at`
  = source-update metadata; `commence_time` = event-start boundary.
- **D28** — Unresolved claims are not automatically retried; any retry/lease/expiry/
  reconciliation mechanism requires an explicit future decision.

## Confirmed gaps (finding · consequence · evidence · owning unit · test-covered?)
- **F40** — production `issue-candidates` loads the full ledger, no `fetched_at<=evaluated_at`
  filter → backdated issuance can freeze post-cutoff evidence · VERIFIED_LOCAL_SOURCE ·
  **Unit 1** · no covering test (becomes Unit 1 acceptance).
- **F11/F29** — no arbitrary decision-cutoff evidence view; correct fix scopes the
  whole result before deriving identities/counts · VERIFIED_LOCAL_SOURCE · **Unit 1** · —.
- **F41** — candidate reference omits `sportsbook_updated_at`/`is_live`; not injective
  over canonical identity; is a cross-artifact lineage key · VERIFIED_LOCAL_SOURCE ·
  Unit 2 · no covering test.
- **F33** — `pregame_observation_count` actually counts non-live rows · VERIFIED_LOCAL_SOURCE ·
  Unit 3 · not exercised.
- **F22/F26/F27** — TOCTOU claim race / receipt not crash-atomic / unexpected
  post-claim failure · VERIFIED_LOCAL_SOURCE · Unit 4 · no covering tests.
- **F20** — store read-modify-write has no multi-writer protection; systemd deployment
  only mitigates ordinary overlap · VERIFIED_LOCAL_SOURCE · no unit until writer
  contract decided.

## Verified evidence
- 159 targeted WS1 tests passed (odds-store 27, parser 13, history 17, collection/
  worker 21, candidate/policy/result/closeout 76, odds-join 5). Not the full suite
  or ruff/Pyrefly gates.
- Real Week-1 ledger (`data/odds/history/season=2026-2027/week=01/observations.parquet`),
  corrected re-run (see FINDINGS §Boundary 7 — the re-run version, not the interim
  "pending" one): 1,680 rows · 2 distinct `fetched_at` timestamps · 840
  historical-series identities · every identity depth-2 · **canonical 11-col
  exact-identity duplicates = 0** · **value-repeats across fetches (10-col) = 0** ·
  moneyline 2 series changed price / spread 0 / total 0; no line changes measured in
  any market. *Derived (per reviewer):* for the 838 series without a price or line
  change, the two rows differ in `sportsbook_updated_at` — given that the
  historical-series identity is constant by construction and `is_live` is unchanged
  (established by F12: the parser stamps `is_live=False` for all pregame observations,
  so both fetches are non-live). This is F18 at scale. *(If the committed FINDINGS
  does not establish unchanged `is_live`, retain only the direct measurements and omit
  this derived statement.)*
- Evidence limitations: CLV/API separation is source-verified (no CLV/API suite ran);
  `test_odds_join` assertions verified by pass, not reconstructed; F40 caller body
  confirmed byte-faithful via owner `sed`.

> **Dependency:** the committed `FINDINGS.md` rev 8 §Boundary 7 must contain the
> **corrected** artifact re-run (canonical 11-col key). If only the interim
> "pending re-run" version is committed, downgrade the two `= 0` duplicate/value-repeat
> lines above to "not measured" until the corrected run is in the evidence log.

## Active implementation state
- Active `PLAN.md` unit: **Point-in-time quote evidence retrieval** (Unit 1).
- Goal: one owned cutoff-visible quote-evidence operation; route production candidate
  issuance through it so no post-cutoff observation can enter an issuance.
- Acceptance boundary: the 28 acceptance criteria and verification checks in `PLAN.md`.
- Next repository action: implement `as_known_at(observations, cutoff)`; wire
  `issue_candidates_cmd` through it; add tests; run gates.

## Do not reopen without new evidence
- Locked product/roadmap decisions (CONSTITUTION/VISION/ROADMAP), D28, D29.
- Verified findings (the REUSE map above).
- Deliberately deferred questions (F16 stability, F19 trust boundary, F23 recovery
  policy, F11 effective-time field).
- The four-unit partition and the "no REPLACE/RETIRE" verdict.

## Full evidence
- `docs/workstreams/quote_observations/FINDINGS.md` (rev 8)
- `DECISIONS.md` (D28, D29)
- `ROADMAP.md` (Units 2–4 + F20 writer-contract note)
