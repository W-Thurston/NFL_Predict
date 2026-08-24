# ROADMAP.md — addendum: Quote Observation (WS1) follow-on units

> Append under the relevant workstream section. These are the deferred WS1 units and
> the writer-contract note from the locked Boundary 8 consolidation. The **active**
> unit (Unit 1 — Point-in-time quote evidence retrieval) lives in root `PLAN.md`,
> not here.

## Quote Observation — remaining units (not yet active)

### Unit 2 — Candidate-reference identity hardening [COMPLETE]
Owns F41. The cross-artifact `candidate_issuance_row_id` (used by
`recommendation_policy`, `recommended_bet_result`, `market_closeout`) omits
`sportsbook_updated_at` and `is_live`, so it is not injective over the canonical
11-col observation identity. Make the reference exact over canonical identity;
regenerate affected development artifacts and tests. First define the required
identity capability, then inspect the affected storage contracts — do **not**
pre-decide whether the ID is physically owned by the store. Clean-sheet latitude
applies (may replace the reference contract outright).

### Unit 3 — Coverage diagnostic semantics [ACTIVE]
Owns F33. `pregame_observation_count` currently computes `len(rows) - live_count`
(a non-live count) and never compares `fetched_at` to `commence_time`. Either rename
to `non_live_observation_count` or enforce genuine `fetched_at < commence_time`
pregame counting and separately report non-live post-kickoff rows. Clean-sheet
latitude applies (may replace the field contract rather than preserve an inaccurate
name).

### Unit 4 — Collection claim & receipt robustness [PENDING]
Owns F22, F26, F27 (one claim/receipt lifecycle boundary): the lost claim-creation
race (uncaught `FileExistsError` → should return `CLAIMED`), create-only-but-not-
crash-atomic receipt publication (adopt stage-and-rename), and unexpected post-claim
/ `write_result` failures leaving an unresolved claim. **Excludes** stale-claim
recovery, which is blocked by **D28** until an explicit recovery-policy decision.

## Deferred / undecided (no unit scheduled)

### Store multi-writer safety (F20)
The store's `append_to_odds_ledger` is a read-modify-write. The repository-owned
systemd deployment (`Type=oneshot`, `Restart=no`, 5-min timer) **mitigates ordinary
timer-driven overlap, but the store itself has no multi-writer protection** — it is
not safe against a second writer (manual CLI, second timer, test harness, direct
caller). **No implementation unit is scheduled until the intended writer contract is
decided.** Possible later outcomes: enforce single-writer as an invariant; add
store-level locking; add partition compare-and-swap / transactional storage; or
replace the artifact storage strategy in a later workstream.

### Open design questions (documented, no decision entry yet)
- Provider-label / provider-event **stability** (F16) — unresolved until evidence a
  shared registry / normalization owner is needed.
- Descriptive/event-time **trust boundary** (F19) — documented trust question; no
  decision entry until a unit proposes changing ownership/validation.
- Additional **`effective_time`** field (F11) — not needed to satisfy D29; revisit
  only if a concrete requirement emerges.
