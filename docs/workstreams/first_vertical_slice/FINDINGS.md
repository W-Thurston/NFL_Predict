## Gridiron Edge Workstream 3 Repository Inspection — First Complete Vertical Decision Slice

### Status: BOUNDARY INSPECTION COMPLETE. Boundaries 1-5 closed. Ready for implementation ROADMAP/PLAN construction.

#### Inspection authority
- CONSTITUTION.md: canonical and locked
- VISION.md: canonical and locked
- ROADMAP.md: canonical and locked
- Scope: Workstream 3 (First Complete Vertical Decision Slice) only

#### Inspected repository snapshot
- Repository: Gridiron Edge
- Inspection snapshot identifier: WS3-SNAPSHOT-001
- Byte-fidelity limitation: findings that depend on exact syntax,
  operators, whitespace-sensitive content, or truncated text require
  verification against the local source file. The SharePoint mirror has
  been confirmed, repeatedly across this and prior workstreams, to serve
  stale and/or corrupted content for recently-modified files — prefer
  owner-supplied local source over mirror search for anything a recent
  unit may have touched.

#### Inspection revisions
- Final inspection revision: 5 (Boundaries 1-5 all closed)
- Authoring thread: Claude
- Reviewing thread: Microsoft 365 Copilot (ChatGPT-side)
- Repository snapshot frozen during author/reviewer cycle: yes

#### Workstream 3 scope

Per the program ROADMAP.md, Workstream 3's goal is to exercise all ten of
VISION.md's first-slice proof obligations for one market (game spread,
locked; moneyline fallback only). This inspection determined which of the
ten obligations the repository already satisfies (in full or in part),
which are absent, and what the smallest coherent implementation path is
to close any gaps — following the reuse/adapt/replace/retire/absent/
partial/blocked/undecided classification discipline used in Workstreams 1
and 2.

**Carried forward from Workstream 2, resolved as far as inspection alone
can resolve it:**
- Forward-impact discoverability (D36) — confirmed absent everywhere
  during this inspection too; no concrete consumer need surfaced across
  five boundaries. Remains deferred, not built speculatively.
- General validity/invalidation beyond Unit 4's single-field precedent
  (D35) — obligation 8 was this workstream's opportunity to test whether
  the pattern generalizes. **No second real case was found during
  inspection alone; this must be resolved by a dedicated implementation
  unit (see below), not left as permanently "Partial."**

#### Explicitly out of scope
- Reopening any locked Workstream 1 or Workstream 2 decision without new
  evidence.
- Building a forward-impact index or a generalized invalidation mechanism
  speculatively, ahead of a concrete need.
- Any of VISION.md/CONSTITUTION.md's explicitly deferred items.

#### Evidence labels

VERIFIED_LOCAL_SOURCE · VERIFIED_LOCAL_TEST · VERIFIED_REAL_ARTIFACT ·
REVIEWED_FULL_ATTACHED_SOURCE · REVIEWED_FULL_MIRROR ·
SUPPORTED_BY_MIRROR_SNIPPET · SUPPORTED_BY_FILENAME_ONLY ·
INDEXING_INCOMPLETE · LOCAL_VERIFICATION_REQUIRED · NOT_INSPECTED

Mirror reads are not verification. Reading a test is not running it —
only a successful local run earns VERIFIED_LOCAL_TEST.

---

### Boundary 1 — Inventory & ownership — CLOSED

Directory-tree inventory against all ten obligations. Candidate owners
named; behavioral classification happened in later boundaries.

**Candidate owners per obligation:** (unchanged from original — see prior
revision for full per-obligation list; superseded by the Boundary 5
coverage table below for final status.)

**Named collision-risk, resolved:** `market/recommendations.py` confirmed
real and distinct from the two API `recommendations.py` files; resolved
Reuse in Boundary 2.

**Disposition:** CLOSED.

---

### Boundary 2 — Contract and data-lifecycle inspection — CLOSED

**Confirmed production chain, traced end-to-end through real source:**
```
preserved quote ledger
  → as_known_at(evaluated_at)                        [Reuse]
  → issue_pregame_candidates → persisted CandidateIssuance
CandidateIssuance + empirical evaluation + governance
  → derive_recommendation_policy → persisted RecommendationPolicy
CandidateIssuance + RecommendationPolicy
  → evaluate_recommendation_issuance
  → persisted RecommendedBetEvaluation + RecommendedBetResult children
```
Sole confirmed production entrypoint: `cli/production_chain.py` (three
manually-invoked, staged commands; no automatic chaining).
`production_chain_preflight.py` confirmed never a creation path — a
read-only readiness auditor (consistent with D27).

`GET /edges` reads analytical edges (`market/recommendations.py` via
`weekly_edge_service.py`) and existing persisted results
(`load_recommended_bet_results_for_week`), composing for presentation
only in `api/serializers/edges.py` — distinct, non-competing
responsibility from the CLI's creation path.

**Reuse:** `ingest/odds/as_known.py`; `market/recommendations.py`;
spread uncertainty/calibration path (`post_process.py`);
`load_recommended_bet_results_for_week`;
`resolve_recommended_bet_recording_evidence`;
`recommendationPresentation.ts`; `recordWager.ts` +
`record_portfolio_bet` (execution/reference separation confirmed sound).

**Adapt:**
- **`evaluate_recommendations_cmd` — missing bankroll evidence. High
  priority.** The sole checked-in production caller of
  `evaluate_recommendation_issuance` supplies a genuinely-empty portfolio
  snapshot and no bankroll. A real, working, user-facing bankroll-driven
  Kelly path exists elsewhere (`gridiron edges report --bankroll`,
  `weekly_predict.py`), but it feeds only the analytical edge layer via
  `build_weekly_edge_result` — confirmed, by full source tracing, to be
  an entirely separate call graph that never intersects
  `evaluate_recommendation_issuance` anywhere in this codebase.
  Production-composition gap, not a missing domain algorithm.
- `cli/production_chain.py` hardcoded policy schema literal (`1` instead
  of `RECOMMENDATION_POLICY_SCHEMA_VERSION`).
- `resolve_recommended_bet_recording_evidence` stale "Unit 24" docstring.
- `visibleRecommendedOffer.ts` naming/presentation risk, no functional
  defect.
- `recordWager.ts` local completeness-check gap (`result_id` not
  checked).

**Undecided:** Runtime scalar normalization in edge-to-recommendation key
matching — not independently confirmed by this inspection.

**Disposition:** CLOSED.

---

### Boundary 3 — Prediction uncertainty, portfolio evidence, and decision-quality evaluation — CLOSED

`historical_backtest.py` + `historical_backtest_report_builder.py` +
`historical_backtest_report.py` + `historical_backtest_report_loader.py`
— **Reuse**, fully confirmed by direct source (all four files read in
full). Complete chain traced end-to-end; schema versioning confirmed at
three independent enforcement points; digest validation confirmed at two
levels; path-versus-identity validation confirmed via the same two-layer
pattern used elsewhere; exact-replay-on-write confirmed
(`stored.equals(expected)`, raises on mismatch).

**Prediction-quality/realized-outcome/evidence-availability separation
confirmed at both row and aggregate-summary levels** — three distinct
dimensions, never conflated.

**What this does NOT establish, stated plainly:** this is confirmed
prediction-quality-vs-realized-outcome separation. It is **not** evidence
of decision-quality evaluation. Nothing in these four files touches
recommendation policy, recommendation decisions, or decision-time
evidence.

**Reuse:** `bankroll.py` (operational role only); `weekly_spread_product.py`;
`product_validation.py`; `EdgeResultStatus.tsx`; `edgeResultStatus.ts`;
`BlockedField.tsx`; the full historical-backtest file family above.

**Confirmed implementation gap, sharpened:** no decision-time
`BankrollBasis` composition exists from the operational ledger; no
checked-in production caller supplies bankroll.

**Still unproven, explicitly carried forward:** complete five-level
transparency in a composed surface; decision-quality evaluation, distinct
from prediction quality/realized result/evidence availability; user-
suitability of roadmap identifiers in tooltips; the exact persistence
contract for a future bankroll snapshot.

**Disposition:** CLOSED.

---

### Boundary 4 — Decision-quality evaluation, remaining prediction layer, and composed transparency — CLOSED

**Decision-quality evaluation: CONFIRMED ABSENT** (not merely unfound),
via two exhaustive searches. Every hit sorts into three buckets — pure
pre-decision math, identity/reference fields, or the confirmed readiness-
auditor role of `production_chain_preflight.py` — none of which is
decision-quality evaluation. The exhaustive, closed consumer list for
persisted recommendation results (write, validate, load-for-presentation,
resolve-for-recording, preflight-audit) has no fifth consumer evaluating
a persisted decision against its policy and decision-time evidence.

**`prediction_availability.py`: CONFIRMED EMPTY — Retire.** Local owner
verification: 1 line, 70 bytes.

**`availability.py` + `prediction_policy.py`: Reuse.** Confirmed
ownership chain: raw input availability → model-selection eligibility
(six-reason rationale enum) → weekly model execution → calibrated
uncertainty → composed-product invariant enforcement. Same "state and
materialized values must agree" invariant confirmed to hold at this
layer too.

**Historical-backtest family further strengthened**
(`historical_backtest_summary.py`,
`historical_backtest_report_selection.py`,
`historical_backtest_report_store.py`): `_write_parquet` re-reads its own
temp file and re-verifies equality before the atomic replace;
`unit_return_status`/`price_evidence_status` are honestly hardcoded
(`UNAVAILABLE` with stated reason; `ASSUMED` with stated methodology),
never fabricated.

**`GameDetail.tsx`: confirmed Adapt.** The composed game-detail surface
receives governed recommendation state on every `EdgeRow` but `RecCell`
and `ModelLeanCallout` both ignore `edge.recommendation`, present the
ungoverned analytical edge under labels implying governed policy output,
and permit adding an analytical candidate to the bet slip with no
requirement that a persisted recommendation exists. Confirmed defect, not
softened by the screen's own "Substep 2b" in-progress marker.

**Reuse:** `availability.py`; `prediction_policy.py`;
`usePendingHighlight.ts` (developer-only visual-audit tooling);
`GameDetail.tsx`'s market/model-fair-value separation, weekly-component
unavailable-state rendering, edge-diagnostic empty/blocked rendering.

**Adapt:** `GameDetail.tsx::RecCell` ignores `edge.recommendation`;
"Policy State" row displays analytical edge data; `ModelLeanCallout`
claims persisted-policy composition but doesn't implement it;
recommendation-specific `WhyLink` used without requiring an attached
recommendation; unrestricted "Add to bet slip" action (high priority —
crosses from labeling into the action boundary).

**Retire:** `prediction_availability.py`.

**Absent:** Decision-quality evaluation of persisted recommendation
decisions against their policy and decision-time evidence.

**Disposition:** CLOSED.

---

### Boundary 5 — Remaining obligation coverage check and closing gaps — CLOSED

#### Obligation-by-obligation coverage table

| # | Obligation | Status |
|---|---|---|
| 1 | Mutable observation preserved without overwrite | Reuse (cutoff-visibility portion); persistence owned by WS1 |
| 2 | Time-valid claim consumes exact source version | Reuse, full chain traced end-to-end |
| 3 | Honest uncertainty/limitation | Reuse |
| 4 | Market price separate from prediction | Reuse |
| 5 | Edge derived without automatically becoming recommendation | Reuse, architecturally enforced |
| 6 | Recommendation policy can recommend or abstain | **Reuse in domain logic for both recommendation and abstention; blocked in current production composition for the recommendation-eligible branch because bankroll evidence is absent; Adapt in every inspected presentation surface because governed state is not consistently rendered.** |
| 7 | Portfolio can allocate zero despite eligible recommendation | **Blocked** — cannot be demonstrated; no eligible recommendation has ever been produced to test the zero-allocation branch against |
| 8 | Later observation can supersede/invalidate downstream artifacts | Partial (WS2 D31, one field, one artifact). **Requires a dedicated Workstream 3 implementation unit to test D35 against a second real case before this workstream can honestly claim its exit criterion is met — not left as a permanent carry-forward.** |
| 9 | Original decision remains reproducible | **Reuse for the inspected candidate, policy, recommended-result, evaluation, and historical-report artifacts.** |
| 10 | Realized outcome and decision-quality evaluation remain separate | Reuse (realized-outcome/prediction-quality/evidence-availability separation); **Absent (decision-quality half) — requires a dedicated implementation unit, not merely a finding.** |

All ten obligations carry an explicit disposition. **Obligations 7, 8,
and 10 each have a confirmed-missing component that requires a dedicated
future implementation unit — not merely a documented gap** — this is
corrected from the prior revision, which listed only 7 and 10 and left 8
as an unassigned carry-forward.

#### `market/recommendations.py` — naming check, resolved without a new read

Confirmed Reuse for behavior and internal naming (unchanged from
Boundary 2). Adapt candidate, very low priority: the module's file path
(not its contents) sits in the same directory as the governed chain and
invites the same class of confusion `visibleRecommendedOffer.ts` risked.

#### Historical-backtest link-to-filename mapping — resolved

Owner-confirmed: the three SharePoint links correspond exactly to
`historical_backtest_report_builder.py`, `historical_backtest_report.py`,
`historical_backtest_report_loader.py`. All citations use filenames; no
anonymous links remain.

#### Presentation evidence for `EdgesTable`, `BetLegCard`, `SlipPanel` — evidence recorded

All three files were read in full during Boundary 5 (owner-pasted plain
text, `REVIEWED_FULL_ATTACHED_SOURCE`):

- **`EdgesTable.tsx`** receives `bankroll`/`kellyMultiplier` as props and
  explicitly marks them unused (`void bankroll; void kellyMultiplier;`),
  with a direct code comment confirming persisted recommendation sizing
  comes from each `EdgeRow`, and these props are Bet-Slip draft-analysis
  inputs never sent to `/edges` — ruling out a hypothesized second
  instance of the bankroll-composition gap. Its "Policy State" table
  column, however, renders a literal empty `<div>` — confirmed, not
  inferred, via direct inspection of `renderOfferRow`. Its "Add" button
  adds any edge to the slip based only on whether it's already added, no
  check for `edge.recommendation` — the same action-boundary gap
  confirmed in `GameDetail.tsx`.
- **`BetLegCard.tsx`** contains an accessibility-labeled section —
  `aria-label="Persisted policy result for {leg}"` — that renders no
  content (an empty `<section>`). Its `ModelSection` analytics panel
  (model probability, edge strength, EV, full Kelly, suggested stake) is
  confirmed sourced entirely from `leg.edgeAnalytics`/local
  `analyzeBetLeg()` computation, not `leg.persistedRecommendation` — the
  primary sizing numbers a user sees during staging and review are
  client-computed analytical figures, not governed sizing.
- **`SlipPanel.tsx`** is considered sound specifically because
  `recordingConfirmation` explicitly branches on
  `leg.persistedRecommendation`, showing the exact persisted
  `result_state`/reference terms/suggested stake when present, or "no
  attached policy result" when absent, and requires explicit
  `window.confirm()` before recording. This is confirmed via direct
  reading of the function body, not inferred from its name.

**Classification:** `EdgesTable`'s "Policy State" column and `BetLegCard`'s
"Persisted policy result" section — Adapt, same root cause as
`GameDetail.tsx`, more specific manifestation (a wired-but-empty slot,
confirmed by direct source, not a labeling mismatch). `SlipPanel`'s
recording confirmation — Reuse, confirmed sound.

#### Remaining `field-status/` files — explicitly not read, by owner decision

Owner confirmed moving away from further `field-status/` inspection.
Recorded as a scoping decision, not an unresolved open item.

#### Final Workstream 3 candidate implementation inventory

The inspection identified **two primary vertical-slice gaps, two missing
proof capabilities, and several bounded cleanup items** (this replaces
the fixed count of "eight," which does not survive the obligation-8
correction above and should not be treated as a durable program fact):

**Primary vertical-slice gaps (high priority):**
1. Bankroll-composition gap — the sole production writer of governed
   recommendations never supplies bankroll evidence; blocks obligation
   6's positive branch and obligation 7 entirely.
2. Governed recommendation presentation across the composed decision
   surface — `GameDetail.tsx`, `EdgesTable`, `BetLegCard` all receive
   governed state but do not render it during staging/review;
   `SlipPanel`'s final confirmation is sound and must be preserved
   unchanged.

**Missing proof capabilities (required for Workstream 3's exit
criterion, not optional):**
3. Decision-quality evaluation for persisted recommendations — confirmed
   absent; obligation 10 cannot be marked satisfied without this.
4. Spread-slice supersession and invalidation proof — a bounded,
   vertical-slice-specific test of D35 against a second real case;
   obligation 8 cannot remain "Partial, unaddressed" if this workstream
   is expected to meet its stated exit criterion.

**Bounded cleanup items (low priority):**
5. `cli/production_chain.py` hardcoded policy schema literal.
6. `resolve_recommended_bet_recording_evidence` stale "Unit 24"
   docstring.
7. `visibleRecommendedOffer.ts` naming/presentation risk.
8. `recordWager.ts` local completeness-check (`result_id`).
9. `prediction_availability.py` — Retire.
10. `market/recommendations.py` file path/naming.

**Disposition:** CLOSED. Obligation-coverage table complete and
corrected for all ten obligations, including the obligation-8 correction.
`recommendations.py` naming resolved. Link-to-filename mapping confirmed.
`EdgesTable`/`BetLegCard`/`SlipPanel` evidence recorded explicitly.
Remaining `field-status/` files explicitly scoped out by owner decision.

---

## Workstream 3 boundary inspection — COMPLETE

Boundaries 1 through 5 are closed. All ten of VISION.md's first-slice
proof obligations carry an explicit, source-confirmed disposition. Four
of ten obligations (6, 7, 8, 10) have a confirmed component requiring
implementation work before Workstream 3 can honestly claim its exit
criterion is met — this is the central, honest finding of the inspection.
Ready to proceed to ROADMAP/PLAN construction of implementation units.
