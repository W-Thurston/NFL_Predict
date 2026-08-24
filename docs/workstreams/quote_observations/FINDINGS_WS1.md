# Gridiron Edge Workstream 1 Repository Inspection
Status: ACTIVE INSPECTION

## Inspection authority
- CONSTITUTION.md: canonical and locked
- VISION.md: canonical and locked
- ROADMAP.md: canonical and locked
- Scope: Workstream 1 only

## Inspected repository snapshot
- Repository: Gridiron Edge SharePoint mirror
- Mirror generated at: 2026-08-23 ~13:30–13:49 UTC (per-file mirror stamps; user to
  confirm exact single mirror-run timestamp)
- Inspection snapshot identifier: WS1-SNAPSHOT-001
- Mirror convention: source files unsupported by SharePoint indexing are mirrored
  with an additional `.txt` suffix
- Source-path rule: all findings record the original repository path; mirrored
  `.txt` paths are references only
- Byte-fidelity limitation: findings that depend on exact syntax, operators,
  whitespace-sensitive content, or truncated text require verification against the
  local source file. (Observed mirror mangling: `int | None` rendered as
  `int  None`; docstrings truncated.)

## Inspection revisions
- Revision: 1 (incorporates reviewer disposition on Boundary 1)
- Authoring thread: Claude
- Reviewing thread: Microsoft 365 Copilot (ChatGPT-side)
- Repository snapshot frozen during author/reviewer cycle: yes

### Ownership terminology (per reviewer)
Existence ≠ ownership. Each candidate is one of:
- **Candidate owner** — file exists; name/location suggests relevance (tree/filename).
- **Documented owner** — a doc (e.g., D25) attributes behavior to it.
- **Verified runtime owner** — behavior confirmed by local source/test/artifact.
Boundary 1 establishes candidate + documented ownership only. Verified runtime
ownership is Boundaries 2–7.

## Workstream 1 scope
Quote observation, identity, persistence, supersession, and cutoff retrieval.

## Explicitly out of scope
- Edge calculation
- Recommendation policy
- Portfolio policy
- Spread prediction
- Opening or closing interpretation
- CLV
- Frontend design
- Claim contract beyond identifying downstream compatibility constraints

## Evidence labels
`VERIFIED_LOCAL_SOURCE` · `VERIFIED_LOCAL_TEST` · `VERIFIED_REAL_ARTIFACT` ·
`REVIEWED_FULL_ATTACHED_SOURCE` (full drag-and-drop attachment; not mirror, not executed) ·
`REVIEWED_FULL_MIRROR` · `SUPPORTED_BY_MIRROR_SNIPPET` ·
`SUPPORTED_BY_FILENAME_ONLY` · `INDEXING_INCOMPLETE` ·
`LOCAL_VERIFICATION_REQUIRED` · `NOT_INSPECTED`

> **Labeling correction (rev 0):** an earlier informal Boundary-1 draft used
> `VERIFIED_TEST` / `VERIFIED_ARTIFACT` for mirror-read files. That was wrong —
> mirror reads are not verification. All such items are corrected DOWN to
> `SUPPORTED_BY_MIRROR_SNIPPET` (partial reads) or `REVIEWED_FULL_MIRROR` (full
> mirrored content), and carry `LOCAL_VERIFICATION_REQUIRED` where a behavioral
> claim depends on them.

---

## Boundary 1 — Inventory & ownership — **CLOSED (rev 1)**
Coverage: file-existence inventory **COMPLETE** via authoritative
`directory_structure.txt`; behavioral ownership **NOT STARTED** (B2–7). No absence
conclusions drawn from mirror indexing; existence is asserted only from the
authoritative tree. All four findings remain **Undecided**.

## Inspection input
- Repository snapshot: WS1-SNAPSHOT-001
- FINDINGS_WS1.md revision: 0
- Scope: Workstream 1
- Source fidelity: SharePoint mirror; exact-logic findings require local verification

## Candidate file set (mirror-derived; relevance only, no classifications)

| # | Repository path | Source lang | Inspection depth | Local verify | Reason relevant (WS1) | Evidence |
|---|---|---|---|---|---|---|
| 1 | `src/gridiron_edge/ingest/odds/store.py` | Python | snippet (import surface via test) | yes | Quote schema, observation identity, ledger append, current-snapshot write, loaders | SUPPORTED_BY_MIRROR_SNIPPET |
| 2 | `src/gridiron_edge/ingest/odds/the_odds_api.py` | Python | snippet | yes | Provider payload → canonical quote rows; fetch; ingest orchestration | SUPPORTED_BY_MIRROR_SNIPPET |
| 3 | `src/gridiron_edge/ingest/odds/nflverse_schedule.py` | Python | filename + D25 ref | yes | Potential schedule-derived odds/market observation adapter; exact provider, sportsbook, and quote semantics require source verification | SUPPORTED_BY_FILENAME_ONLY |
| 4 | `src/gridiron_edge/cli/ingest.py` | Python | snippet (test import) | yes | `gridiron ingest odds --season --week` entry point | SUPPORTED_BY_MIRROR_SNIPPET |
| 5 | `data/odds/` tree | data | artifact listing | yes | `odds_current.parquet`; `history/season=…/week=…/observations.parquet`; `collection_plans/` | SUPPORTED_BY_MIRROR_SNIPPET |
| 6 | `src/gridiron_edge/market/collection_plan.py` | Python | filename/test | yes | Weekly quote-collection plan | SUPPORTED_BY_FILENAME_ONLY |
| 7 | `src/gridiron_edge/market/collection_execution.py` | Python | filename/test | yes | Due-state eval + single-shot collection | SUPPORTED_BY_FILENAME_ONLY |
| 8 | `src/gridiron_edge/market/collection_receipt_store.py` | Python | snippet | yes | Collection claim / terminal-result receipts | SUPPORTED_BY_MIRROR_SNIPPET |
| 9 | `tests/unit/ingest/test_odds_store.py` | Python (test) | partial mirrored content | yes | Provider-aware validation + atomic storage contract | SUPPORTED_BY_MIRROR_SNIPPET |
| 10 | `tests/unit/cli/test_ingest_odds_cli.py` | Python (test) | filename | yes | Ingest command behavior; artifact paths | SUPPORTED_BY_FILENAME_ONLY |
| 11 | `tests/integration/api/test_lines_routes.py` | Python (test) | filename | yes | Line Shopping over current snapshot | SUPPORTED_BY_FILENAME_ONLY |
| 12 | `DECISIONS.md` (D25) | Markdown | full content | no | Quote contract + ledger/snapshot boundary of authority | REVIEWED_FULL_MIRROR |
| 13 | `CHANGELOG.md` (2026-08-05 Market data) | Markdown | full content | no | Real-data validation counts; schema history | REVIEWED_FULL_MIRROR |

## Downstream consumers — inventoried for the Boundary 6 SEPARATION check only (NOT WS1 build targets)

| Repository path | Consumes quotes for | Evidence |
|---|---|---|
| `src/gridiron_edge/market/candidate_issuance.py` | Pregame candidate (edge) issuance | SUPPORTED_BY_MIRROR_SNIPPET |
| `src/gridiron_edge/market/recommendations.py` | Recommendation pivot | SUPPORTED_BY_FILENAME_ONLY (D25 ref) |
| `src/gridiron_edge/market/weekly_edge_service.py` | Weekly edge diagnostics | SUPPORTED_BY_FILENAME_ONLY (D25 ref) |
| `src/gridiron_edge/api/routes/lines.py` | Current Line Shopping serialization | SUPPORTED_BY_FILENAME_ONLY |
| `src/gridiron_edge/market/history_boundaries.py` | Likely returns selected historical observations (earliest-observed / latest-eligible); check it preserves raw-observation semantics | SUPPORTED_BY_FILENAME_ONLY |
| `src/gridiron_edge/market/history_coverage.py` | Potential ledger consumer for completeness analysis; check raw vs derived | SUPPORTED_BY_FILENAME_ONLY |
| `src/gridiron_edge/market/line_shopping.py` | Clear candidate consumer (naming); exact inputs/selection semantics need source review | SUPPORTED_BY_FILENAME_ONLY |
| `src/gridiron_edge/market/market_closeout.py` | May consume historical/selected quotes; determine exact-source vs derived-selection only | SUPPORTED_BY_FILENAME_ONLY |
| `src/gridiron_edge/market/clv.py` | Out of scope for calculations; check only whether it reads exact observations vs an interpreted closing artifact | SUPPORTED_BY_FILENAME_ONLY |
| `src/gridiron_edge/api/serializers/lines.py` | Determine whether API presentation preserves/obscures source observation identity | SUPPORTED_BY_FILENAME_ONLY |
| `src/gridiron_edge/api/schemas/lines.py` | Downstream contract candidate; not a WS1 build target | SUPPORTED_BY_FILENAME_ONLY |

*All downstream rows are behavior-unverified candidates. None are WS1 build
targets; they exist to confirm observation-vs-interpretation separation in Boundary 6.*

### Additional candidate owners added by completeness check (per reviewer)
| Repository path | Candidate responsibility | Routing | Evidence |
|---|---|---|---|
| `src/gridiron_edge/market/collection_plan_store.py` | Collection-plan persistence (intended/selected/current plans) — orchestration metadata, **not** quote storage | B1 candidate; B6 (don't confuse plan selection with quote identity) | SUPPORTED_BY_FILENAME_ONLY |
| `src/gridiron_edge/deployment/quote_collection_worker.py` | Likely observation-creation orchestration (collection-time assignment, ingest invocation) | B2 (fetched_at/invocation); B4 (concurrency/retries/dupes) | SUPPORTED_BY_FILENAME_ONLY |
| `src/gridiron_edge/api/loaders.py` | Possible retrieval delegate; inspect **only if** symbol search confirms `load_current_odds`/`load_odds_ledger`/`odds_current`/`observations.parquet` | B5 (conditional) | SUPPORTED_BY_FILENAME_ONLY |
| `src/gridiron_edge/core/paths.py`, `core/settings.py` | Possible artifact roots / provider config — **do not add as candidates yet**; inspect only if imported by store/api/worker/CLI | conditional follow-on | NOT_INSPECTED |

## Findings

### Finding WS1-F01
**Claim:** A provider-aware normalized quote row is the current observation unit,
separating aggregator `provider` from offered-price `sportsbook`.
**Evidence:** `DECISIONS.md` D25; `CHANGELOG.md` 2026-08-05 "Market data".
**Evidence strength:** REVIEWED_FULL_MIRROR.
**Observed behavior:** D25 defines a normalized row with `fetched_at` (local UTC
observation time), `provider`, `provider_event_id`, `sportsbook` (nullable for
consensus), `sportsbook_updated_at`, `commence_time`, `is_live`, plus canonical
season/week/game/date/team/market/side/American-odds/line fields.
**Scope limitation:** Establishes the documented contract, not the exact code
fields or runtime validation. Column count ("17-column") is from CHANGELOG prose.
**WS1 criterion:** Provider/market/outcome/line/price identity unambiguous.
**Classification:** Undecided (contract-level only).
**Local verification:** Required (confirm `QUOTE_COLUMNS` in `store.py`).
**Reasoning:** Strong documentary alignment with WS1 identity needs, but code-level
confirmation pending.

### Finding WS1-F02
**Claim:** `store.py` treats observation identity as a first-class concept
distinct from the full quote row.
**Evidence:** `tests/unit/ingest/test_odds_store.py` imports
`OBSERVATION_IDENTITY_COLUMNS`, `OBSERVATION_SORT_COLUMNS`, `QUOTE_COLUMNS`,
`append_to_odds_ledger`, `load_current_odds`, `load_odds_ledger`,
`odds_history_partition_path`, `validate_quote_rows`, `write_current_odds_snapshot`.
**Evidence strength:** SUPPORTED_BY_MIRROR_SNIPPET.
**Observed behavior:** The public store surface separates identity columns, sort
columns, and the quote schema, and exposes distinct history-ledger vs
current-snapshot read/write functions.
**Scope limitation:** Import surface only; does not establish how identity columns
are computed or enforced.
**WS1 criterion:** Identity; persistence (history vs current).
**Classification:** Undecided.
**Local verification:** Required.
**Reasoning:** Symbol names strongly suggest WS1-aligned separation; behavior
unproven until `store.py` source is read locally.

### Finding WS1-F03
**Claim:** Persistence appears to append observations to a ledger AND atomically
replace a separate current snapshot.
**Evidence:** D25 ("appends observed quotes to the local observation ledger and
atomically replaces the current snapshot"); CHANGELOG ("atomic Parquet replacement
and row-level idempotency while preserving multiple sportsbooks and distinct
observations").
**Evidence strength:** REVIEWED_FULL_MIRROR (documentary).
**Observed behavior:** Two distinct artifacts described: append-only history vs
atomically-replaced current snapshot.
**Scope limitation:** Documentary; not confirmed against `append_to_odds_ledger` /
`write_current_odds_snapshot` source or the on-disk parquet.
**WS1 criterion:** No overwrite of historical evidence; current-vs-history split.
**Classification:** Undecided (this is the critical Boundary 4 question).
**Local verification:** Required.
**Reasoning:** If confirmed, this is the core WS1 property already present — but it
must be proven at code + artifact level, not from prose.

### Finding WS1-F04
**Claim:** A real ledger of changing observations coexisting without overwrite
plausibly already exists.
**Evidence:** CHANGELOG 2026-08-18: "canonical Week 1 ledger contains 1,680
observations at two distinct UTC fetch timestamps … all exact identities have
repeated depth two: 274 Moneyline, 282 Spread, 284 Total."
**Evidence strength:** VERIFIED_REAL_ARTIFACT **claimed in prose**, but not
independently inspected → treat as SUPPORTED_BY_MIRROR_SNIPPET until the actual
`observations.parquet` is read.
**Observed behavior:** Prose asserts each identity is stored twice across two fetch
timestamps.
**Scope limitation:** Not yet confirmed by reading the parquet or a passing test.
**WS1 criterion:** Two differing observations of the same market state coexist.
**Classification:** Undecided.
**Local verification:** Required (inspect `history/.../observations.parquet`).
**Reasoning:** Direct evidence of the WS1 no-overwrite property if the artifact
confirms depth-two identities; the strongest positive signal so far.

## Time-semantics signals (route to Boundary 2 — not classified)
- Present: `fetched_at` (system-known/collection), `sportsbook_updated_at`
  (source-published), `commence_time` (kickoff), `is_live`.
- Not yet observed: an explicit **decision-cutoff** concept and an explicit
  **effective-time** distinct from `fetched_at`. Potential WS1 gap — Boundary 2/5.

## Open unknowns (Boundary 2–5)
1. Is `fetched_at` assigned once at ingest or recomputed downstream? (B2)
2. Can retrieval answer "what was known at cutoff T" within a week, or only
   partition by season/week and filter `fetched_at`? Inspect `load_odds_ledger`. (B5)
3. How are malformed / conflicting / duplicate observations handled by
   `validate_quote_rows` and the store? (B2/B4)
4. Does `candidate_issuance` read raw observations directly, keeping observation
   storage separate from selection/recommendation? (B6)

## Coverage / completeness
- Coverage: **COMPLETE (inventory).** Index-independent completeness check
  satisfied via authoritative `directory_structure.txt` (`tree` dump, 1,679 files)
  — supersedes the SharePoint index for existence.
- **Working method (decided):** boundary-scoped **local file drops** → full
  byte-fidelity reads (`VERIFIED_LOCAL_SOURCE`); parquet artifacts loaded directly
  (`VERIFIED_REAL_ARTIFACT`). SharePoint mirror retained as ChatGPT's independent
  (structural) read path.

### Authoritative WS1 candidate inventory (from directory tree)
Ingest/odds: `store.py`, `the_odds_api.py`, `nflverse_schedule.py`, `__init__.py`.
Collection/history (`market/`): `collection_plan.py`, `collection_plan_store.py`,
`collection_execution.py`, `collection_receipt_store.py`, `history_boundaries.py`,
`history_coverage.py`, `line_shopping.py`, `odds_math.py`.
Worker/deploy: `deployment/quote_collection_worker.py`,
`deploy/bin/{install,verify}_quote_collection_worker.py`.
CLI: `cli/ingest.py`. Retrieval/API: `api/routes/lines.py`, `api/schemas/lines.py`,
`api/serializers/lines.py`.
Artifacts (`data/odds/`): `odds_current.parquet`,
`history/season=2026-2027/week=01/observations.parquet`,
`collection_plans/{current.json, season=2026-2027/week=01.json}`.
Tests: `tests/unit/ingest/test_odds_store.py`, `test_odds_store_source_neutral.py`,
`tests/unit/ingest/odds/{test_the_odds_api_parser,test_the_odds_api_client,test_the_odds_api_ingest,test_nflverse_adapter}.py`,
`tests/unit/market/{test_collection_plan,test_collection_plan_store,test_collection_execution,test_collection_receipt_store,test_history_boundaries,test_history_coverage,test_line_shopping,test_line_shopping_guidance,test_odds_math}.py`,
`tests/unit/cli/{test_ingest_odds_cli,test_collection_plan_cli,test_collection_execution_cli}.py`,
`tests/unit/deployment/test_quote_collection_worker.py`,
`tests/integration/api/test_lines_routes.py`, `tests/integration/test_odds_join.py`.

### Inventory corrections vs mirror-only pass (completeness check caught these)
- **ADDED** `tests/unit/ingest/test_odds_store_source_neutral.py` — separate
  source-neutrality test; routes to Boundary 6 (observation-vs-selection separation).
- **ADDED** `market/history_boundaries.py`, `market/history_coverage.py` —
  dedicated history/cutoff-retrieval owners; prime Boundary 5 targets. (Aligns with
  prior memory: leakage-safe historical quote boundaries, earliest-observed /
  latest-eligible-pregame selection.)

### Boundary 5 note (per reviewer)
`test_history_boundaries.py` and `test_history_coverage.py` are elevated to
**primary Boundary 5 targets** (as-known-at-cutoff retrieval), not general market tests.

### Boundary 2 drop list (requested from repo owner)
For full-fidelity observation-creation review, drop these here:
`src/gridiron_edge/ingest/odds/the_odds_api.py`,
`src/gridiron_edge/ingest/odds/store.py`,
`tests/unit/ingest/odds/test_the_odds_api_parser.py`,
`tests/unit/ingest/test_odds_store.py`,
`tests/unit/ingest/test_odds_store_source_neutral.py` (source-neutrality → is
observation ownership market-generic & provider-aware), and (for artifact
verification of F03/F04)
`data/odds/history/season=2026-2027/week=01/observations.parquet`.
**Conditionally:** `src/gridiron_edge/deployment/quote_collection_worker.py` —
only if `the_odds_api.py` does not clearly establish where `fetched_at` is assigned
and how a collection attempt invokes persistence.

### Boundary 1 disposition — CLOSED (rev 1)
Boundary reviewed: **1 — inventory & ownership.**
Coverage:
- File-existence inventory: **COMPLETE** for WS1-SNAPSHOT-001 via the authoritative
  local directory tree.
- Behavioral ownership verification: **NOT STARTED** beyond documentary and
  mirror-derived signals; handled in Boundaries 2–7.
Status: **CLOSED.** All seven reviewer modifications incorporated:
(1) status contradiction resolved; (2) `test_odds_store.py` relabeled
`SUPPORTED_BY_MIRROR_SNIPPET`; (3) `nflverse_schedule.py` reason de-attributed to
filename-only; (4) added candidate owners (`collection_plan_store.py`,
`quote_collection_worker.py`, conditional `api/loaders.py`; `core/paths|settings`
held as non-candidates); (5) added seven behavior-unverified Boundary 6 consumers;
(6) added `test_odds_store_source_neutral.py` to the Boundary 2 drop list;
(7) all four findings remain **Undecided**.

Next: **Boundary 2 — observation creation**, on receipt of the drop list above.

Reviewer responds only with: Accepted findings / Accepted with modification /
Rejected findings / Insufficient evidence / Missing inspection targets /
Classification changes / Local verification requests / Scope-control findings /
Boundary disposition (Ready to close / Not).

---

## Boundary 2 — Observation creation — **CLOSED (rev 2)**

## Source-delivery clarification
The files reviewed for Boundary 2 were **drag-and-dropped** into the Microsoft
Copilot interface. Copilot stored those attachments in SharePoint, but they were
**not** retrieved from the automated repository mirror and do **not** carry the
mirror's appended `.txt` convention. They are treated as full-content attached
source files representing the local repository snapshot supplied by the repository
owner. New evidence label: **`REVIEWED_FULL_ATTACHED_SOURCE`** — full attached
source read, not mirror-derived, not runtime-executed. Reserve
`VERIFIED_LOCAL_SOURCE` for protocol-defined verification and `VERIFIED_LOCAL_TEST`
for a successful local test run.

## Inspection input
- Repository snapshot: WS1-SNAPSHOT-001
- FINDINGS_WS1.md revision: 2 (incorporates reviewer disposition on Boundary 2)
- Scope: Workstream 1 — observation creation (parse → normalize → quote row; time assignment; malformed/conflict handling; persistence no-overwrite)
- Source fidelity: **Full-content attached-source reads** of `.py` files (clean, unmangled). Labeled `REVIEWED_FULL_ATTACHED_SOURCE`; behavioral claims cite explicit test *assertions* (read, not executed) — stamp `VERIFIED_LOCAL_TEST` only after a local `pytest` run.

## Files reviewed (full attached source)
- `src/gridiron_edge/ingest/odds/store.py` — 374 lines — REVIEWED_FULL_ATTACHED_SOURCE
- `src/gridiron_edge/ingest/odds/the_odds_api.py` — 339 lines — REVIEWED_FULL_ATTACHED_SOURCE
- `tests/unit/ingest/test_odds_store.py` — 263 lines — REVIEWED_FULL_ATTACHED_SOURCE
- `tests/unit/ingest/test_odds_store_source_neutral.py` — 48 lines — REVIEWED_FULL_ATTACHED_SOURCE *(reviewer lacked this attachment this turn; its unique assertions are not independently reviewer-verified)*
- `tests/unit/ingest/odds/test_the_odds_api_parser.py` — 201 lines — REVIEWED_FULL_ATTACHED_SOURCE

> **Parquet note:** `observations.parquet` could not be uploaded (Copilot rejects
> `.parquet`). Not blocking: the no-overwrite property is proven by the test suite
> across multiple scenarios (stronger than a single artifact snapshot). Optional
> confirmation: run the tests locally, or `df.to_csv()` the parquet and drop the CSV.

## Findings

### Finding WS1-F05 — `fetched_at` is assigned exactly once, at ingest
**Claim:** *Within the reviewed The Odds API ingestion path*, one `fetched_at` value is established per pull and propagated uniformly to all resulting rows; it is not recomputed downstream in the attached parser or store. (Other provider adapters and the worker path are not yet verified.)
**Evidence:** `the_odds_api.py`: `ingest_the_odds_api_current` sets `observed_at = fetched_at or datetime.now(UTC)`; `parse_the_odds_api_payload` does `observed_at = _utc_timestamp(fetched_at, ...)` and writes `"fetched_at": observed_at` on every row.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**Observed behavior:** One timestamp per collection, uniformly applied.
**Scope limitation:** Establishes assignment at ingest; does not establish any *decision-cutoff* concept (see F11).
**WS1 criterion:** Time semantics not conflated. **Classification:** Reuse (provisional). **Local verification:** Recommended (formality). Resolves B2 open-unknown #1.

### Finding WS1-F06 — Three explicit identity tiers (not one)
**Claim:** `store.py` defines three distinct column tuples for three distinct purposes.
**Evidence:** `OBSERVATION_FETCH_IDENTITY_COLUMNS` (7: fetched_at, provider, provider_event_id, sportsbook, game_id, market, side) = **within-fetch conflict key**; `OBSERVATION_IDENTITY_COLUMNS` (11: +odds, line, sportsbook_updated_at, is_live) = **dedup/idempotency key**; `OBSERVATION_SORT_COLUMNS` = deterministic ordering.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**Observed behavior:** Same market-side at same fetch with *different* odds/line ⇒ conflict (rejected); *identical* row re-appended ⇒ deduped (idempotent); different fetch ⇒ retained as new observation.
**Scope limitation:** None for identity; this is the mechanism behind F04.
**WS1 criterion:** Identity unambiguous; two differing observations coexist. **Classification:** Reuse (provisional). Upgrades F02 from snippet to full-source.

### Finding WS1-F07 — Historical persistence is logically append-preserving, physically atomically rewritten *(upgrades F03/F04)*
**Claim:** `append_to_odds_ledger` reads the existing partition, concatenates, canonicalizes (dedup keep=last on the 11-col identity), and atomically rewrites — changing observations coexist without overwrite.
**Evidence (code):** `store.py::append_to_odds_ledger` → `_canonicalize_observations` → `_atomic_write_parquet` (temp file + `.replace()`).
**Evidence (tests, explicit assertions):**
- `test_later_local_observation_is_retained`: two fetches ⇒ **4 rows**.
- `test_later_changed_price_is_retained`: odds ⇒ `[-150.0, -140.0]` (both kept).
- `test_later_changed_line_is_retained`: spread line ⇒ `[3.5, 4.0]` (both kept).
- `test_exact_rerun_is_idempotent_and_multiple_books_survive`: identical re-append ⇒ still **2 rows**.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE (code + test assertions). → `VERIFIED_LOCAL_TEST` on one local run.
**Observed behavior:** Logical history is **append-preserving** (existing distinct observations retained; exact reruns deduplicated); the physical weekly parquet partition is **atomically rewritten** (read-existing → concat → canonicalize → temp-file `.replace()`), not mutated in place.
**Scope limitation:** Proves *append-time* history retention; temporal *cutoff retrieval* is Boundary 5. **Concurrency/crash-consistency NOT established** — atomic single-writer replacement does not prove safe multi-process updates (Boundary 4).
**WS1 criterion:** No loss of historical evidence; two differing observations coexist. **Classification:** **Reuse (strong), provisional** (pending local test run + Boundary 8). The single most important positive WS1 result.

### Finding WS1-F08 — Malformed / conflicting / mixed-scope inputs are rejected without corrupting existing data
**Claim:** Invalid appends raise and leave prior bytes untouched; within-fetch conflicts and mixed season/week scopes are rejected.
**Evidence:** `_validate_observation_conflicts` (raises on 7-col fetch-identity dup with differing values); `_single_partition_scope` (rejects >1 scope). Tests: `test_same_fetch_conflicting_observations_are_rejected` (asserts `path.read_bytes() == before`), `test_invalid_append_preserves_existing_ledger` (schema failure preserves bytes), `test_mixed_scope_append_is_rejected`, `test_appending_one_week_does_not_rewrite_another`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE (code + tests).
**Observed behavior:** Explicit, safe failure states — no partial/corrupting writes.
**WS1 criterion:** Missing/conflicting/malformed remain explicit; no destruction of history. **Classification:** Reuse (strong). Resolves B2 open-unknown #3.

### Finding WS1-F09 — Current snapshot is a separate, atomically-replaced artifact
**Claim:** `odds_current.parquet` (current) is distinct from `history/.../observations.parquet` (append-only), each written via temp-file + atomic `.replace()`.
**Evidence:** `write_current_odds_snapshot` vs `append_to_odds_ledger`; `_atomic_write_parquet`; tests `test_snapshot_atomically_overwrites_and_roundtrips`, `test_current_snapshot_is_separate_from_history`, `test_invalid_snapshot_does_not_replace_existing_file`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE (code + tests).
**Observed behavior:** Snapshot overwrite is intended (current view); history is not. The two roles are cleanly separated — matches VISION "current vs history."
**WS1 criterion:** Current-vs-history separation. **Classification:** Reuse (strong).

### Finding WS1-F10 — Source-neutral & market-generic observation contract *(not moneyline-specialized)*
**Claim:** The quote row separates aggregator `provider` from offered-price `sportsbook` (nullable for consensus), and validation is market-generic across moneyline/spread/total.
**Evidence:** `store.py::_VALID_MARKET_SIDES` (all three families); nullable `sportsbook`/`provider_event_id`; `test_odds_store_source_neutral.py` (nflverse consensus row with null sportsbook, NA odds/line is valid; `" "` rejected as "null or nonempty"). Parser preserves **all** bookmakers, picks no "best."
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE (code + tests).
**Observed behavior:** Observation ownership is generic; honors the WS1 scope-boundary that storage must not specialize around moneyline.
**WS1 criterion:** Provider/market/outcome identity unambiguous; generic. **Classification:** Reuse (strong).

### Finding WS1-F11 — Temporal contract: three world-times present; generic loader exposes no as-known-at-cutoff retrieval *(the key WS1 delta — split per reviewer)*
**Claim:** The observation contract preserves three UTC-enforced timestamps —
`fetched_at` (system-known), `sportsbook_updated_at` (source-published),
`commence_time` (kickoff). The reviewed generic loader `load_odds_ledger` filters
by provider/sportsbook/season/week/market only, exposing **no as-known-at-cutoff
retrieval operation**. Whether the quote schema itself requires additional temporal
fields remains **undecided pending Boundary 5**.
**Evidence:** `store.py`: `_normalize_utc_timestamp` for the three columns;
`load_odds_ledger` filter signature has no time-cutoff parameter.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**Observed behavior:** `fetched_at` *can serve* as a cutoff basis; `sportsbook_updated_at`
*may* already be the source's effective/published time. Whether a *further*
effective-time concept is required depends on the contract set during a future
temporal-design unit — not decided here.
**Scope limitation:** Retrieval semantics fully assessed in Boundary 5, which may
reveal that `history_boundaries.py` already performs cutoff selection over
`fetched_at`. Concurrency not assessed.
**WS1 criterion:** Point-in-time correctness (bitemporal).
**Classification (split, per reviewer):**
- Observation timestamps (`fetched_at`/`sportsbook_updated_at`/`commence_time`) → **Reuse (provisional).**
- Generic ledger loader (`load_odds_ledger`) → **Adapt (likely), pending Boundary 5.**
- As-known-at-cutoff retrieval → **Undecided until Boundary 5** (check `history_boundaries.py`).
- Additional `decision_cutoff` / `effective_time` schema fields → **Do not decide yet** (a decision-cutoff is a *claim/retrieval parameter*, not necessarily raw-observation metadata; adding it to quote rows could conflate observation with downstream claim).

> **First PLAN unit: NOT chosen here.** Retracting the earlier "most likely first
> PLAN unit" claim. Unit selection waits for Boundaries 3–7 and consolidation at
> Boundary 8 — Boundary 5 in particular may show `history_boundaries.py` already
> supplies some/all as-known-at behavior.

### Finding WS1-F12 — Observation creation is pregame-only and excludes started events
**Claim:** Parser stamps `is_live: False` and drops events where `commence_time <= fetched_at`.
**Evidence:** `the_odds_api.py::parse_the_odds_api_payload` (`if commence_time <= observed_at: continue`); test `test_excludes_unmatched_wrong_sport_and_started_events`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE (code + test).
**Observed behavior:** Only pre-kickoff, non-live observations enter the store.
**WS1 criterion:** Eligibility/pre-kickoff discipline. **Classification:** Reuse **for this provider adapter** (another adapter may differ; not a general WS1 temporal-contract proof).

## Classification summary (provisional — consolidated formally at Boundary 8)
| Component | Provisional | Basis |
|---|---|---|
*(All Reuse calls are **provisional** — held until a local test run + Boundary 8 consolidation.)*
| Quote schema / validation (`validate_quote_rows`) | Provisional Reuse | F01/F06/F10 |
| Observation identity (3 tiers) | Provisional Reuse | F06 |
| Historical persistence (logical append / physical atomic rewrite) | Provisional Reuse (strong) | F07/F08 |
| Current snapshot | Provisional Reuse (strong) | F09 |
| Source-neutrality / market-generic | Provisional Reuse (strong) | F10 |
| Provider parser (`the_odds_api.py`) | Provisional Reuse | F05/F12 |
| Existing observation timestamps | Provisional Reuse | F11 (split) |
| **Generic ledger loader (`load_odds_ledger`)** | **Adapt likely, pending Boundary 5** | F11 |
| As-known-at-cutoff retrieval | Undecided → Boundary 5 | F11 |
| Additional `decision_cutoff`/`effective_time` fields | Do not decide yet | F11 |
| Concurrent-writer safety | Undecided → Boundary 4 | F07 scope-limit |
| First PLAN.md unit | Undecided → Boundary 8 | — |

## Updated open unknowns
1. ~~fetched_at once vs recomputed~~ → **Resolved (F05: once at ingest).**
2. as-known-at cutoff retrieval → **Boundary 5** (F11 shows creation-side absence; confirm no hidden cutoff reader; check `history_boundaries.py`).
3. ~~malformed/conflicting/duplicate handling~~ → **Resolved (F08).**
4. candidate_issuance reads raw vs derived → **Boundary 6.**

## Boundary 2 disposition — CLOSED (rev 2)
All eight reviewer modifications incorporated:
(1) "mirror reads" → drag-and-drop **attached** sources; (2) new label
`REVIEWED_FULL_ATTACHED_SOURCE` applied to all five files; (3) F07 reworded as
logical append-preservation + physical atomic rewrite; (4) F05 narrowed to the
reviewed The Odds API path; (5) F11 split into reusable timestamps / likely loader
adaptation / undecided cutoff retrieval / undecided additional fields; (6) "first
PLAN unit" claim **retracted**; (7) all Reuse marked **provisional** (pending local
test run + Boundary 8); (8) parquet recorded **unavailable for direct upload**
(optional CSV/summary path noted).

**Insufficient-evidence carried forward (per reviewer):** actual Week 1 parquet
contents unverified; `test_odds_store_source_neutral.py` not available to reviewer
this turn; worker-level timestamp/retry behavior unverified; concurrent-writer
safety not established; as-known-at-cutoff retrieval unresolved until
`history_boundaries.py` is inspected.

**Optional local verification (owner):**
```
uv run pytest tests/unit/ingest/test_odds_store.py \
  tests/unit/ingest/test_odds_store_source_neutral.py \
  tests/unit/ingest/odds/test_the_odds_api_parser.py
# Parquet → reviewable CSV (do not alter the canonical artifact):
uv run python - <<'PY'
import pandas as pd
df = pd.read_parquet("data/odds/history/season=2026-2027/week=01/observations.parquet")
df.to_csv("observations_ws1_review.csv", index=False)
print(len(df), "rows ->", "observations_ws1_review.csv")
PY
```
A compact derived summary (schema, row count, fetch timestamps, identity depth,
duplicate counts, changed price/line examples) is preferable to all 1,680 rows —
clearly labeled as derived from the parquet, not the parquet itself.

**Next:** Boundary 2 CLOSED → proceed to **Boundary 3 — quote identity** (largely
pre-verified by F06; will confirm sameness keys, coexistence of differing prices/
lines, provider identity stability, and market-vs-quote identity separation).

---

## Boundary 3 — Quote identity — **CLOSED (rev 3)**

> Replaces the prior "Boundary 3 — for reviewer" section. All eight reviewer
> modifications incorporated. On merge, set header revision to 3.

### Inspection input
- Repository snapshot: WS1-SNAPSHOT-001
- FINDINGS_WS1.md revision: 3
- Scope: WS1 — quote identity (sameness keys; price/line coexistence; provider/sportsbook separation; game-identity anchoring; descriptive-field trust boundary)
- Source: `store.py` (374) + `the_odds_api.py` (339) — `REVIEWED_FULL_ATTACHED_SOURCE`. No new drops.

### Findings

#### Finding WS1-F13 — Sameness is defined at two explicit levels
**Claim:** "Same observation" is defined by two keys: a 7-col **within-fetch conflict
key** (`fetched_at, provider, provider_event_id, sportsbook, game_id, market, side`)
and an 11-col **persisted dedup key** (the 7 + `odds, line, sportsbook_updated_at,
is_live`), plus a separate sort tuple. `_canonicalize_observations` first drops
rows identical on the 11-col key, then `_validate_observation_conflicts` rejects
remaining rows sharing the 7-col key.
**Evidence:** `store.py` identity tuples; `_canonicalize_observations`;
`_validate_observation_conflicts`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE (code; tests read, not executed).
**Scope limitation:** Implementation mechanics are established. Whether these keys
are sufficient for **every provider and future market family** is a design-suitability
question for Boundary 8. Tests inspected, not yet executed this cycle.
**WS1 criterion:** Identity unambiguous; sameness precisely defined.
**Classification:** Provisional Reuse.

#### Finding WS1-F14 — Differing price/line coexist across fetches; conflict within one fetch key
**Claim:** Differing `odds`/`line` values for the same `provider, provider_event_id,
sportsbook, game_id, market, side` **coexist when they belong to different local
fetches** (distinct `fetched_at`). Competing values under the **same 7-col
local-fetch identity** are rejected as conflicts.
**Evidence:** `fetched_at` participates in both identity tuples; dedup key includes
`odds`/`line`; `_validate_observation_conflicts` on the 7-col key. Tests
`test_later_changed_price_is_retained` (`[-150.0, -140.0]`),
`test_later_changed_line_is_retained` (`[3.5, 4.0]`),
`test_same_fetch_conflicting_observations_are_rejected`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**WS1 criterion:** Two differing observations of the same market state coexist.
**Classification:** Provisional Reuse (strong). *Directly supports the spread slice:
line movement across polls is captured as coexisting observations.*

#### Finding WS1-F15 — Market-family and outcome-side identity are not conflated with line/price
**Claim:** The stored contract separates `market`, `side`, `line`, and `odds`. The
7-col conflict key includes `market` and `side` but **not** `line` or `odds` — so
the store treats the market family + side as the thing observed and line/price as
values that may change across fetches. A move from `line` 3.5→4.0 stays within the
`spread` family and same side while producing a distinct later observation.
**Evidence:** `OBSERVATION_FETCH_IDENTITY_COLUMNS` excludes `odds`/`line`; `market`
is a fixed family label (`_VALID_MARKET_SIDES`); `line` a separate numeric column;
`test_later_changed_line_is_retained`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**Wording note (per reviewer):** `(game_id, market, side)` is **not** the complete
"market identity" — the enforced fetch identity also includes provider,
provider_event, sportsbook, and `fetched_at`. The precise claim is *market-family
and outcome-side identity are not conflated with the displayed line and price*.
**WS1 criterion:** Market-family/side vs displayed values not conflated (core WS1 ask).
**Classification:** Provisional Reuse (strong). *The key Boundary 3 result for spread.*

#### Finding WS1-F16 — Structural provider/sportsbook separation (stability NOT established)
**Claim:** The contract structurally separates aggregator `provider`, `provider_event_id`,
and offered-price `sportsbook`. `provider` is required non-empty; `provider_event_id`
and `sportsbook` may be null for sources that don't supply them. All participate in
identity, allowing multiple books to coexist. **Canonical stability** of provider
labels and provider-event identifiers (registry, aliasing, capitalization, cross-run
spelling) is **not** established by the generic store — `_validate_required_text`
checks non-empty but does **not** canonicalize.
**Evidence:** `_REQUIRED_TEXT_COLUMNS` (provider); `_NULLABLE_TEXT_COLUMNS`
(`provider_event_id`, `sportsbook`); identity tuples include `sportsbook`;
`_validate_required_text` (no rewrite); `test_exact_rerun_is_idempotent_and_multiple_books_survive`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**WS1 criterion:** Provider/book identity structurally unambiguous.
**Classification (split, per reviewer):**
- Structural provider/sportsbook separation → **Provisional Reuse.**
- Multiple-books coexistence → **Provisional Reuse (strong).**
- Cross-source provider-label / provider-event identifier stability → **Undecided**
  (responsibility of source adapters or a separate normalization boundary).

#### Finding WS1-F17 — Store identity anchored to canonical `game_id`; upstream resolution trusted
**Claim:** Observation identity is anchored to canonical `game_id`, **not** to stored
team-name/team-code fields (which are in `QUOTE_COLUMNS` but not in either identity
tuple). The generic store **trusts** upstream adapters to resolve and populate a
valid `game_id`; it does **not** verify consistency between `game_id`, team names,
`game_date`, or provider event id. In the reviewed The Odds API path, normalized
team-name matching selects a schedule row and emits that row's `game_id`. Other
adapter paths remain unverified.
**Evidence:** `store.py` identity tuples use `game_id`; team fields validated
non-empty but not cross-checked; `the_odds_api.py::_normalize_team` + `_schedule_lookup`
→ `game_id` from `schedule_row`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**Scope note (narrowed, per reviewer):** Relocation variants do not *participate in
identity*, but *may still appear in descriptive team columns* if an adapter supplies
them — the store does not canonicalize them. Boundary 6 should inspect **only whether
the ownership separation is clean**, not launch a general historical team-normalization
audit unless a concrete quote-identity defect requires it.
**WS1 criterion:** Identity anchored on canonical game id (by delegation).
**Classification:** Store identity on `game_id` → Provisional Reuse; The Odds API
schedule matching → Provisional Reuse (that adapter); other provider/team-code
handling → Undecided → Boundary 6.

#### Finding WS1-F18 — `sportsbook_updated_at` in the dedup key: exact-idempotency only *(replaces prior F18)*
**Claim (corrected):** `sportsbook_updated_at` participates in **exact-row
idempotency** but does **not** independently establish cross-fetch observation
identity, because `fetched_at` already does. Two same-fetch rows differing only in
`sportsbook_updated_at` survive the 11-col exact dedup but are then **rejected as a
conflict** by the 7-col fetch key — they do **not** coexist as two persisted
observations.
**Evidence:** `_canonicalize_observations` (11-col `drop_duplicates` → then
`_validate_observation_conflicts` on 7-col); `fetched_at` present in both tuples.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**Retraction note:** The prior F18 claimed a "value-change vs source-published-change"
cross-fetch design choice. That was **incorrect** — the effect it described is driven
by `fetched_at`, not `sportsbook_updated_at`. No temporal-design decision is routed
from this. (Optional future test: two same-fetch rows, identical `odds`/`line`,
differing `sportsbook_updated_at` → expect same-fetch conflict; not currently
demonstrated. Suggested, not a prerequisite.)
**WS1 criterion:** Identity precision. **Classification:** Provisional Reuse (no design question).

#### Finding WS1-F19 — Descriptive/event-time fields trusted to upstream for exact-identity rows *(design/trust-boundary, per reviewer)*
**Claim:** The identity tuples omit `season, week, game_date, away_team, home_team,
commence_time`. Season/week are constrained via physical partition scope, but the
store does **not** establish that a change to `game_date`, team names, or
`commence_time` within an otherwise-identical persisted identity must **conflict**
rather than be silently deduplicated with `keep="last"`.
**Evidence:** `OBSERVATION_IDENTITY_COLUMNS` omits those fields;
`_canonicalize_observations` `drop_duplicates(keep="last")`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**Assessment:** Not automatically a defect — descriptive/event-time consistency is
**trusted to upstream validation** for rows sharing exact persisted identity.
Boundary 8 should consider whether that trust boundary is acceptable.
**WS1 criterion:** Identity completeness / trust boundary. **Classification:** Record
as design/trust question (no reuse/replace call now).

### Boundary 3 open points → later boundaries
- Team-code / relocation handling in non–The-Odds-API paths → **Boundary 6**
  (separation only; do not expand into a normalization project).
- Provider-label / provider-event **stability** → **Boundary 8** design question.
- Descriptive/event-time trust boundary (F19) → **Boundary 8**.
- `is_live` always `False` pregame (F12) — in identity defensively; out of scope now.

### Classification summary (Boundary 3 — provisional, per reviewer)
| Component | Provisional | Basis |
|---|---|---|
| Two-level sameness keys | Provisional Reuse | F13 |
| Price/line coexistence across fetches | Provisional Reuse (strong) | F14 |
| Market-family/side vs line/price separation | Provisional Reuse (strong) | F15 |
| Structural provider/sportsbook separation | Provisional Reuse | F16 |
| Multiple-books coexistence | Provisional Reuse (strong) | F16 |
| Provider-label stability | Undecided | F16 |
| Provider-event stability | Undecided | F16 |
| Store identity anchored on `game_id` | Provisional Reuse | F17 |
| The Odds API schedule matching | Provisional Reuse (adapter) | F17 |
| Team-name/code normalization (other paths) | Undecided → Boundary 6 | F17 |
| `sportsbook_updated_at`-in-identity | Provisional Reuse (corrected) | F18 |
| Omitted descriptive-field consistency | Design/trust question → Boundary 8 | F19 |

### Boundary 3 disposition — CLOSED (rev 3)
All eight reviewer modifications incorporated: (1) F13 scope limitation added;
(2) F14 reworded around the full 7-col fetch key; (3) F15 renamed to market-family/
outcome-side vs line/price; (4) F16 narrowed to structural separation, stability
Undecided; (5) F17 narrowed to `game_id`-anchored store identity + explicit upstream
trust; (6) F18 replaced with corrected same-fetch-conflict / cross-fetch-`fetched_at`
behavior (design question retracted); (7) omitted descriptive-field consistency
recorded as F19 trust-boundary question; (8) all Reuse kept provisional (pending
local test run + Boundary 8).

### Next — Boundary 4 drop list (persistence & overwrite behavior)
Concurrency / retry / crash-consistency — the F07 scope-limit lives in the worker/
orchestration path, not `store.py`. Please drop:
- `src/gridiron_edge/deployment/quote_collection_worker.py`
- `tests/unit/deployment/test_quote_collection_worker.py`
- `src/gridiron_edge/market/collection_execution.py`
- `src/gridiron_edge/market/collection_receipt_store.py`
- `tests/unit/market/test_collection_execution.py`
- `tests/unit/market/test_collection_receipt_store.py`
(`store.py` already in hand for the atomic-write path.)

---

## Boundary 4 — Persistence & overwrite behavior — **for reviewer (rev 3 → rev 4)**

### Inspection input
- Repository snapshot: WS1-SNAPSHOT-001
- FINDINGS_WS1.md revision: 3 → proposing 4
- Scope: WS1 — persistence, overwrite, concurrency, retry, crash-consistency (the F07 scope-limit)
- Source (full attached / full read): `store.py` (in hand), `collection_execution.py` (271), `collection_receipt_store.py` (292), `deployment/quote_collection_worker.py` (540), `test_collection_execution.py` (130), `test_collection_receipt_store.py` (92), `test_quote_collection_worker.py` (457). All `REVIEWED_FULL_ATTACHED_SOURCE`.

### Findings

#### Finding WS1-F20 — Concurrency safety comes from the DEPLOYMENT, not the store
**Claim:** The store's `append_to_odds_ledger` is a **read-modify-write** (read
existing partition → `concat` → canonicalize → atomic `.replace()`). The
**individual write** is atomic (temp-file + `.replace()`), but the **read→write
sequence is not**. Two concurrent appends could each read the same existing
partition and the later `.replace()` would **silently drop** the other's rows.
Concurrency safety is therefore **not** a store property — it is provided by the
**deployment**: the systemd unit is `Type=oneshot`, `Restart=no`, timer
`OnUnitActiveSec=5min` (next run fires 5 min after the previous *finishes*), so
invocations **do not overlap**.
**Evidence:** `store.py::append_to_odds_ledger` (read→concat→`_atomic_write_parquet`);
`quote_collection_worker.py` SERVICE template `Type=oneshot`/`Restart=no`, `render_timer`
requires `OnUnitActiveSec=5min`; `test_rendering_preserves_dynamic_selected_plan_boundary`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**WS1 criterion:** No loss of historical evidence under concurrency.
**Classification:** **Adapt (flagged).** Reusable *as deployed*, but the safety
invariant lives outside the store. If any second writer (manual CLI, a second
timer, a test harness) ever runs concurrently, silent row loss is possible. Worth
an explicit note in the temporal-contract unit and/or a store-level guard.

#### Finding WS1-F21 — Exclusive claim file provides single-execution coordination
**Claim:** Each planned poll is coordinated by an **exclusive-create** claim file:
`write_claim` uses `path.open("x")` (fails `FileExistsError` if it already exists).
`evaluate_collection_due` returns `CLAIMED` when a claim exists but no result, so a
subsequent run **skips** an already-claimed poll. Claims and results are **immutable**
(rewrite raises `FileExistsError`).
**Evidence:** `collection_receipt_store.py::write_claim`/`write_result` (`open("x")`);
`collection_execution.py::evaluate_collection_due` (result→skip / claim→CLAIMED /
due-window logic); tests `test_existing_claim_blocks_retry`,
`test_claim_and_result_are_immutable`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE (code + tests).
**WS1 criterion:** No overwrite; single execution per poll.
**Classification:** Provisional Reuse (strong).

#### Finding WS1-F22 — **GAP: TOCTOU between due-check and claim is not gracefully handled**
**Claim:** There is a check-then-act window: `execute_due_collection` calls
`evaluate_collection_due` (which reads `claim.exists()`), then later calls
`write_claim`. If two processes both observe DUE before either writes the claim,
the **first** `write_claim` succeeds and the **second** raises an **uncaught**
`FileExistsError` — `execute_due_collection` catches only `OddsIngest*` /
`OddsRequestError`, not `FileExistsError`. The loser therefore **crashes** instead
of returning `CLAIMED`.
**Evidence:** `collection_execution.py::execute_due_collection` (`write_claim(claim,…)`
outside the `try`; the `except` list is `OddsIngestPartialPersistenceError,
OddsRequestError, OddsIngestError`); `collection_receipt_store.py::write_claim` (`open("x")`).
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**Assessment:** **In the deployed oneshot configuration this window never opens**
(no overlap — F20), so it is not a live defect today. But it is a latent
concurrency bug: the exclusive-create is doing its job (preventing double
execution) yet the caller treats the "lost race" as a crash rather than the
graceful `CLAIMED` outcome the design clearly intends. Cheap fix: catch
`FileExistsError` around `write_claim` and return `CollectionDueResult(CLAIMED, poll)`.
**WS1 criterion:** Concurrency/retry robustness. **Classification:** **Adapt** —
small, well-scoped hardening; candidate for (or adjacent to) the first PLAN unit.

#### Finding WS1-F23 — Crash between claim and result strands the poll (deliberate no-auto-retry)
**Claim:** If the worker dies **after** `write_claim` but **before** `write_result`,
the poll has a claim and no result. `evaluate_collection_due` then returns
`CLAIMED` (not DUE), so it is **never automatically retried**. This is detected but
not self-healed: the verifier's `unresolved_claims` check reports WARNING → DEGRADED.
**Evidence:** `collection_execution.py::evaluate_collection_due` (claim→CLAIMED);
`quote_collection_worker.py::_artifact_checks` (`unresolved_claims` WARNING);
`test_verifier_reports_degraded_for_unresolved_claim`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE (code + tests).
**Assessment:** Deliberate — no automatic retry avoids double-spending API credits
and double-writing on an ambiguous partial. Cost: a crashed poll is **stranded**
until manual resolution; the WARNING is the only signal. Reasonable for a
single-node Pi worker; worth an explicit decision record. Not a WS1 defect.
**WS1 criterion:** Crash-consistency (explicit, surfaced). **Classification:**
Provisional Reuse (with recorded operational caveat).

#### Finding WS1-F24 — Partial persistence (ledger written, snapshot not) is explicit and history-safe
**Claim:** `ingest_the_odds_api_current` appends to the **ledger first**, then
writes the current snapshot; if the snapshot write fails it raises
`OddsIngestPartialPersistenceError`. Because the ledger is append-preserving (F07),
history is intact; only the *current* view lags. The worker records this as a
terminal `PARTIAL_PERSISTENCE` result rather than a crash.
**Evidence:** `the_odds_api.py::ingest_the_odds_api_current` (ledger→snapshot,
partial-persistence raise — Boundary 2); `collection_execution.py::_failure_status`
(→ `PARTIAL_PERSISTENCE`); `collection_receipt_store.py` status enum.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**WS1 criterion:** Partial-write states explicit; history not corrupted.
**Classification:** Provisional Reuse (strong).

#### Finding WS1-F25 — Deployment writes are themselves transactional (snapshot/replace/restore)
**Claim:** Beyond quote data, the *installer* uses staged temp-file writes with
snapshot-and-restore rollback (`_snapshot_deployment_set` / `_replace_deployment_set`
/ `_restore_deployment_set`): a failed daemon-reload restores prior bytes+modes; a
missing destination parent fails before any command; activation failure keeps
installed files. Same atomic-replace discipline as the store.
**Evidence:** `quote_collection_worker.py` (`_replace_deployment_set` NamedTemporaryFile
+ `.replace()` + except-restore); tests `test_daemon_reload_failure_restores_previous_deployment`,
`test_staged_verification_failure_preserves_live_files`, `test_missing_destination_parent_fails_before_commands`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE (code + tests).
**Scope note:** Deployment mechanics are **out of WS1 build scope**; recorded only
to confirm the codebase's persistence discipline is consistent. No WS1 classification.

### Boundary 4 open points → later boundaries / PLAN
- **F22 TOCTOU** → smallest concrete hardening in WS1 (catch `FileExistsError` →
  `CLAIMED`); candidate first-unit or adjacent. Confirm at Boundary 8.
- **F20 store-level concurrency guard** vs. rely-on-deployment → temporal-contract /
  Boundary 8 design decision (do we want the store safe independent of systemd?).
- **F23 stranded-claim recovery** (manual vs. assisted) → operational decision record.

### Classification summary (Boundary 4 — provisional)
| Component | Provisional | Basis |
|---|---|---|
| Store read-modify-write under concurrency | **Adapt (deployment-dependent)** | F20 |
| Exclusive-claim single-execution | Provisional Reuse (strong) | F21 |
| **TOCTOU due-check→claim handling** | **Adapt (small hardening)** | F22 |
| Crash-between-claim-and-result | Provisional Reuse (operational caveat) | F23 |
| Partial-persistence handling | Provisional Reuse (strong) | F24 |
| Deployment write transactionality | (out of WS1 build scope) | F25 |

### Handoff to reviewer (ChatGPT)
Boundary reviewed: **4 — persistence & overwrite behavior.** Full source+tests for all six files.
Reviewer questions:
1. Concur that concurrency safety is **deployment-provided (systemd oneshot), not a
   store property** (F20), and that this belongs as an explicit caveat rather than a
   "Reuse (strong)"?
2. Is **F22 (TOCTOU → uncaught `FileExistsError`)** correctly characterized as a
   *latent* bug (dormant under oneshot) and the right-sized first hardening?
3. Concur that **F23 stranded-claim / no-auto-retry** is a deliberate design to
   record (not a defect), given the credit-double-spend rationale?
4. Any component you'd hold at Undecided until Boundary 8 rather than Provisional Reuse?
Reviewer responds with standard headers; lead reconciles into rev 4 and proceeds to
**Boundary 5 — retrieval & cutoff behavior** (the as-known-at-T question; primary
targets `market/history_boundaries.py` + `history_coverage.py` and their tests —
next drop list).

---

## Boundary 4 — Persistence & overwrite behavior — **CLOSED (rev 4)**

> Replaces the prior "Boundary 4 — for reviewer" section. All nine reviewer
> modifications incorporated. On merge, set header revision to 4.

### Inspection input
- Repository snapshot: WS1-SNAPSHOT-001
- FINDINGS_WS1.md revision: 4
- Scope: WS1 — persistence, overwrite, concurrency, retry, crash-consistency
- Source (full attached / full read): `store.py`, `collection_execution.py` (271),
  `collection_receipt_store.py` (292), `deployment/quote_collection_worker.py` (540),
  `test_collection_execution.py` (130), `test_collection_receipt_store.py` (92),
  `test_quote_collection_worker.py` (457). All `REVIEWED_FULL_ATTACHED_SOURCE`.

### Findings

#### Finding WS1-F20 — Deployment MITIGATES overlap; it does not provide a storage concurrency guarantee
**Claim:** `append_to_odds_ledger` is a **read-modify-write** (read partition →
concat → canonicalize → atomic `.replace()`). The *individual* file replacement is
atomic, but the *read→write* sequence is not, so two concurrent writers of the same
weekly partition can lose-update (A reads, B reads, A writes, B writes without A's
rows). The repository-owned deployment configures a `Type=oneshot`, `Restart=no`
service on an `OnUnitActiveSec=5min` timer, which **serializes ordinary
timer-driven invocations on a single installation**. It is **not** a general
guarantee against manual CLI runs, a second timer, a test harness, or any direct
`append_to_odds_ledger` caller.
**Evidence:** `store.py::append_to_odds_ledger`; `quote_collection_worker.py`
(`render_service` requires `Type=oneshot`/`Restart=no`; `render_timer` requires
`OnUnitActiveSec=5min`); `test_rendering_preserves_dynamic_selected_plan_boundary`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**WS1 criterion:** No loss of historical evidence under concurrency.
**Classification (split, per reviewer):**
- Single-writer atomic publication → **Provisional Reuse.**
- Deployment-level ordinary-overlap mitigation → **Provisional Reuse.**
- Store-level concurrent-writer safety → **Absent.**
- Overall → **Adapt likely**, pending the intended writer contract (do we require
  the store safe independent of systemd?). Do **not** describe the store as
  "reusable as deployed" without the explicit single-writer assumption.

#### Finding WS1-F21 — Exclusive claim = duplicate-execution suppression for one planned-poll identity
**Claim:** `write_claim` uses exclusive-create (`open("x")`); `evaluate_collection_due`
returns `CLAIMED` when the deterministic claim path exists without a result, so a
later run skips that poll. Only one claim file can be **successfully created** for a
given `(season, week, scheduled_at)`. This is **duplicate-execution suppression for
one planned-poll identity** — not proof of exactly-once provider execution across
all crash boundaries.
**Evidence:** `collection_receipt_store.py::write_claim`/`write_result` (`open("x")`);
`collection_execution.py::evaluate_collection_due`; tests `test_existing_claim_blocks_retry`,
`test_claim_and_result_are_immutable`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE (code + tests).
**WS1 criterion:** No overwrite; single successful claim per poll.
**Classification:** Provisional Reuse (strong).

#### Finding WS1-F22 — TOCTOU between due-check and claim is not gracefully handled
**Claim:** `execute_due_collection` evaluates due state (reads `claim.exists()`)
then later calls `write_claim` **outside** the handled `try`. If another process
creates the claim in that window, `write_claim` raises `FileExistsError`, which is
**not** converted into a `CLAIMED` result — the loser crashes instead of returning
the graceful `CLAIMED` outcome the design intends.
**Evidence:** `collection_execution.py::execute_due_collection` (`write_claim` outside
try; except = `OddsIngestPartialPersistenceError, OddsRequestError, OddsIngestError`);
`collection_receipt_store.py::write_claim` (`open("x")`).
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**Assessment (reworded, per reviewer):** **Latent in the execution contract and
mitigated, but not eliminated, by the repository-owned deployment configuration**
(the function is callable independently of systemd — its own tests call it directly).
Cheap fix: catch `FileExistsError` around `write_claim` → return
`CollectionDueResult(CLAIMED, poll)`.
**WS1 criterion:** Concurrency/retry robustness. **Classification:** Adapt (small
hardening). Valid candidate, but **implementation priority deferred to Boundary 8**
(Boundary 5 owns the exit-critical retrieval capability).

#### Finding WS1-F23 — Stranded claim on crash: detection reusable, recovery absent
**Claim:** A crash after `write_claim` but before `write_result` leaves a claim with
no result; `evaluate_collection_due` returns `CLAIMED`, so it is **not** auto-retried.
The verifier reports `unresolved_claims` as WARNING → DEGRADED. The reviewed sources
establish this behavior and its detection but do **not** document a formal
stale-claim recovery policy or the rationale for permanently withholding retry.
**Evidence:** `collection_execution.py::evaluate_collection_due` (claim→CLAIMED);
`quote_collection_worker.py::_artifact_checks` (`unresolved_claims`); test
`test_verifier_reports_degraded_for_unresolved_claim`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE (code + tests).
**Assessment (reworded, per reviewer):** The implementation intentionally or
incidentally favors non-retry once a claim exists — avoiding an automatic repeated
attempt under an ambiguous prior outcome — but no stale-claim recovery policy or
rationale is documented in the reviewed code. (The credit-double-spend rationale is
a plausible *interpretation*, not a verified repository fact.)
**WS1 criterion:** Crash-consistency (explicit, surfaced).
**Classification (split, per reviewer):** Unresolved-claim **detection** →
Provisional Reuse; automatic stale-claim **recovery** → Absent; manual/assisted
recovery policy → Undecided. **Overall → Undecided with operational caveat.** The
recovery choice (never retry / manual retry / lease-expiry / reconcile-before-retry)
is irreversible and belongs in DECISIONS.md when chosen.

#### Finding WS1-F24 — Partial persistence (ledger written, snapshot not) is explicit and history-safe
**Claim:** `ingest_the_odds_api_current` appends to the ledger first, then writes
the snapshot; a snapshot failure after ledger success raises
`OddsIngestPartialPersistenceError`, mapped to terminal `PARTIAL_PERSISTENCE`.
Ledger and snapshot are **separately published artifacts with an explicit
partial-persistence outcome, not one transaction** — history is intact; only the
current view lags. (Does not establish that *every* result-publication failure is
captured — see F27.)
**Evidence:** `the_odds_api.py::ingest_the_odds_api_current`;
`collection_execution.py::_failure_status`; `collection_receipt_store.py` status enum.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**WS1 criterion:** Partial-write states explicit; history not corrupted.
**Classification:** Provisional Reuse (strong).

#### Finding WS1-F25 — Deployment installer writes are transactional (out of WS1 build scope)
**Claim:** The installer stages temp-file writes with snapshot-and-restore rollback;
failed daemon-reload restores prior bytes+modes, missing parent fails before any
command, activation failure keeps installed files.
**Evidence:** `quote_collection_worker.py` (`_replace_deployment_set` /
`_restore_deployment_set`); tests `test_daemon_reload_failure_restores_previous_deployment`,
`test_staged_verification_failure_preserves_live_files`,
`test_missing_destination_parent_fails_before_commands`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE (code + tests).
**Scope note:** Out of WS1 build scope; recorded as positive supporting evidence of
consistent persistence discipline. **No WS1 classification.**

#### Finding WS1-F26 — Receipt publication is create-only but NOT crash-atomic *(new, per reviewer)*
**Claim:** Unlike the quote parquet and the deployment installer (temp-file +
rename), `write_claim`/`write_result` open the **final destination** with `"x"` and
`json.dump` **directly** into it. Exclusive creation prevents replacement/duplicate
creation, but a process interruption during serialization can leave a final-path
file that **exists but is incomplete**. This matters because `evaluate_collection_due`
uses `path.exists()`, not successful artifact validation, to decide claimed/completed.
**Evidence:** `collection_receipt_store.py::write_claim`/`write_result`
(`with path.open("x") … json.dump(...)`); `collection_execution.py::evaluate_collection_due`
(`result.exists()` / `claim.exists()`).
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**WS1 criterion:** Crash-atomic complete-file publication.
**Classification:** Adapt likely (stage-and-rename receipts) **if** WS1 requires
terminal artifacts to be wholly absent or fully valid after interruption.

#### Finding WS1-F27 — Unexpected post-claim failures can leave an unresolved claim *(new, per reviewer)*
**Claim:** `write_claim` runs before the ingestion `try`; the handled exceptions are
only `OddsIngestPartialPersistenceError`, `OddsRequestError`, `OddsIngestError`.
**Other** exceptions after claim creation — including a failure while writing the
terminal result itself — are **not** converted into a terminal result inside
`execute_due_collection`. Such a failure leaves a claim with no result (and possibly
successful quote artifacts with no receipt). Broader than F23's crash scenario.
**Evidence:** `collection_execution.py::execute_due_collection` (claim before try;
narrow except list; `write_result` outside any catch).
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**Scope note:** Whether outer CLI/service boundaries recover these is not
established by the reviewed sources.
**WS1 criterion:** Post-claim failure handling. **Classification:** Adapt likely /
Undecided, depending on the desired recovery contract.

### Boundary 4 open points → Boundary 8 / DECISIONS
- Store-level concurrent-writer protection (F20) → intended-writer contract decision.
- F22 graceful lost-race handling → small hardening, priority set at Boundary 8.
- F23 stale-claim recovery policy → irreversible DECISIONS.md choice.
- F26 crash-atomic receipts → adopt stage-and-rename if required.
- F27 post-claim / result-write failure contract → recovery-contract decision.

### Classification summary (Boundary 4 — provisional, per reviewer)
| Component | Provisional | Basis |
|---|---|---|
| Single-writer logical ledger retention | Provisional Reuse (strong) | F20/F07 |
| Atomic physical partition replacement | Provisional Reuse | F20 |
| Repository-owned timer overlap mitigation | Provisional Reuse | F20 |
| Store-level multi-writer protection | **Absent** | F20 |
| Same-poll exclusive claim | Provisional Reuse (strong) | F21 |
| Graceful lost-claim-race handling | **Adapt** | F22 |
| Unresolved-claim detection | Provisional Reuse | F23 |
| Unresolved-claim recovery policy | **Absent / Undecided** | F23 |
| Receipt immutability (create-only) | Provisional Reuse | F26 |
| Receipt crash-atomic publication | **Adapt likely** | F26 |
| Explicit partial-persistence state | Provisional Reuse (strong) | F24 |
| Ledger+snapshot transactionality | Not provided by design | F24 |
| Unexpected post-claim failure handling | **Adapt likely / Undecided** | F27 |
| Deployment installer rollback | Out of WS1 scope (positive) | F25 |
| First PLAN.md unit | Undecided → Boundary 8 | — |

### Suggested future tests (not required to close)
Concurrent same-poll claim (only one succeeds); loser returns `CLAIMED` after
`FileExistsError`; interrupted receipt publication leaves no authoritative malformed
artifact; successful ingestion then `write_result` failure; two concurrent weekly
ledger writers (demonstrate current lost-update risk before choosing mitigation).

### Boundary 4 disposition — CLOSED (rev 4)
All nine reviewer modifications incorporated: (1) F20 reworded to deployment-level
overlap *mitigation*, store multi-writer safety Absent, overall Adapt-likely;
(2) F21 narrowed to duplicate-execution suppression for one planned-poll identity;
(3) F22 kept, "dormant" removed → latent-but-mitigated, priority deferred;
(4) F23 split (detection Reuse / recovery Absent), overall Undecided, rationale
de-asserted; (5) F24 retained + "not one transaction" wording; (6) F25 retained
out-of-scope; (7) F26 added (create-only ≠ crash-atomic); (8) F27 added
(post-claim/result-write failures); (9) concurrent-writer protection, recovery
policy, and first-unit selection left for Boundary 8.

### Next — Boundary 5 drop list (retrieval & cutoff behavior — the as-known-at-T question)
- `src/gridiron_edge/market/history_boundaries.py`
- `src/gridiron_edge/market/history_coverage.py`
- `tests/unit/market/test_history_boundaries.py`
- `tests/unit/market/test_history_coverage.py`
(`store.py` in hand for `load_odds_ledger`.) This boundary decides whether
`history_boundaries.py` already implements cutoff selection over `fetched_at` —
directly resolving the F11 as-known-at-cutoff question.

---

## Boundary 5 — Retrieval & cutoff behavior — **CLOSED (rev 5)**

> Replaces the prior "Boundary 5 — for reviewer" section. All eight reviewer
> modifications incorporated. On merge, set header revision to 5.

### Inspection input
- Repository snapshot: WS1-SNAPSHOT-001
- FINDINGS_WS1.md revision: 5
- Scope: WS1 — retrieval, as-known-at-cutoff, leakage-safe pregame selection (F11)
- Source (full read): `history_boundaries.py` (144), `history_coverage.py` (88),
  `test_history_boundaries.py` (231), `test_history_coverage.py` (103); `store.py`
  `load_odds_ledger` in hand. All `REVIEWED_FULL_ATTACHED_SOURCE`.

### Findings

#### Finding WS1-F28 — Kickoff-relative latest-eligible selection exists (NOT a complete as-known-at-kickoff view)
**Claim:** For each historical quote-series identity with **one unambiguous**
kickoff, `select_quote_history_boundaries` selects the **last non-live observation
satisfying `fetched_at < commence_time`**. This proves the core temporal-selection
predicate and is well-tested. **Caveat (per reviewer):** only
`latest_eligible_pregame` is cutoff-bounded. `earliest_observed`,
`observation_count`, `distinct_fetch_count`, `repeated_observation_evidence_available`,
and kickoff-conflict detection are computed over the **full** supplied history — so
the complete returned boundary object is **not** a general point-in-time snapshot.
**Evidence:** `history_boundaries.py::_select_identity_boundary`
(`eligible = ordered.loc[is_live.eq(False) & fetched_at.lt(kickoff)]` → `.iloc[-1]`;
counts/earliest computed on `ordered`, not `eligible`); tests
`test_latest_eligible_excludes_live_and_post_kickoff_rows` (selects 3.5; excludes
live 4.0, at-kickoff 4.5, post 5.0).
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE (code + tests).
**WS1 criterion:** Leakage-safe pregame selection. **Classification:** Provisional
Reuse (strong) **for kickoff-relative selection**. Complete as-known-at-kickoff
boundary object → **not established**.

#### Finding WS1-F29 — F11 NARROWED & CONFIRMED: arbitrary decision-cutoff retrieval is absent; adaptation must scope the whole result, not just swap the predicate
**Claim:** No reviewed function accepts a caller-supplied decision cutoff;
`history_boundaries.py` derives its cutoff solely from `commence_time`, and
`load_odds_ledger` has no cutoff parameter. Building general as-known-at-T is **not**
merely replacing `fetched_at < kickoff` with `fetched_at < T`: because identities,
counts, `earliest_observed`, repeated-evidence, and `KICKOFF_CONFLICT` are all
derived from the full history, a correct as-known-at-T must **restrict the visible
input set before deriving any of them** — otherwise the result leaks post-T
information (post-T fetches, future temporal depth, kickoff conflicts learned after
T, even identity existence).
**Cutoff-contract questions to design explicitly (per reviewer):** strict `<` vs
inclusive `<=`; effective boundary = cutoff, kickoff, or `min(cutoff, kickoff)`;
required UTC validation; explicit "no observation by cutoff" status; whether live
rows before cutoff stay visible-but-ineligible; whether coverage receives the same
cutoff contract; whether the loader, the selector, or a new composed retrieval
boundary owns the cutoff.
**Evidence:** `history_boundaries.py` (cutoff = `kickoff_values.iloc[0]`, no cutoff
arg; counts/earliest over `ordered`); `store.py::load_odds_ledger` (no cutoff filter).
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**WS1 criterion:** As-known-at-cutoff retrieval (bitemporal).
**Classification:** **Adapt** (not labeled "small"). Reuses grouping, ordering,
status, and exact-observation contracts; arbitrary-cutoff support requires an
explicitly designed semantic scope. **Leading first-unit candidate entering
Boundary 8 — not yet selected** (Boundaries 6–7 must complete first).

#### Finding WS1-F30 — Explicit boundary statuses; earliest-observed never erased
**Claim:** Statuses `AVAILABLE`, `KICKOFF_UNAVAILABLE`, `KICKOFF_CONFLICT`,
`NO_ELIGIBLE_PREGAME_OBSERVATION`; `earliest_observed` always returned for a
nonempty identity, even when no pregame selection is possible.
**Evidence:** `QuoteBoundaryStatus`; tests `test_missing_kickoff_is_explicit`,
`test_conflicting_kickoffs_are_explicit`,
`test_no_eligible_pregame_observation_preserves_earliest`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE (code + tests).
**WS1 criterion:** Missing/conflict first-class (VISION invariant 5).
**Classification:** Provisional Reuse (strong).

#### Finding WS1-F31 — Historical quote-series grouping identity `HISTORY_IDENTITY_COLUMNS` (6-col)
**Claim:** `HISTORY_IDENTITY_COLUMNS = (provider, provider_event_id, sportsbook,
game_id, market, side)` — the 7-col fetch key minus `fetched_at` — is the
**chosen historical quote-series grouping identity**. Both coverage and boundary
selection group by it (`dropna=False` keeps nullable consensus separate).
**Evidence:** `history_coverage.py::HISTORY_IDENTITY_COLUMNS`; `history_boundaries.py`
groups by it; `test_consensus_and_sportsbook_histories_remain_separate`.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**Wording (per reviewer):** "historical quote-series **grouping** identity," not
"the correct time-series identity" — provider-label / provider-event / upstream
game-identity stability questions (F16/F17) remain open for Boundary 8.
**WS1 criterion:** Temporal grouping identity. **Classification:** Provisional Reuse.

#### Finding WS1-F32 — Interpretation-free boundary selection (partial Boundary-6 separation, in advance)
**Claim:** Pure (DataFrame in → frozen dataclass out); docstrings + tests disclaim
opening/closing/movement/CLV/backtest/recommendation. `repeated_observation_evidence_available`
reports fetch depth without claiming movement.
**Evidence:** module docstring; `test_repeated_unchanged_observations_preserve_fetch_depth`;
frozen-contract tests.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**WS1 criterion:** Observation vs interpretation separation. **Classification:**
Provisional Reuse (strong).

#### Finding WS1-F33 — `pregame_observation_count` is actually a NON-LIVE count *(contract mismatch, per reviewer)*
**Claim:** `evaluate_quote_history_coverage` computes
`pregame_observation_count = len(rows) - live_count` — it counts **non-live** rows
and does **not** compare `fetched_at` to `commence_time`. A non-live observation
collected **at or after** kickoff would be counted as "pregame." The field name
overstates the semantics; under the current contract it means *non-live observation
count*.
**Evidence:** `history_coverage.py` (`pregame_observation_count = len(rows) -
live_count`); `test_multi_source_live_and_missing_kickoff_coverage` verifies
live-vs-non-live counting but **no** non-live post-kickoff row is tested.
**Evidence strength:** REVIEWED_FULL_ATTACHED_SOURCE.
**WS1 criterion:** Coverage-diagnostic truthfulness. **Classification:** **Adapt
likely** — either rename to `non_live_observation_count`, or enforce genuine
`fetched_at < commence_time` pregame eligibility and separately report non-live
post-kickoff rows. A real contract mismatch, not findings wording.

### F11 status: NARROWED & CONFIRMED (not resolved)
| Temporal capability | Status |
|---|---|
| Three UTC world-times (fetched/updated/commence) | Present — Reuse |
| Kickoff-relative latest-eligible selection (`fetched_at < kickoff`) | **Present — Reuse (strong)** |
| Explicit missing/conflict kickoff + earliest preserved | Present — Reuse (strong) |
| Complete as-known-at-kickoff **boundary object** | **Not established** (counts/earliest over full history) |
| As-known-at-**arbitrary decision-cutoff T** | **Absent — Adapt (scope whole result before deriving)** |
| Coverage computed from cutoff-visible subset | Absent |
| `pregame_observation_count` = true pregame | **No — non-live count (Adapt likely)** |
| Bitemporal known-at vs effective / difference view | Absent (later workstream) |

### Boundary 5 unresolved → Boundary 6 / 8
- **Exact persisted-observation reference:** `SelectedQuoteObservation` +
  6-col identity appear sufficient to reconstruct much of the source row, but there
  is **no** immutable observation ID / hash / partition-path / row reference. Whether
  downstream claims can reference the exact persisted observation without value-based
  reconstruction is **Undecided** → carry to Boundary 6/8.
- **Retrieval composition from storage:** both modules take an already-loaded
  DataFrame; neither loads partitions or exposes a repository-level query. A complete
  public retrieval path from persisted history is **not yet established**.
- **Cutoff-visible missing state:** no status yet for "identity exists later, but no
  observation was known by cutoff T" — needed if arbitrary-cutoff retrieval is built.
- **Minimum WS1 temporal semantics (per reviewer):** even the first as-known unit
  must explicitly choose `fetched_at` as the knowledge-visibility boundary
  (distinct from source-updated time and from the caller's decision cutoff); this
  minimal distinction cannot be deferred wholesale, though the full three-view
  difference experience remains later-workstream.

### Classification summary (Boundary 5 — provisional, per reviewer)
| Component | Provisional | Basis |
|---|---|---|
| Historical-series grouping identity | Provisional Reuse | F31 |
| Kickoff-relative eligible selection | Provisional Reuse (strong) | F28 |
| Complete as-known-at-kickoff boundary object | **Not established** | F28 |
| Arbitrary decision-cutoff retrieval | **Adapt** | F29 |
| Generic ledger cutoff filtering | Absent | F29 |
| Explicit missing/conflict kickoff statuses | Provisional Reuse (strong) | F30 |
| Earliest-observation preservation | Provisional Reuse (strong) | F30 |
| Pure interpretation-free selection | Provisional Reuse (strong) | F32 |
| Coverage counts / temporal depth | Provisional Reuse | F33 |
| `pregame_observation_count` semantics | **Adapt likely** | F33 |
| Exact persisted-observation reference | Undecided | (unresolved) |
| Additional temporal fields | Undecided | F11 |
| Full difference view | Later workstream | F11 |
| Leading first-unit candidate | As-known-at-cutoff contract, **not yet locked** | F29 |

### Suggested future tests (for the eventual as-known-at-cutoff unit)
Cutoff before first observation; between two fetches; after a changed spread line;
exactly at cutoff; kickoff after cutoff; cutoff after kickoff; conflicting kickoff
learned only after cutoff; non-live post-kickoff row in coverage; identity appearing
only after cutoff; counts/repeated-evidence excluding post-cutoff rows.

### Boundary 5 disposition — CLOSED (rev 5)
All eight reviewer modifications incorporated: (1) F28 reworded to kickoff-relative
latest-eligible selection, complete as-known-at-kickoff view **not established**;
(2) F29 reworded — reusable-foundation adaptation, **not** a small predicate swap
(whole-result scoping required); (3) "F11 resolved" → **F11 narrowed & confirmed**;
(4) arbitrary-cutoff retrieval kept **Adapt**; (5) cutoff-contract questions recorded
explicitly; (6) F33 corrected to non-live counting, semantic mismatch **Adapt likely**;
(7) exact persisted-observation referenceability recorded **Undecided**; (8) first
unit kept **leading candidate only**, not selected until Boundary 8.

### Next — Boundary 6 drop list (downstream coupling & separation)
Does each consumer read **exact observations** vs a **derived selection**, retain
the 6-col identity, reference the source deterministically, recompute its own
cutoff/"latest" logic, or bypass the boundary selector by reading the ledger directly?
- `src/gridiron_edge/market/candidate_issuance.py`
- `src/gridiron_edge/market/line_shopping.py`
- `src/gridiron_edge/market/clv.py` (separation check only — not its calculations)
- `src/gridiron_edge/api/routes/lines.py` + `src/gridiron_edge/api/serializers/lines.py`
- `tests/integration/test_odds_join.py` (most telling separation test)

---

## Boundary 6 — Downstream coupling & separation — **CLOSED (rev 6, final)**

> Finalizes the caller check. F40 upgraded conditional → **VERIFIED_LOCAL_SOURCE**
> from the owner-supplied `sed` body. All reviewer modifications incorporated.

### Byte-faithful caller body (owner `sed` output, verified)
`issue_candidates_cmd` composition:
```
timestamp = _utc_option(evaluated_at, label="evaluated-at")
product   = load_current_weekly_product(settings.repo_root, season=season, week=week)
events    = load_forecast_events(season=season, week=week, run_id=run_ids[0], repo=...)
quotes    = load_odds_ledger(season=season, week=week, repo=settings.repo_root)   # no fetched_at arg
issuance  = issue_pregame_candidates(product=product, forecast_events=events,
                                     quotes=quotes, evaluated_at=timestamp)        # unfiltered
```
No `fetched_at` filter exists between load and call. Confirmed against
`candidate_issuance.py` (enforces `fetched_at < commence_time`, **not**
`fetched_at <= evaluated_at`).

### Finding WS1-F40 — VERIFIED reachable point-in-time leak in the production candidate-issuance path
**Claim:** The production candidate-issuance path is **not point-in-time correct for
historical/backdated execution**. `issue_candidates_cmd` loads the complete weekly
history ledger (`load_odds_ledger(season, week, repo)` — no `fetched_at` cutoff) and
passes it **unfiltered** to `issue_pregame_candidates`. Candidate issuance excludes
live and post-kickoff observations but does **not** exclude observations learned
after its declared `evaluated_at`. A backdated issuance can therefore **freeze
post-cutoff evidence**:
```
evaluated_at = 12:00 · quote A fetched 11:00 · quote B fetched 13:00 · kickoff 20:00
→ both A and B are pregame; both enter the issuance, though B was unknown at 12:00
```
**Evidence:** `production_chain.py::issue_candidates_cmd` body (owner `sed`, byte-faithful);
`candidate_issuance.py::issue_pregame_candidates` (`evaluable_mask` uses
`fetched_at < commence_time`; kickoff guard `known_kickoffs.le(evaluated)` only proves
issuance-before-kickoff).
**Evidence strength:** **VERIFIED_LOCAL_SOURCE** (command body) + REVIEWED_FULL_ATTACHED_SOURCE (issuance).
**Scope clarification (per reviewer):** This is a confirmed **code** defect for any
invocation where the weekly ledger contains observations after the supplied
`evaluated_at`. It does **not** establish that already-stored issuances are
contaminated — that requires comparing each stored issuance's `evaluated_at` against
its rows' `fetched_at`. **Code defect confirmed; historical impact unassessed.**
**WS1 criterion:** Point-in-time correctness end-to-end.
**Classification:** **Adapt — confirmed, high priority.**

### Finding WS1-F41 — Candidate reference is cross-artifact but NOT injective over canonical observation identity
**Claim:** `candidate_issuance_row_id` is a cross-artifact candidate reference —
`_resolve_candidate` / `resolve_row` require it to resolve to **exactly one**
issuance row; it is generated per candidate in `evaluate_recommendation_issuance`,
validated in `validate_recommended_bet_result`, and adapted into
`MarketCloseoutReference` (`market_closeout.py`). Its hash **omits
`sportsbook_updated_at` and `is_live`**, both part of the store's canonical 11-col
identity. Therefore it is **not injective** over that identity: two canonically
distinct observations differing only in an omitted field can map to the same
reference, and downstream "resolve exactly once" resolution can become ambiguous.
**Evidence:** `candidate_issuance_row_id` hash payload; `recommendation_policy.py`
`_resolve_candidate` ("must resolve to exactly one issuance row");
`recommended_bet_result.py` `resolve_row` + `validate_recommended_bet_result`
(`candidate_reference_id != candidate_issuance_row_id(...)` raises);
`market_closeout.py::_candidate_reference`.
**Evidence strength:** VERIFIED_LOCAL_SOURCE (call sites via owner `sed`) +
REVIEWED_FULL_ATTACHED_SOURCE (hash).
**Reviewer-precise wording:** cross-artifact candidate reference = **yes**; exact
persisted-observation reference = **no**; globally stable source-observation ID =
**no**. Canonical collision pairs entering one issuance is *permitted by code*
(`_reject_duplicate_quotes` dedups on the full 11-col identity, so two rows differing
only in an omitted field are **not** rejected) but not yet demonstrated by a test.
**WS1 criterion:** Exact-observation referenceability / lineage integrity.
**Classification:** **Adapt.**

### F40 vs F41 — kept SEPARATE (per reviewer)
- **F40 = visibility defect:** *was this observation knowable at the declared
  decision time?* Fix: `full ledger → cutoff-visible evidence set → issuance`.
- **F41 = identity defect:** *which exact observation does this reference identify?*
  Fix: `canonical exact identity → complete stable reference → candidate/policy/
  recommendation/closeout`.
Related through provenance, but one must not be used to conceal or implicitly solve
the other. Boundary 8 decides whether they are adjacent units or one coherent
contract replacement.

### First-unit implication (leading candidate, not locked)
**Create one owned as-known-at-cutoff evidence boundary and route production
candidate issuance through it** — not merely a filter inside the CLI (that repairs
one caller while preserving the ownership problem). The shared boundary should
define: UTC cutoff validation; which timestamp governs visibility (`fetched_at`);
strict vs inclusive semantics; filtering **before** identity/counts/evaluation;
explicit empty/unavailable behavior; **preservation of every visible quote** (not
one-per-series); and compatibility with consumer-specific selection afterward. Then
candidate issuance evaluates every cutoff-visible quote, while `history_boundaries`
selects earliest/latest eligible **within the same cutoff-visible set**. F41 may
join this unit (if a common observation-reference contract is established) or be the
immediately following unit — **Boundary 8 decides**.

### Boundary 6 — final classification
| Concern | Disposition |
|---|---|
| Production candidate caller | Verified (`issue_candidates_cmd`) |
| Full weekly ledger loading | Verified |
| Unfiltered full ledger → issuance | **Verified** |
| Candidate-owned cutoff enforcement | Absent |
| Caller-trust boundary for cutoff-valid inputs | Confirmed |
| Reachable backdated point-in-time leak | **Verified** |
| Pregame eligibility | Present but insufficient for decision-time validity |
| Candidate reference used across artifacts | Verified |
| Reference complete over canonical observation identity | **No** |
| Exact persisted-observation reference | Still absent / unresolved |
| Cutoff-visible evidence-set owner | Absent (leading structural gap) |
| Current-market lines path | Separate responsibility |
| CLV quote-selection separation | Reuse (strong) |
| First implementation unit | Deferred to Boundary 8 |

### Boundary 6 disposition — CLOSED
1. Production candidate command loads the full weekly ledger.
2. It applies no `fetched_at` cutoff.
3. Candidate issuance applies only pregame eligibility, not decision-time visibility.
4. A backdated issuance can therefore consume future-known observations. **(Verified.)**
5. The candidate reference is cross-artifact but incomplete over canonical identity.
6. Cutoff retrieval (F40) and reference identity (F41) remain **separate** remediation concerns.
7. First implementation unit not locked until Boundary 8.

### Next — Boundary 7 (tests & real artifacts), owner-run
- `uv run pytest` on the WS1 suite → stamp `VERIFIED_LOCAL_TEST` (esp.
  `test_odds_store`, `test_history_boundaries/coverage`, `test_candidate_issuance*`).
- Parquet → CSV/summary → confirm F04/F07 depth-two on the real Week 1 ledger.
- One residual `LOCAL_VERIFICATION_REQUIRED`: `test_odds_join` assertion bodies (F39).
- High-value new tests to add (F40/F41): quote fetched after `evaluated_at` but
  pregame must be excluded; two candidate rows differing only in
  `sportsbook_updated_at`/`is_live` must not share a reference.

---

## Boundary 7 — Tests & real artifacts — **CLOSED after two artifact-summary corrections (rev 7)**

> Replaces the prior Boundary 7 close. All ten reviewer modifications incorporated.
> One number (canonical exact-identity duplicates) is **PENDING RE-RUN** of the
> corrected script; everything else is final. On merge, set header revision to 7.

### Test execution — VERIFIED_LOCAL_TEST (targeted WS1 groups only)
> Scope note (per reviewer): these are **targeted WS1 + connected-consumer** groups,
> **not** the full repository suite or ruff/Pyrefly gates (those belong to the
> implementation unit).

| Group | Tests | Result |
|---|---|---|
| A1 `test_odds_store` + `_source_neutral` | 27 | passed |
| A2 `test_the_odds_api_parser` | 13 | passed |
| A3 `test_history_boundaries` + `_coverage` | 17 | passed |
| A4 `test_collection_execution` + `_receipt_store` + `_quote_collection_worker` | 21 | passed |
| A5 candidate issuance + policy + result + closeout | 76 | passed |
| A6 `test_odds_join` | 5 | passed |
| **Total** | **159** | **passed, 0 failed** |

**Behavior-specific upgrade (per reviewer — NOT a blanket claim):** the executed
tests verify the **existing behaviors they cover**. They do **not** verify findings
explicitly identified as uncovered. Precisely:
- **VERIFIED_LOCAL_TEST** (existing behavior): F05, F06, F07(logical-append), F08,
  F09, F10, F12, F21, F23, F24, F25, F28, F30, F31, F32, F34(existing issuance),
  F39.
- **NOT upgraded** — uncovered gaps remain **VERIFIED_LOCAL_SOURCE** (source-
  established, no covering test): F22, F26, F27, F40, F41.

**F35 (split, per reviewer — NOT blanket VERIFIED_LOCAL_TEST):**
- Existing candidate issuance + pregame checks → **VERIFIED_LOCAL_TEST.**
- Duplicated temporal predicate → **VERIFIED_LOCAL_SOURCE.**
- Missing decision-time cutoff enforcement → **VERIFIED_LOCAL_SOURCE, uncovered.**
- Correct cutoff behavior → **not verified (not implemented).**

**F39 (per reviewer):** the complete local `test_odds_join.py` passed all five tests,
verifying its current persisted-snapshot and canonical-game-join integration
contract → **VERIFIED_LOCAL_TEST.** (No assertion detail reconstructed beyond the
successful run.)

### Real Week-1 ledger — VERIFIED_REAL_ARTIFACT (with one correction + one pending)
`data/odds/history/season=2026-2027/week=01/observations.parquet`:
- **rows: 1680** — VERIFIED_REAL_ARTIFACT
- **distinct fetched_at: 2** (`…14:23:18.347996+00:00`, `…14:40:57.207288+00:00`) — verified
- **series identities: 840** (6-col), **depth value_counts {2: 840}** — every identity
  depth-two, all three markets True — VERIFIED_REAL_ARTIFACT. (840×2 = 1,680; matches
  CHANGELOG 274 ML / 282 spread / 284 total.)

**CORRECTION — duplicate check was mis-keyed (my error), now re-run:** the original
script's `ID` list had **10 fields and omitted `fetched_at`**, mislabeled "11-col."
Corrected re-run (`boundary7_corrected_check.py`, EXACT_ID field count = 11):
- **canonical exact-identity duplicates (11-col incl. `fetched_at`): 0** — VERIFIED_REAL_ARTIFACT.
- **repeated value-identities across fetches (10-col, no `fetched_at`): 0** — VERIFIED_REAL_ARTIFACT.

**Interpretation (informative):** value-repeats-across-fetches = 0 means **every one
of the 840 series changed in at least one field between 14:23 and 14:40.** Yet
price/line moved on almost nothing (below). Therefore **838 of 840 series** differ
across fetches by **`sportsbook_updated_at`** (book re-stamping its own update time)
and/or `is_live`, **not** by quote value. The two polls captured mostly *metadata*
movement, with only 2 genuine moneyline price changes in the 17-minute window.
**This is F18 operating at scale** (source-published-time in the dedup identity → a
same-price re-poll with a new `updated_at` is retained as a distinct observation),
and it is why depth-two coexistence holds despite near-zero price movement.

**Depth-two conclusion (reworded, per reviewer):** *Two temporally distinct
observations coexist for every historical series. At least two moneyline series
contain changed price values; unchanged repeated observations also remain valid
temporal evidence.* (Not every depth-two pair necessarily changed price/line —
repeated unchanged evidence proves temporal depth without claiming movement.)

**Movement (corrected re-run, price AND line per market):**
- Moneyline: changed price = **2**, changed line = **0**. (Real changed-price
  preservation — supports F14.)
- Spread: changed price = **0**, changed line = **0**.
- Total: changed price = **0**, changed line = **0**.
All VERIFIED_REAL_ARTIFACT. So in this 17-minute window, quote *values* were nearly
static (2 ML price ticks; no line moves), while the depth-two coexistence is driven
by `sportsbook_updated_at`/`fetched_at` — see interpretation above.

### F41 — structural gap, low demonstrated reachability (confirmed)
Structural non-injectivity over canonical identity: **confirmed from code.** Collision
in the inspected Week-1 ledger: **not observed** (repeated pairs differ in `fetched_at`,
which *is* in the reference hash). Production incidence: **unestablished.** Covering
distinctness test for the omitted fields (`sportsbook_updated_at`/`is_live`): **absent**
(C2 — those symbols appear only as fixture setup at lines 81/83). → *Confirmed
structural lineage weakness, not a demonstrated production collision.*

### F40 — verified defect (C1)
`issue_candidates_cmd`: `load_odds_ledger(...)` then `evaluated_at=timestamp` passed
through with **no `fetched_at` filter line**. Production path permits post-cutoff,
pre-kickoff observations into an issuance. **Adapt, verified high-priority.** No
covering test. *Historical contamination of already-stored issuances: unestablished*
(would require comparing each stored issuance's `evaluated_at` to its rows' `fetched_at`).

### F33 — unchanged
`test_history_coverage` (6) does not test a non-live post-kickoff row → the
`pregame_observation_count` misnomer is not exercised. Remains **Adapt likely**.

### Uncovered findings → route to APPLICABLE units (per reviewer, NOT all to first unit)
- **F40** → directly applicable to the leading **cutoff-visible retrieval** unit.
- **F22** (claim-race), **F26** (receipt crash-atomicity), **F27** (post-claim
  failure), **F41** (reference injectivity) → **Boundary 8 partitioning decisions**;
  each seeds acceptance tests for the unit that eventually owns it. Bundling all five
  into the first unit would violate one-coherent-unit discipline.

### Corrected Boundary 7 evidence table
| Evidence area | Disposition |
|---|---|
| 159 targeted tests | Verified passed (WS1-focused groups, not full suite/gates) |
| Existing odds-store / parser / history / receipt / candidate behaviors | VERIFIED_LOCAL_TEST |
| Real ledger row & fetch counts; 840-series depth-two | VERIFIED_REAL_ARTIFACT |
| Canonical exact-identity duplicates (11-col) | **0 — VERIFIED_REAL_ARTIFACT** |
| Repeated value-identities across fetches (10-col) | **0** → every series changed ≥1 field (mostly `sportsbook_updated_at`) |
| Moneyline price / line movement | price 2 / line 0 — verified |
| Spread price / line movement | 0 / 0 — verified |
| Total price / line movement | 0 / 0 — verified |
| F35 cutoff enforcement / F40 / F41 / F22 / F26 / F27 | Source-established gaps, no covering test |
| F33 semantic mismatch | Adapt likely |

## Boundary 7 disposition — CLOSED (all numbers final)
159/159 targeted WS1 tests pass; real ledger depth-two VERIFIED_REAL_ARTIFACT;
canonical 11-col exact-identity duplicates = 0 (corrected); value-identities across
fetches = 0 (→ F18 metadata-movement at scale); price/line movement measured for all
three markets (ML 2/0, spread 0/0, total 0/0); F39 resolved; F40 confirmed; F41
tempered to structural-only; F35 evidence split. No classification changed. **All
inspection boundaries (1–6) + verification (7) CLOSED.**

**Next → Boundary 8: consolidated reuse map, irreversible decisions, and selection
of the first PLAN.md unit** (leading candidate: owned as-known-at-cutoff evidence
boundary; F40 belongs to it; F22/F26/F27/F41/F33 partitioned to their own units).

---

## Boundary 8 — Consolidation, decisions & first-unit selection — **CLOSED (rev 8)**

> Reconciles all reviewer corrections. **WS1 INSPECTION COMPLETE.** No new source
> review. On merge, set header revision to 8 and Status → INSPECTION CLOSED.

### Inspection input
- Repository snapshot: WS1-SNAPSHOT-001
- FINDINGS_WS1.md revision: 8 (final)
- Scope: WS1 consolidation only

---

## 1. Reuse map — consolidated (corrected evidence strengths)

### REUSE (verified — keep as-is)
| Component | Findings | Evidence |
|---|---|---|
| Provider-aware quote schema / `validate_quote_rows` | F01 F10 | VERIFIED_LOCAL_TEST |
| Explicit fetch-conflict (7), exact-observation (11), and historical-series (6) identities, plus deterministic ordering | F06 F13 F31 | VERIFIED_LOCAL_TEST |
| Historical persistence — logical-append / physical-atomic | F03 F07 | VERIFIED_LOCAL_TEST + VERIFIED_REAL_ARTIFACT |
| Real depth-two coexistence: **1,680 rows, 840 historical-series identities, two fetches per identity; canonical 11-col exact-identity duplicates = 0** | F04 | VERIFIED_REAL_ARTIFACT |
| Malformed/conflict/mixed-scope rejection (bytes preserved) | F08 | VERIFIED_LOCAL_TEST |
| Current snapshot separate + atomic | F09 | VERIFIED_LOCAL_TEST |
| Source-neutral / market-generic | F10 | VERIFIED_LOCAL_TEST |
| `fetched_at` assigned once at ingest (Odds API path) | F05 | VERIFIED_LOCAL_TEST |
| Pregame-only / started-event exclusion (adapter) | F12 | VERIFIED_LOCAL_TEST |
| Price/line coexist across fetches; conflict within fetch | F14 | VERIFIED_LOCAL_TEST + real (2 ML ticks) |
| Market-family/side ≠ line/price conflation | F15 | VERIFIED_LOCAL_TEST |
| Structural provider/sportsbook separation; multi-book coexist | F16 | VERIFIED_LOCAL_TEST |
| Store identity anchored on `game_id`; **Odds API adapter resolution verified, other adapters undecided** | F17 | VERIFIED_LOCAL_TEST (that adapter) |
| `sportsbook_updated_at` exact-idempotency (corrected) | F18 | VERIFIED_LOCAL_TEST |
| Kickoff-relative latest-eligible selection | F28 | VERIFIED_LOCAL_TEST |
| Explicit boundary statuses; earliest never erased | F30 | VERIFIED_LOCAL_TEST |
| Interpretation-free selection/coverage | F32 | VERIFIED_LOCAL_TEST |
| **Same-planned-poll duplicate-execution suppression** | F21 | VERIFIED_LOCAL_TEST |
| **Receipt create-only immutability** | F21 | VERIFIED_LOCAL_TEST |
| Explicit partial-persistence state | F24 | VERIFIED_LOCAL_TEST |
| CLV observation-selection separation | F37 | **VERIFIED_LOCAL_SOURCE** (no CLV suite in the 159) |
| API orchestration + serialization separation | F38 | **VERIFIED_LOCAL_SOURCE** (no API suite in the 159) |
| Candidate issuance **validates & evaluates every supplied canonical quote row, retaining substantial quote evidence** (does NOT preserve the complete source row; reference not exact over canonical identity — see F41) | F34 | VERIFIED_LOCAL_TEST (existing behavior) |

### ADAPT (keep substrate, change contract)
| Gap | Findings | Severity | Owning unit |
|---|---|---|---|
| As-known-at-arbitrary-cutoff evidence view absent | F11 F29 | High | **Unit 1** |
| Production issuance consumes post-cutoff observations | F35 F40 | High, verified reachable | **Unit 1** |
| Candidate reference not injective over canonical identity | F41 | Med (structural, low reachability) | **Unit 2** |
| `pregame_observation_count` misnomer | F33 | Low | **Unit 3** |
| Receipt create-only ≠ crash-atomic | F26 | Low–Med | **Unit 4** |
| TOCTOU due-check→claim (uncaught `FileExistsError`) | F22 | Low (latent, mitigated) | **Unit 4** |
| Unexpected post-claim / `write_result` failure | F27 | Low–Med | **Unit 4** |
| Store-level multi-writer safety (deployment-provided only) | F20 | Med (contingent) | **No unit scheduled until writer contract decided** (see §5) |

### UNDECIDED → documented design questions (NOT decision entries yet)
| Question | Findings | Status |
|---|---|---|
| Stale-claim recovery policy | F23 | Requires an explicit decision **before** any retry/reclaim code (locked present behavior — see D-B below) |
| Provider-label / provider-event stability | F16 | Unresolved until evidence a shared registry/normalization owner is needed |
| Descriptive/event-time trust boundary | F19 | Documented trust question; no decision entry until a unit proposes changing ownership/validation |
| Additional `effective_time` schema field | F11 | Do not decide now; Unit 1 sets visibility = `fetched_at` without adding a field |

### REPLACE / RETIRE
**None.** The substrate is fundamentally sound; WS1 needs focused contract
adaptations and policy decisions, not a replacement odds system.

---

## 2. DECISIONS.md entries (TWO — per reviewer)

**D-A — System-known visibility is governed by `fetched_at`.** An observation is
available to an as-known view only if its local system-known timestamp satisfies the
declared cutoff contract. `sportsbook_updated_at` remains source-provided update
metadata; `commence_time` remains the event-start boundary. *(Irreversible semantic
decision; basis for Unit 1.)*

**D-B — Unresolved claims are not automatically retried.** Any future retry, lease,
expiry, reconciliation, or manual-resolution mechanism requires an explicit decision
defining ambiguous-prior-execution and provider-cost handling. *(Locks present
behavior without pre-choosing the long-term recovery policy — F23.)*

*(Rejected as a new entry: "clean-sheet artifact latitude" — already a repository-wide
premise (never live; no legacy compat). Reference the existing governing statement
when Unit 2 replaces the reference contract or Unit 3 renames the coverage field.)*

---

## 3. Unit partition (accepted)
- **Unit 1 — Point-in-time quote evidence retrieval** *(active first unit)*: F11, F29,
  F40, and the decision-time-visibility portion of F35.
- **Unit 2 — Candidate-reference identity hardening**: F41; exact cross-artifact
  reference completeness; regenerate affected dev artifacts/tests. *(Do not pre-decide
  store-physical ownership; first define the required identity capability.)*
- **Unit 3 — Coverage diagnostic semantics**: F33; rename to non-live count or replace
  with genuine pregame counting (clean-sheet latitude applies).
- **Unit 4 — Collection claim & receipt robustness**: F22, F26, F27 (one lifecycle
  boundary). Stale-claim **recovery** excluded until D-B's policy is chosen.
- **F20 (store multi-writer safety):** recorded in ROADMAP as *no implementation unit
  scheduled until the intended writer contract is decided* — later outcomes may be
  enforced single-writer, store-level locking, partition CAS/transactional storage, or
  a storage-strategy replacement in a later workstream. **Not dropped.**

---

## 4. FIRST PLAN.md unit — LOCKED

### Title
**Point-in-time quote evidence retrieval.**

### Goal
Introduce a single owned cutoff-visible quote-evidence operation and route production
candidate issuance through it, so **no observation learned after the declared
evaluation time can enter an issuance.** The contract states `fetched_at` is the
system-known basis (D-A).

### Contract (visibility and eligibility are SEPARATE, composed — per reviewer)
```
full ledger
  ↓ visibility:   fetched_at <= cutoff          (inclusive; D-A)
cutoff-visible evidence
  ↓ eligibility:  is_live is False AND fetched_at < commence_time   (strict kickoff)
  ↓ (or consumer-specific selection)
candidate issuance / history boundaries / coverage
```
- **Visibility** answers *"known to Gridiron Edge by the decision cutoff?"* →
  `fetched_at <= cutoff` (inclusive: a quote stamped exactly at cutoff is in the set).
- **Pregame eligibility** answers *"usable pregame?"* → `is_live is False and
  fetched_at < commence_time` (strict: a quote exactly at kickoff is not pregame).
- **Do NOT compress into `min(cutoff, kickoff)`** — they must remain separately
  explainable (Observed → Visible → Eligible, per VISION).
- **UTC-validated cutoff** (reject naïve/non-UTC).
- **Empty cutoff-visible evidence is first-class** — the primitive returns a canonical
  **empty quote frame**; it does **not** invent identity-specific "no observation by
  cutoff" statuses (identities appearing only after cutoff are, by definition, not
  visible; distinguishing them needs out-of-view info and must not leak in). Candidate
  issuance must preserve empty as a **zero-row issuance**, not an error.
- **Coverage** is computed from the already cutoff-visible frame (F33's naming stays
  Unit 3).
- **Live rows known by cutoff remain visible** (ineligible for pregame selection, not
  erased).

### `history_boundaries` scope (per reviewer)
Prefer **composition, not modification**: `visible = as_known_at(observations, cutoff)`
then `select_quote_history_boundaries(visible)`. Leave the selector **unchanged** if it
already behaves correctly over the supplied frame; modify only if a public composed
operation belongs there or a test exposes a behavior unattainable by clean composition.
Do not force every consumer through one selector — centralize *visibility*, permit
consumer-specific *interpretation*.

### Acceptance tests (reviewer's final set, 28)
**Cutoff evidence operation (1–12):** before-cutoff included; exactly-at-cutoff
included; after-cutoff excluded; empty input → canonical empty frame; cutoff-before-all
→ empty frame; input not mutated; output preserves canonical schema; deterministic
ordering; naïve cutoff rejected; non-UTC rejected; identities appearing only after
cutoff absent; live-before-cutoff visible.
**Composition with history boundaries (13–17):** between-fetch cutoff exposes only the
earlier; post-cutoff fetches don't affect counts/repeated-evidence; post-cutoff kickoff
conflict absent from cutoff view; kickoff eligibility strict though visibility
inclusive; visible live obs remains in evidence but not selected as latest-eligible.
**Production candidate issuance (18–23):** post-cutoff-but-pre-kickoff quote cannot
enter; exactly-at-cutoff can enter if otherwise eligible; issuance still evaluates every
visible quote (not one-per-series); empty cutoff-visible ledger → valid zero-row
issuance; CLI `--evaluated-at` controls the cutoff; backdated CLI execution is
point-in-time correct.
**Regression & gates (24–28):** WS1-focused tests green; new focused tests pass; full
project unit suite passes; ruff + Pyrefly pass; real ledger unchanged (read-only).

### Non-regression guardrails
Do not: change canonical quote identity; mutate/rewrite the ledger; modify the current
snapshot; select opening/closing quotes; collapse visible evidence to one row per
historical identity; add decision cutoffs to stored observation rows; alter candidate
reference identity (that's Unit 2); silently filter live observations from the evidence
layer; compute coverage from the unfiltered ledger when presenting an as-known view.

---

## 5. Boundary 8 disposition — CLOSED → **WS1 INSPECTION COMPLETE**
Reuse map accepted (evidence corrected); **no REPLACE/RETIRE**; four-unit partition
accepted; **first unit LOCKED: Point-in-time quote evidence retrieval**; two DECISIONS
entries (D-A, D-B); F20 retained in ROADMAP as writer-contract-pending.

### Finalization sequence (post-close)
1. Commit finalized WS1 findings → `docs/workstreams/quote_observations/FINDINGS.md`.
2. Create `docs/workstreams/quote_observations/HANDOFF.md` from this consolidated result.
3. Create procedural `AI_BOOTSTRAP.md` (method only, no state).
4. Update root `HANDOFF.md` → active workstream + first unit.
5. Record D-A and D-B in `DECISIONS.md`.
6. Replace root `PLAN.md` with exactly one active unit: **Point-in-time quote evidence
   retrieval**.
7. Keep Units 2–4 (and the F20 writer-contract note) in `ROADMAP.md`, not `PLAN.md`.
8. Commit the whole package at one Git SHA; mirror that committed state to SharePoint so
   both model threads begin implementation from the same Git-addressed context package.
