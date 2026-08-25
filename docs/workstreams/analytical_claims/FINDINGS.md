# Gridiron Edge Workstream 2 Repository Inspection — Analytical Claims

Status: ACTIVE INSPECTION

**File-continuity note:** the working environment lost this file multiple
times during authoring (after Boundary 2, after Boundary 5 batch 3's initial
draft, and again after Boundary 5's full closure). Each time it was
reconstructed from the conversation record and/or an owner-supplied canonical
copy, with the owner's copy treated as authoritative whenever available. This
revision reconstructs the file through Boundary 5's close (confirmed against
the owner's own uploaded copy) and appends the fully reconciled Boundary 8
consolidation, incorporating every reviewer correction to the initial
Boundary 8 draft.

### Inspection authority
- CONSTITUTION.md: canonical and locked
- VISION.md: canonical and locked
- ROADMAP.md: canonical and locked
- Scope: Workstream 2 (Analytical Claims) only

### Inspected repository snapshot
- Repository: Gridiron Edge (local + SharePoint mirror)
- Inspection snapshot identifier: WS2-SNAPSHOT-001
- Mirror convention: source files unsupported by SharePoint indexing are mirrored
  with an additional .txt suffix
- Source-path rule: all findings record the original repository path; mirrored
  .txt paths are references only
- Byte-fidelity limitation: findings that depend on exact syntax, operators,
  whitespace-sensitive content, or truncated text require verification against
  the local source file.

### Inspection revisions
- Revision: 8 (Boundary 8 consolidation — reconciled, for final owner ratification)
- Authoring thread: Claude
- Reviewing thread: Microsoft 365 Copilot (ChatGPT-side)
- Repository snapshot frozen during author/reviewer cycle: yes

#### Ownership terminology (carried over from WS1, per reviewer)

Existence ≠ ownership. Each candidate is one of:
- **Candidate owner** — file exists; name/location suggests relevance (tree/filename).
- **Documented owner** — a doc (e.g. VISION.md, an ADR) attributes behavior to it.
- **Verified runtime owner** — behavior confirmed by local source/test/artifact.

### Workstream 2 scope

Per VISION.md's locked build order, Stage 2: **"Establish the common claim and
lineage contract."** The versioned Analytical Claim is VISION's central object — a
*conceptual contract* (identity/version, claim kind, subject, evidence cutoff,
inputs, method identity, uncertainty/limitations, backward+forward lineage,
invalidation contract, lifecycle status) that domain artifacts *specialize or
reference*, not a universal physical schema.

### Explicitly out of scope
- Redesigning candidate_issuance.py, market_closeout.py, or
  recommended_bet_result.py wholesale.
- New market/edge/recommendation calculation logic.
- Frontend claim presentation.
- Portfolio, fantasy, or CLV interpretation logic.
- Reopening D28/D29 (WS1 decisions) without new evidence.

### Evidence labels (carried over from WS1)

VERIFIED_LOCAL_SOURCE · VERIFIED_LOCAL_TEST · VERIFIED_REAL_ARTIFACT ·
REVIEWED_FULL_ATTACHED_SOURCE · REVIEWED_FULL_MIRROR ·
SUPPORTED_BY_MIRROR_SNIPPET · SUPPORTED_BY_FILENAME_ONLY ·
INDEXING_INCOMPLETE · LOCAL_VERIFICATION_REQUIRED · NOT_INSPECTED

---

## Prior incident — seed motivating case (pre-Boundary-1)

WS1 Unit 2 changed candidate_issuance_row_id's hash payload to close a
non-injectivity gap (F41) — a correct, closed fix. Running the real frontend
afterward against pre-existing, previously-persisted
recommended_bet_results/*.json artifacts produced:
`ValueError: Recommended-bet candidate identity does not match offer evidence.`
**Root cause (confirmed):** validate_recommended_bet_result re-derives
candidate_reference_id on every read; artifacts persisted before Unit 2
carried a reference computed under the old hash. **This is the validator
working as designed**, not a defect. **Resolution:** the owner regenerated
the production chain (issue-candidates → derive-policy →
evaluate-recommendations) in dependency order; all three succeeded.
**Why this matters for WS2:** the identity-contract change was invisible
until runtime (no declared payload-version separate from schema_version);
CandidateIssuanceRow's reference behaves like an ad hoc claim identity with
no forward lineage or invalidation contract; RecommendedBetResult's
self-validation detects mismatch but has no lifecycle status distinguishing
"superseded contract" from "corrupt"; recovery required tribal-knowledge
dependency ordering.

---

## Boundary 1 — Inventory & ownership — **CLOSED (rev 1)**

Three production modules + three test modules at full source
(candidate_issuance.py, market_closeout.py, recommended_bet_result.py);
remainder discovered via directory trees and three rg rounds. **15 candidate
claim-shaped objects inventoried** (see full table in the prior revision;
summarized in Boundary 8's reuse map below). **Identity/fingerprint content
producers** identified across candidate_issuance.py, recommendation_governance.py,
recommendation_policy.py, recommended_bet_result.py, production_chain_preflight*.py
— each owning canonicalization locally, no shared implementation found.
**Identity validators** (store-layer digest-shape checks) confirmed distinct
from identity producers.

**Headline:** Gridiron Edge already contains substantial claim-shaped
capabilities, deterministic content identities, strict domain-local artifact
schemas, and extensive backward provenance. These capabilities are
distributed across domain owners. No common claim, claim-identity,
reference-version, or lineage primitive was identified. The appropriate
version owner, evolution mechanism, and minimum shared contract remain open.

## Boundary 2 — Recommendation policy & governance identity boundary — **CLOSED (rev 2)**

8 files at full source. **Q1:** source_evidence_fingerprint is an opaque
aggregate content digest, not a lineage pointer; governance lineage is
embedded and re-verifiable (an accepted asymmetry). **Q2:** schema versioning
does not interpret alternate embedded-reference definitions. **WS2-B2-1:**
policy and governance stores share sequential-use intent but **neither
implements concurrency-safe create-only publication** — POSIX
`Path.replace()` unconditionally overwrites (Adapt candidate). **WS2-B2-2:**
`created_at` excluded from identity but retained in content — same-identity/
different-creation-time behavior **unresolved and untested.** **WS2-B2-3:**
`_resolve_candidate`/`_resolve_row` reimplement the same re-derive-and-match
pattern — a reference-resolution ownership question, not a second incident.
**WS2-B2-4/5:** `RecommendationPolicyDecision` and `PortfolioExposureSnapshot`
confirmed non-identified/non-persisted, structurally — "deliberate" not
established. **WS2-B2-6:** "Unit 22" dev-era language — cleanup finding.

**Headline:** Strong domain-specific integrity and selective persistence, but
both stores rely on overwrite-capable publication — neither is
concurrency-safe. No common lineage or identity-evolution contract emerges.

## Boundary 3 — Persistence recurrence, lineage audit, portfolio-allocation identity — **CLOSED (final)**

**WS2-B3-1:** candidate_issuance_store.py is create-only-correct but not
atomic (Adapt candidate — exposes a partial file if interrupted mid-write).

**WS2-B3-2/B3-7 (persistence recurrence, corrected taxonomy):**
Responsibility-based, not a running-ordinal count. Immutable JSON stores: 4
replace-based (governance, policy, preflight, recommended-result), 1
exclusive-create (candidate issuance). Mutable operational stores: bet
ledger, bankroll transactions. Cross-store orchestration: recorded wager
(compensating snapshot/restore — a distinct responsibility). Composite
artifact persistence: recommended evaluation (child-first, non-transactional).

**Direct confirmation the correct fix already exists in this codebase** (via
rg across `src/gridiron_edge/market/*_store.py`):

| Store | Mechanism (confirmed) |
|---|---|
| candidate_issuance_store.py | `open("x")` direct-to-final-path — create-only, NOT atomic |
| collection_plan_store.py | `temporary.replace(path)` — overwrite-capable, unguarded |
| production_chain_preflight_store.py | exists-check → `temporary.replace(path)` — overwrite-capable |
| collection_receipt_store.py | `temporary.open("x")` → `os.link(temporary, path)` — **CORRECT** (WS1 Unit 4) |
| recommendation_governance_store.py | exists-check → `temporary.replace(path)` — overwrite-capable |
| recommended_bet_result_store.py | exists-check → `temporary.replace(path)` — overwrite-capable |
| recommendation_policy_store.py | `temporary.replace(path)` inside a dead `except FileExistsError` — overwrite-capable |
| betting/ledger.py (`_write_ledger`) | full-DataFrame `to_parquet()` rewrite every write — **no temp-file, no atomicity, no concurrency guard whatsoever** |

**This is eight named, surveyed persistence owners, not seven** (a
consolidation-stage arithmetic error is corrected in Boundary 8 below — the
narrative below is preserved as originally authored per-boundary, but the
*count* is corrected at consolidation). **This confirms the fix is not
hypothetical — it is one file away, in the same directory, already shipped
and tested.**

**WS2-B3-3 (resolves B1 Q3):** production_chain_preflight.py is a read-only,
on-demand pipeline auditor, NOT a persisted forward-impact index. Its
16-component enumeration is the richest lineage-stage map in the codebase —
design input, not itself the contract.

**WS2-B3-4 (resolves B1 Q4):** No separate persisted allocation-claim identity
was found **in the inspected recommendation-policy, recording, ledger, and
(later) portfolio-API path** — narrower than a repository-wide claim. The
betting ledger is a Parquet-backed, uuid-identified operational store (NOT
content-addressed); its "append-only" docstring describes only the logical
contract — the physical mechanism is whole-file rewrite with no atomicity or
concurrency protection (a new, more basic Adapt candidate than any JSON
store's gap). `bet_id` confirmed `uuid.uuid4()` via full local source.

**WS2-B3-5 (recording trust boundary):** resolved via rg + full source —
`RecommendationRecordingEvidence` is constructed at `api/loaders.py`
(loader-owned), not a bare pass-through. Fully closed with complete internal
logic in Boundary 5 batch 3.

**WS2-B3-6:** "Unit 24" — second confirmed instance of dev-era phase-naming
(later a fourth, across Boundary 5).

**WS2-B3-7b:** `recommended_bet_result_store.py`'s reflection-based codec is
the strongest existing precedent for a reusable dataclass JSON codec — scoped
prior art, not yet a universal primitive.

**WS2-B3-8:** Composite evaluation persistence is child-first but not
transactional; missing-child reads propagate raw `FileNotFoundError` — an
undecided error-model question for Boundary 8.

**WS2-B3-9:** Provenance-agreement enforcement (`_validate_evaluation`) is
confirmed Reuse — strong prior art for composite-artifact consistency.

**Headline:** The persistence layer is, with one exception, not
concurrency-safe or crash-atomic. The correct fix already exists in this
codebase. Preflight is a correctly-scoped auditor. No independent allocation
identity was found in the inspected path. The recording trust boundary is
honored at the correct layer.

## Boundary 4 — Analytical-edge, diagnostics, qualification & market-family evaluation — **CLOSED (final)**

All 8 files at full, unelided source. **WS2-B4-1:** edge.py is pure domain
math — a valuable negative boundary example for where identity/lineage
should NOT be introduced. **WS2-B4-2:** EdgeDiagnostics is a rich immutable
evidence report, no identity/store. **WS2-B4-3 (RESOLVED):**
qualification.py's production wiring is **confirmed absent** — no caller
anywhere in the repository. Reclassified as **isolated diagnostic
infrastructure**, disposition explicitly undecided (Remove/Adapt/Retain).
**WS2-B4-4:** market_family_evaluation.py confirmed as the producer of
Boundary 2's opaque fingerprint. Its AST-enforced import-denylist test is the
strongest mechanically-enforced separation prior art in the workstream —
recommend the *technique*, not the specific denylist, as a future template.
**WS2-B4-5 (central finding, corrected):** Six named reference-attribution
operations across three purpose-specific strategies — canonical
authentication (reference production, issuance-row resolution,
persisted-result self-validation) vs. structural attribution (closeout,
history-group, wager matching). The closeout operation's issuance-prefix
check does **not** validate the row-digest suffix — a real integrity
ambiguity, not merely undocumented behavior (reclassified at Boundary 8,
below).

**Headline:** The analytical layer cleanly separates pure calculations,
active evidence reports, transient decisions, and durable claims.
qualification.py is isolated, unwired infrastructure. Reference handling is
purpose-specific across three strategies with different integrity/evolution
properties.

## Boundary 5 — API representation boundary — **CLOSED (final, all three batches, all follow-ups)**

Three directory-scoped batches (schemas → serializers → routes) plus a final
DECISIONS.md canonical-text resolution.

**WS2-B5-1:** api/schemas/recommendations.py preserves the **major**
recommendation-chain identities (several schema/version fields, e.g. issuance
schema version, governance ID/version, confirmed **absent**), nested by
domain concern, with domain enums imported directly.

**WS2-B5-2:** Offer-provenance response preserves complete materialized
offer-reference evidence — not the whole candidate row.

**WS2-B5-3:** Abstention/unavailable metadata confirmed extensive — 9
distinct fields, governed by D20.

**WS2-B5-4:** `serialize_recommendation_result` confirmed, by full-source
trace, to perform zero analytical recomputation — two honest, named
exceptions (externally-supplied `evaluation_id`; `suggested_stake` as a
presentation alias, not a duplicate field).

**WS2-B5-5:** `api/serializers/portfolio.py` fully resolved via owner local
source — confirmed no I/O anywhere, portfolio analytics supplied via `perf`
(never recomputed), narrow non-analytical response-summary logic is real and
correctly scoped, full four-identifier recommendation-chain preserved on bet
rows.

**WS2-B5-6/10 (D17 correspondence, final):** Confirmed **NOT exception-free**
— `GET /portfolio/splits` bypasses `serialize_splits` on its empty-ledger
branch, losing the D20-governed `no_split_data` metadata. Confirmed, via
exact D18 canonical text, as a real violation. **Adapt candidate**, no new
decision needed.

**WS2-B5-7/11:** The route performs genuine orchestration plus one small,
real, non-analytical join (`get_portfolio_splits`'s merge of pre-computed
`record_df`/`roi_df`) — zero recommendation-analytical computation.

**WS2-B5-8/12 (recommendation-provenance trust boundary, fully resolved):**
Three separately-owned validation layers, now completely confirmed:
(1) `RecordBetRequest`'s exact all-or-none validator, confirmed via full
local source; (2) `resolve_recommended_bet_recording_evidence`, confirmed via
full local source, constructs trusted evidence from the persisted result's
own fields, not client-supplied values (exact but scan-based, not
index-based); (3) the recording/ledger domain's own validation. One honest
gap: only `ValueError` is confirmed translated to HTTP 400.

**WS2-B5-9/13:** Read-after-write confirmed for the wager row only (not the
bankroll transaction).

**D17–D20 canonical-text resolution (decisive):**
- **D18** governs `_meta.field_status` ownership — confirms the
  empty-`/splits` violation exactly.
- **D17** governs the per-endpoint serializer pattern — implicated, but D18
  is the primary violation.
- **D19** governs `api/loaders.py` threading `settings.repo_root` — **NOT**
  serializer I/O purity. The serializer docstring's own D19 citation is a
  genuine **documentation-attribution defect**.
- **D20** governs the `Unavailable` slug family — the actual authority
  behind the 9-field abstention-metadata finding.

**Headline:** The portfolio API preserves the recommendation identity chain
through three explicit, now-fully-confirmed validation boundaries.
Serializers remain no-I/O presentation owners. Recommendation serialization
is mechanical and non-analytical with two named exceptions. One route path
violates documented D18. The serializer docstring misattributes its no-I/O
claim to D19.

**Boundary 5's closure completed the planned WS2 file-inspection phase.**

---

## Boundary 8 — Consolidation: reuse map, locked decisions, minimum shared contract — **CLOSED (rev 8, fully reconciled)**

> This section supersedes the initial "for reviewer" Boundary 8 draft in its
> entirety. All twelve required reconciliation items from review are
> incorporated below. Per reviewer guidance, architectural decisions,
> confirmed implementation units, and explicitly deferred decisions are now
> presented as three separate lists, not one undifferentiated decision set.

### Consolidated verdict (corrected wording)

**Rejected as originally stated:** "fundamentally sound" — too strong given
the inspection's own findings of repeated persistence-correctness defects and
one confirmed API metadata-loss path.

**Corrected verdict:**
> The inspected analytical-evidence substrate is **substantially reusable**.
> It demonstrates deterministic domain identities, strict current-version
> artifact validation, extensive named backward provenance, explicit
> unavailable/conflicting/abstention states, selective (not indiscriminate)
> persistence, strong read-only API presentation behavior, and multiple
> domain-specific reference/attribution mechanisms. These capabilities are
> distributed across independent owners without an inspected common claim,
> lineage, reference-version, or invalidation representation. The primary WS2
> gap is the **absence of an explicit composition contract** across existing
> domain artifacts — compounded by concrete, repeated persistence-correctness
> defects that should be corrected before new artifacts are built to depend on
> these stores.

### Reuse map — one row per Boundary-1-through-5 candidate object (roles corrected per review)

| # | Object / file | Disposition | Corrected role |
|---|---|---|---|
| 1 | `CandidateIssuance` / `CandidateIssuanceRow` | **Reuse** | **Durable claim-bearing root evidence artifact** (not "root evidence claim" — avoids implying each row is independently persisted as a claim) |
| 2 | `MarketCloseoutReference` / `MarketCloseoutResult` | **Reuse, reclassified** | **Domain-specific reference adapter and evidence result** — the inspected source establishes a resolution/adapter result, not a confirmed durable identity-addressed claim specialization. Do not call it a claim specialization unless its persistence, independent identity, and lifecycle meet the final claim profile (undetermined by this inspection). |
| 3 | `RecommendedBetResult` / `RecommendedBetEvaluation` | **Reuse**, split into two roles | `RecommendedBetResult` remains the **strongest inspected durable claim specialization**. `RecommendedBetEvaluation` is reclassified as a **durable composite manifest**, not necessarily a separate analytical claim in its own right. |
| 4 | `RecommendationPolicyGovernance` | **Reuse, reclassified** | **Durable governance/method-config artifact** — defines controlled policy inputs; not itself an analytical claim. |
| 5 | `RecommendationPolicy` | **Reuse, reclassified** | **Durable method/policy artifact consumed by recommendation claims** — content-addressed and governed, claim-adjacent, but its primary inspected role is method ownership, not claim assertion. |
| 6 | `RecommendationPolicyDecision` | **Reuse** | **Active transient decision output.** "Confirmed-by-pattern" removed — transience is structurally confirmed; the design rationale remains inferred, not established. |
| 7 | `PortfolioExposureSnapshot` | **Reuse** | **Deterministically identified supplied evidence.** "Intentionally unstored" and repository-wide allocation language removed — no dedicated store or separate persisted allocation-claim identity was found **in the inspected path**, not proven absent architecture-wide. |
| 8 | `ProductionChainPreflight` | **Reuse** | **Scope-bound audit report whose assessment may be persisted** — behaves more like a durable audit report than an ordinary transient evidence report; do not group with "non-persisted objects." |
| 9 | `EdgeDiagnostics` | **Reuse** | Evidence report — unchanged. |
| 10 | `EmpiricalMarketFamilyEvaluation` (`market_family_evaluation.py`) | **Reuse** | Evidence report; AST-enforcement **technique** flagged as a candidate template (not the specific five-prefix denylist). |
| 11 | `QualificationResult` / `qualification.py` | **Explicitly undecided** (Remove/Adapt/Retain) | Confirmed isolated, unwired diagnostic infrastructure. Its exhaustive ordered-check **technique** is reusable independently of the module's ultimate fate — do not classify the module as Reuse merely because the technique is useful. |
| 12 | `edge.py` | **Reuse** | Pure domain math — negative claim-boundary example. |
| 13 | `api/schemas/recommendations.py` + `api/serializers/recommendations.py` | **Reuse** | Strongest evidence of API-layer "no reinterpretation," at both schema and serializer levels, with two small named exceptions. |
| 14 | `api/schemas/portfolio.py` + `api/serializers/portfolio.py` + `api/routes/portfolio.py` | **Reuse, with one confirmed Adapt item** | Confirmed exact request-schema validation, fully-resolved trusted-evidence resolver, D17 correspondence (5 of 6 endpoints exact, one confirmed exception — see D-WS2-Adapt-2 below), D20-governed abstention metadata. |
| 15 | `betting/ledger.py` + `betting/recording.py` | **Reuse for domain logic; Adapt for persistence mechanism** | No independent allocation-claim identity (correct, by design, in the inspected path); full recommendation-chain provenance retention. The whole-file-rewrite persistence mechanism is the single worst-scoped persistence gap found in the workstream. |
| 16 | `api/routes/props.py` + schemas/serializers | **Not inspected — explicitly out of evidentiary scope** | This workstream makes **no claim** about this family. Any future capability profile should either be explicitly scoped to the inspected game-market recommendation chain, or gated on a later validation unit proving a representative props path can specialize it. Do not claim the resulting contract is domain-complete while props remain uninspected. |

### Correcting the persistence-recurrence tally (reviewer-identified arithmetic error)

**The Boundary 3/8 draft's "6 of 7" tally was internally inconsistent** —
the same section named eight distinct persistence owners (six
overwrite-capable/unguarded, one create-only-but-not-atomic, one fully
correct). **Corrected, final tally, replacing every prior "6 of 7"
statement in this document and in any future ROADMAP/PLAN acceptance
criteria:**

> **Six of eight surveyed persistence owners** (`collection_plan_store.py`,
> `production_chain_preflight_store.py`, `recommendation_governance_store.py`,
> `recommendation_policy_store.py`, `recommended_bet_result_store.py`,
> `betting/ledger.py`) **are overwrite-capable or fully unguarded. One of
> eight** (`candidate_issuance_store.py`) **is create-only but exposes the
> final path before serialization completes. One of eight**
> (`collection_receipt_store.py`, WS1 Unit 4) **uses the hardened create-only,
> atomic publication pattern** — the proven template for correcting the
> other seven.

This correction propagates to the persistence-hardening implementation unit
below and to every prior boundary section's "6 of 7" language (Boundary 3's
headline and the consolidated-open-items list are both understood to be
superseded by this corrected count).

### Proposed architectural decisions (5 — separated from implementation patches, per reviewer)

**AD-1 — Capability profile, not base class.** Durable claim-bearing
artifacts must declare how they satisfy a shared set of analytical-claim
capabilities. They are not required to inherit from one base class or use
one physical persistence schema. Nothing inspected justifies a universal
`AnalyticalClaim` dataclass, one physical persistence schema, or one
reference-resolution algorithm; the repository's own architecture (the
AST-enforced separation, the evidence-report/claim/manifest split, three
legitimately different attribution strategies) independently corroborates
this.

**Required core capabilities for a durable claim** (corrected — the initial
draft's six-item checklist was too thin; this expands to match VISION's full
Stage 2 charge):
1. **Claim identity** — stable identity value, identity method/contract
   owner, evolution rule.
2. **Claim kind** — machine-readable domain claim category.
3. **Subject** — the exact entity/offer/event/market/policy scope/decision
   addressed.
4. **Evidence cutoff** — the latest permissible evidence time, or an explicit
   domain-specific equivalent.
5. **Inputs and backward provenance** — named upstream artifact/evidence
   references; aggregate fingerprints only where constituent recovery is
   intentionally unavailable (as with `source_evidence_fingerprint`).
6. **Method identity** — model, derivation method, policy, governance, or
   other relevant method owner.
7. **Uncertainty and limitations** — explicit unavailable/insufficient/
   conflicting/bounded-evidence states as applicable.
8. **Attribution strategy** — canonical reference authentication, structural
   evidence attribution, history-group attribution, or another explicitly
   named domain mechanism (never implicit).
9. **Lifecycle and validity** — current validity state; rules for
   incompatibility, supersession, corruption, or regeneration.
10. **Invalidation contract** — what input/method changes invalidate the
    claim; what behavior readers must apply.
11. **Forward-impact discoverability** — not necessarily embedded in every
    artifact, but the architecture must define how downstream consumers can
    be discovered or assessed. **The physical mechanism (embedded IDs,
    reverse index, relationship manifest, query service, extended preflight
    scanning, or no new mechanism at current scale) is explicitly left
    undecided by this inspection** — insufficient evidence to lock one.

Not every evidence report needs all eleven capabilities — this profile
applies to **durable claim-bearing artifacts**, not every frozen dataclass.

**AD-2 — Evidence reports, method/governance artifacts, durable claims, and
composite manifests are distinct, permanent categories.** Pure calculations,
evidence reports, supplied evidence, governance/method artifacts, transient
decisions, durable claims, and composite manifests remain separate
architectural roles (per the corrected reuse-map roles above). Identity and
persistence are required only where the role's replay, audit, or
external-reference obligations demand them. Do not collapse governance,
policy, result, and evaluation into one homogeneous "claim" category — they
play distinct roles (governance: controlled method inputs; policy: governed
method artifact; result: analytical decision claim; evaluation: composite
manifest).

**AD-3 — Persisted references require explicit evolution ownership (replaces
the initial draft's premature "separate derivation-version field" mandate).**
Every persisted reference contract must identify which schema or method
contract governs its meaning and what readers do when that contract changes.
The representation **may** use enclosing-schema dispatch, a reference-method
discriminator, regeneration-on-incompatibility, method identity folded into
the identity payload, or another ratified mechanism — **the first
implementation unit decides the owner and mechanism; this inspection does
not preselect one.** For `candidate_issuance_row_id` specifically, that unit
must determine: which schema/method identifier owns row-reference semantics;
how readers distinguish valid-current / valid-under-an-older-supported-
definition / intentionally-incompatible-requiring-regeneration / corrupt;
whether the reference needs a method/version discriminator, enclosing-schema
dispatch, or both; which downstream artifacts must change together; what
tests prove old-vs-corrupt behavior; whether regeneration remains the
intended clean-sheet policy.

**AD-4 — Canonical authentication and structural attribution are separate,
explicitly named capabilities; neither may imply the other.** Consumers must
document which capability they perform and what disagreement means. **The
initial draft's disposition for `_closeout_matches` (document the
limitation) is corrected to a real Adapt item**, since the current behavior
is an integrity ambiguity, not merely undocumented: the function receives a
`reference_id` presented as a candidate reference, but accepts an arbitrary
suffix once the issuance prefix and materialized fields match — the test
fixture itself normalizes this acceptance. The owning implementation unit
must choose among: (Option A) validate the canonical row reference and then
compare fields; (Option B) remove reliance on the suffix and explicitly model
the operation as materialized-evidence attribution only; (Option C) expose
separate authentication and attribution results to the caller. Documentation
alone is insufficient.

**AD-5 — Forward-impact discoverability is required as a capability; its
mechanism is deferred.** (Folded into AD-1 item 11 above; listed separately
here because it was originally a standalone proposed decision and remains
the least-resolved of the five.)

### Confirmed implementation units (patches — separated from architectural decisions, per reviewer)

1. **Persistence hardening** (corrected scope: 6 of 8 owners overwrite-
   capable/unguarded, 1 of 8 create-only-but-not-atomic, 1 of 8 correct).
   Must distinguish, not conflate: (a) immutable JSON create-only publication
   (the `os.link` pattern generalizes directly); (b) candidate issuance's
   partial-final-path problem (same fix, `os.link` in place of `open("x")`);
   (c) the mutable Parquet ledger's atomic-rewrite problem (the receipt-store
   pattern is **not** automatically sufficient here — a full-file rewrite
   needs its own design, likely temp-file + atomic rename of the whole
   file, not a hard-link); (d) the cross-file recorded-wager compensation
   mechanism (already a distinct, working responsibility — not itself
   broken, per Boundary 3).
2. **Empty-`/portfolio/splits` serializer bypass** — minimal, isolated fix
   (`if bets.empty: return serialize_splits(pd.DataFrame(), dimension)`); no
   new decision required, D18 already governs it.
3. **Development-era phase-naming cleanup** — one repository-wide discovery
   pass ("Unit 22"/"Unit 24" across ≥4 files, 3 directories), but **each
   replacement reviewed and reworded in its local domain context** — not a
   blind global string substitution.
4. **D19 citation correction** in `api/serializers/portfolio.py`'s module
   docstring — corrected disposition: **remove the incorrect D19 citation
   and retain the factual no-I/O description without a decision citation**
   unless an existing decision explicitly owns it. (The initial draft's
   suggestion to cite D18 as an "implicit corollary" is rejected — D18
   governs `_meta.field_status` ownership specifically and should not be
   repurposed as authority for a different behavioral claim. If the project
   wants this behavior formally governed, that requires a new or amended
   decision through normal ratification, not a citation of convenience.)
5. **`_closeout_matches` authentication/attribution correction** (per AD-4)
   — implementation design (Option A/B/C) deferred to the owning unit.

### Explicitly deferred decisions (not resolved by this workstream — carried to future units, not silently answered)

- `QualificationResult`'s ultimate disposition (Remove/Adapt/Retain).
- Whether same-identity/different-`created_at` should be a write conflict or
  resolve to first-write-wins (untested either way).
- The missing-child-in-a-composite-manifest error model (`FileNotFoundError`
  vs. a domain-modeled state).
- The props family's relationship (if any) to this claim contract — never
  inspected.
- Whether `suggested_stake`'s duplication of `sizing.actionable_stake` is
  worth removing (a presentation-ergonomics call, not an architecture one).
- Whether the resolver's scan-based (not index-based) lookup pattern
  warrants a future relationship-index unit, or remains acceptable at
  current scale.
- The exact physical mechanism for forward-impact discoverability (AD-1
  item 11) — capability required, mechanism unselected.

### Lifecycle — corrected framing (reviewer: "absent everywhere" was too broad)

**Rejected as originally stated:** lifecycle status is "currently absent
everywhere."

**Corrected:** Inspected objects already include extensive **domain outcome
and availability states** — candidate issuance state, recommendation decision
state, recommended result state, closeout resolution status (17 states),
preflight component state, and the D20-governed evidence-availability
states. **What is missing is a common artifact-*validity* lifecycle** —
distinguishing concepts such as current / superseded / incompatible / invalid
/ corrupt — not domain outcome states generally. **The workstream should not
replace existing, richer domain states with one generic lifecycle enum;**
AD-1's "lifecycle and validity" capability (item 9) is scoped to this
narrower validity-and-supersession concept, layered alongside — not
replacing — the domain states already in place.

### Recommended implementation sequence (corrected — persistence hardening first, per reviewer)

1. **Persistence hardening** — precedes the identity-evolution unit. It
   addresses active, repeated write-integrity defects with an existing
   local, shipped precedent; it protects the artifacts every later
   claim-contract unit will depend on; and it does not require the
   identity-evolution design question to be resolved first.
2. **Identity-evolution contract for candidate references** (AD-3) — the
   highest-priority *claim-contract* capability, decided second because it
   is a design question (owner/mechanism not yet locked) rather than an
   already-scoped correctness fix.
3. **Common claim capability protocol/profile** (AD-1/AD-2) — formalize the
   required capabilities (documented protocol, narrow typed reference
   structures, validation helpers, contract/architecture tests) once Unit 2
   has established the identity-evolution pattern to generalize from.
4. **Attribution-operation ownership** (AD-4) — separate and name the six
   confirmed operations; correct `_closeout_matches`'s ambiguity per the
   locked contract.
5. **Small API and documentation cleanup** (confirmed units 2–4 above) —
   combinable as one low-risk, non-architectural batch if scope stays
   coherent, or kept separate if the phase-naming discovery pass produces a
   broad diff.

### Boundary 8 disposition — CLOSED (rev 8)

**Final headline (revised per reviewer):**
> Gridiron Edge's analytical substrate is substantially reusable, but its
> durable artifacts lack an explicit composition contract for identity
> evolution, claim semantics, lineage, validity, and attribution. WS2 should
> formalize those capabilities as an artifact-specific conformance profile
> rather than a universal base class. Before introducing that contract, the
> project should harden the persistence mechanisms that will store its
> conforming artifacts. Canonical reference authentication, structural
> evidence attribution, domain states, method artifacts, evidence reports,
> durable claims, and composite manifests must remain distinct, explicitly
> named roles rather than being collapsed into one physical schema or lookup
> mechanism.

All twelve required reconciliation items incorporated: (1) persistence
inventory corrected to eight named owners; (2) verdict wording softened from
"fundamentally sound" to "substantially reusable"; (3) capability profile
expanded to the full eleven-item VISION-aligned list; (4) the mandatory
separate derivation-version field replaced with an owner-and-policy
requirement, decision deferred to the first unit; (5) domain state vs.
artifact-validity lifecycle distinguished; (6) governance/policy/closeout/
evaluation reclassified by actual role, not lumped as claim specializations;
(7) `QualificationResult` preserved as explicitly undecided; (8)
`_closeout_matches` converted from a documentation note to a real AD-4 Adapt
item with three named options; (9) the D19→D18 citation suggestion removed
in favor of "remove the citation, don't repurpose another decision"; (10)
architectural decisions, confirmed implementation units, and deferred
decisions presented as three separate lists; (11) persistence hardening
sequenced before the identity-evolution unit; (12) props explicitly held
outside the evidentiary scope of any resulting contract pending a future
representative-path validation unit.

**This closes Workstream 2's inspection phase.** The next step is translating
the confirmed implementation units and the sequenced architectural-decision
units into ROADMAP/PLAN entries, beginning with persistence hardening (Unit 1) per the corrected sequence above — the same handoff pattern WS1 used from
its own Boundary 8 close into its first implementation unit.
