# Game-Spread Ten-Obligation Proof Matrix

## Purpose

This document is the durable exit proof for Workstream 3, the first
complete vertical decision slice.

It verifies one controlled game-spread subject against all ten
first-slice obligations. It does not infer completion from accumulated
unit tests, implementation intent, local runtime artifacts, or prior
workstream closeouts.

Every obligation is tied to:

- an exact artifact, response, or persisted relationship;
- a canonical identity or repository-relative path;
- an explicit evidence cutoff;
- a relevant method or policy identity;
- an expected and observed state;
- an exact verification test;
- a declared historical or current interpretation.

## Authority

The ten obligations below are copied verbatim from
`docs/workstreams/VISION.md`, under “What the first vertical slice must
prove.”

The same obligations are reproduced in
`docs/workstreams/first_vertical_slice/ROADMAP.md`.

## Evidence classification

This matrix is backed by one controlled real-store integration
repository.

“Controlled” means the source values, cutoffs, policy contexts,
portfolio evidence, later observation, and final scores are fixed test
inputs.

“Real-store-backed” means durable artifacts are written and read through
their production persistence owners. Canonical identities are produced
by their production identity functions. Domain states are produced by
the production evaluators. Artifact paths are checked against embedded
identities by the production readers.

The proof does not depend on developer runtime artifacts under the
repository’s local `data/output` tree. A pre-implementation inspection
found a valid local production spread lineage, but none of its proposed
baseline artifacts were Git-tracked. Those artifacts were therefore
rejected as durable proof dependencies.

## Canonical subject

| Field | Value |
|---|---|
| Game | `2026_01_KC_LAC` |
| Market | `spread` |
| Side | `home` |
| Provider | `the_odds_api` |
| Provider event | `provider-event-1` |
| Sportsbook | `draftkings` |
| Season | `2026-2027` |
| Week | `1` |
| Product ID | `spread-vertical-slice-product` |
| Product run ID | `spread-vertical-slice-run` |
| Forecast event ID | `spread-vertical-slice-forecast` |
| Forecast model | `win_prob / elo` |
| Product-generated time | `2026-09-01T11:00:00+00:00` |
| Kickoff | `2026-09-10T00:20:00+00:00` |

“One case” means this stable game-spread subject and its declared
evidence lineage. It does not mean one immutable result object.

The recommendation, abstention, zero-allocation, later-observation, and
pre/post-outcome obligations require mutually exclusive evaluation
contexts. Every such context below identifies the policy, portfolio,
cutoff, or outcome evidence that differs while holding the canonical
subject fixed.

## Evidence cutoffs and observations

### Historical T1

| Field | Value |
|---|---|
| Evidence cutoff | `2026-09-01T12:00:00+00:00` |
| Quote fetched at | `2026-09-01T11:30:00+00:00` |
| Sportsbook updated at | `2026-09-01T11:29:00+00:00` |
| Line | `-1.0` |
| American price | `-110` |
| Model probability | `0.5588870740382756` |
| Expected value | `0.06696623225488985` |
| Candidate state | `candidate` |
| Candidate reason | `positive_expected_value` |

### Historical T2

| Field | Value |
|---|---|
| Evidence cutoff | `2026-09-02T09:00:00+00:00` |
| Later quote fetched at | `2026-09-02T08:30:00+00:00` |
| Sportsbook updated at | `2026-09-02T08:29:00+00:00` |
| Later line | `-9.5` |
| American price | `-110` |
| Recomputed model probability | `0.3150871690837551` |
| Recomputed expected value | `-0.3984699499310129` |
| Latest row state in comparison scope | `not_candidate` |
| Latest row reason | `expected_value_not_positive` |

T2 includes both the original T1 observation and the later observation.
“Latest at T2” refers only to the maximum `fetched_at` within the declared
provider, provider-event, sportsbook, game, market, and side comparison
scope.

## Canonical identities

### Candidate issuances

| Context | Issuance ID |
|---|---|
| Historical T1 | `77ea2c34dd1ebe20fa68eaddc6e8a09bd387f8f98e4764b7443a7d70d63312b5` |
| Historical T2 | `1bf694fb1044949dcc4279e2fb911f3b8c56adb90566fe6b4f86c8d69eb398a7` |

Canonical paths:

```text
data/output/candidate_issuance/issuances/77ea2c34dd1ebe20fa68eaddc6e8a09bd387f8f98e4764b7443a7d70d63312b5.json
data/output/candidate_issuance/issuances/1bf694fb1044949dcc4279e2fb911f3b8c56adb90566fe6b4f86c8d69eb398a7.json
```

### Exact T1 candidate reference

```text
77ea2c34dd1ebe20fa68eaddc6e8a09bd387f8f98e4764b7443a7d70d63312b5:b182c0d64886b8f83030846a8c99c58e553f1c7ec0b5a43dd95a6fab642ae5cd
```

Candidate-reference derivation version:

```text
1
```

### Recommendation policies

| Context | Spread state | Policy ID |
|---|---|---|
| Active | `active / derived` | `1587d4aae00bc678bfdf28ac6fe89f0021fd8e9e1d0c2c4c95277e75a2ced351` |
| Abstaining | `insufficient_evidence / no_validated_threshold_selection_method` | `a3d030c8242edf78865b22cd4b64437093bd2c512cb452cb0abb03809840524a` |

Policy derivation method:

```text
market_family_empirical_evidence_v1
```

Canonical paths:

```text
data/output/recommendation_policies/schema=1/1587d4aae00bc678bfdf28ac6fe89f0021fd8e9e1d0c2c4c95277e75a2ced351.json
data/output/recommendation_policies/schema=1/a3d030c8242edf78865b22cd4b64437093bd2c512cb452cb0abb03809840524a.json
```

### Recommendation evaluations and results

| Context | Evaluation ID | Result ID |
|---|---|---|
| Positive allocation | `6426c558f6cc92bb291c33501c45f3f9688e165bfef56eec2460682ebef3e81f` | `d12141db771d9a434d1cee39b2ecbbe71bbfb7bfed3ffd1a89a803fc1c47c698` |
| Policy abstention | `46b385763b68cd126af2b71898e7de05677f9ef5e72ce6207880d5cb9232e2a9` | `c93683c93a71764fe8bbfe69a1ade5e8eaea1cf1774f6cc0685854e60eb6c612` |
| Eligible zero allocation | `d201736d9ffa9ba996b93a8eb076ec575afbf60772f9af85eca7e79e3e52d81e` | `3d0707fb1e8e1fdae899ccd591e399647fbc318de5a87e6d6f1ebd775e28455a` |

Canonical evaluation paths:

```text
data/output/recommended_bet_results/schema=3/evaluations/6426c558f6cc92bb291c33501c45f3f9688e165bfef56eec2460682ebef3e81f.json
data/output/recommended_bet_results/schema=3/evaluations/46b385763b68cd126af2b71898e7de05677f9ef5e72ce6207880d5cb9232e2a9.json
data/output/recommended_bet_results/schema=3/evaluations/d201736d9ffa9ba996b93a8eb076ec575afbf60772f9af85eca7e79e3e52d81e.json
```

Canonical result paths:

```text
data/output/recommended_bet_results/schema=3/results/d12141db771d9a434d1cee39b2ecbbe71bbfb7bfed3ffd1a89a803fc1c47c698.json
data/output/recommended_bet_results/schema=3/results/c93683c93a71764fe8bbfe69a1ade5e8eaea1cf1774f6cc0685854e60eb6c612.json
data/output/recommended_bet_results/schema=3/results/3d0707fb1e8e1fdae899ccd591e399647fbc318de5a87e6d6f1ebd775e28455a.json
```

### Decision-quality evaluations

| Context | Decision status | Realized outcome | Evaluation ID |
|---|---|---|---|
| Pre-outcome | `consistent` | `unavailable` | `181cbfe8fbba8cc324d208060630c20ceed9dc4b569c83a07fabd6607e491ed3` |
| Post-outcome | `consistent` | `win` | `9b720ef4974703c6931eac0d5ad27db7e24155a1e9861dd9f5b089a32e3d6aae` |

Both evaluations reference:

```text
Result ID:
d12141db771d9a434d1cee39b2ecbbe71bbfb7bfed3ffd1a89a803fc1c47c698

Recommendation evaluation ID:
6426c558f6cc92bb291c33501c45f3f9688e165bfef56eec2460682ebef3e81f
```

Canonical paths:

```text
data/output/decision_quality_evaluations/schema=1/evaluations/181cbfe8fbba8cc324d208060630c20ceed9dc4b569c83a07fabd6607e491ed3.json
data/output/decision_quality_evaluations/schema=1/evaluations/9b720ef4974703c6931eac0d5ad27db7e24155a1e9861dd9f5b089a32e3d6aae.json
```

## Ten-obligation matrix

### 1. A mutable source observation is preserved without overwrite.

**Context and proof:** T1 is appended through the canonical odds-history
writer. T2 is later appended to the same season/week partition. The
loaded ledger contains both exact observations rather than replacing T1.

**Exact artifact or response:**

```text
data/odds/history/season=2026-2027/week=01/observations.parquet
```

**Evidence cutoff:** T1 and T2.

**Method or policy identity:** canonical quote-observation schema and
`append_to_odds_ledger`.

**Expected and observed state:** two rows remain: T1 line `-1.0` and T2
line `-9.5`, with distinct `fetched_at` values.

**Temporal meaning:** preserved history through T2.

**Verification test:**
`test_source_observations_are_preserved_without_overwrite`.

**Status:** PROVEN.

### 2. A time-valid analytical claim consumes an exact source version.

**Context and proof:** `as_known_at` at T1 excludes T2. The T1 candidate
result records the exact candidate reference and forecast event.

**Exact artifact or response:** T1 issuance
`77ea2c34dd1ebe20fa68eaddc6e8a09bd387f8f98e4764b7443a7d70d63312b5`,
candidate reference
`77ea2c34dd1ebe20fa68eaddc6e8a09bd387f8f98e4764b7443a7d70d63312b5:b182c0d64886b8f83030846a8c99c58e553f1c7ec0b5a43dd95a6fab642ae5cd`,
and forecast event `spread-vertical-slice-forecast`.

**Evidence cutoff:** `2026-09-01T12:00:00+00:00`.

**Method or policy identity:** candidate-reference derivation version
`1`; product run `spread-vertical-slice-run`.

**Expected and observed state:** exactly one quote is visible at T1.
Result provenance agrees with the exact issuance row and forecast event.

**Temporal meaning:** historical T1.

**Verification test:**
`test_time_valid_claim_consumes_an_exact_source_version`.

**Status:** PROVEN.

### 3. An estimated output includes honest uncertainty or limitation.

**Context and proof:** the selected weekly product stores spread
uncertainty alongside its estimate, and the result stores the model
probability and expected value derived from that estimate.

**Exact artifact or response:**

```text
data/output/weekly_products/products/spread-vertical-slice-product.parquet
```

**Evidence cutoff:** product generated at
`2026-09-01T11:00:00+00:00`.

**Method or policy identity:** spread calibration key `win_prob_elo`;
model `win_prob / elo`.

**Expected and observed state:** `spread_status=available`,
`spread_uncertainty=13.5`, and model probability and expected value are
present.

**Temporal meaning:** current selected product inside the controlled
repository.

**Verification test:**
`test_estimated_output_preserves_uncertainty_or_limitation`.

**Status:** PROVEN.

### 4. A market price remains separate from the prediction.

**Context and proof:** the immutable result records model probability
separately from the exact sportsbook line and American price.

**Exact artifact or response:** positive result
`d12141db771d9a434d1cee39b2ecbbe71bbfb7bfed3ffd1a89a803fc1c47c698`.

**Evidence cutoff:** T1.

**Method or policy identity:** model `win_prob / elo`; active policy
`1587d4aae00bc678bfdf28ac6fe89f0021fd8e9e1d0c2c4c95277e75a2ced351`.

**Expected and observed state:** model probability
`0.5588870740382756`, line `-1.0`, and price `-110`. They occupy separate
fields and roles.

**Temporal meaning:** historical T1.

**Verification test:**
`test_market_price_remains_separate_from_prediction`.

**Status:** PROVEN.

### 5. An analytical edge is derived without automatically becoming a recommendation.

**Context and proof:** the T1 issuance row is a positive-EV candidate.
Evaluation under the separate abstaining policy produces an unavailable
recommendation result.

**Exact artifact or response:** T1 issuance
`77ea2c34dd1ebe20fa68eaddc6e8a09bd387f8f98e4764b7443a7d70d63312b5`,
abstaining result
`c93683c93a71764fe8bbfe69a1ade5e8eaea1cf1774f6cc0685854e60eb6c612`,
and parent evaluation
`46b385763b68cd126af2b71898e7de05677f9ef5e72ce6207880d5cb9232e2a9`.

**Evidence cutoff:** T1.

**Method or policy identity:** abstaining policy
`a3d030c8242edf78865b22cd4b64437093bd2c512cb452cb0abb03809840524a`;
method `market_family_empirical_evidence_v1`.

**Expected and observed state:** candidate is
`candidate / positive_expected_value`, while the governed result is
`unavailable / insufficient_evidence / recommendation_ineligible`.

**Temporal meaning:** historical T1 under abstaining policy.

**Verification test:**
`test_positive_edge_does_not_automatically_become_a_recommendation`.

**Status:** PROVEN.

### 6. Recommendation policy can recommend or abstain.

**Context and proof:** the same exact T1 candidate is evaluated under an
active policy and an abstaining policy. Both results are persisted
through the recommendation store.

**Exact artifact or response:** positive evaluation
`6426c558f6cc92bb291c33501c45f3f9688e165bfef56eec2460682ebef3e81f`,
positive result
`d12141db771d9a434d1cee39b2ecbbe71bbfb7bfed3ffd1a89a803fc1c47c698`,
abstaining evaluation
`46b385763b68cd126af2b71898e7de05677f9ef5e72ce6207880d5cb9232e2a9`,
and abstaining result
`c93683c93a71764fe8bbfe69a1ade5e8eaea1cf1774f6cc0685854e60eb6c612`.

**Evidence cutoff:** T1.

**Method or policy identity:** active policy
`1587d4aae00bc678bfdf28ac6fe89f0021fd8e9e1d0c2c4c95277e75a2ced351`;
abstaining policy
`a3d030c8242edf78865b22cd4b64437093bd2c512cb452cb0abb03809840524a`.

**Expected and observed state:** active context is
`recommended / recommendation_eligible / allocated`. Abstaining context
is `unavailable / insufficient_evidence / not_evaluated`.

**Temporal meaning:** two declared T1 policy contexts.

**Verification test:** `test_policy_can_recommend_and_abstain`.

**Status:** PROVEN.

### 7. Portfolio policy can allocate zero despite an eligible recommendation.

**Context and proof:** the active policy evaluates the same exact T1
candidate with complete correlation-capacity evidence linked to its
candidate reference. The result and parent evaluation are persisted.

**Exact artifact or response:** zero-allocation evaluation
`d201736d9ffa9ba996b93a8eb076ec575afbf60772f9af85eca7e79e3e52d81e`
and zero-allocation result
`3d0707fb1e8e1fdae899ccd591e399647fbc318de5a87e6d6f1ebd775e28455a`.

**Evidence cutoff:** T1.

**Method or policy identity:** active policy
`1587d4aae00bc678bfdf28ac6fe89f0021fd8e9e1d0c2c4c95277e75a2ced351`;
correlation group `spread-game-risk`.

**Expected and observed state:**
`recommended / recommendation_eligible / zero_allocation /
correlation_capacity_exhausted`; allocated stake `0.0`. Eligibility
remains true.

**Temporal meaning:** historical T1 under a capacity-exhausted portfolio
context.

**Verification test:**
`test_eligible_recommendation_can_receive_zero_allocation`.

**Status:** PROVEN.

### 8. A later observation can supersede or invalidate downstream artifacts.

**Context and proof:** T2 adds a later quote in the same declared
comparison scope. Re-evaluation changes the latest row-owned candidate
outcome without rewriting T1.

**Exact artifact or response:** T1 issuance
`77ea2c34dd1ebe20fa68eaddc6e8a09bd387f8f98e4764b7443a7d70d63312b5`
and T2 issuance
`1bf694fb1044949dcc4279e2fb911f3b8c56adb90566fe6b4f86c8d69eb398a7`.

**Evidence cutoff:** T1 and T2.

**Method or policy identity:** candidate issuance evaluator and the
comparison scope of provider, provider event, sportsbook, game, market,
and side.

**Expected and observed state:** T1 is
`candidate / positive_expected_value`; the latest T2 row is
`not_candidate / expected_value_not_positive`. T1 remains unchanged.

**Temporal meaning:** historical T1 plus latest visible row within scope
at T2.

**Verification test:**
`test_later_observation_changes_the_recomputed_outcome`.

**Status:** PROVEN.

### 9. The original decision remains reproducible.

**Context and proof:** T1 and T2 issuances, the active policy, and the
positive recommendation evaluation are re-read through strict public
readers. Re-evaluating the original T1 inputs produces an object-equal
parent evaluation.

**Exact artifact or response:** the T1 issuance, active policy, positive
result, and positive parent paths listed above.

**Evidence cutoff:** T1, replayed after T2 exists.

**Method or policy identity:** candidate identity owner, policy identity
owner, and recommendation evaluation identity owner.

**Expected and observed state:** strict store round trips equal the
original objects. Repeated T1 evaluation equals evaluation
`6426c558f6cc92bb291c33501c45f3f9688e165bfef56eec2460682ebef3e81f`.

**Temporal meaning:** historical T1 replay after later evidence.

**Verification test:** `test_original_decision_remains_reproducible`.

**Status:** PROVEN.

### 10. Realized outcome and decision-quality evaluation remain separate.

**Context and proof:** the same positive recommendation is evaluated
before and after final scores. Final outcome is produced by
`grade_candidate_outcome`, not inserted as an ungrounded literal. Both
decision-quality artifacts are persisted.

**Exact artifact or response:** pre-outcome evaluation
`181cbfe8fbba8cc324d208060630c20ceed9dc4b569c83a07fabd6607e491ed3`,
post-outcome evaluation
`9b720ef4974703c6931eac0d5ad27db7e24155a1e9861dd9f5b089a32e3d6aae`,
and shared result
`d12141db771d9a434d1cee39b2ecbbe71bbfb7bfed3ffd1a89a803fc1c47c698`.

**Evidence cutoff:** T1 decision evidence, with distinct pre-outcome and
post-outcome evaluation contexts.

**Method or policy identity:** decision-quality schema `1`; active policy
`1587d4aae00bc678bfdf28ac6fe89f0021fd8e9e1d0c2c4c95277e75a2ced351`;
shared outcome grader.

**Expected and observed state:** checks remain equal and decision status
remains `consistent`; realized outcome changes from `unavailable` to
`win`; evaluation IDs remain distinct.

**Temporal meaning:** pre-outcome and post-outcome.

**Verification test:**
`test_realized_outcome_remains_separate_from_decision_quality`.

**Status:** PROVEN.

## Detailed verification relationships

### Source observation ownership

The source ledger is written and read through:

```text
gridiron_edge.ingest.odds.store.append_to_odds_ledger
gridiron_edge.ingest.odds.store.load_odds_ledger
gridiron_edge.ingest.odds.as_known.as_known_at
```

The test verifies that appending T2 retains T1 and that applying the T1
cutoff to the complete ledger still returns only T1.

### Product and forecast ownership

The selected weekly product is written, explicitly selected, and loaded
through:

```text
gridiron_edge.models.game_prediction.weekly_product_store.write_weekly_product
gridiron_edge.models.game_prediction.weekly_product_store.select_current_weekly_product
gridiron_edge.models.game_prediction.weekly_product_store.load_current_weekly_product
```

Forecast events are written and loaded through:

```text
gridiron_edge.evaluation.forecast_store.write_forecast_events
gridiron_edge.evaluation.forecast_store.load_forecast_events
```

The product records:

```text
model_spread = -3.0
spread_uncertainty = 13.5
spread_source_event_id = spread-vertical-slice-forecast
spread_model_name = win_prob
spread_model_type = elo
```

### Candidate issuance ownership

Candidate outcomes are computed through:

```text
gridiron_edge.market.candidate_issuance.issue_pregame_candidates
```

They are persisted and read through:

```text
gridiron_edge.market.candidate_issuance_store.write_candidate_issuance
gridiron_edge.market.candidate_issuance_store.read_candidate_issuance
```

The T1 and T2 issuance paths differ because `evaluated_at` participates
in the deterministic issuance identity.

The T2 issuance contains both visible observations. The test selects the
latest row only inside the declared business comparison scope. It does
not infer replacement from tuple position, row count, file recency, or
modification time.

### Recommendation ownership

Policies are persisted and read through:

```text
gridiron_edge.market.recommendation_policy_store.write_recommendation_policy
gridiron_edge.market.recommendation_policy_store.read_recommendation_policy
```

Recommendation evaluations are produced through:

```text
gridiron_edge.market.recommended_bet_result.evaluate_recommendation_issuance
```

They are persisted and read through:

```text
gridiron_edge.market.recommended_bet_result_store.write_recommended_bet_evaluation
gridiron_edge.market.recommended_bet_result_store.read_recommended_bet_evaluation
```

The positive and zero-allocation contexts use the same active policy and
exact T1 candidate. Their allocation evidence differs explicitly.

The zero-allocation correlation evidence includes the exact candidate
reference in `member_reference_ids`. It has `existing_stake=49.0`, which
leaves constrained capacity below the governed five-dollar minimum. The
zero is therefore a completed portfolio-policy outcome rather than an
ineligible recommendation, zero bankroll, stale quote, inactive policy,
or missing evidence.

### Decision-quality ownership

Decision quality is computed through:

```text
gridiron_edge.market.decision_quality.evaluate_decision_quality
```

It is persisted and read through:

```text
gridiron_edge.market.decision_quality_store.write_decision_quality_evaluation
gridiron_edge.market.decision_quality_store.read_decision_quality_evaluation
```

The evaluator validates:

```text
result_integrity
recommendation_evaluation_reference
policy_reference
candidate_reference
allocation_recomputation
```

The pre-outcome and post-outcome artifacts have identical check results
and the same `consistent` decision status. Realized outcome participates
in the decision-quality artifact identity but does not determine the
decision-quality conclusion.

## Canonical path verification

The additional integration test:

```text
test_canonical_paths_agree_with_embedded_identities
```

verifies that generated artifact paths agree with the paths returned by
their public path owners for:

- the weekly product;
- current weekly-product selection;
- T1 candidate issuance;
- active recommendation policy;
- positive recommendation evaluation and result;
- zero-allocation recommendation evaluation and result;
- pre-outcome decision-quality evaluation;
- post-outcome decision-quality evaluation.

Strict readers also verify embedded identities against canonical paths.

## Reproduction commands

Run the complete Workstream 3 proof:

```bash
uv run pytest \
  tests/integration/market/test_spread_vertical_slice_proof.py \
  -v
```

Expected:

```text
11 passed
```

The file contains ten obligation tests plus one cross-cutting canonical
path and embedded-identity test.

Run the established Python quality gates:

```bash
uv run ruff check . --fix \
&& uvx pyrefly check \
&& uv run pytest -m "unit and not slow"
```

Generate an inspectable proof repository without using local runtime
artifacts:

```bash
rm -rf /tmp/gridiron-edge-spread-proof

uv run pytest \
  tests/integration/market/test_spread_vertical_slice_proof.py \
  --basetemp=/tmp/gridiron-edge-spread-proof \
  -q
```

The canonical-path test repository must contain:

- two candidate issuances;
- two recommendation policies;
- three recommendation evaluations;
- three recommendation results;
- two decision-quality evaluations;
- one selected immutable weekly product;
- the referenced forecast event;
- both T1 and T2 source observations.

## Scope and limitations

This matrix proves the ten first-slice obligations for one controlled
game-spread subject. It does not claim:

- generalized downstream discovery across all artifact families;
- a global reverse-impact index;
- universal validity or lifecycle machinery;
- an advanced portfolio correlation model;
- complete transparency across every application surface;
- proof for moneyline, total, props, or other market families.

Obligation 8 is satisfied through a concrete second validity case: the
later source observation changes the explicit row-owned candidate state
and reason for the same comparison scope while the historical T1
artifact remains unchanged.

D36 remains deferred. This proof knows the exact bounded chain it
constructed and validates those relationships directly. It does not
implement general discovery of every arbitrary downstream dependent.

## Local runtime discovery note

Before selecting the controlled proof architecture, a local runtime
inspection found one strict-reader-valid schema-3 recommendation
evaluation containing 226 spread results. All had the state combination:

```text
unavailable
insufficient_evidence
not_evaluated
recommendation_ineligible
```

The corresponding spread policy reported required evidence unavailable,
including unavailable CLV and realized-return evidence and insufficient
observation-count and distinct-fetch-count cohorts.

Those runtime product, forecast, issuance, policy, evaluation, and result
artifacts were not Git-tracked. They are discovery evidence only and are
not dependencies of this durable proof.

## Exit determination

**PROVEN: all ten first-slice obligations are demonstrated end to end for
the canonical game-spread subject.**

The proof is reproducible from a clean checkout because it constructs its
repository through checked-in fixtures and production writers, readers,
validators, identity owners, cutoff semantics, and evaluators.

Workstream 3 satisfies its stated exit criterion, subject to the explicit
scope and deferred D36 limitation above.
