# PLAN.md — Active implementation unit

Exactly ONE active unit. Future units live in ROADMAP.md, not here.

### Unit — Identity-evolution contract for candidate references

#### Completed

Candidate-reference derivation (`candidate_issuance_row_id`) now has an
independently owned version, separate from `CandidateIssuance`'s own
artifact schema. Version 1 is the exact pre-unit algorithm, extracted
verbatim into `_candidate_issuance_row_id_v1` with no new field in its
hashed payload — the version selects which implementation runs, it is not
an input to that implementation's digest, so every reference explicitly
derived under v1 remains stable. A dedicated
`UnsupportedCandidateReferenceVersionError` (a `ValueError` subtype)
propagates directly, uncaught, when a recorded or requested version has no
known implementation, keeping that failure mode structurally distinct from
ordinary evidence-mismatch corruption.

`RecommendedBetResult`'s schema incremented from 1 to 2, adding
`candidate_reference_derivation_version` immediately after
`candidate_reference_id`. `build_recommended_bet_result` records the
current derivation version at construction time.
`validate_recommended_bet_result` dispatches re-derivation through the
*recorded* version, not a hardcoded current-version comparison.

The recommendation API's offer provenance
(`RecommendationOfferProvenanceResponse`) now exposes
`candidate_reference_derivation_version` alongside `issuance_id` and
`candidate_reference_id`, copied mechanically by the serializer with no
reinterpretation.

**Two full ChatGPT review rounds were required before closure.** The first
round confirmed the core design; the second round found the initial
closure diff incomplete and one test imprecisely scoped. Both rounds'
required corrections are reflected below — this record intentionally shows
that history rather than presenting only the final state.

**Gaps found and closed during the first review round (already in the
initial diff):**
1. Two docstring formatting violations (`D205`, pydocstyle).
2. The checked-in `api-schema.json` OpenAPI snapshot needed regeneration.
3. A frontend TypeScript fixture (`recommendationPresentation.test.ts`)
   constructed the old API shape and failed to compile — a genuine scope
   gap in the unit's original file list, caught by the frontend's own
   build gate, not anticipated in the original design.

**Gaps found and closed during the second review round (after the first
"complete" diff was submitted for review):**
4. `tests/unit/market/test_recommended_bet_result_store.py` was omitted
   from the original diff entirely, despite schema 2 changing the exact
   persisted shape and path namespace. Four tests added (see Tests,
   below).
5. No direct evidence had been produced that no test/fixture constructs
   `RecommendedBetResult` positionally (a real risk once a required field
   is inserted). Resolved by a repository-wide `rg` search: confirmed only
   two direct-constructor call sites exist, both inside
   `recommended_bet_result.py` itself (the builder and the keyword-based
   reflective decoder) — no fixture or test required any change.
6. The Python recommendation serializer had no direct runtime assertion
   that the new field is copied correctly — only the generated TypeScript
   contract and frontend fixture proved this. One assertion added to an
   existing serializer test.
7. A newly written evaluation-identity test
   (`test_evaluation_identity_changes_when_result_ids_change`) was found to
   duplicate existing coverage (`test_result_identity_is_deterministic_and_changes_with_decision_time`)
   without actually proving that the *derivation-version field specifically*
   propagates into evaluation identity, despite its name implying that. It
   was removed and replaced with a correctly-scoped, narrower test.
8. `api-schema.json`'s regenerated diff showed a missing trailing newline
   — confirmed corrected before final regeneration.
9. The dispatcher's docstring claimed "every existing reference remains
   stable when a new version is introduced" — too broad; narrowed to state
   the guarantee precisely (v1-derived references are stable because v1's
   implementation and payload are unchanged; a future version bump to the
   *default* would intentionally change what new default calls produce).
10. The invalid-version test parametrization
    (`[True, 0, -1]`) did not cover non-integer runtime values; expanded to
    `[True, 0, -1, 1.0, "1", None]`.

All existing schema-1 development artifacts under
`data/output/recommended_bet_results/schema=1/` were regenerated under
schema 2 via the real, verified production-chain command sequence (see
Tests, below) and the old `schema=1/` tree was deleted, per D31's locked
Option B policy.

#### Goal

Give candidate-reference derivation an independently owned version,
distinct from `CandidateIssuance`'s own artifact schema. Preserve the exact
existing algorithm and output as version 1. Persist the version used by
each recommended result under a new, incremented result schema. Dispatch
validation through that recorded, supported version. Distinguish an
unsupported/incompatible reference contract from ordinary supported-contract
evidence corruption, propagating the distinction as a typed exception.
Expose the derivation version in API offer provenance. Regenerate all
existing schema-1 development artifacts under the new schema.

#### Files Added/Removed/Changed

Changed:
- `src/gridiron_edge/market/candidate_issuance.py` — added
  `CANDIDATE_REFERENCE_DERIVATION_VERSION_V1`,
  `CURRENT_CANDIDATE_REFERENCE_DERIVATION_VERSION`,
  `UnsupportedCandidateReferenceVersionError`;
  `candidate_issuance_row_id` is now a version-dispatching wrapper (with a
  precisely-scoped docstring, corrected during review); the original
  implementation moved verbatim into `_candidate_issuance_row_id_v1`.
- `src/gridiron_edge/market/recommended_bet_result.py` —
  `RECOMMENDED_BET_RESULT_SCHEMA_VERSION` incremented 1 → 2;
  `RecommendedBetResult` gained `candidate_reference_derivation_version`;
  `build_recommended_bet_result` records the current version;
  `validate_recommended_bet_result` dispatches on the recorded version and
  lets the typed exception propagate uncaught.
- `src/gridiron_edge/api/schemas/recommendations.py` —
  `RecommendationOfferProvenanceResponse` gained
  `candidate_reference_derivation_version`.
- `src/gridiron_edge/api/serializers/recommendations.py` —
  `serialize_recommendation_result` copies the new field unchanged.
- `tests/unit/market/test_candidate_issuance.py` — added
  `test_default_version_matches_explicit_v1`,
  `test_v1_output_is_pinned_exactly` (pinned exact-output regression proof),
  `test_unrecognized_version_raises_unsupported_error`,
  `test_invalid_version_raises_unsupported_error` (parametrized over
  `[True, 0, -1, 1.0, "1", None]`).
- `tests/unit/market/test_recommended_bet_result.py` — added
  `test_new_result_records_current_derivation_version`,
  `test_derivation_version_field_participates_in_result_identity_only`
  (pure identity-function test; explicitly does not treat a version-tagged
  object as "a valid result" — a corrected replacement for an earlier,
  imprecisely-scoped draft removed during review),
  `test_unsupported_recorded_version_raises_distinct_from_corruption`,
  `test_supported_version_with_altered_evidence_still_raises_corruption_message`.
- `tests/unit/market/test_recommended_bet_result_store.py` — added
  `test_schema_2_result_round_trip_preserves_derivation_version`,
  `test_schema_2_evaluation_manifest_uses_schema_2_namespace`,
  `test_result_missing_derivation_version_field_is_rejected`,
  `test_schema_1_shaped_result_is_rejected_as_unsupported` — added during
  the second review round; the first closure diff omitted this file
  entirely despite the schema-2 path/shape change.
- `tests/unit/api/serializers/test_recommendation_serializers.py` — added
  an assertion inside the existing
  `test_serializer_preserves_persisted_result_without_recalculation`
  confirming `candidate_reference_derivation_version` is copied unchanged —
  added during the second review round; the first diff relied only on the
  generated TypeScript contract and frontend fixture as indirect proof.
- `frontend/src/components/recommendations/recommendationPresentation.test.ts` —
  added the new field to the offer-provenance test fixture.
- `api-schema.json` — regenerated via `gridiron api export-schema`;
  confirmed to end with a trailing newline (a missing-newline diff was
  flagged and corrected during the second review round).
- `frontend/src/api/schema.ts` — regenerated via `pnpm gen:api`.

Not changed (confirmed unnecessary — a real scope reduction, verified by
direct repository search, not assumed):
- `src/gridiron_edge/market/recommended_bet_result_store.py` — its
  reflection-based codec and schema-version-parameterized path builders
  absorb the new field and the `schema=2/` path automatically.
- `tests/fixtures/recommended_bet_results.py` — confirmed via
  `rg -n 'RecommendedBetResult\(' src tests frontend`: the only two direct
  constructor call sites are inside `recommended_bet_result.py` itself
  (the builder, and the keyword-based reflective decoder); no fixture or
  test constructs the dataclass positionally.
- `src/gridiron_edge/market/market_closeout.py`,
  `src/gridiron_edge/market/recommendation_policy.py`, and their tests —
  both call `candidate_issuance_row_id` at its default (current) version;
  zero source changes required; existing tests pass unchanged.

Removed:
- `data/output/recommended_bet_results/schema=1/` (real development-state
  artifacts, deleted after schema-2 regeneration was verified, per D31).

#### Tests

`uv run ruff check . --fix && uvx pyrefly check && uv run pytest -m "unit and not slow"`
passed; all tests green, including the four new store tests and the one
new serializer assertion added during the second review round.
`frontend`: `pnpm gen:api && pnpm build` passed (`tsc -b && vite build`
clean) after the fixture fix.

Real-data regeneration executed and verified end to end:
- `gridiron production-chain issue-candidates --season 2026-2027 --week 1
  --evaluated-at 2026-08-26T16:52:20.077Z --write` → 1,680 quote
  observations evaluated, 698 candidates issued (228 moneyline / 226 spread
  / 244 total), issuance persisted.
- `gridiron production-chain create-governance ... --write` → governance
  persisted.
- `gridiron production-chain derive-policy --issuance-id <issuance>
  --governance-id <governance> --created-at 2026-08-26T17:05:21.618Z
  --write` → policy persisted (schema 1, unaffected by this unit).
- `gridiron production-chain evaluate-recommendations --issuance-id
  <issuance> --policy-id <policy> --decision-at 2026-08-26T17:06:33.604Z
  --write` → 698 results + 1 evaluation manifest persisted under
  `schema=2/`; every one of the 698 results passed
  `validate_recommended_bet_result` with zero errors.
- `schema=2/results/` and `schema=2/evaluations/` confirmed populated by
  direct directory listing; `schema=1/` deleted.

**Honest caveat, recorded rather than glossed over:** all 698 regenerated
results came back `result_state=unavailable` (no historical outcome data
available to policy derivation in this environment). This confirms the
schema-2 migration and version-dispatch machinery are mechanically sound
end-to-end against real data volume, but does not itself exercise
`validate_recommended_bet_result`'s reference re-derivation against a
`RECOMMENDED`/`QUALIFIED` result under interesting conditions — that case
is covered directly by
`test_supported_version_with_altered_evidence_still_raises_corruption_message`
and `test_unsupported_recorded_version_raises_distinct_from_corruption`,
not by this particular regeneration run.

#### Acceptance

Every existing call site of `candidate_issuance_row_id`
(`market_closeout.py`, `recommendation_policy.py`) required zero source
changes and continues to pass its existing tests. The v1 algorithm is
proven byte-identical to its pre-unit output via a pinned regression test.
An unsupported or invalid version is structurally distinguishable from
ordinary content corruption, both in the raised exception type and in
dedicated tests — including the corrected, precisely-scoped identity test
and the second-round-added store-level schema-1-rejection test.
`RECOMMENDED_BET_RESULT_SCHEMA_VERSION == 2`; the store path is
`schema=2/`, directly verified by dedicated round-trip and namespace tests;
no migration decoder exists; real schema-1 artifacts were regenerated and
the old tree deleted. The recommendation API exposes the new field as
mechanically-projected, read-only provenance, verified through the Python
schema/serializer test suite (including the second-round-added direct
assertion), the frontend TypeScript build, and the regenerated OpenAPI
contract with its trailing newline confirmed present. No test or fixture
constructs `RecommendedBetResult` positionally, confirmed by repository-wide
search. `DECISIONS.md` D31 records this decision, its evidence basis, its
rejected alternatives, and its revisit triggers. The unit is implemented,
reviewed across two full ChatGPT ratification rounds with all required
corrections from both rounds applied, validated against both synthetic
tests and a real, verified production-chain data regeneration, and closed.
