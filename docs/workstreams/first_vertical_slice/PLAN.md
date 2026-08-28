## PLAN.md — Active implementation unit (Workstream 3)

Exactly ONE active unit. Future units live in ROADMAP.md, not here.

### Unit — Spread-slice later-evidence reproducibility and recomputation proof

#### Completed

Proved, using only existing code (zero new domain logic, zero new
persisted artifact types), all four clauses of the Goal against a real,
controlled two-quote scenario: T1 (home spread -1.0 at -110, a confirmed
real positive-EV candidate) and T2 (same comparison scope, later, home
spread -9.5 at -110, a confirmed real negative-EV non-candidate). The
controlled lines were selected from the evaluator's actual
spread-probability and expected-value behavior and placed comfortably on
opposite sides of the zero-EV boundary. The tests verify the resulting
signs and domain states through the real evaluator rather than
duplicating its calculation.

Implemented as `tests/integration/market/test_spread_later_evidence_reproducibility.py`,
eight tests, all passing:
1. `as_known_at(T1)` excludes the T2 quote.
2. `as_known_at(T2)` includes both quotes.
3. Repeating the T1 evaluation chain against identical inputs reproduces
   an object-equal `CandidateIssuance`; also confirms T1 is a genuine,
   real positive-EV candidate.
4. The T2 evaluation produces a distinct `issuance_id`; because
   `CandidateIssuance` evaluates every supplied visible row (confirmed
   exhaustive, not selective, from source), the T2 issuance contains
   both the original and the later row, each with its own distinct
   `candidate_issuance_row_id`.
5. Running the T2 evaluation does not mutate the T1 in-memory artifact.
6. **The Goal's "affected downstream artifact receives an explicit,
   artifact-owned status or recomputation outcome" clause**, satisfied
   by the existing, already-owned `CandidateIssuanceRow.state`/`.reason`/
   `.expected_value` contract: T1 evaluates independently to
   `CANDIDATE`/`POSITIVE_EXPECTED_VALUE`; among rows sharing T1's exact
   business comparison scope, the maximum-`fetched_at` (latest visible)
   row at T2 independently evaluates to `NOT_CANDIDATE`/
   `EXPECTED_VALUE_NOT_POSITIVE`. No new artifact was required.
7. The T1-vs-T2 difference is attributable to exact, named fields (line,
   fetch time, sportsbook-update time) via business comparison-scope
   matching, not tuple position.
8. The real persisted-bytes proof, using the actual
   `candidate_issuance_store.py` API (`write_candidate_issuance`,
   `read_candidate_issuance`): T1's persisted bytes are confirmed
   unchanged after the T2 issuance is separately written and persisted
   under its own distinct path.

**Design corrections made before implementation, preserved as real
findings, not smoothed over:** an initial hypothesis that distinct
artifact identity alone would satisfy "artifact-owned status" was
rejected — identity is a property, not a claim about relative standing,
and D35 explicitly distinguishes decision-outcome enums from artifact-
validity lifecycle semantics. A second hypothesis, that
`CandidateIssuance` behaves as a "current quote selector," was also
rejected from source: it is confirmed exhaustive over every supplied
visible row. A repository-wide search
(`rg "superseded|invalidat|supersede|is_current|lifecycle_status"`)
confirmed zero existing lifecycle mechanisms anywhere in the codebase.
`market_closeout.py` was considered as the artifact-owned recomputation
owner and correctly ruled out on closer inspection — its real,
already-owned recomputation outcome (`CandidateIssuanceRow.state`/
`.reason`) was the correct, simpler answer, requiring no closeout
machinery at all.

**A real bug caught only by the first actual test run, not by any
review round:** the initial T1 fixture (home -3.5 at -110) was, in fact,
already negative EV against the shared `model_spread=-3.0` fixture — an
inherited-fixture assumption that was never actually validated against
this specific market scenario. The real assertion failure was the only
thing that caught this; five prior tests passed coincidentally because
none of them asserted the candidate state directly. The corrected T1/T2
values were verified through the real evaluator, not re-guessed.

#### Goal (verbatim from ROADMAP.md)

Using one real or controlled later quote, demonstrate: the original
as-known decision remains reproducible; the later quote changes the
latest-current interpretation; affected downstream artifacts receive an
explicit, artifact-owned status or recomputation outcome; the difference
can be explained without rewriting the original.

#### Files Changed
- `tests/integration/market/test_spread_later_evidence_reproducibility.py` —
  new.

#### Tests
Focused integration proof:
`uv run pytest tests/integration/market/test_spread_later_evidence_reproducibility.py -v`
— 8 passed.

Full backend quality gates:
`uv run ruff check . --fix && uvx pyrefly check && uv run pytest -m "unit and not slow"`
— green.

#### Acceptance
All four Goal clauses are proven using exclusively existing, already-
shipped code — `as_known_at`, `issue_pregame_candidates`,
`candidate_issuance_row_id`, `CandidateIssuanceRow.state`/`.reason`/
`.expected_value`, and `candidate_issuance_store.py`. No lifecycle field,
validity enum, or comparison artifact was added to any existing
immutable artifact. The T1 decision remains historically valid for its
own cutoff and is never marked invalid — at the later cutoff, the latest
newly visible observation within the same declared comparison scope
receives a different persisted evaluation outcome; this is a separate,
independently-evaluated, real, changed downstream outcome, not a
mutation or invalidation of the original. D35 remains correctly open:
this scenario never required distinguishing "superseded" from
"corrupt," since the T1 artifact was never unsupported or corrupt — it
was, and remains, correct for its own cutoff. D36 remains correctly
deferred: this proof compares two explicitly supplied artifacts; it
creates no requirement for arbitrary forward discovery of downstream
consumers. The unit is implemented, its design corrected across one
review round before implementation and one real defect corrected after
the first test run, verified against real, passing focused and full
quality-gate execution, and closed.
