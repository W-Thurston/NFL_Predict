# PLAN.md — Active implementation unit

Exactly ONE active unit. Future units live in ROADMAP.md, not here.

### Unit — Attribution-operation ownership

#### Completed

Seven reference-attribution operations are formally named and classified
into two families (canonical authentication vs. structural attribution),
confirmed from full current source of `market_closeout.py`,
`bet_reference_matching.py`, and `market_family_evaluation.py`. This
corrects Boundary 4's original six-operation count: `bet_reference_matching.py::match_bet_references`
is confirmed as a genuine seventh operation, not a duplicate of canonical
reference production — it validates a recorded wager's self-reported
reference against real observed quote history and has real failure modes
operation 1 structurally cannot have.

`market_family_evaluation.py::_closeout_matches`'s confirmed integrity
ambiguity is corrected: it previously checked an issuance-ID prefix plus
11 individually-compared fields (every field the `candidate_issuance_row_id`
v1 hash covers) without ever re-deriving and comparing the digest suffix
itself — meaning a reference with the correct fields but a forged suffix
would incorrectly match. It now re-derives the canonical reference
directly via `candidate_issuance_row_id` and separately checks `kickoff`
(the one field outside the hash payload), per Option A (validate-then-
compare) of the three options the locked ROADMAP text named.

`_history_matches` and `_wager_return_for_row` are confirmed correct as-is
and unchanged: both perform group/aggregate-style structural attribution,
not exact 1:1 digest-backed reference resolution, and have no analogous
gap to close.

Recorded as `DECISIONS.md` D37.

#### Goal

Separate and formally name the reference-attribution operations as
explicit, non-interchangeable capabilities, satisfying the "attribution
capability" item Unit 5 deliberately deferred. Correct
`_closeout_matches`'s confirmed integrity ambiguity.

#### Files Changed

- `src/gridiron_edge/market/market_family_evaluation.py` —
  `_closeout_matches` rewritten to re-derive the canonical reference via
  `candidate_issuance_row_id` instead of checking a prefix plus 11
  redundant fields; `kickoff` retained as a separate explicit check.
  One new import (`candidate_issuance_row_id`).
- `tests/unit/market/test_market_family_evaluation.py` — `_closeout`
  fixture helper now constructs genuine digest-backed references instead
  of a fake test suffix; one new test
  (`test_closeout_with_mismatched_digest_but_matching_fields_does_not_match`)
  proves the specific defect is closed. One new import
  (`candidate_issuance_row_id`).
- `DECISIONS.md` — added D37.

#### Files Confirmed Correct, Not Changed

- `src/gridiron_edge/market/market_closeout.py` (operation 1 — read in
  full, confirmed unconditional/correct as-is).
- `src/gridiron_edge/market/bet_reference_matching.py` (operation 5 —
  read in full, formally named as the seventh operation, no defect found).
- `src/gridiron_edge/market/recommendation_policy.py` (operation 2),
  `src/gridiron_edge/market/recommended_bet_result.py` (operation 3) —
  both confirmed unchanged from Boundary 4/Unit 4's characterization.
- `_history_matches`, `_wager_return_for_row` within
  `market_family_evaluation.py` (operations 6, 7) — confirmed correctly
  scoped as structural, non-authentication attribution; no digest
  mechanism added by mistaken analogy to operation 4's fix.

#### Tests

Full diff applied and confirmed by the owner via `git diff`. Pending: full
quality gate run
(`uv run ruff check . --fix && uvx pyrefly check && uv run pytest -m "unit and not slow"`)
to confirm all pre-existing `test_market_family_evaluation.py` tests pass
unchanged (expected, since none asserted on the fake suffix's content) and
the one new race/forgery test passes.

#### Acceptance

Seven operations are named, classified, and documented in D37. The
`_closeout_matches` fix is the smallest correct change: it reuses the
function's existing parameters (`issuance`, `row`) rather than requiring a
signature change, and replaces field-by-field redundancy with a single
authoritative digest re-derivation plus one explicit non-hash field check.
`_history_matches` and `_wager_return_for_row` are confirmed correct by
their structural difference from operation 4, not merely left unexamined.
The seventh operation (`match_bet_references`) is confirmed to have no
equivalent gap, closing this unit's own open design question from before
implementation. Pending final gate confirmation before full closure.
