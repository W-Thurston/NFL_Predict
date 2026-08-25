# PLAN.md — Active implementation unit

Exactly ONE active unit. Future units live in ROADMAP.md, not here.

### Unit — Immutable artifact publication hardening

#### Completed

Five immutable JSON persistence modules
(`recommendation_policy_store.py`, `recommendation_governance_store.py`,
`production_chain_preflight_store.py`, `recommended_bet_result_store.py`,
`candidate_issuance_store.py`) now publish through the same create-only,
atomically visible mechanism already proven in `collection_receipt_store.py`
(WS1 Unit 4): serialize completely to a colocated temporary file, then
attempt `os.link(temporary, destination)`. On success, publication is
complete and race-safe. On `FileExistsError` — now the authoritative race
signal rather than a best-effort pre-check — each store inspects the
existing destination using its own pre-existing replay contract (byte
equality for governance/preflight/recommended-result; reconstructed-object
equality for policy/candidate issuance) and either accepts an identical
replay or raises its own existing conflict error. No store's replay
semantics, error messages, reader behavior, or return values changed —
only the publish primitive did.

`collection_plan_store.py` was investigated and excluded from this unit
entirely. Source inspection of both its writers
(`select_current_collection_plan` → `current.json`;
`write_collection_plan` → `season=X/week=NN.json`), their CLI callers
(`cli/ingest.py`), and their existing tests established that both are
intentionally replaceable scoped state — a mutable current-selection
pointer and a season/week-scoped artifact addressed by scope rather than
content identity, with no test anywhere asserting create-once, exact-replay,
or conflicting-replay semantics. This corrects a Boundary 8 inspection
overclassification (both writers had been listed among the "overwrite-
capable" defects); no runtime behavior in that module changed.

#### Goal

Eliminate overwrite-capable publication and partial-final-file exposure
across the affected immutable JSON artifact writers, while preserving each
store's existing path, schema, canonical serialization, replay-equality, and
conflicting-replay behavior.

#### Files Added/Removed/Changed

Added: None.

Changed:
- `src/gridiron_edge/market/recommendation_policy_store.py` — publish step
  changed from `temporary.replace(path)` to `os.link(temporary, path)`; the
  existing `except FileExistsError` handler (previously unreachable dead
  code, since `.replace()` never raised it) is now the live, correct race
  handler.
- `src/gridiron_edge/market/recommendation_governance_store.py` — restructured
  from an `if path.exists(): ... else: temporary.replace(path)` shape to
  `try: os.link(...) except FileExistsError: ...`, preserving byte-comparison
  replay equality.
- `src/gridiron_edge/market/production_chain_preflight_store.py` — same
  restructuring, same byte-comparison preservation.
- `src/gridiron_edge/market/recommended_bet_result_store.py` — the shared
  `_immutable_write` helper (serving both `write_recommended_bet_result` and
  `write_recommended_bet_evaluation`) restructured identically; both public
  write paths hardened through one change.
- `src/gridiron_edge/market/candidate_issuance_store.py` — a new colocated
  temporary-file stage was introduced (previously wrote directly into the
  final path via `open("x")`, which was create-only but not atomic); now
  serializes to a temp file first, then links, closing the
  partial-final-file exposure gap.
- `tests/unit/market/test_recommendation_policy_store.py`,
  `test_recommendation_governance_store.py`,
  `test_production_chain_preflight_store.py`,
  `test_recommended_bet_result_store.py`,
  `test_candidate_issuance_store.py` — each gained a destination-race
  conflicting-content test, a destination-race identical-content test, and a
  pre-publication-failure cleanup test; candidate issuance additionally
  gained a temporary-serialization-failure test (the one store where
  serialization and publication were previously combined); the
  recommended-result module's tests separately exercise the race on both the
  result and the evaluation-manifest write paths, since both flow through
  the shared helper.

Removed: None (no store's dead code beyond the now-load-bearing exception
handler in `recommendation_policy_store.py`, which is retained and now
reachable, not removed).

#### Tests

`uv run ruff check . --fix && uvx pyrefly check && uv run pytest -m "unit and not slow"`
passed; all tests green. Race-specific tests (destination-race,
pre-publication-failure, and — for candidate issuance —
temporary-serialization-failure) pass alongside every store's pre-existing
round-trip, idempotent-replay, and conflicting-replay tests, unchanged in
observable behavior.

#### Acceptance

Every write path in every scoped module was classified by persistence
semantics before modification, per the locked design question; only
write paths confirmed immutable and identity-addressed were hardened.
`collection_plan_store.py`'s two writers were confirmed, from real callers
and tests, to be intentionally replaceable scoped state and were excluded
without any behavior change. The five hardened stores now use
`os.link`-based create-only, atomically visible publication, matching the
proven WS1 Unit 4 template, with `FileExistsError` as the authoritative race
signal rather than a `path.exists()` pre-check. Each store's own replay-
equality contract (byte or object) is preserved and independently tested.
`collection_receipt_store.py` remains unchanged as the verified reference.
The unit is implemented, reviewed across two full ChatGPT ratification
rounds, validated, and ready for downstream use.
