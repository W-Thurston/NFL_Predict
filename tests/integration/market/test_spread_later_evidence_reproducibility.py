# tests/integration/market/test_spread_supersession_reproducibility.py
"""Integration proof for reproducible spread evidence and changed
per-observation recomputation outcomes."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
from pandas import DataFrame

from gridiron_edge.evaluation.forecast_store import FORECAST_EVENT_COLUMNS
from gridiron_edge.ingest.odds.as_known import as_known_at
from gridiron_edge.ingest.odds.store import QUOTE_COLUMNS
from gridiron_edge.market.candidate_issuance import (
    CandidateIssuanceReason,
    CandidateIssuanceRow,
    CandidateIssuanceState,
    candidate_issuance_row_id,
    issue_pregame_candidates,
)
from gridiron_edge.market.candidate_issuance_store import (
    read_candidate_issuance,
    write_candidate_issuance,
)

# --- Real scenario -------------------------------------------------------
# model_spread=-3.0, spread_uncertainty=13.5 (from the shared _product()
# fixture). home_cover_prob(line) = norm.cdf((line - model_spread) / uncertainty),
# reverse-engineered and confirmed directly from this evaluator's own real
# output -- not assumed.
#
# T1: home -1.0 at -110  -> confirmed real cover probability ~0.559,
#     EV ~ +0.067 (a genuine, confirmed positive-EV candidate).
# T2: same comparison scope, later, home -9.5 at -110 -> confirmed real
#     cover probability ~0.315, EV ~ -0.399 (a genuine, confirmed
#     negative-EV non-candidate). The shift is large on both line and
#     magnitude of EV, deliberately far from any break-even boundary.

GAME_ID = "2026_01_KC_LAC"
KICKOFF = datetime(2026, 9, 10, 0, 20, tzinfo=UTC)
PRODUCT_GENERATED_AT = datetime(2026, 9, 1, 11, tzinfo=UTC)

T1 = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
T2 = datetime(2026, 9, 2, 9, 0, tzinfo=UTC)  # T2 > T1, both strictly before kickoff

T1_FETCHED_AT = datetime(2026, 9, 1, 11, 30, tzinfo=UTC)
T1_UPDATED_AT = datetime(2026, 9, 1, 11, 29, tzinfo=UTC)
T2_FETCHED_AT = datetime(2026, 9, 2, 8, 30, tzinfo=UTC)
T2_UPDATED_AT = datetime(2026, 9, 2, 8, 29, tzinfo=UTC)


def _product(**overrides: object) -> DataFrame:
    """One fully validated stored weekly-product row, matching the real
    confirmed contract from test_candidate_issuance_evaluation.py."""
    row: dict[str, object] = {
        "product_schema_version": 1,
        "product_id": "product-1",
        "product_run_id": "run-1",
        "product_generated_at": PRODUCT_GENERATED_AT,
        "season": "2026-2027",
        "week": 1,
        "game_id": GAME_ID,
        "away_team": "Kansas City Chiefs",
        "home_team": "Los Angeles Chargers",
        "neutral_site": False,
        "win_status": "available",
        "win_selection_status": "selected",
        "away_win_prob": 0.40,
        "home_win_prob": 0.60,
        "win_model_name": "win_prob",
        "win_model_type": "elo",
        "win_event_id": "win-1",
        "win_run_id": "run-1",
        "win_generated_at": PRODUCT_GENERATED_AT,
        "win_role": "live",
        "spread_status": "available",
        "model_spread": -3.0,
        "spread_uncertainty": 13.5,
        "spread_source_event_id": "win-1",
        "spread_model_name": "win_prob",
        "spread_model_type": "elo",
        "spread_calibration_key": "win_prob_elo",
        "spread_calibration_updated_at": "2026-07-30T12:00:00+00:00",
        "total_status": "available",
        "model_total": 44.0,
        "total_uncertainty": 12.8,
        "total_model_name": "total",
        "total_model_type": "xgboost",
        "total_event_id": "total-1",
        "total_run_id": "run-1",
        "total_generated_at": PRODUCT_GENERATED_AT,
        "total_role": "live",
        "total_selection_status": "selected",
        "total_uncertainty_trained_at": "2026-07-01T14:20:00",
        "projected_score_status": "available",
        "projected_home_score": 23.5,
        "projected_away_score": 20.5,
    }
    row.update(overrides)
    return DataFrame([row])


def _event(
    *,
    event_id: str,
    model_name: str,
    model_type: str,
    home_win_prob: float | None = None,
    model_total: float | None = None,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "run_id": "run-1",
        "role": "live",
        "generated_at": PRODUCT_GENERATED_AT,
        "season": "2026-2027",
        "week": 1,
        "game_id": GAME_ID,
        "model_name": model_name,
        "model_type": model_type,
        "game_date": "2026-09-10",
        "away_team": "Kansas City Chiefs",
        "home_team": "Los Angeles Chargers",
        "away_elo": None,
        "home_elo": None,
        "away_win_prob": (None if home_win_prob is None else 1.0 - home_win_prob),
        "home_win_prob": home_win_prob,
        "model_spread": None,
        "model_total": model_total,
        "projected_home_score": None,
        "projected_away_score": None,
        "margin_std": None,
        "win_prob_lo": None,
        "win_prob_hi": None,
        "confidence_tier": None,
    }


def _events() -> DataFrame:
    return DataFrame(
        [
            _event(event_id="win-1", model_name="win_prob", model_type="elo", home_win_prob=0.60),
            _event(event_id="total-1", model_name="total", model_type="xgboost", model_total=44.0),
        ],
        columns=FORECAST_EVENT_COLUMNS,
    )


def _spread_quote(
    *,
    fetched_at: datetime,
    sportsbook_updated_at: datetime,
    line: float,
    odds: float,
) -> dict[str, object]:
    """One canonical spread quote observation at the shared comparison
    scope (provider/provider_event_id/sportsbook/game/market/side)."""
    return {
        "fetched_at": fetched_at,
        "provider": "the_odds_api",
        "provider_event_id": "provider-event-1",
        "sportsbook": "draftkings",
        "sportsbook_updated_at": sportsbook_updated_at,
        "commence_time": KICKOFF,
        "is_live": False,
        "season": "2026-2027",
        "week": 1,
        "game_id": GAME_ID,
        "game_date": "2026-09-10",
        "away_team": "Kansas City Chiefs",
        "home_team": "Los Angeles Chargers",
        "market": "spread",
        "side": "home",
        "odds": odds,
        "line": line,
    }


def _ledger_with_both_quotes() -> DataFrame:
    """The full quote ledger: T1's original spread quote (confirmed real
    positive EV) plus T2's later quote for the identical comparison scope
    (confirmed real negative EV) -- both values derived from this
    evaluator's own reverse-engineered, confirmed cover-probability
    relationship, not estimated intuitively."""
    rows = [
        _spread_quote(
            fetched_at=T1_FETCHED_AT,
            sportsbook_updated_at=T1_UPDATED_AT,
            line=-1.0,
            odds=-110.0,
        ),
        _spread_quote(
            fetched_at=T2_FETCHED_AT,
            sportsbook_updated_at=T2_UPDATED_AT,
            line=-9.5,
            odds=-110.0,
        ),
    ]
    return DataFrame(rows, columns=list(QUOTE_COLUMNS))


def _comparison_scope_key(
    row: CandidateIssuanceRow,
) -> tuple[str, str | None, str | None, str, str, str]:
    """The declared business comparison scope for matching rows across two
    issuances -- never tuple position or an assumed 1:1 replacement."""
    return (row.provider, row.provider_event_id, row.sportsbook, row.game_id, row.market, row.side)


def _issue_at(ledger: DataFrame, cutoff: datetime, evaluated_at: datetime):
    return issue_pregame_candidates(
        product=_product(),
        forecast_events=_events(),
        quotes=as_known_at(ledger, cutoff),
        evaluated_at=evaluated_at,
    )


def _latest_row_in_scope(
    rows: tuple[CandidateIssuanceRow, ...],
    scope: tuple[str, str | None, str | None, str, str, str],
) -> CandidateIssuanceRow:
    """Return the maximum-fetched_at row within the declared business
    comparison scope -- proves T2 is the latest visible observation in
    scope, not merely a row that happens to share fetched_at with T2."""
    matching = tuple(row for row in rows if _comparison_scope_key(row) == scope)
    assert matching
    return max(matching, key=lambda row: row.fetched_at)


# --- Proof obligations ---------------------------------------------------


def test_as_known_at_t1_excludes_the_later_quote() -> None:
    ledger = _ledger_with_both_quotes()
    visible_t1 = as_known_at(ledger, T1)
    assert len(visible_t1) == 1
    assert visible_t1.iloc[0]["fetched_at"] == pd.Timestamp(T1_FETCHED_AT)
    assert visible_t1.iloc[0]["line"] == -1.0


def test_as_known_at_t2_includes_both_quotes() -> None:
    ledger = _ledger_with_both_quotes()
    visible_t2 = as_known_at(ledger, T2)
    assert len(visible_t2) == 2
    assert set(visible_t2["line"]) == {-1.0, -9.5}


def test_original_decision_is_reproducible_at_t1() -> None:
    """The central reproducibility proof: re-running the T1 evaluation
    chain against identical inputs produces an object-equal
    CandidateIssuance, and confirms T1 is a genuine, real positive-EV
    candidate -- the confirmed baseline this unit's changed-outcome proof
    depends on."""
    ledger = _ledger_with_both_quotes()

    issuance_t1_first = _issue_at(ledger, T1, T1)
    issuance_t1_second = _issue_at(ledger, T1, T1)

    assert issuance_t1_first == issuance_t1_second
    assert issuance_t1_first.issuance_id == issuance_t1_second.issuance_id
    assert len(issuance_t1_first.rows) == 1

    t1_row = issuance_t1_first.rows[0]
    assert t1_row.line == -1.0
    assert t1_row.state is CandidateIssuanceState.CANDIDATE
    assert t1_row.reason is CandidateIssuanceReason.POSITIVE_EXPECTED_VALUE
    assert t1_row.expected_value is not None
    assert t1_row.expected_value > 0.0


def test_t2_evaluation_is_a_separately_identified_artifact() -> None:
    """A later evaluation at T2 produces a distinct issuance_id (evaluated_at
    is part of candidate_issuance_id) and, because the T2 issuance sees a
    changed-terms quote for the same comparison scope, a distinct row
    reference for that row as well."""
    ledger = _ledger_with_both_quotes()

    issuance_t1 = _issue_at(ledger, T1, T1)
    issuance_t2 = _issue_at(ledger, T2, T2)

    assert issuance_t1.issuance_id != issuance_t2.issuance_id

    # CandidateIssuance is exhaustive over every supplied visible row --
    # the T2 issuance contains BOTH the original and the later quote row.
    assert len(issuance_t1.rows) == 1
    assert len(issuance_t2.rows) == 2

    t1_reference = candidate_issuance_row_id(issuance_t1.issuance_id, issuance_t1.rows[0])
    t2_original_row = next(r for r in issuance_t2.rows if r.line == -1.0)
    t2_later_row = next(r for r in issuance_t2.rows if r.line == -9.5)

    assert _comparison_scope_key(t2_original_row) == _comparison_scope_key(t2_later_row)

    t2_original_reference = candidate_issuance_row_id(issuance_t2.issuance_id, t2_original_row)
    t2_later_reference = candidate_issuance_row_id(issuance_t2.issuance_id, t2_later_row)

    assert t1_reference != t2_original_reference
    assert t2_original_reference != t2_later_reference
    assert t1_reference != t2_later_reference


def test_original_artifact_is_unchanged_after_t2_evaluation_runs() -> None:
    """Running the T2 evaluation must never mutate the T1 artifact -- both
    remain independently valid for their own cutoffs."""
    ledger = _ledger_with_both_quotes()

    issuance_t1_before = _issue_at(ledger, T1, T1)
    _issue_at(ledger, T2, T2)  # run T2, discard -- must not affect T1
    issuance_t1_after = _issue_at(ledger, T1, T1)

    assert issuance_t1_before == issuance_t1_after
    assert issuance_t1_before.issuance_id == issuance_t1_after.issuance_id


def test_later_quote_produces_explicit_changed_candidate_outcome() -> None:
    """The Goal's 'affected downstream artifact receives an explicit,
    artifact-owned status or recomputation outcome' clause, satisfied by
    the existing CandidateIssuanceRow.state/.reason/.expected_value
    contract -- no new artifact required. Both T1's positive EV and T2's
    negative EV are confirmed real values from this evaluator's own
    output, not estimated."""
    ledger = _ledger_with_both_quotes()

    issuance_t1 = _issue_at(ledger, T1, T1)
    issuance_t2 = _issue_at(ledger, T2, T2)

    t1_row = issuance_t1.rows[0]
    t2_new_row = _latest_row_in_scope(issuance_t2.rows, _comparison_scope_key(t1_row))
    assert t2_new_row.fetched_at == T2_FETCHED_AT

    assert _comparison_scope_key(t1_row) == _comparison_scope_key(t2_new_row)

    # T1: real, confirmed positive-EV candidate.
    assert t1_row.state is CandidateIssuanceState.CANDIDATE
    assert t1_row.reason is CandidateIssuanceReason.POSITIVE_EXPECTED_VALUE
    assert t1_row.expected_value is not None
    assert t1_row.expected_value > 0.0

    # T2: the later quote's substantially worse line crosses the existing
    # candidate boundary -- a real, independently-evaluated, changed
    # artifact-owned outcome.
    assert t2_new_row.state is CandidateIssuanceState.NOT_CANDIDATE
    assert t2_new_row.reason is CandidateIssuanceReason.EXPECTED_VALUE_NOT_POSITIVE
    assert t2_new_row.expected_value is not None
    assert t2_new_row.expected_value <= 0.0

    # T1's own row remains exactly as it was -- no mutation.
    assert t1_row.state is CandidateIssuanceState.CANDIDATE
    assert t1_row.expected_value > 0.0


def test_difference_between_t1_and_t2_is_explained_by_comparison_scope_not_position() -> None:
    """The changed interpretation is attributable to exact, named evidence
    fields -- not inferred from row order or count alone."""
    ledger = _ledger_with_both_quotes()

    issuance_t1 = _issue_at(ledger, T1, T1)
    issuance_t2 = _issue_at(ledger, T2, T2)

    t1_row = issuance_t1.rows[0]
    t2_new_row = next(r for r in issuance_t2.rows if r.fetched_at == T2_FETCHED_AT)

    assert _comparison_scope_key(t1_row) == _comparison_scope_key(t2_new_row)

    changed_fields = {
        field: (getattr(t1_row, field), getattr(t2_new_row, field))
        for field in ("line", "american_price", "fetched_at", "sportsbook_updated_at")
        if getattr(t1_row, field) != getattr(t2_new_row, field)
    }
    assert changed_fields == {
        "line": (-1.0, -9.5),
        "fetched_at": (T1_FETCHED_AT, T2_FETCHED_AT),
        "sportsbook_updated_at": (T1_UPDATED_AT, T2_UPDATED_AT),
    }


def test_original_persisted_issuance_bytes_remain_unchanged(tmp_path) -> None:
    """The real store's write/read functions, not an in-memory stand-in --
    confirms persisted T1 bytes are untouched after the T2 issuance is
    separately written."""
    ledger = _ledger_with_both_quotes()
    issuance_t1 = _issue_at(ledger, T1, T1)
    issuance_t2 = _issue_at(ledger, T2, T2)

    path_t1 = write_candidate_issuance(issuance_t1, repo=tmp_path)
    bytes_before = path_t1.read_bytes()

    path_t2 = write_candidate_issuance(issuance_t2, repo=tmp_path)

    assert path_t1 != path_t2
    assert path_t1.read_bytes() == bytes_before
    assert read_candidate_issuance(path_t1) == issuance_t1
    assert read_candidate_issuance(path_t2) == issuance_t2
