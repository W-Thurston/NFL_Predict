"""Tests for foundational empirical market-family evaluation."""

from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
from pandas import DataFrame
import pytest

from gridiron_edge.market.candidate_issuance import (
    CANDIDATE_ISSUANCE_SCHEMA_VERSION,
    CandidateIssuance,
    CandidateIssuanceReason,
    CandidateIssuanceRow,
    CandidateIssuanceState,
    candidate_issuance_id,
    candidate_issuance_row_id,
)
from gridiron_edge.market.history_boundaries import (
    QuoteBoundaryStatus,
    QuoteHistoryBoundary,
    SelectedQuoteObservation,
)
from gridiron_edge.market.market_closeout import (
    MarketCloseoutReference,
    MarketCloseoutReferenceKind,
    MarketCloseoutResult,
    MarketCloseoutStatus,
    MarketClvKind,
)
from gridiron_edge.market.market_family_evaluation import (
    CandidateOutcome,
    EmpiricalMarketFamilyEvaluation,
    EvaluationEvidenceStatus,
    evaluate_market_families,
)

EVALUATED = datetime(2026, 9, 1, 12, tzinfo=UTC)
FETCHED = EVALUATED - timedelta(minutes=30)
KICKOFF = datetime(2026, 9, 10, 0, 20, tzinfo=UTC)


def _row(market: str, side: str, **overrides: object) -> CandidateIssuanceRow:
    line = None if market == "moneyline" else (3.5 if market == "spread" else 44.0)
    values = {
        "game_id": f"game-{market}",
        "market": market,
        "side": side,
        "provider": "the_odds_api",
        "provider_event_id": f"event-{market}",
        "sportsbook": "draftkings",
        "line": line,
        "american_price": -110,
        "fetched_at": FETCHED,
        "sportsbook_updated_at": FETCHED,
        "kickoff": KICKOFF,
        "is_live": False,
        "forecast_event_id": "forecast",
        "forecast_run_id": "run-1",
        "forecast_role": "live",
        "forecast_generated_at": FETCHED,
        "model_name": "model",
        "model_type": "type",
        "model_probability": 0.60,
        "expected_value": 0.10,
        "state": CandidateIssuanceState.CANDIDATE,
        "reason": CandidateIssuanceReason.POSITIVE_EXPECTED_VALUE,
    }
    values.update(overrides)
    return CandidateIssuanceRow(**values)  # pyrefly: ignore [bad-argument-type]


def _issuance(*rows: CandidateIssuanceRow, evaluated_at: datetime = EVALUATED) -> CandidateIssuance:
    issuance_id = candidate_issuance_id(
        product_id="product",
        product_run_id="run-1",
        season="2026-2027",
        week=1,
        evaluated_at=evaluated_at,
    )
    return CandidateIssuance(
        CANDIDATE_ISSUANCE_SCHEMA_VERSION,
        issuance_id,
        "product",
        "run-1",
        EVALUATED,
        "2026-2027",
        1,
        evaluated_at,
        tuple(rows),
    )


def _closeout(
    issuance: CandidateIssuance, row: CandidateIssuanceRow, **overrides: object
) -> MarketCloseoutResult:
    reference = MarketCloseoutReference(
        reference_id=candidate_issuance_row_id(issuance.issuance_id, row),
        reference_kind=MarketCloseoutReferenceKind.CANDIDATE_ISSUANCE,
        provider=row.provider,
        provider_event_id=row.provider_event_id,
        sportsbook=row.sportsbook,
        game_id=row.game_id,
        market=row.market,
        side=row.side,
        reference_fetched_at=row.fetched_at,
        reference_sportsbook_updated_at=row.sportsbook_updated_at,
        reference_kickoff=row.kickoff,
        reference_is_live=row.is_live,
        reference_american_price=row.american_price,
        reference_line=row.line,
    )
    values = {
        "reference": reference,
        "status": MarketCloseoutStatus.AVAILABLE,
        "closeout_fetched_at": KICKOFF - timedelta(minutes=10),
        "closeout_kickoff": KICKOFF,
        "closeout_is_live": False,
        "closeout_american_price": -120,
        "closeout_line": row.line,
        "clv_kind": MarketClvKind.MONEYLINE_PRICE
        if row.market == "moneyline"
        else (
            MarketClvKind.SPREAD_POINTS if row.market == "spread" else MarketClvKind.TOTAL_POINTS
        ),
        "clv": 0.05,
    }
    values.update(overrides)
    return MarketCloseoutResult(**values)  # pyrefly: ignore [bad-argument-type]


def _boundary(row: CandidateIssuanceRow, **overrides: object) -> QuoteHistoryBoundary:
    observed = SelectedQuoteObservation(FETCHED, FETCHED, KICKOFF, False, -110.0, row.line)
    values = {
        "status": QuoteBoundaryStatus.AVAILABLE,
        "provider": row.provider,
        "provider_event_id": row.provider_event_id,
        "sportsbook": row.sportsbook,
        "game_id": row.game_id,
        "market": row.market,
        "side": row.side,
        "observation_count": 4,
        "distinct_fetch_count": 3,
        "repeated_observation_evidence_available": True,
        "earliest_observed": observed,
        "latest_eligible_pregame": observed,
    }
    values.update(overrides)
    return QuoteHistoryBoundary(**values)  # pyrefly: ignore [bad-argument-type]


def _games(*rows: tuple[str, object, object]) -> DataFrame:
    return DataFrame(rows, columns=["GAME_ID", "AWAY_SCORE", "HOME_SCORE"])


def test_families_remain_structurally_separate() -> None:
    ml, spread, total = _row("moneyline", "home"), _row("spread", "away"), _row("total", "over")
    issuance = _issuance(ml, spread, total)
    result = evaluate_market_families(
        issuance=issuance,
        closeouts=[
            _closeout(issuance, ml),
            _closeout(issuance, spread),
            _closeout(issuance, total),
        ],
        games=_games((ml.game_id, 20, 27), (spread.game_id, 24, 20), (total.game_id, 24, 27)),
        history_boundaries=[_boundary(ml), _boundary(spread), _boundary(total)],
    )
    assert isinstance(result, EmpiricalMarketFamilyEvaluation)
    assert result.moneyline.coverage.candidate_count == 1
    assert result.spread.coverage.candidate_count == 1
    assert result.total.coverage.candidate_count == 1
    assert result.moneyline.reliability.evaluable_count == 1


def test_all_issuance_states_enter_coverage_but_only_candidates_enter_metrics() -> None:
    candidate = _row("moneyline", "home")
    not_candidate = replace(
        candidate,
        side="away",
        state=CandidateIssuanceState.NOT_CANDIDATE,
        reason=CandidateIssuanceReason.EXPECTED_VALUE_NOT_POSITIVE,
    )
    unavailable = replace(
        candidate,
        sportsbook="fanduel",
        state=CandidateIssuanceState.UNAVAILABLE,
        reason=CandidateIssuanceReason.MODEL_UNAVAILABLE,
        model_probability=None,
        expected_value=None,
    )
    issuance = _issuance(candidate, not_candidate, unavailable)
    report = evaluate_market_families(
        issuance=issuance,
        closeouts=[],
        games=_games((candidate.game_id, 20, 27)),
        history_boundaries=[],
    ).moneyline
    assert (
        report.coverage.issued_count,
        report.coverage.candidate_count,
        report.coverage.not_candidate_count,
        report.coverage.unavailable_count,
    ) == (3, 1, 1, 1)
    assert report.reliability.evaluable_count == 1


@pytest.mark.parametrize(
    ("market", "side", "line", "scores", "expected"),
    [
        ("moneyline", "home", None, (20, 27), CandidateOutcome.WIN),
        ("moneyline", "away", None, (27, 20), CandidateOutcome.WIN),
        ("spread", "home", -3.5, (20, 27), CandidateOutcome.WIN),
        ("spread", "away", 3.5, (24, 20), CandidateOutcome.WIN),
        ("total", "over", 44.0, (24, 27), CandidateOutcome.WIN),
        ("total", "under", 44.0, (20, 17), CandidateOutcome.WIN),
    ],
)
def test_all_market_sides_grade_from_exact_issued_line(
    market: str, side: str, line: float | None, scores: tuple[int, int], expected: CandidateOutcome
) -> None:
    row = _row(market, side, line=line)
    report = evaluate_market_families(
        issuance=_issuance(row),
        closeouts=[],
        games=_games((row.game_id, *scores)),
        history_boundaries=[],
    )
    family = getattr(report, market)
    assert family.reliability.win_count == (1 if expected is CandidateOutcome.WIN else 0)


@pytest.mark.parametrize(
    ("row", "scores"),
    [
        (_row("moneyline", "home"), (20, 20)),
        (_row("spread", "home", line=-3.0), (20, 23)),
        (_row("total", "over", line=44.0), (20, 24)),
    ],
)
def test_pushes_are_explicit_and_excluded_from_reliability(
    row: CandidateIssuanceRow, scores: tuple[int, int]
) -> None:
    family = getattr(
        evaluate_market_families(
            issuance=_issuance(row),
            closeouts=[],
            games=_games((row.game_id, *scores)),
            history_boundaries=[],
        ),
        row.market,
    )
    assert family.reliability.push_count == 1
    assert family.reliability.evaluable_count == 0
    assert family.reliability.brier.status is EvaluationEvidenceStatus.UNAVAILABLE


def test_reliability_uses_public_metrics_with_sample_size_one() -> None:
    row = _row("moneyline", "home", model_probability=0.60)
    family = evaluate_market_families(
        issuance=_issuance(row),
        closeouts=[],
        games=_games((row.game_id, 20, 27)),
        history_boundaries=[],
    ).moneyline
    assert family.reliability.brier.value == pytest.approx(0.16)
    assert family.reliability.brier.sample_size == 1
    assert family.reliability.brier.status is EvaluationEvidenceStatus.AVAILABLE


def test_closeout_depth_age_and_categorical_cohorts() -> None:
    row = _row("spread", "away")
    issuance = _issuance(row)
    family = evaluate_market_families(
        issuance=issuance,
        closeouts=[_closeout(issuance, row)],
        games=_games((row.game_id, 24, 20)),
        history_boundaries=[_boundary(row)],
    ).spread
    assert family.coverage.closeout_available_count == 1
    assert family.coverage.clv_available_count == 1
    assert family.quote_age.median_seconds == pytest.approx(1800.0)
    assert family.observation_depth.median_observation_count == pytest.approx(4.0)
    assert family.observation_depth.median_distinct_fetch_count == pytest.approx(3.0)
    assert family.sportsbook_cohorts[0].cohort_key == "the_odds_api/draftkings"
    assert family.market_side_cohorts[0].cohort_key == "spread/away"


def test_missing_and_duplicate_evidence_are_explicit() -> None:
    row = _row("moneyline", "home")
    issuance = _issuance(row)
    closeout = _closeout(issuance, row)
    boundary = _boundary(row)
    family = evaluate_market_families(
        issuance=issuance,
        closeouts=[closeout, closeout],
        games=_games(),
        history_boundaries=[boundary, boundary],
    ).moneyline
    assert family.coverage.closeout_status_counts == (("evaluation_closeout_conflict", 1),)
    assert family.observation_depth.status is EvaluationEvidenceStatus.CONFLICTING_EVIDENCE


def test_negative_quote_age_is_conflicting() -> None:
    row = _row("moneyline", "home", fetched_at=EVALUATED + timedelta(seconds=1))
    family = evaluate_market_families(
        issuance=_issuance(row), closeouts=[], games=_games(), history_boundaries=[]
    ).moneyline
    assert family.quote_age.status is EvaluationEvidenceStatus.CONFLICTING_EVIDENCE
    assert family.quote_age.conflict_count == 1


def test_missing_and_conflicting_games_are_explicit() -> None:
    row = _row("moneyline", "home")
    unavailable = evaluate_market_families(
        issuance=_issuance(row), closeouts=[], games=_games(), history_boundaries=[]
    ).moneyline
    assert unavailable.reliability.unavailable_count == 1
    conflict = evaluate_market_families(
        issuance=_issuance(row),
        closeouts=[],
        games=_games((row.game_id, "bad", 20)),
        history_boundaries=[],
    ).moneyline
    assert conflict.reliability.conflict_count == 1
    with pytest.raises(ValueError, match="duplicate GAME_ID"):
        evaluate_market_families(
            issuance=_issuance(row),
            closeouts=[],
            games=_games((row.game_id, 20, 21), (row.game_id, 17, 18)),
            history_boundaries=[],
        )


def test_reordered_inputs_are_deterministic_and_games_are_not_mutated() -> None:
    row = _row("moneyline", "home")
    issuance = _issuance(row)
    closeout, boundary = _closeout(issuance, row), _boundary(row)
    games = _games((row.game_id, 20, 27))
    before = games.copy(deep=True)
    first = evaluate_market_families(
        issuance=issuance, closeouts=[closeout], games=games, history_boundaries=[boundary]
    )
    second = evaluate_market_families(
        issuance=issuance,
        closeouts=list(reversed([closeout])),
        games=games.iloc[::-1].reset_index(drop=True),
        history_boundaries=list(reversed([boundary])),
    )
    assert first == second
    pd.testing.assert_frame_equal(games, before)


def test_contracts_are_frozen_and_source_has_no_policy_dependencies() -> None:
    result = evaluate_market_families(
        issuance=_issuance(), closeouts=[], games=_games(), history_boundaries=[]
    )
    with pytest.raises(FrozenInstanceError):
        result.moneyline.market = "spread"  # pyrefly: ignore [read-only]

    source = Path("src/gridiron_edge/market/market_family_evaluation.py").read_text()
    tree = ast.parse(source)
    imported_modules = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_modules.update(
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    )
    forbidden_prefixes = (
        "gridiron_edge.api",
        "gridiron_edge.betting.bankroll",
        "gridiron_edge.market.kelly",
        "gridiron_edge.market.qualification",
        "gridiron_edge.market.recommendations",
    )
    assert not any(
        module == prefix or module.startswith(f"{prefix}.")
        for module in imported_modules
        for prefix in forbidden_prefixes
    )


def test_numeric_cohorts_report_unavailable_and_insufficient_evidence() -> None:
    empty = evaluate_market_families(
        issuance=_issuance(), closeouts=[], games=_games(), history_boundaries=[]
    ).moneyline
    assert empty.expected_value_cohorts.status is EvaluationEvidenceStatus.UNAVAILABLE

    row = _row("moneyline", "home", expected_value=0.10)
    single = evaluate_market_families(
        issuance=_issuance(row), closeouts=[], games=_games(), history_boundaries=[]
    ).moneyline
    assert single.expected_value_cohorts.status is EvaluationEvidenceStatus.INSUFFICIENT_EVIDENCE
    assert single.expected_value_cohorts.value_available_count == 1
    assert single.expected_value_cohorts.cohorts == ()


def test_empirical_ev_cohorts_are_deterministic_and_exhaustive() -> None:
    rows = tuple(
        _row(
            "moneyline",
            "home" if index % 2 == 0 else "away",
            sportsbook=f"book-{index}",
            expected_value=value,
            fetched_at=FETCHED - timedelta(seconds=index),
        )
        for index, value in enumerate((0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08))
    )
    issuance = _issuance(*rows)
    report = evaluate_market_families(
        issuance=issuance, closeouts=[], games=_games(), history_boundaries=[]
    ).moneyline.expected_value_cohorts
    reversed_report = evaluate_market_families(
        issuance=_issuance(*reversed(rows)), closeouts=[], games=_games(), history_boundaries=[]
    ).moneyline.expected_value_cohorts
    assert report.status is EvaluationEvidenceStatus.AVAILABLE
    assert sum(cohort.candidate_count for cohort in report.cohorts) == len(rows)
    assert all(cohort.candidate_count > 0 for cohort in report.cohorts)
    assert report == reversed_report


def test_identical_and_nonfinite_ev_values_are_explicit() -> None:
    same = tuple(
        _row("spread", "home", sportsbook=f"book-{index}", expected_value=0.10)
        for index in range(3)
    )
    insufficient = evaluate_market_families(
        issuance=_issuance(*same), closeouts=[], games=_games(), history_boundaries=[]
    ).spread.expected_value_cohorts
    assert insufficient.status is EvaluationEvidenceStatus.INSUFFICIENT_EVIDENCE

    conflict_row = _row("spread", "home", expected_value=float("inf"))
    conflict = evaluate_market_families(
        issuance=_issuance(conflict_row), closeouts=[], games=_games(), history_boundaries=[]
    ).spread.expected_value_cohorts
    assert conflict.status is EvaluationEvidenceStatus.CONFLICTING_EVIDENCE
    assert conflict.conflict_count == 1


def test_clv_cohorts_require_market_family_specific_kind() -> None:
    rows = tuple(
        _row("spread", "away", sportsbook=f"book-{index}", line=3.5 + index) for index in range(4)
    )
    issuance = _issuance(*rows)
    closeouts = [
        _closeout(issuance, row, clv=float(index), clv_kind=MarketClvKind.SPREAD_POINTS)
        for index, row in enumerate(rows)
    ]
    available = evaluate_market_families(
        issuance=issuance, closeouts=closeouts, games=_games(), history_boundaries=[]
    ).spread.clv_cohorts
    assert available.status is EvaluationEvidenceStatus.AVAILABLE
    assert available.value_available_count == 4

    wrong = [_closeout(issuance, rows[0], clv=0.1, clv_kind=MarketClvKind.MONEYLINE_PRICE)]
    conflict = evaluate_market_families(
        issuance=_issuance(rows[0]), closeouts=wrong, games=_games(), history_boundaries=[]
    ).spread.clv_cohorts
    assert conflict.status is EvaluationEvidenceStatus.CONFLICTING_EVIDENCE


def test_quote_age_and_depth_use_separate_empirical_cohort_sets() -> None:
    rows = tuple(
        _row(
            "total",
            "over",
            sportsbook=f"book-{index}",
            fetched_at=FETCHED - timedelta(minutes=index),
        )
        for index in range(4)
    )
    issuance = _issuance(*rows)
    boundaries = [
        _boundary(row, observation_count=index + 2, distinct_fetch_count=index + 1)
        for index, row in enumerate(rows)
    ]
    report = evaluate_market_families(
        issuance=issuance, closeouts=[], games=_games(), history_boundaries=boundaries
    ).total
    assert report.quote_age_cohorts.status is EvaluationEvidenceStatus.AVAILABLE
    assert report.observation_count_cohorts.status is EvaluationEvidenceStatus.AVAILABLE
    assert report.distinct_fetch_count_cohorts.status is EvaluationEvidenceStatus.AVAILABLE
    assert report.observation_count_cohorts.cohort_kind == "observation_count"
    assert report.distinct_fetch_count_cohorts.cohort_kind == "distinct_fetch_count"


def _wager(row: CandidateIssuanceRow, **overrides: object) -> dict[str, object]:
    """Build one exact recorded-wager return row."""
    values: dict[str, object] = {
        "bet_id": "bet-1",
        "game_id": row.game_id,
        "market_type": row.market,
        "side": row.side,
        "reference_provider": row.provider,
        "reference_provider_event_id": row.provider_event_id,
        "reference_sportsbook": row.sportsbook,
        "reference_market_fetched_at": row.fetched_at,
        "reference_sportsbook_updated_at": row.sportsbook_updated_at,
        "reference_commence_time": row.kickoff,
        "reference_american_odds": row.american_price,
        "reference_line": row.line,
        "status": "won",
        "stake": 100.0,
        "pnl": 90.0,
    }
    values.update(overrides)
    return values


def _wagers(*rows: dict[str, object]) -> DataFrame:
    """Build narrow recorded-wager return evidence."""
    return DataFrame(rows)


def test_exact_settled_wager_provides_realized_return() -> None:
    row = _row("moneyline", "home")
    wagers = _wagers(_wager(row))
    before = wagers.copy(deep=True)
    report = evaluate_market_families(
        issuance=_issuance(row),
        closeouts=[],
        games=_games(),
        history_boundaries=[],
        wagers=wagers,
    ).moneyline.realized_return
    assert report.status is EvaluationEvidenceStatus.AVAILABLE
    assert report.available_count == 1
    assert report.total_stake == pytest.approx(100.0)
    assert report.total_pnl == pytest.approx(90.0)
    assert report.mean_per_wager_return == pytest.approx(0.9)
    assert report.aggregate_return == pytest.approx(0.9)
    pd.testing.assert_frame_equal(wagers, before)


@pytest.mark.parametrize(
    ("overrides", "status", "conflicts"),
    [
        ({"reference_provider": "other"}, EvaluationEvidenceStatus.UNAVAILABLE, 0),
        ({"status": "open", "pnl": None}, EvaluationEvidenceStatus.UNAVAILABLE, 0),
        ({"stake": 0.0}, EvaluationEvidenceStatus.CONFLICTING_EVIDENCE, 1),
        ({"stake": -1.0}, EvaluationEvidenceStatus.CONFLICTING_EVIDENCE, 1),
        ({"pnl": float("nan")}, EvaluationEvidenceStatus.CONFLICTING_EVIDENCE, 1),
    ],
)
def test_wager_return_unavailable_and_conflicting_states(
    overrides: dict[str, object],
    status: EvaluationEvidenceStatus,
    conflicts: int,
) -> None:
    row = _row("spread", "away")
    report = evaluate_market_families(
        issuance=_issuance(row),
        closeouts=[],
        games=_games(),
        history_boundaries=[],
        wagers=_wagers(_wager(row, **overrides)),
    ).spread.realized_return
    assert report.status is status
    assert report.conflict_count == conflicts
    assert report.aggregate_return is None


def test_duplicate_exact_wagers_are_conflicting() -> None:
    row = _row("total", "over")
    first = _wager(row, bet_id="bet-1")
    second = _wager(row, bet_id="bet-2")
    report = evaluate_market_families(
        issuance=_issuance(row),
        closeouts=[],
        games=_games(),
        history_boundaries=[],
        wagers=_wagers(first, second),
    ).total.realized_return
    assert report.status is EvaluationEvidenceStatus.CONFLICTING_EVIDENCE
    assert report.conflict_count == 1


def test_push_is_valid_zero_realized_return() -> None:
    row = _row("spread", "home")
    report = evaluate_market_families(
        issuance=_issuance(row),
        closeouts=[],
        games=_games(),
        history_boundaries=[],
        wagers=_wagers(_wager(row, status="push", pnl=0.0)),
    ).spread.realized_return
    assert report.status is EvaluationEvidenceStatus.AVAILABLE
    assert report.mean_per_wager_return == pytest.approx(0.0)
    assert report.aggregate_return == pytest.approx(0.0)


def test_mean_and_aggregate_return_remain_distinct() -> None:
    first = _row("moneyline", "home", sportsbook="book-1")
    second = _row("moneyline", "away", sportsbook="book-2")
    wagers = _wagers(
        _wager(first, bet_id="bet-1", stake=100.0, pnl=100.0),
        _wager(second, bet_id="bet-2", stake=300.0, pnl=-300.0),
    )
    report = evaluate_market_families(
        issuance=_issuance(first, second),
        closeouts=[],
        games=_games(),
        history_boundaries=[],
        wagers=wagers,
    ).moneyline.realized_return
    assert report.mean_per_wager_return == pytest.approx(0.0)
    assert report.aggregate_return == pytest.approx(-0.5)


def test_wager_schema_and_identity_are_strict() -> None:
    row = _row("moneyline", "home")
    with pytest.raises(ValueError, match="missing columns"):
        evaluate_market_families(
            issuance=_issuance(row),
            closeouts=[],
            games=_games(),
            history_boundaries=[],
            wagers=DataFrame([{"bet_id": "bet-1"}]),
        )
    duplicate = _wagers(_wager(row), _wager(row))
    with pytest.raises(ValueError, match="duplicate bet_id"):
        evaluate_market_families(
            issuance=_issuance(row),
            closeouts=[],
            games=_games(),
            history_boundaries=[],
            wagers=duplicate,
        )


def test_return_coverage_counts_are_market_family_specific() -> None:
    moneyline = _row("moneyline", "home")
    spread = _row("spread", "away")
    wagers = _wagers(
        _wager(moneyline, bet_id="bet-ml", stake=100.0, pnl=50.0),
        _wager(spread, bet_id="bet-spread", stake=0.0, pnl=0.0),
    )
    report = evaluate_market_families(
        issuance=_issuance(moneyline, spread),
        closeouts=[],
        games=_games(),
        history_boundaries=[],
        wagers=wagers,
    )
    assert report.moneyline.coverage.return_available_count == 1
    assert report.moneyline.coverage.return_unavailable_count == 0
    assert report.moneyline.coverage.return_conflict_count == 0
    assert report.spread.coverage.return_available_count == 0
    assert report.spread.coverage.return_conflict_count == 1
    assert report.total.coverage.return_available_count == 0


def test_ev_cohorts_report_only_exactly_attributed_realized_return() -> None:
    rows = tuple(
        _row(
            "moneyline",
            "home" if index % 2 == 0 else "away",
            sportsbook=f"book-{index}",
            expected_value=value,
            fetched_at=FETCHED - timedelta(seconds=index),
        )
        for index, value in enumerate((0.01, 0.02, 0.03, 0.04))
    )
    wagers = _wagers(
        *(
            _wager(
                row,
                bet_id=f"bet-{index}",
                stake=100.0,
                pnl=100.0 if index < 2 else -100.0,
            )
            for index, row in enumerate(rows)
        )
    )
    cohorts = evaluate_market_families(
        issuance=_issuance(*rows),
        closeouts=[],
        games=_games(),
        history_boundaries=[],
        wagers=wagers,
    ).moneyline.expected_value_cohorts
    assert cohorts.status is EvaluationEvidenceStatus.AVAILABLE
    assert sum(cohort.candidate_count for cohort in cohorts.cohorts) == 4
    assert all(cohort.mean_realized_return is not None for cohort in cohorts.cohorts)
    assert all(cohort.aggregate_realized_return is not None for cohort in cohorts.cohorts)


def test_numeric_cohorts_do_not_substitute_ev_or_outcomes_for_return() -> None:
    rows = tuple(
        _row(
            "total",
            "over",
            sportsbook=f"book-{index}",
            expected_value=value,
            game_id=f"game-total-{index}",
        )
        for index, value in enumerate((0.01, 0.02, 0.03, 0.04))
    )
    cohorts = evaluate_market_families(
        issuance=_issuance(*rows),
        closeouts=[],
        games=_games(*((row.game_id, 20, 27) for row in rows)),
        history_boundaries=[],
        wagers=None,
    ).total.expected_value_cohorts
    assert cohorts.status is EvaluationEvidenceStatus.AVAILABLE
    assert all(cohort.mean_realized_return is None for cohort in cohorts.cohorts)
    assert all(cohort.aggregate_realized_return is None for cohort in cohorts.cohorts)


def test_closeout_with_mismatched_digest_but_matching_fields_does_not_match() -> None:
    """A reference whose suffix digest does not correspond to the row,
    but whose individual materialized fields happen to equal the row's
    fields, must not be attributed -- this is the exact ambiguity the
    digest re-derivation closes."""
    row = _row("spread", "away")
    issuance = _issuance(row)
    genuine = _closeout(issuance, row)
    forged = replace(
        genuine,
        reference=replace(
            genuine.reference,
            reference_id=f"{issuance.issuance_id}:not-the-real-digest",
        ),
    )
    family = evaluate_market_families(
        issuance=issuance,
        closeouts=[forged],
        games=_games((row.game_id, 24, 20)),
        history_boundaries=[],
    ).spread
    assert family.coverage.closeout_available_count == 0
