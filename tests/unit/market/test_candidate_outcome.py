# tests/unit/market/test_candidate_outcome.py
"""Tests for realized-outcome grading of one issued market side."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from gridiron_edge.market.candidate_issuance import (
    CandidateIssuanceReason,
    CandidateIssuanceRow,
    CandidateIssuanceState,
)
from gridiron_edge.market.candidate_outcome import (
    CandidateOutcome,
    grade_candidate_outcome,
)

_FETCHED = datetime(2026, 9, 1, 12, tzinfo=UTC)
_KICKOFF = datetime(2026, 9, 1, 20, tzinfo=UTC)


def _row(**overrides: object) -> CandidateIssuanceRow:
    base: dict[str, object] = {
        "game_id": "2026_01_KC_LAC",
        "market": "moneyline",
        "side": "home",
        "provider": "the_odds_api",
        "provider_event_id": "event-1",
        "sportsbook": "draftkings",
        "line": None,
        "american_price": -110,
        "fetched_at": _FETCHED,
        "sportsbook_updated_at": _FETCHED,
        "kickoff": _KICKOFF,
        "is_live": False,
        "forecast_event_id": "forecast-1",
        "forecast_run_id": "run-1",
        "forecast_role": "champion",
        "forecast_generated_at": _FETCHED,
        "model_name": "model",
        "model_type": "type",
        "model_probability": 0.6,
        "expected_value": 0.1,
        "state": CandidateIssuanceState.CANDIDATE,
        "reason": CandidateIssuanceReason.POSITIVE_EXPECTED_VALUE,
    }
    base.update(overrides)
    return CandidateIssuanceRow(**base)


class TestMoneylineGrading:
    def test_home_win(self) -> None:
        row = _row(market="moneyline", side="home")
        assert grade_candidate_outcome(row, (20.0, 27.0)) is CandidateOutcome.WIN

    def test_home_loss(self) -> None:
        row = _row(market="moneyline", side="home")
        assert grade_candidate_outcome(row, (27.0, 20.0)) is CandidateOutcome.LOSS

    def test_away_win(self) -> None:
        row = _row(market="moneyline", side="away")
        assert grade_candidate_outcome(row, (27.0, 20.0)) is CandidateOutcome.WIN

    def test_moneyline_tie_is_push(self) -> None:
        row = _row(market="moneyline", side="home")
        assert grade_candidate_outcome(row, (24.0, 24.0)) is CandidateOutcome.PUSH


class TestSpreadGrading:
    def test_home_covers_exact_half_point_line(self) -> None:
        # home -3.5, away 20, home 27 -> 27 + (-3.5) = 23.5 > 20 -> WIN
        row = _row(market="spread", side="home", line=-3.5)
        assert grade_candidate_outcome(row, (20.0, 27.0)) is CandidateOutcome.WIN

    def test_home_fails_to_cover(self) -> None:
        # home -3.5, away 20, home 23 -> 23 + (-3.5) = 19.5 < 20 -> LOSS
        row = _row(market="spread", side="home", line=-3.5)
        assert grade_candidate_outcome(row, (20.0, 23.0)) is CandidateOutcome.LOSS

    def test_spread_push_requires_a_whole_number_line(self) -> None:
        # A half-point line cannot push against integer scores. Push
        # requires a whole-number line: home -7.0, away 20, home 27 ->
        # 27 + (-7.0) = 20 == 20 -> PUSH.
        row = _row(market="spread", side="home", line=-7.0)
        assert grade_candidate_outcome(row, (20.0, 27.0)) is CandidateOutcome.PUSH

    def test_away_side_grading(self) -> None:
        row = _row(market="spread", side="away", line=3.5)
        assert grade_candidate_outcome(row, (20.0, 27.0)) is CandidateOutcome.LOSS

    def test_missing_line_is_unavailable(self) -> None:
        row = _row(market="spread", side="home", line=None)
        assert grade_candidate_outcome(row, (20.0, 27.0)) is CandidateOutcome.UNAVAILABLE


class TestTotalGrading:
    def test_over_wins(self) -> None:
        row = _row(market="total", side="over", line=45.5)
        assert grade_candidate_outcome(row, (20.0, 27.0)) is CandidateOutcome.WIN

    def test_under_wins(self) -> None:
        row = _row(market="total", side="under", line=50.5)
        assert grade_candidate_outcome(row, (20.0, 27.0)) is CandidateOutcome.WIN

    def test_total_push_requires_a_whole_number_line(self) -> None:
        row = _row(market="total", side="over", line=47.0)
        assert grade_candidate_outcome(row, (20.0, 27.0)) is CandidateOutcome.PUSH

    def test_missing_line_is_unavailable(self) -> None:
        row = _row(market="total", side="over", line=None)
        assert grade_candidate_outcome(row, (20.0, 27.0)) is CandidateOutcome.UNAVAILABLE


class TestUniversalGrading:
    def test_no_scores_is_unavailable(self) -> None:
        row = _row()
        assert grade_candidate_outcome(row, None) is CandidateOutcome.UNAVAILABLE

    def test_non_finite_scores_is_conflict(self) -> None:
        row = _row()
        assert grade_candidate_outcome(row, (float("nan"), 27.0)) is CandidateOutcome.CONFLICT
        assert grade_candidate_outcome(row, (20.0, float("inf"))) is CandidateOutcome.CONFLICT

    def test_unsupported_market_is_rejected_without_scores(self) -> None:
        row = _row(market="unsupported_market")
        with pytest.raises(ValueError, match="Unsupported candidate market"):
            grade_candidate_outcome(row, None)

    def test_unsupported_market_is_rejected_with_scores(self) -> None:
        row = _row(market="unsupported_market")
        with pytest.raises(ValueError, match="Unsupported candidate market"):
            grade_candidate_outcome(row, (20.0, 27.0))

    @pytest.mark.parametrize(
        ("market", "side"),
        [
            ("moneyline", "over"),
            ("spread", "under"),
            ("total", "home"),
        ],
    )
    def test_unsupported_market_side_is_rejected(self, market: str, side: str) -> None:
        row = _row(market=market, side=side, line=-3.5)
        with pytest.raises(ValueError, match="Unsupported candidate market-side pair"):
            grade_candidate_outcome(row, (20.0, 27.0))
