# tests/unit/api/test_schemas_teams.py

"""Unit tests for teams schemas."""

from __future__ import annotations

from pydantic import ValidationError
import pytest

from gridiron_edge.api.schemas.teams import (
    RatingHistoryPoint,
    RecentResult,
    TeamProfile,
    TeamRankingRow,
    TeamRankingsList,
    TeamRatingOffseasonTransition,
    TeamRatingTimeline,
    TeamRatingTimelinePoint,
    TeamRecord,
)


class TestTeamRecord:
    def test_default(self) -> None:
        r = TeamRecord()
        assert r.wins == 0 and r.losses == 0 and r.ties == 0

    def test_populated(self) -> None:
        r = TeamRecord(wins=10, losses=2, ties=0)
        assert r.wins == 10


class TestTeamRankingRow:
    def test_minimum(self) -> None:
        row = TeamRankingRow(abbr="BAL", name="Baltimore Ravens")
        assert row.abbr == "BAL"
        assert row.rating is None

    def test_populated(self) -> None:
        row = TeamRankingRow(
            abbr="BAL",
            name="Baltimore Ravens",
            rating=1642.3,
            rank=1,
            record=TeamRecord(wins=10, losses=2),
        )
        assert row.rank == 1
        assert row.record is not None
        assert row.record.wins == 10


class TestTeamRankingsList:
    def test_empty(self) -> None:
        rl = TeamRankingsList()
        assert rl.items == []
        assert rl.season is None

    def test_populated(self) -> None:
        rl = TeamRankingsList(
            season="2025-2026",
            as_of_week=12,
            items=[
                TeamRankingRow(abbr="BAL", name="Baltimore Ravens", rating=1642.3, rank=1),
            ],
            total=1,
        )
        assert rl.season == "2025-2026"


class TestTeamProfile:
    def test_minimum(self) -> None:
        p = TeamProfile(abbr="BAL", name="Baltimore Ravens")
        assert p.rating is None
        assert p.rating_history is None
        assert p.rating_timeline is None

    def test_populated(self) -> None:
        p = TeamProfile(
            abbr="BAL",
            name="Baltimore Ravens",
            season="2025-2026",
            as_of_week=12,
            rating=1642.3,
            rank=1,
            record=TeamRecord(wins=10, losses=2),
            rating_history=[RatingHistoryPoint(week=1, rating=1600.0)],
            recent_results=[
                RecentResult(
                    week=12,
                    opponent="CLE",
                    is_home=True,
                    result="W",
                    score_for=31,
                    score_against=14,
                ),
            ],
        )
        assert p.rating == 1642.3
        assert p.recent_results is not None
        assert p.recent_results[0].opponent == "CLE"


class TestRecentResult:
    def test_default(self) -> None:
        # Test that a fully null RecentResult can construct.
        # Requires only week; other fields default None.
        r = RecentResult(week=1)
        assert r.result is None

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(ValidationError):
            RecentResult.model_validate({"week": 1, "foo": "bar"})


class TestTeamRatingTimeline:
    def test_populated(self) -> None:
        timeline = TeamRatingTimeline(
            range="season",
            completed_through_week=0,
            current_rating_week=1,
            points=[
                TeamRatingTimelinePoint(
                    season="2026-2027",
                    week=1,
                    rating=1500.0,
                    state="current",
                )
            ],
            offseason_transition=TeamRatingOffseasonTransition(
                from_season="2025-2026",
                from_rating=1510.0,
                to_season="2026-2027",
                to_rating=1500.0,
            ),
        )
        assert timeline.points[0].state == "current"
        assert timeline.offseason_transition is not None

    def test_rejects_week_twenty_three(self) -> None:
        with pytest.raises(ValidationError):
            TeamRatingTimelinePoint(
                season="2025-2026",
                week=23,
                rating=1500.0,
                state="observed",
            )
