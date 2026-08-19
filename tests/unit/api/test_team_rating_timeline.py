"""Focused tests for historical team rating timelines."""

from __future__ import annotations

import pandas as pd

from gridiron_edge.api.serializers.teams import build_team_rating_timeline

LONG_TO_SHORT = {
    "Miami Dolphins": "MIA",
    "New England Patriots": "NE",
    "Seattle Seahawks": "SEA",
}


def _game(team: str, opponent: str, season: str, week: int, won: bool) -> dict:
    return {
        "GAME_ID": f"{season[:4]}_{week:02d}_TST_TST",
        "YEAR": season,
        "WEEK_NUM": week,
        "GAME_DATE": "2026-01-01",
        "AWAY_TEAM": opponent,
        "HOME_TEAM": team,
        "AWAY_SCORE": 10 if won else 20,
        "HOME_SCORE": 20 if won else 10,
        "IS_NEUTRAL_SITE": 0,
    }


def test_early_season_bridge_carries_inactive_weeks_and_offseason() -> None:
    elo = pd.DataFrame(
        [
            {"NFL_TEAM": "Miami Dolphins", "NFL_YEAR": "2025-2026", "NFL_WEEK": week, "ELO": 1468.0}
            for week in range(18, 23)
        ]
        + [{"NFL_TEAM": "Miami Dolphins", "NFL_YEAR": "2026-2027", "NFL_WEEK": 1, "ELO": 1477.0}]
    )
    games = pd.DataFrame([_game("Miami Dolphins", "New England Patriots", "2025-2026", 18, False)])

    timeline = build_team_rating_timeline(
        elo=elo,
        games=games,
        team_long_name="Miami Dolphins",
        long_to_short=LONG_TO_SHORT,
        season="2026-2027",
        completed_through_week=0,
        current_rating_week=1,
        timeline_range="season",
    )

    assert timeline is not None
    assert [(p.season, p.week) for p in timeline.points[:5]] == [
        ("2025-2026", 18),
        ("2025-2026", 19),
        ("2025-2026", 20),
        ("2025-2026", 21),
        ("2025-2026", 22),
    ]
    assert timeline.points[0].state == "observed"
    assert all(point.state == "carried_forward" for point in timeline.points[1:5])
    assert timeline.points[5].state == "current"
    assert timeline.offseason_transition is not None
    assert timeline.offseason_transition.from_rating == 1468.0


def test_super_bowl_postgame_state_is_semantic_final_not_week_twenty_three() -> None:
    elo = pd.DataFrame(
        [
            {
                "NFL_TEAM": "Seattle Seahawks",
                "NFL_YEAR": "2025-2026",
                "NFL_WEEK": 22,
                "ELO": 1621.0,
            },
            {
                "NFL_TEAM": "Seattle Seahawks",
                "NFL_YEAR": "2025-2026",
                "NFL_WEEK": 23,
                "ELO": 1630.0,
            },
            {"NFL_TEAM": "Seattle Seahawks", "NFL_YEAR": "2026-2027", "NFL_WEEK": 1, "ELO": 1585.0},
        ]
    )
    games = pd.DataFrame([_game("Seattle Seahawks", "New England Patriots", "2025-2026", 22, True)])

    timeline = build_team_rating_timeline(
        elo=elo,
        games=games,
        team_long_name="Seattle Seahawks",
        long_to_short=LONG_TO_SHORT,
        season="2026-2027",
        completed_through_week=0,
        current_rating_week=1,
        timeline_range="season",
    )

    assert timeline is not None
    assert all(point.week <= 22 for point in timeline.points)
    assert timeline.prior_season_final is not None
    assert timeline.prior_season_final.rating == 1630.0
    assert timeline.prior_season_final.source_week == 22
    assert timeline.offseason_transition is not None
    assert timeline.offseason_transition.from_rating == 1630.0


def test_prior_season_bridge_falls_away_at_week_six() -> None:
    elo = pd.DataFrame(
        [
            {"NFL_TEAM": "Miami Dolphins", "NFL_YEAR": "2025-2026", "NFL_WEEK": 22, "ELO": 1468.0},
        ]
        + [
            {
                "NFL_TEAM": "Miami Dolphins",
                "NFL_YEAR": "2026-2027",
                "NFL_WEEK": week,
                "ELO": 1470.0 + week,
            }
            for week in range(1, 7)
        ]
    )
    games = pd.DataFrame(
        [
            _game("Miami Dolphins", "New England Patriots", "2026-2027", week, week % 2 == 0)
            for week in range(1, 6)
        ]
    )

    timeline = build_team_rating_timeline(
        elo=elo,
        games=games,
        team_long_name="Miami Dolphins",
        long_to_short=LONG_TO_SHORT,
        season="2026-2027",
        completed_through_week=5,
        current_rating_week=6,
        timeline_range="season",
    )

    assert timeline is not None
    assert {point.season for point in timeline.points} == {"2026-2027"}
    assert timeline.prior_season_final is None
    assert timeline.offseason_transition is None
    assert timeline.points[5].state == "current"


def test_recent_range_has_season_aware_history_current_and_unavailable_future() -> None:
    elo = pd.DataFrame(
        [
            {
                "NFL_TEAM": "Miami Dolphins",
                "NFL_YEAR": "2025-2026",
                "NFL_WEEK": week,
                "ELO": 1460.0 + week,
            }
            for week in range(18, 23)
        ]
        + [
            {
                "NFL_TEAM": "Miami Dolphins",
                "NFL_YEAR": "2026-2027",
                "NFL_WEEK": week,
                "ELO": 1470.0 + week,
            }
            for week in range(1, 4)
        ]
    )
    games = pd.DataFrame(
        columns=["YEAR", "WEEK_NUM", "AWAY_TEAM", "HOME_TEAM", "AWAY_SCORE", "HOME_SCORE"]
    )

    timeline = build_team_rating_timeline(
        elo=elo,
        games=games,
        team_long_name="Miami Dolphins",
        long_to_short=LONG_TO_SHORT,
        season="2026-2027",
        completed_through_week=2,
        current_rating_week=3,
        timeline_range="recent",
    )

    assert timeline is not None
    assert len([point for point in timeline.points if point.state != "unavailable"]) == 8
    assert any(point.season == "2025-2026" for point in timeline.points)
    assert next(point for point in timeline.points if point.state == "current").week == 3
    assert [point.week for point in timeline.points if point.state == "unavailable"] == list(
        range(4, 11)
    )
