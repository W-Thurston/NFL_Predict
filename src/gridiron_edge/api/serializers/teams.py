# src/gridiron_edge/api/serializers/teams.py

"""Serializers for /teams and /teams/{abbr}.

Per D17, hand-written. Per D18, owns _meta.field_status construction.
"""

from __future__ import annotations

from typing import Any, Literal

import pandas as pd
from pandas import DataFrame, Series

from gridiron_edge.api.loaders import WeeklyEloForecastLoad
from gridiron_edge.api.meta import Blocker, ResponseMeta, Unavailable
from gridiron_edge.api.schemas.teams import (
    RatingHistoryPoint,
    RecentResult,
    TeamProfile,
    TeamRankingRow,
    TeamRankingsList,
    TeamRatingOffseasonTransition,
    TeamRatingSeasonFinal,
    TeamRatingTimeline,
    TeamRatingTimelinePoint,
    TeamRecord,
)


def _none_if_nan(v: Any) -> Any:  # noqa: ANN401
    """Return None for NaN or None; else the value."""
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    return v


def _trend_for_team(
    trends: DataFrame,
    team_abbr: str,
) -> float | None:
    """Return the Elo trend (delta from prior week) for a team.

    Returns None if trends DataFrame is empty or the team isn't found.
    """
    if trends.empty:
        return None
    match = trends.loc[trends["team_abbr"] == team_abbr]
    if match.empty:
        return None
    return _none_if_nan(match.iloc[0].get("elo_delta"))


def _percentile_for_team(
    percentiles: DataFrame,
    team_abbr: str,
) -> dict[str, float | None]:
    """Extract the four percentile fields for a team.

    Returns dict with rating_pct, avg_wins_pct, make_playoffs_pct,
    win_sb_pct keys. All None if the team isn't found or percentiles
    DataFrame is empty.
    """
    empty: dict[str, float | None] = {
        "rating_pct": None,
        "avg_wins_pct": None,
        "make_playoffs_pct": None,
        "win_sb_pct": None,
    }
    if percentiles.empty:
        return empty

    match: DataFrame | Series = percentiles.loc[percentiles["team_abbr"] == team_abbr]
    if match.empty:
        return empty

    row: Series | Any = match.iloc[0]
    return {
        "rating_pct": _none_if_nan(row.get("rating_pct")),
        "avg_wins_pct": _none_if_nan(row.get("avg_wins_pct")),
        "make_playoffs_pct": _none_if_nan(row.get("make_playoffs_pct")),
        "win_sb_pct": _none_if_nan(row.get("win_sb_pct")),
    }


def _compute_record(
    games: DataFrame,
    team_long_name: str,
) -> TeamRecord:
    """Count canonical Away/Home results for one team."""
    if games.empty:
        return TeamRecord()

    away_mask: Series[bool] = games["AWAY_TEAM"] == team_long_name
    home_mask: Series[bool] = games["HOME_TEAM"] == team_long_name
    participant_mask: Series[bool] = away_mask | home_mask

    away_scores = Series(
        pd.to_numeric(games["AWAY_SCORE"], errors="coerce"),
        index=games.index,
    )
    home_scores = Series(
        pd.to_numeric(games["HOME_SCORE"], errors="coerce"),
        index=games.index,
    )
    completed_mask = away_scores.notna() & home_scores.notna()

    away_wins = away_mask & completed_mask & (away_scores > home_scores)
    home_wins = home_mask & completed_mask & (home_scores > away_scores)
    away_losses = away_mask & completed_mask & (away_scores < home_scores)
    home_losses = home_mask & completed_mask & (home_scores < away_scores)
    ties = participant_mask & completed_mask & (away_scores == home_scores)

    return TeamRecord(
        wins=int((away_wins | home_wins).sum()),
        losses=int((away_losses | home_losses).sum()),
        ties=int(ties.sum()),
    )


def _latest_ratings(elo: DataFrame, season: str, week: int) -> DataFrame:
    """Filter Elo state to the latest week ≤ `week` per team within `season`."""
    if elo.empty:
        return elo

    scope = elo.loc[
        (elo["NFL_YEAR"] == season) & (elo["NFL_WEEK"] <= week),
        :,
    ]
    if scope.empty:
        return scope

    return scope.sort_values(["NFL_TEAM", "NFL_WEEK"]).groupby("NFL_TEAM", as_index=False).tail(1)


def _previous_season_label(season: str) -> str | None:
    """Return the prior consecutive NFL season label."""
    parts = season.split("-")
    if len(parts) != 2:
        return None
    try:
        start = int(parts[0])
        end = int(parts[1])
    except ValueError:
        return None
    if end != start + 1:
        return None
    return f"{start - 1}-{start}"


def _team_game_by_week(
    games: DataFrame,
    team_long_name: str,
    season: str,
) -> dict[int, Series]:
    """Index completed team games by calendar week for one season."""
    if games.empty:
        return {}
    scoped = games.loc[
        (games["YEAR"] == season)
        & ((games["AWAY_TEAM"] == team_long_name) | (games["HOME_TEAM"] == team_long_name)),
        :,
    ].dropna(subset=["AWAY_SCORE", "HOME_SCORE"])
    return {int(row["WEEK_NUM"]): row for _, row in scoped.iterrows()}


def _timeline_point(
    *,
    season: str,
    week: int,
    rating: float | None,
    current: bool,
    game: Series | None,
    team_long_name: str,
    long_to_short: dict[str, str],
) -> TeamRatingTimelinePoint:
    """Serialize one explicit team-week Elo state."""
    result = _serialize_result(game, team_long_name, long_to_short) if game is not None else None
    if current:
        state: Literal["observed", "carried_forward", "current", "unavailable"] = "current"
    elif rating is None:
        state = "unavailable"
    elif result is None:
        state = "carried_forward"
    else:
        state = "observed"
    timeline_result: Literal["L", "T", "W"] | None = None
    if result is not None and result.result in ("L", "T", "W"):
        timeline_result = result.result

    return TeamRatingTimelinePoint(
        season=season,
        week=week,
        rating=rating,
        state=state,
        game_played=result is not None,
        result=timeline_result,
        opponent=result.opponent if result is not None else None,
    )


def build_team_rating_timeline(  # noqa: PLR0912, PLR0915
    *,
    elo: DataFrame,
    games: DataFrame,
    team_long_name: str,
    long_to_short: dict[str, str],
    season: str,
    completed_through_week: int,
    current_rating_week: int,
    timeline_range: Literal["season", "recent"],
    forecast_load: WeeklyEloForecastLoad | None = None,
) -> TeamRatingTimeline | None:
    """Build a season-aware historical Elo timeline without forecasting."""
    if elo.empty or not season:
        return None

    current_rows = elo.loc[
        (elo["NFL_TEAM"] == team_long_name)
        & (elo["NFL_YEAR"] == season)
        & (elo["NFL_WEEK"].between(1, 22)),
        ["NFL_WEEK", "ELO"],
    ]
    current_ratings = {
        int(row["NFL_WEEK"]): float(row["ELO"]) for _, row in current_rows.iterrows()
    }
    if not current_ratings:
        return None

    current_games = _team_game_by_week(games, team_long_name, season)
    points: list[TeamRatingTimelinePoint] = []
    previous_season = _previous_season_label(season)
    previous_ratings: dict[int, float] = {}
    previous_games: dict[int, Series] = {}
    prior_final: TeamRatingSeasonFinal | None = None
    transition: TeamRatingOffseasonTransition | None = None

    if previous_season is not None:
        previous_rows = elo.loc[
            (elo["NFL_TEAM"] == team_long_name) & (elo["NFL_YEAR"] == previous_season),
            ["NFL_WEEK", "ELO"],
        ]
        previous_ratings = {
            int(row["NFL_WEEK"]): float(row["ELO"]) for _, row in previous_rows.iterrows()
        }
        previous_games = _team_game_by_week(games, team_long_name, previous_season)
        if 23 in previous_ratings:
            final_result = previous_games.get(22)
            serialized_final = (
                _serialize_result(final_result, team_long_name, long_to_short)
                if final_result is not None
                else None
            )
            final_result_value: Literal["L", "T", "W"] | None = None
            if serialized_final is not None and serialized_final.result in ("L", "T", "W"):
                final_result_value = serialized_final.result
            prior_final = TeamRatingSeasonFinal(
                season=previous_season,
                rating=previous_ratings[23],
                result=final_result_value,
                opponent=(serialized_final.opponent if serialized_final is not None else None),
            )

    show_bridge = (
        current_rating_week <= 5 and previous_season is not None and bool(previous_ratings)
    )
    bridge_start = 17 + current_rating_week
    if timeline_range == "season":
        if show_bridge:
            for week in range(bridge_start, 23):
                points.append(
                    _timeline_point(
                        season=previous_season,
                        week=week,
                        rating=previous_ratings.get(week),
                        current=False,
                        game=previous_games.get(week),
                        team_long_name=team_long_name,
                        long_to_short=long_to_short,
                    )
                )
        for week in range(1, 23):
            points.append(
                _timeline_point(
                    season=season,
                    week=week,
                    rating=current_ratings.get(week) if week <= current_rating_week else None,
                    current=week == current_rating_week,
                    game=current_games.get(week) if week <= completed_through_week else None,
                    team_long_name=team_long_name,
                    long_to_short=long_to_short,
                )
            )
    else:
        historical: list[tuple[str, int, float]] = []
        if previous_season is not None:
            historical.extend(
                (previous_season, week, rating)
                for week, rating in previous_ratings.items()
                if 1 <= week <= 22
            )
        historical.extend(
            (season, week, rating)
            for week, rating in current_ratings.items()
            if 1 <= week < current_rating_week
        )
        historical = historical[-7:]
        for point_season, week, rating in historical:
            point_games = current_games if point_season == season else previous_games
            points.append(
                _timeline_point(
                    season=point_season,
                    week=week,
                    rating=rating,
                    current=False,
                    game=point_games.get(week),
                    team_long_name=team_long_name,
                    long_to_short=long_to_short,
                )
            )
        points.append(
            _timeline_point(
                season=season,
                week=current_rating_week,
                rating=current_ratings.get(current_rating_week),
                current=True,
                game=None,
                team_long_name=team_long_name,
                long_to_short=long_to_short,
            )
        )
        for week in range(current_rating_week + 1, min(22, current_rating_week + 7) + 1):
            points.append(
                _timeline_point(
                    season=season,
                    week=week,
                    rating=None,
                    current=False,
                    game=None,
                    team_long_name=team_long_name,
                    long_to_short=long_to_short,
                )
            )

    forecast_values: dict[int, Series] = {}
    forecast_computed_at: str | None = None
    forecast_simulation_count: int | None = None
    forecast_lower_quantile: float | None = None
    forecast_center_quantile: float | None = None
    forecast_upper_quantile: float | None = None
    forecast_quantile_method: str | None = None
    if forecast_load is not None and forecast_load.state == "available":
        forecast_values = {int(row["week"]): row for _, row in forecast_load.frame.iterrows()}
        first_forecast = forecast_load.frame.iloc[0]
        forecast_computed_at = str(first_forecast["computed_at"])
        forecast_simulation_count = int(first_forecast["simulation_count"])
        forecast_lower_quantile = float(first_forecast["lower_quantile"])
        forecast_center_quantile = float(first_forecast["center_quantile"])
        forecast_upper_quantile = float(first_forecast["upper_quantile"])
        forecast_quantile_method = str(first_forecast["quantile_method"])
        points = [
            point.model_copy(
                update={
                    "rating": float(forecast_values[point.week]["elo_median"]),
                    "state": "forecast",
                    "lower_rating": float(forecast_values[point.week]["elo_p10"]),
                    "upper_rating": float(forecast_values[point.week]["elo_p90"]),
                    "win_out_rating": float(forecast_values[point.week]["win_out_elo_median"]),
                    "lose_out_rating": float(forecast_values[point.week]["lose_out_elo_median"]),
                }
            )
            if (
                point.season == season
                and current_rating_week < point.week <= 18
                and point.week in forecast_values
            )
            else point
            for point in points
        ]

    if show_bridge and 1 in current_ratings:
        source_rating = prior_final.rating if prior_final is not None else previous_ratings.get(22)
        if source_rating is not None:
            transition = TeamRatingOffseasonTransition(
                from_season=previous_season,
                from_rating=source_rating,
                to_season=season,
                to_rating=current_ratings[1],
            )

    return TeamRatingTimeline(
        range=timeline_range,
        completed_through_week=completed_through_week,
        current_rating_week=current_rating_week,
        points=points,
        prior_season_final=prior_final if show_bridge else None,
        offseason_transition=transition,
        forecast_computed_at=forecast_computed_at,
        forecast_simulation_count=forecast_simulation_count,
        forecast_lower_quantile=forecast_lower_quantile,
        forecast_center_quantile=forecast_center_quantile,
        forecast_upper_quantile=forecast_upper_quantile,
        forecast_quantile_method=forecast_quantile_method,
    )


def serialize_team_rankings(
    elo: DataFrame,
    games: DataFrame,
    long_to_short: dict[str, str],
    season: str,
    as_of_week: int,
    percentiles: DataFrame,
    trends: DataFrame,
    team_metadata: dict[str, dict],
) -> TeamRankingsList:
    """Build the /teams power rankings response."""
    latest: DataFrame = _latest_ratings(elo, season, as_of_week)

    if latest.empty:
        meta: ResponseMeta = ResponseMeta().with_blocked("items", *Unavailable.NO_EVALUATION_DATA)
        return TeamRankingsList(
            season=season,
            as_of_week=as_of_week,
            items=[],
            total=0,
            response_meta=meta,  # pyrefly: ignore[unexpected-keyword]
        )

    ranked: DataFrame = latest.sort_values("ELO", ascending=False).reset_index(drop=True)
    season_games = games.loc[games["YEAR"] == season, :]

    rows: list[TeamRankingRow] = []
    for rank_idx, (_, r) in enumerate(ranked.iterrows()):
        long_name = r["NFL_TEAM"]
        abbr = long_to_short.get(long_name, long_name[:3].upper())
        pcts = _percentile_for_team(percentiles, abbr)
        team_meta = team_metadata.get(long_name, {})
        rows.append(
            TeamRankingRow(
                abbr=abbr,
                name=long_name,
                city=team_meta.get("city"),
                conference=team_meta.get("conference"),
                division=team_meta.get("division"),
                primary_color=team_meta.get("primary_color"),
                secondary_color=team_meta.get("secondary_color"),
                rating=_none_if_nan(r["ELO"]),
                rank=rank_idx + 1,
                record=_compute_record(season_games, long_name),
                trend=_trend_for_team(trends, abbr),
                rating_pct=pcts["rating_pct"],
                avg_wins_pct=pcts["avg_wins_pct"],
                make_playoffs_pct=pcts["make_playoffs_pct"],
                win_sb_pct=pcts["win_sb_pct"],
            ),
        )

    # Trend, off_rating, def_rating are null for every row.
    # Mark once at the items-level path.
    meta = ResponseMeta()
    meta = meta.with_blocked("items.off_rating", *Unavailable.OFF_DEF_DECOMPOSITION)
    meta = meta.with_blocked("items.def_rating", *Unavailable.OFF_DEF_DECOMPOSITION)

    return TeamRankingsList(
        season=season,
        as_of_week=as_of_week,
        items=rows,
        total=len(rows),
        response_meta=meta,  # pyrefly: ignore[unexpected-keyword]
    )


def _serialize_result(
    row: pd.Series,
    team_long_name: str,
    long_to_short: dict[str, str],
) -> RecentResult:
    """Serialize one canonical game from the requested team's view."""
    is_away = row["AWAY_TEAM"] == team_long_name
    is_designated_home = row["HOME_TEAM"] == team_long_name

    if not is_away and not is_designated_home:
        raise ValueError(
            f"Team {team_long_name!r} is not a participant in game {row.get('GAME_ID', '')!r}."
        )

    if is_away:
        opponent_long = str(row["HOME_TEAM"])
        score_for = int(row["AWAY_SCORE"])
        score_against = int(row["HOME_SCORE"])
    else:
        opponent_long = str(row["AWAY_TEAM"])
        score_for = int(row["HOME_SCORE"])
        score_against = int(row["AWAY_SCORE"])

    if score_for > score_against:
        result: Literal["L", "T", "W"] = "W"
    elif score_for < score_against:
        result = "L"
    else:
        result = "T"

    is_neutral = int(row.get("IS_NEUTRAL_SITE", 0)) == 1
    is_home = is_designated_home and not is_neutral

    opponent_short = long_to_short.get(
        opponent_long,
        opponent_long[:3].upper(),
    )

    return RecentResult(
        week=int(row["WEEK_NUM"]),
        date=str(row.get("GAME_DATE", "")),
        opponent=opponent_short,
        is_home=is_home,
        result=result,
        score_for=score_for,
        score_against=score_against,
    )


def serialize_team_profile(
    abbr: str,
    elo: DataFrame,
    games: DataFrame,
    long_to_short: dict[str, str],
    season: str,
    as_of_week: int,
    percentiles: DataFrame,
    trends: DataFrame,
    team_metadata: dict[str, dict],
    cohort_splits: dict[str, dict] | None = None,
    *,
    completed_through_week: int | None = None,
    current_rating_week: int | None = None,
    timeline_range: Literal["season", "recent"] = "season",
    forecast_load: WeeklyEloForecastLoad | None = None,
) -> TeamProfile:
    """Build the /teams/{abbr} response."""
    short_to_long: dict[str, str] = {v: k for k, v in long_to_short.items()}
    long_name: str | None = short_to_long.get(abbr.upper())

    if long_name is None:
        # Unknown abbreviation. The route will 404 before this is reached,
        # but return a defensive shape if called directly.
        meta: ResponseMeta = ResponseMeta().with_blocked("name", *Unavailable.NO_EVALUATION_DATA)
        return TeamProfile(
            abbr=abbr,
            name=abbr,
            season=season,
            as_of_week=as_of_week,
            response_meta=meta,  # pyrefly: ignore[unexpected-keyword]
        )

    # ------------------------------------------------------------------
    # Team metadata (colors, city, conference, division)
    # ------------------------------------------------------------------
    team_meta = team_metadata.get(long_name, {})

    # ------------------------------------------------------------------
    # Rating + rank (from latest week ≤ as_of_week within season)
    # ------------------------------------------------------------------
    latest: DataFrame = _latest_ratings(elo, season, as_of_week)
    if latest.empty:
        rating: float | None = None
        rank: int | None = None
    else:
        ranked: DataFrame = latest.sort_values("ELO", ascending=False).reset_index(drop=True)
        team_row: DataFrame = ranked.loc[ranked["NFL_TEAM"] == long_name, :]
        if team_row.empty:
            rating, rank = None, None
        else:
            # pyrefly: ignore [bad-argument-type]
            rank = int(team_row.index[0]) + 1
            rating = float(team_row.iloc[0]["ELO"])

    # ------------------------------------------------------------------
    # Record within the season
    # ------------------------------------------------------------------
    season_games = games.loc[games["YEAR"] == season, :]
    record: TeamRecord = _compute_record(season_games, long_name)

    # ------------------------------------------------------------------
    # Rating history for this team within the season
    # ------------------------------------------------------------------
    hist = elo.loc[
        (elo["NFL_TEAM"] == long_name) & (elo["NFL_YEAR"] == season),
        ["NFL_WEEK", "ELO"],
    ].sort_values("NFL_WEEK")
    history: list[RatingHistoryPoint] | None = (
        [
            RatingHistoryPoint(week=int(r["NFL_WEEK"]), rating=float(r["ELO"]))
            for _, r in hist.iterrows()
        ]
        if not hist.empty
        else None
    )

    resolved_completed_week = (
        completed_through_week if completed_through_week is not None else max(0, as_of_week - 1)
    )
    resolved_rating_week = current_rating_week or as_of_week
    rating_timeline = build_team_rating_timeline(
        elo=elo,
        games=games,
        team_long_name=long_name,
        long_to_short=long_to_short,
        season=season,
        completed_through_week=resolved_completed_week,
        current_rating_week=resolved_rating_week,
        timeline_range=timeline_range,
        forecast_load=forecast_load,
    )

    # ------------------------------------------------------------------
    # Recent results (last 6 games of the season)
    # ------------------------------------------------------------------
    team_games = (
        season_games.loc[
            (season_games["AWAY_TEAM"] == long_name) | (season_games["HOME_TEAM"] == long_name),
            :,
        ]
        .dropna(
            subset=[
                "AWAY_SCORE",
                "HOME_SCORE",
            ]
        )
        .sort_values("WEEK_NUM")
        .tail(6)
    )
    recent: list[RecentResult] | None = (
        [_serialize_result(r, long_name, long_to_short) for _, r in team_games.iterrows()]
        if not team_games.empty
        else None
    )

    # ------------------------------------------------------------------
    # Field-status metadata
    # ------------------------------------------------------------------
    trend = _trend_for_team(trends, abbr.upper())
    meta = ResponseMeta()
    if trend is None:
        meta = meta.with_blocked("trend", *Unavailable.NO_PRIOR_SNAPSHOT)
    if recent is None:
        meta = meta.with_blocked("recent_results", *Unavailable.NO_EVALUATION_DATA)
    if forecast_load is None or forecast_load.state != "available":
        meta = meta.with_blocked("rating_timeline.forecast", *Unavailable.NO_ELO_FORECAST)
    meta = meta.with_blocked("off_rating", *Unavailable.OFF_DEF_DECOMPOSITION)
    meta = meta.with_blocked("def_rating", *Unavailable.OFF_DEF_DECOMPOSITION)
    meta = meta.with_pending("schedule_difficulty")
    meta = meta.with_pending("playoff_probability")
    if cohort_splits is None:
        meta = meta.with_pending("cohort_splits")
    meta = meta.with_blocked("top_players", *Blocker.WAR)

    pcts: dict[str, float | None] = _percentile_for_team(percentiles, abbr.upper())

    return TeamProfile(
        abbr=abbr.upper(),
        name=long_name,
        city=team_meta.get("city"),
        conference=team_meta.get("conference"),
        division=team_meta.get("division"),
        primary_color=team_meta.get("primary_color"),
        secondary_color=team_meta.get("secondary_color"),
        season=season,
        as_of_week=as_of_week,
        rating=rating,
        rank=rank,
        record=record,
        trend=trend,
        rating_pct=pcts["rating_pct"],
        avg_wins_pct=pcts["avg_wins_pct"],
        make_playoffs_pct=pcts["make_playoffs_pct"],
        win_sb_pct=pcts["win_sb_pct"],
        rating_history=history,
        rating_timeline=rating_timeline,
        recent_results=recent,
        cohort_splits=cohort_splits,
        response_meta=meta,  # pyrefly: ignore[unexpected-keyword]
    )
