# src/gridiron_edge/api/schemas/teams.py

"""Schemas for /teams and /teams/{abbr}."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from gridiron_edge.api.schemas._base import BaseListResponse, BaseResponse


class TeamRecord(BaseModel):
    """Win/loss/tie record for a team within a season."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    wins: int = 0
    losses: int = 0
    ties: int = 0


class TeamRankingRow(BaseModel):
    """A single row in the /teams power rankings list."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    abbr: str
    name: str
    city: str | None = None
    conference: str | None = None
    division: str | None = None
    primary_color: str | None = None
    secondary_color: str | None = None
    rating: float | None = None
    rank: int | None = None
    record: TeamRecord | None = None
    trend: float | None = None
    off_rating: float | None = None
    def_rating: float | None = None
    rating_pct: float | None = Field(
        default=None,
        description="Percentile rank of Elo rating within the league (0-1).",
    )
    avg_wins_pct: float | None = Field(
        default=None,
        description="Percentile rank of projected average wins (0-1).",
    )
    make_playoffs_pct: float | None = Field(
        default=None,
        description="Percentile rank of playoff probability (0-1).",
    )
    win_sb_pct: float | None = Field(
        default=None,
        description="Percentile rank of Super Bowl win probability (0-1).",
    )


class TeamRankingsList(BaseListResponse[TeamRankingRow]):
    """Response for GET /teams."""

    season: str | None = Field(default=None)
    as_of_week: int | None = Field(default=None)


class RatingHistoryPoint(BaseModel):
    """A single (week, rating) point in the team's Elo trajectory."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    week: int
    rating: float


class TeamRatingTimelinePoint(BaseModel):
    """One season-aware entering-week Elo state on a team timeline."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    season: str
    week: int = Field(ge=1, le=22)
    date: str | None = None
    rating: float | None = None
    state: Literal["observed", "carried_forward", "current", "forecast", "unavailable"]
    game_played: bool = False
    result: Literal["W", "L", "T"] | None = None
    opponent: str | None = None
    lower_rating: float | None = None
    upper_rating: float | None = None
    win_out_rating: float | None = None
    lose_out_rating: float | None = None


class TeamRatingSeasonFinal(BaseModel):
    """Semantic final postgame Elo state after the Super Bowl."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    season: str
    rating: float
    source_week: Literal[22] = 22
    game_played: bool = True
    result: Literal["W", "L", "T"] | None = None
    opponent: str | None = None


class TeamRatingOffseasonTransition(BaseModel):
    """Deterministic regression from one season's final state to Week 1."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["offseason_adjustment"] = "offseason_adjustment"
    from_season: str
    from_rating: float
    to_season: str
    to_week: Literal[1] = 1
    to_rating: float


class TeamRatingTimeline(BaseModel):
    """Historical team Elo timeline with explicit temporal semantics."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    range: Literal["season", "recent"]
    completed_through_week: int = Field(ge=0, le=22)
    current_rating_week: int = Field(ge=1, le=22)
    points: list[TeamRatingTimelinePoint]
    prior_season_final: TeamRatingSeasonFinal | None = None
    offseason_transition: TeamRatingOffseasonTransition | None = None
    forecast_computed_at: str | None = None
    forecast_simulation_count: int | None = None
    forecast_lower_quantile: float | None = None
    forecast_center_quantile: float | None = None
    forecast_upper_quantile: float | None = None
    forecast_quantile_method: str | None = None


class RecentResult(BaseModel):
    """A single completed game in the team's recent history."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    week: int
    date: str | None = None
    opponent: str | None = None
    is_home: bool | None = None
    result: str | None = Field(default=None, description="'W', 'L', or 'T'.")
    score_for: int | None = None
    score_against: int | None = None


class TeamProfile(BaseResponse):
    """Response for GET /teams/{abbr}."""

    abbr: str
    name: str
    city: str | None = None
    conference: str | None = None
    division: str | None = None
    primary_color: str | None = None
    secondary_color: str | None = None
    season: str | None = None
    as_of_week: int | None = None
    rating: float | None = None
    rank: int | None = None
    record: TeamRecord | None = None
    trend: float | None = None
    off_rating: float | None = None
    def_rating: float | None = None
    rating_pct: float | None = Field(
        default=None,
        description="Percentile rank of Elo rating within the league (0-1).",
    )
    avg_wins_pct: float | None = Field(
        default=None,
        description="Percentile rank of projected average wins (0-1).",
    )
    make_playoffs_pct: float | None = Field(
        default=None,
        description="Percentile rank of playoff probability (0-1).",
    )
    win_sb_pct: float | None = Field(
        default=None,
        description="Percentile rank of Super Bowl win probability (0-1).",
    )
    rating_history: list[RatingHistoryPoint] | None = None
    rating_timeline: TeamRatingTimeline | None = None
    recent_results: list[RecentResult] | None = None
    schedule_difficulty: float | None = None
    playoff_probability: float | None = None
    top_players: list[dict] | None = Field(
        default=None,
        description="Top players by WAR — blocked pending WAR computation.",
    )
    cohort_splits: dict | None = Field(
        default=None,
        description=(
            "Per-team cohort splits: {cohort_name: {metric: value, "
            "'rank_metric': int, 'sample_size': int}}. Cohorts include "
            "season, l4, home, away. Populated from team_cohort_splits.parquet."
        ),
    )
