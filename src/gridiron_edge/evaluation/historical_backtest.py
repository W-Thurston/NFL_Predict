"""Pure historical walk-forward Moneyline and Total backtest evidence."""

from __future__ import annotations

import math
from typing import Any, Final, cast

import pandas as pd
from pandas import DataFrame, Series

from gridiron_edge.evaluation.backtest_run_selection import (
    BacktestRunComponent,
    BacktestRunSelection,
    validate_backtest_run_selection,
)
from gridiron_edge.market.candidate_outcome import CandidateOutcome

ASSUMED_STANDARD_AMERICAN_PRICE: Final[int] = -110
_STANDARD_PRICE_WIN_RETURN: Final[float] = 100.0 / 110.0
_EVENT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "event_id",
        "run_id",
        "role",
        "season",
        "week",
        "game_id",
        "game_date",
        "away_team",
        "home_team",
        "model_name",
        "model_type",
        "home_win_prob",
        "model_total",
    }
)
_GAME_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "GAME_ID",
        "YEAR",
        "WEEK_NUM",
        "GAME_DATE",
        "AWAY_TEAM",
        "HOME_TEAM",
        "AWAY_SCORE",
        "HOME_SCORE",
        "OVER_UNDER",
    }
)


def build_historical_backtest_evidence(
    *,
    selection: BacktestRunSelection,
    forecast_events: DataFrame,
    games: DataFrame,
) -> DataFrame:
    """Build one immutable-input evidence row per game in either selected run."""
    validate_backtest_run_selection(selection)
    events = _validated_events(forecast_events)
    game_rows = _validated_games(games)
    win = _selected_events(events, selection.win)
    total = _selected_events(events, selection.total)

    win = win.rename(
        columns={
            "event_id": "win_event_id",
            "run_id": "win_run_id",
            "season": "win_season",
            "week": "win_week",
            "game_date": "win_game_date",
            "away_team": "win_away_team",
            "home_team": "win_home_team",
        }
    )
    total = total.rename(
        columns={
            "event_id": "total_event_id",
            "run_id": "total_run_id",
            "season": "total_season",
            "week": "total_week",
            "game_date": "total_game_date",
            "away_team": "total_away_team",
            "home_team": "total_home_team",
        }
    )
    evidence = win.loc[
        :,
        [
            "game_id",
            "win_event_id",
            "win_run_id",
            "win_season",
            "win_week",
            "win_game_date",
            "win_away_team",
            "win_home_team",
            "home_win_prob",
        ],
    ].merge(
        total.loc[
            :,
            [
                "game_id",
                "total_event_id",
                "total_run_id",
                "total_season",
                "total_week",
                "total_game_date",
                "total_away_team",
                "total_home_team",
                "model_total",
            ],
        ],
        on="game_id",
        how="outer",
        validate="one_to_one",
    )
    evidence = evidence.merge(
        game_rows,
        left_on="game_id",
        right_on="GAME_ID",
        how="left",
        validate="one_to_one",
    )
    return _derive_evidence(evidence, selection.selection_id)


def _validated_events(events: DataFrame) -> DataFrame:
    missing = sorted(_EVENT_COLUMNS - set(events.columns))
    if missing:
        raise ValueError("Forecast events are missing columns: " + ", ".join(missing))
    rows = events.loc[:, sorted(_EVENT_COLUMNS)].copy(deep=True)
    if rows["event_id"].astype(str).duplicated().any():
        raise ValueError("Forecast events contain duplicate event IDs.")
    return rows


def _validated_games(games: DataFrame) -> DataFrame:
    missing = sorted(_GAME_COLUMNS - set(games.columns))
    if missing:
        raise ValueError("Completed games are missing columns: " + ", ".join(missing))
    rows = games.loc[:, sorted(_GAME_COLUMNS)].copy(deep=True)
    if rows["GAME_ID"].astype(str).duplicated().any():
        raise ValueError("Completed games contain duplicate GAME_ID values.")
    return rows


def _selected_events(events: DataFrame, component: BacktestRunComponent) -> DataFrame:
    rows = events.loc[events["run_id"].astype(str).eq(component.run_id), :].copy()
    if len(rows) != component.event_count:
        raise ValueError(f"Selected run {component.run_id!r} event count does not match selection.")
    if set(rows["role"].astype(str)) != {"backfilled"}:
        raise ValueError("Selected historical run must contain only backfilled events.")
    if set(rows["model_name"].astype(str)) != {component.model_name}:
        raise ValueError("Selected historical run model_name does not match selection.")
    if set(rows["model_type"].astype(str)) != {component.model_type}:
        raise ValueError("Selected historical run model_type does not match selection.")
    if rows["game_id"].astype(str).duplicated().any():
        raise ValueError("Selected historical run contains duplicate game IDs.")
    return rows


def _derive_evidence(rows: DataFrame, selection_id: str) -> DataFrame:
    records = [_derive_row(row, selection_id) for _, row in rows.iterrows()]
    result = DataFrame.from_records(records)
    return result.sort_values(["season", "week", "game_id"], kind="stable").reset_index(drop=True)


def _derive_row(row: Series, selection_id: str) -> dict[str, object]:
    game_available = pd.notna(row.get("GAME_ID"))
    _validate_join_identity(row, game_available=game_available)
    away_score = _finite_or_none(row.get("AWAY_SCORE"))
    home_score = _finite_or_none(row.get("HOME_SCORE"))
    outcome_available = away_score is not None and home_score is not None
    actual_total = away_score + home_score if outcome_available else None
    actual_home_margin = home_score - away_score if outcome_available else None

    home_probability = _finite_or_none(row.get("home_win_prob"))
    if home_probability is not None and not 0.0 <= home_probability <= 1.0:
        raise ValueError("Historical home_win_prob must be between 0 and 1.")
    tied = outcome_available and away_score == home_score
    moneyline_evaluable = home_probability is not None and outcome_available and not tied
    actual_home_win: bool | None = None
    predicted_home_win: bool | None = None
    moneyline_correct: bool | None = None
    squared_error: float | None = None
    log_loss: float | None = None
    if moneyline_evaluable:
        assert away_score is not None
        assert home_score is not None
        assert home_probability is not None
        actual_home_win = home_score > away_score
        predicted_home_win = home_probability >= 0.5
        moneyline_correct = predicted_home_win == actual_home_win
        squared_error = (home_probability - float(actual_home_win)) ** 2
        log_loss = _log_loss(home_probability, actual_home_win)

    model_total = _finite_or_none(row.get("model_total"))
    market_total = _finite_or_none(row.get("OVER_UNDER"))
    total_evaluable = (
        model_total is not None and market_total is not None and actual_total is not None
    )
    total_error: float | None = None
    total_side: str | None = None
    total_outcome: CandidateOutcome | None = None
    if total_evaluable:
        assert model_total is not None
        assert market_total is not None
        assert actual_total is not None
        total_error = model_total - actual_total
        total_side = _total_side(model_total, market_total)
        total_outcome = _total_outcome(total_side, actual_total, market_total)
    total_return = _one_unit_return(total_outcome)

    season = _coalesce_text(row.get("YEAR"), row.get("win_season"), row.get("total_season"))
    week = _coalesce_int(row.get("WEEK_NUM"), row.get("win_week"), row.get("total_week"))
    return {
        "selection_id": selection_id,
        "season": season,
        "week": week,
        "game_id": str(row["game_id"]),
        "game_date": _coalesce_text(
            row.get("GAME_DATE"), row.get("win_game_date"), row.get("total_game_date")
        ),
        "away_team": _coalesce_text(
            row.get("AWAY_TEAM"), row.get("win_away_team"), row.get("total_away_team")
        ),
        "home_team": _coalesce_text(
            row.get("HOME_TEAM"), row.get("win_home_team"), row.get("total_home_team")
        ),
        "away_score": away_score,
        "home_score": home_score,
        "actual_home_margin": actual_home_margin,
        "actual_total": actual_total,
        "win_event_id": _text_or_none(row.get("win_event_id")),
        "win_run_id": _text_or_none(row.get("win_run_id")),
        "home_win_prob": home_probability,
        "actual_home_win": actual_home_win,
        "moneyline_evaluable": moneyline_evaluable,
        "moneyline_correct": moneyline_correct,
        "moneyline_squared_error": squared_error,
        "moneyline_log_loss": log_loss,
        "moneyline_unit_return": None,
        "total_event_id": _text_or_none(row.get("total_event_id")),
        "total_run_id": _text_or_none(row.get("total_run_id")),
        "model_total": model_total,
        "market_total": market_total,
        "total_error": total_error,
        "total_absolute_error": abs(total_error) if total_error is not None else None,
        "total_side": total_side,
        "total_outcome": total_outcome.value if total_outcome is not None else None,
        "total_evaluable": total_evaluable,
        "total_unit_return": total_return,
        "total_assumed_american_price": (
            ASSUMED_STANDARD_AMERICAN_PRICE if total_side not in {None, "no_bet"} else None
        ),
    }


def _validate_join_identity(row: Series, *, game_available: bool) -> None:
    if not game_available:
        return
    pairs = (
        ("YEAR", "win_season"),
        ("YEAR", "total_season"),
        ("WEEK_NUM", "win_week"),
        ("WEEK_NUM", "total_week"),
        ("AWAY_TEAM", "win_away_team"),
        ("AWAY_TEAM", "total_away_team"),
        ("HOME_TEAM", "win_home_team"),
        ("HOME_TEAM", "total_home_team"),
    )
    for game_column, event_column in pairs:
        event_value = row.get(event_column)
        if pd.notna(event_value) and str(row[game_column]) != str(event_value):
            raise ValueError(
                f"Historical event {event_column} does not match completed game {game_column}."
            )


def _total_side(model_total: float, market_total: float) -> str:
    if model_total > market_total:
        return "over"
    if model_total < market_total:
        return "under"
    return "no_bet"


def _total_outcome(
    side: str,
    actual_total: float,
    market_total: float,
) -> CandidateOutcome | None:
    if side == "no_bet":
        return None
    if actual_total == market_total:
        return CandidateOutcome.PUSH
    if side == "over":
        return CandidateOutcome.WIN if actual_total > market_total else CandidateOutcome.LOSS
    return CandidateOutcome.WIN if actual_total < market_total else CandidateOutcome.LOSS


def _one_unit_return(outcome: CandidateOutcome | None) -> float | None:
    if outcome is CandidateOutcome.WIN:
        return _STANDARD_PRICE_WIN_RETURN
    if outcome is CandidateOutcome.LOSS:
        return -1.0
    if outcome is CandidateOutcome.PUSH:
        return 0.0
    return None


def _log_loss(probability: float, actual_home_win: bool) -> float:
    clipped = min(max(probability, 1e-7), 1 - 1e-7)
    outcome = float(actual_home_win)
    return -(outcome * math.log(clipped) + (1 - outcome) * math.log(1 - clipped))


def _is_scalar_null(value: object) -> bool:
    """Return whether one DataFrame-cell value is a recognized scalar null."""
    if value is None:
        return True
    return bool(pd.isna(cast(Any, value)))


def _finite_or_none(value: object) -> float | None:
    if _is_scalar_null(value):
        return None
    try:
        result = float(cast(float | int | str, value))
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _text_or_none(value: object) -> str | None:
    return None if _is_scalar_null(value) else str(value)


def _coalesce_text(*values: object) -> str:
    concrete = [str(value) for value in values if not _is_scalar_null(value)]
    if not concrete:
        raise ValueError("Historical evidence row has no textual scope value.")
    if len(set(concrete)) != 1:
        raise ValueError("Historical evidence row contains conflicting textual scope values.")
    return concrete[0]


def _coalesce_int(*values: object) -> int:
    concrete = [int(cast(int | str, value)) for value in values if not _is_scalar_null(value)]
    if not concrete:
        raise ValueError("Historical evidence row has no numeric scope value.")
    if len(set(concrete)) != 1:
        raise ValueError("Historical evidence row contains conflicting numeric scope values.")
    return concrete[0]
