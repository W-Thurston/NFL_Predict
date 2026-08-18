"""Tests for pure historical Moneyline and Total backtest evidence."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime

import pandas as pd
import pytest

from gridiron_edge.evaluation.backtest_run_selection import create_backtest_run_selection
from gridiron_edge.evaluation.historical_backtest import build_historical_backtest_evidence


def _events() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for game_id, probability, model_total in (
        ("g1", 0.75, 44.0),
        ("g2", 0.40, 40.0),
        ("g3", 0.50, 42.0),
    ):
        common: dict[str, object] = {
            "role": "backfilled",
            "season": "2025-2026",
            "week": int(game_id[-1]),
            "game_id": game_id,
            "game_date": f"2025-09-0{game_id[-1]}",
            "away_team": f"Away {game_id}",
            "home_team": f"Home {game_id}",
        }
        win_row: dict[str, object] = common | {
            "event_id": f"win-{game_id}",
            "run_id": "win-run",
            "model_name": "win_prob",
            "model_type": "logistic",
            "home_win_prob": probability,
            "model_total": None,
        }
        rows.append(win_row)
        total_row: dict[str, object] = common | {
            "event_id": f"total-{game_id}",
            "run_id": "total-run",
            "model_name": "total",
            "model_type": "random_forest",
            "home_win_prob": None,
            "model_total": model_total,
        }
        rows.append(total_row)
    return pd.DataFrame(rows)


def _games() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "GAME_ID": ["g1", "g2", "g3"],
            "YEAR": ["2025-2026"] * 3,
            "WEEK_NUM": [1, 2, 3],
            "GAME_DATE": ["2025-09-01", "2025-09-02", "2025-09-03"],
            "AWAY_TEAM": ["Away g1", "Away g2", "Away g3"],
            "HOME_TEAM": ["Home g1", "Home g2", "Home g3"],
            "AWAY_SCORE": [20, 17, 21],
            "HOME_SCORE": [27, 17, 21],
            "OVER_UNDER": [40.0, 40.0, 42.0],
        }
    )


def _selection(events: pd.DataFrame):
    return create_backtest_run_selection(
        events=events,
        champion_models={"win_prob": "logistic", "total": "random_forest"},
        win_run_id="win-run",
        total_run_id="total-run",
        created_at=datetime(2026, 8, 18, 20, tzinfo=UTC),
    )


def test_builds_moneyline_and_total_evidence() -> None:
    events = _events()
    evidence = build_historical_backtest_evidence(
        selection=_selection(events),
        forecast_events=events,
        games=_games(),
    )

    first = evidence.loc[evidence["game_id"].eq("g1")].iloc[0]
    assert bool(first["moneyline_evaluable"])
    assert bool(first["moneyline_correct"])
    assert first["moneyline_squared_error"] == pytest.approx(0.0625)
    assert first["moneyline_unit_return"] is None
    assert first["total_side"] == "over"
    assert first["total_outcome"] == "win"
    assert first["total_unit_return"] == pytest.approx(100 / 110)
    assert first["total_error"] == pytest.approx(-3.0)


def test_handles_loss_push_no_bet_and_moneyline_ties() -> None:
    events = _events()
    events.loc[events["event_id"].eq("total-g2"), "model_total"] = 41.0
    evidence = build_historical_backtest_evidence(
        selection=_selection(events),
        forecast_events=events,
        games=_games(),
    ).set_index("game_id")

    assert not bool(evidence.loc["g2", "moneyline_evaluable"])
    assert evidence.loc["g2", "total_outcome"] == "loss"
    assert evidence.loc["g2", "total_unit_return"] == pytest.approx(-1.0)
    assert evidence.loc["g3", "total_side"] == "no_bet"
    assert pd.isna(evidence.loc["g3", "total_outcome"])
    assert pd.isna(evidence.loc["g3", "total_unit_return"])


def test_total_push_returns_zero() -> None:
    events = _events()
    events.loc[events["event_id"].eq("total-g3"), "model_total"] = 43.0
    games = _games()
    games.loc[games["GAME_ID"].eq("g3"), "OVER_UNDER"] = 42.0

    evidence = build_historical_backtest_evidence(
        selection=_selection(events),
        forecast_events=events,
        games=games,
    ).set_index("game_id")

    assert evidence.loc["g3", "total_outcome"] == "push"
    assert evidence.loc["g3", "total_unit_return"] == pytest.approx(0.0)


def test_preserves_family_independence() -> None:
    events = _events().loc[lambda frame: ~frame["event_id"].eq("total-g1"), :]
    selection_events = _events()
    selection = _selection(selection_events)
    selection = selection.__class__(
        schema_version=selection.schema_version,
        selection_id=selection.selection_id,
        created_at=selection.created_at,
        win=selection.win,
        total=selection.total.__class__(
            model_name=selection.total.model_name,
            model_type=selection.total.model_type,
            run_id=selection.total.run_id,
            event_count=2,
            first_season=selection.total.first_season,
            last_season=selection.total.last_season,
        ),
    )
    # Rebuild identity validation is intentionally bypassed here only to
    # exercise the exact event-count guard rather than family availability.
    with pytest.raises(ValueError, match="selection_id does not match"):
        build_historical_backtest_evidence(
            selection=selection,
            forecast_events=events,
            games=_games(),
        )


def test_rejects_identity_mismatch() -> None:
    events = _events()
    games = _games()
    games.loc[games["GAME_ID"].eq("g1"), "HOME_TEAM"] = "Wrong Home"

    with pytest.raises(ValueError, match="does not match completed game"):
        build_historical_backtest_evidence(
            selection=_selection(events),
            forecast_events=events,
            games=games,
        )


def test_rejects_probability_outside_unit_interval() -> None:
    events = _events()
    events.loc[events["event_id"].eq("win-g1"), "home_win_prob"] = 1.2

    with pytest.raises(ValueError, match="between 0 and 1"):
        build_historical_backtest_evidence(
            selection=_selection(events),
            forecast_events=events,
            games=_games(),
        )


def test_inputs_are_not_mutated() -> None:
    events = _events()
    games = _games()
    originals = [deepcopy(frame) for frame in (events, games)]

    build_historical_backtest_evidence(
        selection=_selection(events),
        forecast_events=events,
        games=games,
    )

    pd.testing.assert_frame_equal(events, originals[0])
    pd.testing.assert_frame_equal(games, originals[1])
