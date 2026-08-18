"""Tests for exact historical backtest run selection."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pytest

from gridiron_edge.evaluation.backtest_run_selection import (
    create_backtest_run_selection,
)


def _events() -> pd.DataFrame:
    rows = []
    for model_name, model_type, run_id, value_column in (
        ("win_prob", "logistic", "win-run", "home_win_prob"),
        ("total", "random_forest", "total-run", "model_total"),
    ):
        for index, season in enumerate(("2024-2025", "2025-2026"), start=1):
            row: dict[str, object] = {
                "event_id": f"{run_id}-{index}",
                "run_id": run_id,
                "role": "backfilled",
                "season": season,
                "week": index,
                "game_id": f"game-{run_id}-{index}",
                "model_name": model_name,
                "model_type": model_type,
                "home_win_prob": None,
                "model_total": None,
            }
            row[value_column] = 0.6 if value_column == "home_win_prob" else 44.0
            rows.append(row)
    return pd.DataFrame(rows)


def _create(events: pd.DataFrame | None = None):
    return create_backtest_run_selection(
        events=_events() if events is None else events,
        champion_models={"win_prob": "logistic", "total": "random_forest"},
        win_run_id="win-run",
        total_run_id="total-run",
        created_at=datetime(2026, 8, 18, 19, tzinfo=UTC),
    )


def test_creates_content_addressed_exact_run_selection() -> None:
    selection = _create()

    assert selection.win.run_id == "win-run"
    assert selection.win.event_count == 2
    assert selection.win.first_season == "2024-2025"
    assert selection.total.run_id == "total-run"
    assert selection.total.event_count == 2
    assert len(selection.selection_id) == 64
    assert selection.selection_id == _create().selection_id


def test_rejects_missing_run() -> None:
    with pytest.raises(ValueError, match="run is missing"):
        create_backtest_run_selection(
            events=_events(),
            champion_models={"win_prob": "logistic", "total": "random_forest"},
            win_run_id="missing",
            total_run_id="total-run",
            created_at=datetime(2026, 8, 18, 19, tzinfo=UTC),
        )


def test_rejects_non_backfilled_event() -> None:
    events = _events()
    events.loc[events["run_id"].eq("win-run"), "role"] = "live"

    with pytest.raises(ValueError, match="only backfilled"):
        _create(events)


def test_rejects_champion_type_mismatch() -> None:
    with pytest.raises(ValueError, match="champion model_type"):
        create_backtest_run_selection(
            events=_events(),
            champion_models={"win_prob": "random_forest", "total": "random_forest"},
            win_run_id="win-run",
            total_run_id="total-run",
            created_at=datetime(2026, 8, 18, 19, tzinfo=UTC),
        )


def test_rejects_duplicate_game_within_run() -> None:
    events = _events()
    duplicate = events.loc[events["event_id"].eq("win-run-1"), :].copy()
    duplicate["event_id"] = "win-run-duplicate"
    events = pd.concat([events, duplicate], ignore_index=True)

    with pytest.raises(ValueError, match="duplicate game IDs"):
        _create(events)
