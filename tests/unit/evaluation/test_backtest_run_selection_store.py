"""Tests for immutable historical backtest run-selection storage."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
import json
from pathlib import Path

import pandas as pd
import pytest

from gridiron_edge.evaluation.backtest_run_selection import create_backtest_run_selection
from gridiron_edge.evaluation.backtest_run_selection_store import (
    read_backtest_run_selection,
    write_backtest_run_selection,
)


def _selection():
    events = pd.DataFrame(
        [
            {
                "event_id": "win-event",
                "run_id": "win-run",
                "role": "backfilled",
                "season": "2025-2026",
                "week": 1,
                "game_id": "win-game",
                "model_name": "win_prob",
                "model_type": "logistic",
            },
            {
                "event_id": "total-event",
                "run_id": "total-run",
                "role": "backfilled",
                "season": "2025-2026",
                "week": 1,
                "game_id": "total-game",
                "model_name": "total",
                "model_type": "random_forest",
            },
        ]
    )
    return create_backtest_run_selection(
        events=events,
        champion_models={"win_prob": "logistic", "total": "random_forest"},
        win_run_id="win-run",
        total_run_id="total-run",
        created_at=datetime(2026, 8, 18, 19, tzinfo=UTC),
    )


def test_round_trips_and_accepts_exact_replay(tmp_path: Path) -> None:
    selection = _selection()
    path = write_backtest_run_selection(selection, repo=tmp_path)
    original = path.read_bytes()

    assert write_backtest_run_selection(selection, repo=tmp_path) == path
    assert path.read_bytes() == original
    assert read_backtest_run_selection(path) == selection


def test_rejects_identity_content_conflict(tmp_path: Path) -> None:
    selection = _selection()
    path = write_backtest_run_selection(selection, repo=tmp_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["win"]["event_count"] = 99
    path.write_text(json.dumps(raw, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="selection_id does not match"):
        read_backtest_run_selection(path)


def test_rejects_wrong_identity_path(tmp_path: Path) -> None:
    selection = _selection()
    path = write_backtest_run_selection(selection, repo=tmp_path)
    wrong = path.with_name("0" * 64 + ".json")
    path.replace(wrong)

    with pytest.raises(ValueError, match="path and embedded identity disagree"):
        read_backtest_run_selection(wrong)


def test_rejects_invalid_selection_before_write(tmp_path: Path) -> None:
    selection = replace(_selection(), selection_id="0" * 64)

    with pytest.raises(ValueError, match="does not match selection content"):
        write_backtest_run_selection(selection, repo=tmp_path)
