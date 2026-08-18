"""Tests for historical backtest report build orchestration."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from gridiron_edge.evaluation.backtest_run_selection import (
    create_backtest_run_selection,
)
from gridiron_edge.evaluation.backtest_run_selection_store import (
    write_backtest_run_selection,
)
from gridiron_edge.evaluation.historical_backtest_report_builder import (
    build_and_write_historical_backtest_report,
)
from gridiron_edge.evaluation.historical_backtest_report_store import (
    verify_historical_backtest_report,
)


def _events() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for game_id, probability, total in (
        ("g1", 0.70, 44.0),
        ("g2", 0.40, 41.0),
    ):
        common: dict[str, object] = {
            "role": "backfilled",
            "season": "2025-2026",
            "week": 1,
            "game_id": game_id,
            "game_date": "2025-09-01",
            "away_team": f"Away {game_id}",
            "home_team": f"Home {game_id}",
        }
        rows.append(
            common
            | {
                "event_id": f"win-{game_id}",
                "run_id": "win-run",
                "model_name": "win_prob",
                "model_type": "logistic",
                "home_win_prob": probability,
                "model_total": None,
            }
        )
        rows.append(
            common
            | {
                "event_id": f"total-{game_id}",
                "run_id": "total-run",
                "model_name": "total",
                "model_type": "random_forest",
                "home_win_prob": None,
                "model_total": total,
            }
        )
    return pd.DataFrame(rows)


def _games() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "GAME_ID": ["g1", "g2"],
            "YEAR": ["2025-2026", "2025-2026"],
            "WEEK_NUM": [1, 1],
            "GAME_DATE": ["2025-09-01", "2025-09-01"],
            "AWAY_TEAM": ["Away g1", "Away g2"],
            "HOME_TEAM": ["Home g1", "Home g2"],
            "AWAY_SCORE": [20, 17],
            "HOME_SCORE": [27, 21],
            "OVER_UNDER": [40.0, 40.0],
        }
    )


def _selection_path(tmp_path: Path, events: pd.DataFrame) -> Path:
    selection = create_backtest_run_selection(
        events=events,
        champion_models={
            "win_prob": "logistic",
            "total": "random_forest",
        },
        win_run_id="win-run",
        total_run_id="total-run",
        created_at=datetime(2026, 8, 18, 20, tzinfo=UTC),
    )
    return write_backtest_run_selection(selection, repo=tmp_path)


def test_builds_persists_and_verifies_exact_report(tmp_path: Path) -> None:
    events = _events()
    selection_path = _selection_path(tmp_path, events)
    generated_at = datetime(2026, 8, 18, 21, tzinfo=UTC)

    with (
        patch(
            "gridiron_edge.evaluation.historical_backtest_report_builder.load_forecast_events",
            return_value=events,
        ),
        patch(
            "gridiron_edge.evaluation.historical_backtest_report_builder.load_games",
            return_value=_games(),
        ),
    ):
        result = build_and_write_historical_backtest_report(
            selection_path=selection_path,
            generated_at=generated_at,
            repo=tmp_path,
        )

    assert result.manifest_path.is_file()
    assert result.evidence_row_count == 2
    assert result.series_row_count == 2
    assert result.report.summary.moneyline.evaluated_count == 2
    assert result.report.summary.total.decision_count == 2
    assert result.report.evidence.artifact.endswith("-20260818T210000000000Z.parquet")
    evidence, series = verify_historical_backtest_report(
        result.report,
        repo=tmp_path,
    )
    assert len(evidence) == 2
    assert len(series) == 2


def test_rejects_non_utc_generation_timestamp(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="timezone-aware UTC"):
        build_and_write_historical_backtest_report(
            selection_path=tmp_path / "missing.json",
            generated_at=datetime(2026, 8, 18, 21),
            repo=tmp_path,
        )
