"""Tests for immutable historical backtest report persistence."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from gridiron_edge.evaluation.historical_backtest_report import (
    HistoricalBacktestReport,
    create_historical_backtest_report,
)
from gridiron_edge.evaluation.historical_backtest_report_store import (
    verify_historical_backtest_report,
    write_historical_backtest_report,
)
from gridiron_edge.evaluation.historical_backtest_summary import (
    build_historical_backtest_series,
    summarize_historical_backtest,
)


def _evidence() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "selection_id": ["a" * 64],
            "season": ["2025-2026"],
            "week": [1],
            "game_id": ["g1"],
            "game_date": ["2025-09-01"],
            "moneyline_evaluable": [True],
            "moneyline_correct": [True],
            "moneyline_squared_error": [0.04],
            "moneyline_log_loss": [0.22],
            "total_evaluable": [True],
            "total_outcome": ["win"],
            "total_absolute_error": [3.0],
            "total_error": [-3.0],
            "total_unit_return": [100 / 110],
            "total_assumed_american_price": [-110],
        }
    )


def _report() -> tuple[HistoricalBacktestReport, pd.DataFrame, pd.DataFrame]:
    evidence = _evidence()
    series = build_historical_backtest_series(evidence)
    summary = summarize_historical_backtest(evidence)
    report = create_historical_backtest_report(
        generated_at=datetime(2026, 8, 18, 20, tzinfo=UTC),
        run_selection_id="a" * 64,
        win_model_type="logistic",
        win_run_id="win-run",
        total_model_type="random_forest",
        total_run_id="total-run",
        summary=summary,
        evidence=evidence,
        series=series,
        evidence_artifact="schema=1/evidence/selection-invocation.parquet",
        series_artifact="schema=1/series/selection-invocation.parquet",
    )
    return report, evidence, series


def test_exact_round_trip_and_replay(tmp_path: Path) -> None:
    report, evidence, series = _report()
    path = write_historical_backtest_report(
        report,
        evidence=evidence,
        series=series,
        repo=tmp_path,
    )
    first = path.read_bytes()
    loaded_evidence, loaded_series = verify_historical_backtest_report(
        report,
        repo=tmp_path,
    )

    assert (
        write_historical_backtest_report(
            report,
            evidence=evidence,
            series=series,
            repo=tmp_path,
        )
        == path
    )
    assert path.read_bytes() == first
    pd.testing.assert_frame_equal(loaded_evidence, evidence)
    pd.testing.assert_frame_equal(loaded_series, series)


def test_tampered_frame_is_rejected(tmp_path: Path) -> None:
    report, evidence, series = _report()
    write_historical_backtest_report(
        report,
        evidence=evidence,
        series=series,
        repo=tmp_path,
    )
    evidence_path = tmp_path / "data/output/model_performance" / report.evidence.artifact
    changed = evidence.copy()
    changed.loc[0, "total_error"] = 99.0
    changed.to_parquet(evidence_path, index=False)

    with pytest.raises(ValueError, match="digest"):
        verify_historical_backtest_report(report, repo=tmp_path)


def test_conflicting_frame_replay_is_rejected(tmp_path: Path) -> None:
    report, evidence, series = _report()
    write_historical_backtest_report(
        report,
        evidence=evidence,
        series=series,
        repo=tmp_path,
    )
    changed = evidence.copy()
    changed.loc[0, "total_error"] = 99.0

    with pytest.raises(ValueError, match="digest"):
        write_historical_backtest_report(
            report,
            evidence=changed,
            series=series,
            repo=tmp_path,
        )
