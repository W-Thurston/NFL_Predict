"""Build and persist one historical model-performance report."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from pandas import DataFrame

from gridiron_edge.datasets.loaders import load_games
from gridiron_edge.evaluation.backtest_run_selection_store import (
    read_backtest_run_selection,
)
from gridiron_edge.evaluation.forecast_store import load_forecast_events
from gridiron_edge.evaluation.historical_backtest import (
    build_historical_backtest_evidence,
)
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


@dataclass(frozen=True, slots=True)
class HistoricalBacktestReportBuildResult:
    """Accounting for one persisted and replay-verified report build."""

    report: HistoricalBacktestReport
    manifest_path: Path
    evidence_row_count: int
    series_row_count: int


def build_and_write_historical_backtest_report(
    *,
    selection_path: Path,
    generated_at: datetime,
    repo: Path,
) -> HistoricalBacktestReportBuildResult:
    """Compose validated owners and persist one exact historical report."""
    _require_utc(generated_at)
    selection = read_backtest_run_selection(selection_path)
    events = load_forecast_events(repo=repo)
    games = load_games(repo)
    evidence = build_historical_backtest_evidence(
        selection=selection,
        forecast_events=events,
        games=games,
    )
    summary = summarize_historical_backtest(evidence)
    series = build_historical_backtest_series(evidence)
    token = _artifact_token(selection.selection_id, generated_at)
    report = create_historical_backtest_report(
        generated_at=generated_at,
        run_selection_id=selection.selection_id,
        win_model_type=selection.win.model_type,
        win_run_id=selection.win.run_id,
        total_model_type=selection.total.model_type,
        total_run_id=selection.total.run_id,
        summary=summary,
        evidence=evidence,
        series=series,
        evidence_artifact=f"schema=1/evidence/{token}.parquet",
        series_artifact=f"schema=1/series/{token}.parquet",
    )
    manifest_path = write_historical_backtest_report(
        report,
        evidence=evidence,
        series=series,
        repo=repo,
    )
    stored_evidence, stored_series = verify_historical_backtest_report(
        report,
        repo=repo,
    )
    _require_exact_replay(
        expected=evidence,
        stored=stored_evidence,
        label="evidence",
    )
    _require_exact_replay(
        expected=series,
        stored=stored_series,
        label="series",
    )
    return HistoricalBacktestReportBuildResult(
        report=report,
        manifest_path=manifest_path,
        evidence_row_count=len(stored_evidence),
        series_row_count=len(stored_series),
    )


def _artifact_token(selection_id: str, generated_at: datetime) -> str:
    """Return one filename-safe invocation identity."""
    timestamp = generated_at.strftime("%Y%m%dT%H%M%S%fZ")
    return f"{selection_id}-{timestamp}"


def _require_utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("generated_at must be timezone-aware UTC.")


def _require_exact_replay(
    *,
    expected: DataFrame,
    stored: DataFrame,
    label: str,
) -> None:
    if not stored.equals(expected):
        raise ValueError(f"Stored historical {label} does not exactly replay input.")
