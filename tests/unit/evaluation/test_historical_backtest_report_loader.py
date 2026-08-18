"""Tests for strict historical backtest report loading."""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path

import pandas as pd
import pytest

from gridiron_edge.evaluation.historical_backtest_report import (
    create_historical_backtest_report,
)
from gridiron_edge.evaluation.historical_backtest_report_loader import (
    load_current_historical_backtest_report,
    read_historical_backtest_report,
)
from gridiron_edge.evaluation.historical_backtest_report_selection import (
    select_current_historical_backtest_report,
)
from gridiron_edge.evaluation.historical_backtest_report_store import (
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


def _stored_report(tmp_path: Path) -> tuple[Path, pd.DataFrame, pd.DataFrame]:
    evidence = _evidence()
    series = build_historical_backtest_series(evidence)
    report = create_historical_backtest_report(
        generated_at=datetime(2026, 8, 18, 20, tzinfo=UTC),
        run_selection_id="a" * 64,
        win_model_type="logistic",
        win_run_id="win-run",
        total_model_type="random_forest",
        total_run_id="total-run",
        summary=summarize_historical_backtest(evidence),
        evidence=evidence,
        series=series,
        evidence_artifact="schema=1/evidence/report.parquet",
        series_artifact="schema=1/series/report.parquet",
    )
    path = write_historical_backtest_report(
        report,
        evidence=evidence,
        series=series,
        repo=tmp_path,
    )
    return path, evidence, series


def test_strict_report_round_trip(tmp_path: Path) -> None:
    path, _, _ = _stored_report(tmp_path)

    report = read_historical_backtest_report(path)

    assert report.summary.moneyline.win_count == 1
    assert report.summary.moneyline.unit_return_status.value == "unavailable"
    assert report.summary.total.price_evidence_status.value == "assumed"
    assert report.total_model_type == "random_forest"


def test_loads_current_report_and_verified_frames(tmp_path: Path) -> None:
    path, evidence, series = _stored_report(tmp_path)
    report = read_historical_backtest_report(path)
    selected_at = datetime(2026, 8, 18, 21, tzinfo=UTC)
    select_current_historical_backtest_report(
        report.report_id,
        selected_at=selected_at,
        repo=tmp_path,
    )

    current = load_current_historical_backtest_report(repo=tmp_path)

    assert current.report == report
    assert current.selected_at == selected_at
    pd.testing.assert_frame_equal(current.evidence, evidence)
    pd.testing.assert_frame_equal(current.series, series)


def test_manifest_extra_key_is_rejected(tmp_path: Path) -> None:
    path, _, _ = _stored_report(tmp_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["extra"] = True
    path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="keys"):
        read_historical_backtest_report(path)


def test_embedded_report_identity_mismatch_is_rejected(tmp_path: Path) -> None:
    path, _, _ = _stored_report(tmp_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["report_id"] = "b" * 64
    path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="identity"):
        read_historical_backtest_report(path)


def test_wrong_manifest_filename_is_rejected(tmp_path: Path) -> None:
    path, _, _ = _stored_report(tmp_path)
    wrong = path.with_name("b" * 64 + ".json")
    path.replace(wrong)

    with pytest.raises(ValueError, match="path and embedded identity"):
        read_historical_backtest_report(wrong)


def test_current_loader_rejects_tampered_evidence(tmp_path: Path) -> None:
    path, evidence, _ = _stored_report(tmp_path)
    report = read_historical_backtest_report(path)
    select_current_historical_backtest_report(
        report.report_id,
        selected_at=datetime(2026, 8, 18, 21, tzinfo=UTC),
        repo=tmp_path,
    )
    evidence_path = tmp_path / "data/output/model_performance" / report.evidence.artifact
    changed = evidence.copy()
    changed.loc[0, "total_error"] = 99.0
    changed.to_parquet(evidence_path, index=False)

    with pytest.raises(ValueError, match="digest"):
        load_current_historical_backtest_report(repo=tmp_path)
