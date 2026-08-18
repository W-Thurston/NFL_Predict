"""Tests for historical model-performance API serializers."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from gridiron_edge.api.serializers.model_performance import (
    serialize_historical_model_performance,
    serialize_historical_model_performance_series,
)
from gridiron_edge.evaluation.historical_backtest_report import (
    create_historical_backtest_report,
)
from gridiron_edge.evaluation.historical_backtest_report_loader import (
    CurrentHistoricalBacktestReport,
)
from gridiron_edge.evaluation.historical_backtest_summary import (
    build_historical_backtest_series,
    summarize_historical_backtest,
)


def _current() -> CurrentHistoricalBacktestReport:
    evidence = pd.DataFrame(
        {
            "selection_id": ["a" * 64, "a" * 64],
            "season": ["2024-2025", "2025-2026"],
            "week": [1, 1],
            "game_id": ["g1", "g2"],
            "game_date": ["2024-09-01", "2025-09-01"],
            "moneyline_evaluable": [True, True],
            "moneyline_correct": [True, False],
            "moneyline_squared_error": [0.04, 0.36],
            "moneyline_log_loss": [0.22, 0.92],
            "total_evaluable": [True, True],
            "total_outcome": ["win", "loss"],
            "total_absolute_error": [3.0, 7.0],
            "total_error": [-3.0, 7.0],
            "total_unit_return": [100 / 110, -1.0],
            "total_assumed_american_price": [-110, -110],
        }
    )
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
    return CurrentHistoricalBacktestReport(
        report=report,
        evidence=evidence,
        series=series,
        selected_at=datetime(2026, 8, 18, 21, tzinfo=UTC),
    )


def test_serializes_historical_summary_without_recomputation() -> None:
    response = serialize_historical_model_performance(_current())

    assert response.moneyline.model_type == "logistic"
    assert response.moneyline.wins == 1
    assert response.moneyline.losses == 1
    assert response.moneyline.price_evidence_status == "unavailable"
    assert response.total.model_type == "random_forest"
    assert response.total.price_evidence_status == "assumed"
    assert response.total.assumed_american_price == -110
    assert response.spread.status == "unavailable"
    assert response.final_values.moneyline_cumulative_net_wins == 0.0
    assert response.final_values.total_cumulative_units == (100 / 110) - 1


def test_serializes_only_chart_facing_series_fields() -> None:
    response = serialize_historical_model_performance_series(_current())

    assert response.total == 2
    assert response.report_id
    assert response.items[0].game_id == "g1"
    assert response.items[-1].moneyline_cumulative_net_wins == 0.0
    dumped = response.items[0].model_dump()
    assert "moneyline_decision_score" not in dumped
    assert "total_decision_score" not in dumped
