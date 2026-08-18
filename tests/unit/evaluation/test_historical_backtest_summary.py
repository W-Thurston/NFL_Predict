"""Tests for historical backtest summaries and chart series."""

from __future__ import annotations

from copy import deepcopy

import pandas as pd
import pytest

from gridiron_edge.evaluation.historical_backtest_summary import (
    HistoricalPriceEvidenceStatus,
    build_historical_backtest_series,
    summarize_historical_backtest,
)


def _evidence() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "selection_id": ["a" * 64] * 4,
            "season": ["2024-2025", "2024-2025", "2025-2026", "2025-2026"],
            "week": [1, 2, 1, 2],
            "game_id": ["g1", "g2", "g3", "g4"],
            "game_date": ["2024-09-01", "2024-09-08", "2025-09-01", "2025-09-08"],
            "moneyline_evaluable": [True, True, False, True],
            "moneyline_correct": [True, False, None, True],
            "moneyline_squared_error": [0.04, 0.36, None, 0.09],
            "moneyline_log_loss": [0.22, 0.92, None, 0.36],
            "total_evaluable": [True, True, True, True],
            "total_outcome": ["win", "loss", "push", None],
            "total_absolute_error": [3.0, 7.0, 2.0, 0.0],
            "total_error": [-3.0, 7.0, -2.0, 0.0],
            "total_unit_return": [100 / 110, -1.0, 0.0, None],
            "total_assumed_american_price": [-110, -110, -110, None],
        }
    )


def test_summarizes_prediction_quality_and_assumed_price_return() -> None:
    summary = summarize_historical_backtest(_evidence())

    assert summary.evidence_row_count == 4
    assert summary.first_season == "2024-2025"
    assert summary.last_season == "2025-2026"
    assert summary.moneyline.evaluated_count == 3
    assert summary.moneyline.win_count == 2
    assert summary.moneyline.loss_count == 1
    assert summary.moneyline.net_wins == 1
    assert summary.moneyline.accuracy == pytest.approx(2 / 3)
    assert summary.moneyline.brier == pytest.approx((0.04 + 0.36 + 0.09) / 3)
    assert summary.moneyline.unit_return_status is HistoricalPriceEvidenceStatus.UNAVAILABLE
    assert summary.total.decision_count == 3
    assert summary.total.win_count == 1
    assert summary.total.loss_count == 1
    assert summary.total.push_count == 1
    assert summary.total.no_bet_count == 1
    assert summary.total.net_wins == 0
    assert summary.total.hit_rate_excluding_pushes == pytest.approx(0.5)
    assert summary.total.net_units == pytest.approx((100 / 110) - 1)
    assert summary.total.assumed_american_price == -110
    assert summary.total.price_evidence_status is HistoricalPriceEvidenceStatus.ASSUMED


def test_builds_cumulative_net_win_and_unit_series() -> None:
    series = build_historical_backtest_series(_evidence()).set_index("game_id")

    assert series.loc["g1", "moneyline_cumulative_net_wins"] == pytest.approx(1.0)
    assert series.loc["g2", "moneyline_cumulative_net_wins"] == pytest.approx(0.0)
    assert series.loc["g4", "moneyline_cumulative_net_wins"] == pytest.approx(1.0)
    assert series.loc["g1", "total_cumulative_net_wins"] == pytest.approx(1.0)
    assert series.loc["g2", "total_cumulative_net_wins"] == pytest.approx(0.0)
    assert series.loc["g3", "total_cumulative_net_wins"] == pytest.approx(0.0)
    assert series.loc["g2", "total_cumulative_units"] == pytest.approx((100 / 110) - 1)
    assert series.loc["g3", "total_cumulative_units"] == pytest.approx((100 / 110) - 1)
    assert series.loc["g4", "moneyline_cumulative_accuracy"] == pytest.approx(2 / 3)
    assert pd.isna(series.loc["g4", "moneyline_rolling_accuracy_100"])


def test_rejects_conflicting_assumed_prices() -> None:
    evidence = _evidence()
    evidence.loc[evidence["game_id"].eq("g2"), "total_assumed_american_price"] = -105

    with pytest.raises(ValueError, match="conflicting total_assumed_american_price"):
        summarize_historical_backtest(evidence)


def test_inputs_are_not_mutated() -> None:
    evidence = _evidence()
    original = deepcopy(evidence)

    summarize_historical_backtest(evidence)
    build_historical_backtest_series(evidence)

    pd.testing.assert_frame_equal(evidence, original)
