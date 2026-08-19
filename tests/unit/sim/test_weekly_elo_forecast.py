"""Tests for weekly regular-season Elo forecast distributions."""

from __future__ import annotations

import numpy as np
import pytest

from gridiron_edge.sim._engine import simulate_remaining_regular_season
from gridiron_edge.sim._types import N_TEAMS, N_WEEKS_REG, TeamIndex
from gridiron_edge.sim.season import build_weekly_elo_forecast_df


def _kernel_inputs() -> tuple:
    schedule_home = np.asarray([0], dtype=np.int16)
    schedule_away = np.asarray([1], dtype=np.int16)
    week_offsets = np.ones(N_WEEKS_REG + 2, dtype=np.int32)
    week_offsets[0] = 0
    week_offsets[1] = 0
    conf_id = np.zeros(N_TEAMS, dtype=np.int8)
    div_id = np.zeros(N_TEAMS, dtype=np.int8)
    elo = np.linspace(1400.0, 1600.0, N_TEAMS, dtype=np.float32)
    pts = np.zeros(N_TEAMS, dtype=np.int16)
    matrix_u8 = np.zeros((N_TEAMS, N_TEAMS), dtype=np.uint8)
    matrix_i8 = np.zeros((N_TEAMS, N_TEAMS), dtype=np.int8)
    wins = np.zeros((N_TEAMS, N_WEEKS_REG + 1), dtype=np.int32)
    return (
        8,
        schedule_home,
        schedule_away,
        week_offsets,
        0,
        conf_id,
        div_id,
        elo,
        pts,
        pts.copy(),
        pts.copy(),
        matrix_u8,
        matrix_i8,
        matrix_u8.copy(),
        wins,
        20.0,
        0.0,
        1337,
        480.0,
    )


def test_kernel_retains_entering_week_elo_and_bye_carry_forward() -> None:
    outputs = simulate_remaining_regular_season(*_kernel_inputs())
    end_elo = outputs[6]
    weekly = outputs[7]

    assert weekly.shape == (8, N_WEEKS_REG + 1, N_TEAMS)
    assert weekly.dtype == np.float32
    assert np.isnan(weekly[:, 0, :]).all()
    expected = _kernel_inputs()[7]
    np.testing.assert_array_equal(weekly[:, 1, :], np.broadcast_to(expected, (8, N_TEAMS)))
    expected_bye_path = np.broadcast_to(
        weekly[:, 1:2, 2],
        weekly[:, 2:, 2].shape,
    )
    np.testing.assert_array_equal(weekly[:, 2:, 2], expected_bye_path)
    np.testing.assert_allclose(weekly[:, 2, :2].sum(axis=1), weekly[:, 1, :2].sum(axis=1))
    np.testing.assert_array_equal(end_elo[:, 2], weekly[:, 18, 2])


def test_kernel_leaves_completed_weeks_null_and_is_deterministic() -> None:
    inputs = list(_kernel_inputs())
    inputs[4] = 5
    first = simulate_remaining_regular_season(*inputs)[7]
    second = simulate_remaining_regular_season(*inputs)[7]

    assert np.isnan(first[:, :6, :]).all()
    assert not np.isnan(first[:, 6:, :]).any()
    np.testing.assert_array_equal(first, second)


def test_build_weekly_elo_forecast_df_has_exact_quantile_contract() -> None:
    weekly = np.full((4, N_WEEKS_REG + 1, 2), np.nan, dtype=np.float32)
    weekly[:, 6, :] = np.asarray(
        [
            [1500.0, 1400.0],
            [1520.0, 1420.0],
            [1540.0, 1440.0],
            [1560.0, 1460.0],
        ],
        dtype=np.float32,
    )
    for week in range(7, N_WEEKS_REG + 1):
        weekly[:, week, :] = weekly[:, 6, :] + float(week - 6)
    teams = TeamIndex(
        short_names=["AAA", "BBB"],
        short_to_id={"AAA": 0, "BBB": 1},
        long_to_short={},
    )

    scenario = np.full((2, N_WEEKS_REG + 1), np.nan, dtype=np.float64)
    scenario[:, 6:] = np.asarray([[1530.0], [1430.0]])
    result = build_weekly_elo_forecast_df(
        weekly_elo_by_sim=weekly,
        win_out_elo_median=scenario + 10.0,
        lose_out_elo_median=scenario - 10.0,
        team_index=teams,
        season="2026-2027",
        forecast_origin_week=6,
        simulation_count=4,
        computed_at="2026-08-18T00:00:00+00:00",
    )

    assert len(result) == 2 * (N_WEEKS_REG - 5)
    assert not result.duplicated(["season", "forecast_origin_week", "team", "week"]).any()
    assert result["week"].min() == 6
    assert result["week"].max() == 18
    assert (result["elo_p10"] <= result["elo_median"]).all()
    assert (result["elo_median"] <= result["elo_p90"]).all()
    assert set(result["quantile_method"]) == {"linear"}
    assert set(result["simulation_count"]) == {4}
    origin_aaa = result.loc[(result["team"] == "AAA") & (result["week"] == 6)].iloc[0]
    assert origin_aaa["elo_p10"] == pytest.approx(1506.0)
    assert origin_aaa["elo_median"] == pytest.approx(1530.0)
    assert origin_aaa["elo_p90"] == pytest.approx(1554.0)


def test_build_weekly_elo_forecast_df_rejects_null_forecast_week() -> None:
    weekly = np.full((2, N_WEEKS_REG + 1, 1), np.nan, dtype=np.float32)
    weekly[:, 1, 0] = 1500.0
    teams = TeamIndex(
        short_names=["AAA"],
        short_to_id={"AAA": 0},
        long_to_short={},
    )

    with pytest.raises(ValueError, match="contains null values for week 2"):
        scenario = np.full((1, N_WEEKS_REG + 1), np.nan, dtype=np.float64)
        scenario[:, 1:] = 1500.0
        build_weekly_elo_forecast_df(
            weekly_elo_by_sim=weekly,
            win_out_elo_median=scenario,
            lose_out_elo_median=scenario,
            team_index=teams,
            season="2026-2027",
            forecast_origin_week=1,
            simulation_count=2,
            computed_at="2026-08-18T00:00:00+00:00",
        )
