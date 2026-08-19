"""Tests for conditioned Elo scenario medians."""

from __future__ import annotations

import numpy as np

from gridiron_edge.sim._engine import simulate_conditioned_team_elo
from gridiron_edge.sim._types import N_TEAMS, N_WEEKS_REG
from gridiron_edge.sim.season import build_conditioned_elo_scenario_medians


def _inputs() -> tuple:
    home = np.asarray([0, 2], dtype=np.int16)
    away = np.asarray([1, 3], dtype=np.int16)
    offsets = np.full(N_WEEKS_REG + 2, 2, dtype=np.int32)
    offsets[0] = 0
    offsets[1] = 0
    offsets[2] = 2
    elo = np.linspace(1400.0, 1600.0, N_TEAMS, dtype=np.float32)
    return home, away, offsets, elo


def test_conditioned_paths_share_origin_and_force_selected_direction() -> None:
    home, away, offsets, elo = _inputs()
    win = simulate_conditioned_team_elo(
        16, home, away, offsets, 0, elo, 0, True, 20.0, 0.01, 1337, 480.0
    )
    lose = simulate_conditioned_team_elo(
        16, home, away, offsets, 0, elo, 0, False, 20.0, 0.01, 1337, 480.0
    )

    assert win.shape == (16, N_WEEKS_REG + 1)
    assert np.isnan(win[:, 0]).all()
    np.testing.assert_array_equal(win[:, 1], elo[0])
    np.testing.assert_array_equal(lose[:, 1], elo[0])
    assert (win[:, 2] > win[:, 1]).all()
    assert (lose[:, 2] < lose[:, 1]).all()
    assert (lose[:, 2] <= win[:, 2]).all()


def test_conditioned_paths_carry_selected_team_through_byes() -> None:
    home, away, offsets, elo = _inputs()
    paths = simulate_conditioned_team_elo(
        8, home, away, offsets, 0, elo, 4, True, 20.0, 0.01, 1337, 480.0
    )

    expected = np.broadcast_to(paths[:, 1:2], paths[:, 2:].shape)
    np.testing.assert_array_equal(paths[:, 2:], expected)


def test_conditioned_paths_are_deterministic() -> None:
    home, away, offsets, elo = _inputs()
    first = simulate_conditioned_team_elo(
        8, home, away, offsets, 0, elo, 0, True, 20.0, 0.01, 1337, 480.0
    )
    second = simulate_conditioned_team_elo(
        8, home, away, offsets, 0, elo, 0, True, 20.0, 0.01, 1337, 480.0
    )
    np.testing.assert_array_equal(first, second)


def test_all_team_scenario_medians_have_exact_origin_and_ordering() -> None:
    home, away, offsets, elo = _inputs()
    win, lose = build_conditioned_elo_scenario_medians(
        n_sims=8,
        schedule_home=home,
        schedule_away=away,
        week_offsets=offsets,
        final_actual_week=0,
        elo_entering_next_week=elo,
        k_factor=20.0,
        p_tie=0.01,
        base_seed=1337,
        divisor=480.0,
    )

    assert win.shape == (N_TEAMS, N_WEEKS_REG + 1)
    assert lose.shape == win.shape
    np.testing.assert_array_equal(win[:, 1], elo)
    np.testing.assert_array_equal(lose[:, 1], elo)
    assert (lose[:, 2] <= win[:, 2]).all()
