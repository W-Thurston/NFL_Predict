"""Tests for strict weekly Elo forecast loading."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from gridiron_edge.api.loaders import load_weekly_elo_forecast


@dataclass(frozen=True)
class _Settings:
    repo_root: Path


def _rows() -> list[dict]:
    return [
        {
            "season": "2026-2027",
            "forecast_origin_week": 1,
            "team": team,
            "week": week,
            "elo_p10": 1400.0,
            "elo_median": 1500.0,
            "elo_p90": 1600.0,
            "win_out_elo_median": 1700.0,
            "lose_out_elo_median": 1300.0,
            "simulation_count": 100,
            "lower_quantile": 0.1,
            "center_quantile": 0.5,
            "upper_quantile": 0.9,
            "quantile_method": "linear",
            "computed_at": "2026-08-19T00:00:00+00:00",
        }
        for team in ("MIA", "SEA")
        for week in range(1, 19)
    ]


def _write(tmp_path: Path, rows: list[dict]) -> None:
    path = tmp_path / "data" / "output" / "temp" / "weekly_elo_forecast.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_missing_artifact_is_explicit(tmp_path: Path) -> None:
    result = load_weekly_elo_forecast(
        _Settings(tmp_path), season="2026-2027", forecast_origin_week=1, team_abbr="MIA"
    )
    assert result.state == "missing"
    assert result.frame.empty


def test_valid_artifact_is_team_scoped(tmp_path: Path) -> None:
    _write(tmp_path, _rows())
    result = load_weekly_elo_forecast(
        _Settings(tmp_path), season="2026-2027", forecast_origin_week=1, team_abbr="MIA"
    )
    assert result.state == "available"
    assert len(result.frame) == 18
    assert set(result.frame["team"]) == {"MIA"}


def test_duplicate_identity_is_rejected(tmp_path: Path) -> None:
    rows = _rows()
    rows.append(dict(rows[0]))
    _write(tmp_path, rows)
    result = load_weekly_elo_forecast(
        _Settings(tmp_path), season="2026-2027", forecast_origin_week=1, team_abbr="MIA"
    )
    assert result.state == "identity_conflict"


def test_invalid_quantile_ordering_is_rejected(tmp_path: Path) -> None:
    rows = _rows()
    rows[0]["elo_p10"] = 1601.0
    _write(tmp_path, rows)
    result = load_weekly_elo_forecast(
        _Settings(tmp_path), season="2026-2027", forecast_origin_week=1, team_abbr="MIA"
    )
    assert result.state == "malformed"


def test_scope_mismatches_are_explicit(tmp_path: Path) -> None:
    _write(tmp_path, _rows())
    season = load_weekly_elo_forecast(
        _Settings(tmp_path), season="2027-2028", forecast_origin_week=1, team_abbr="MIA"
    )
    origin = load_weekly_elo_forecast(
        _Settings(tmp_path), season="2026-2027", forecast_origin_week=2, team_abbr="MIA"
    )
    team = load_weekly_elo_forecast(
        _Settings(tmp_path), season="2026-2027", forecast_origin_week=1, team_abbr="BUF"
    )
    assert season.state == "season_mismatch"
    assert origin.state == "origin_mismatch"
    assert team.state == "team_missing"
