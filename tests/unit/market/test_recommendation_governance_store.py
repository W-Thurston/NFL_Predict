"""Tests for immutable recommendation-governance persistence."""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path

import pytest

from gridiron_edge.market.recommendation_governance import create_recommendation_governance
from gridiron_edge.market.recommendation_governance_store import (
    read_recommendation_governance,
    recommendation_governance_path,
    write_recommendation_governance,
)
from gridiron_edge.market.recommendation_policy import StakeRoundingMode


def _version():
    return create_recommendation_governance(
        created_at=datetime(2026, 8, 18, 15, 30, tzinfo=UTC),
        fractional_kelly_multiplier=0.25,
        minimum_actionable_stake=5.0,
        stake_increment=1.0,
        stake_rounding=StakeRoundingMode.DOWN,
        maximum_candidate_bankroll_fraction=0.02,
        maximum_game_bankroll_fraction=0.05,
        maximum_portfolio_bankroll_fraction=0.20,
        prohibit_opposing_positions=True,
        correlation_check_mandatory=True,
        exposure_eligible_statuses=("open",),
    )


def test_round_trip_and_exact_replay(tmp_path: Path) -> None:
    value = _version()
    path = write_recommendation_governance(value, repo=tmp_path)
    before = path.read_text(encoding="utf-8")
    assert path == recommendation_governance_path(
        value.schema_version, value.governance_id, repo=tmp_path
    )
    assert read_recommendation_governance(path) == value
    assert write_recommendation_governance(value, repo=tmp_path) == path
    assert path.read_text(encoding="utf-8") == before


def test_malformed_schema_and_unsafe_identity_are_rejected(tmp_path: Path) -> None:
    value = _version()
    path = write_recommendation_governance(value, repo=tmp_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["extra"] = True
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="keys"):
        read_recommendation_governance(path)
    with pytest.raises(ValueError, match="SHA-256"):
        recommendation_governance_path(1, "../escape", repo=tmp_path)


def test_publication_race_rejects_conflicting_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    value = _version()
    path = recommendation_governance_path(value.schema_version, value.governance_id, repo=tmp_path)

    def racing_link(src: Path, dst: Path) -> None:
        destination = Path(dst)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text('{"conflicting": true}', encoding="utf-8")
        raise FileExistsError

    monkeypatch.setattr("gridiron_edge.market.recommendation_governance_store.os.link", racing_link)
    with pytest.raises(
        ValueError,
        match="Recommendation-governance identity cannot be reused with different content",
    ):
        write_recommendation_governance(value, repo=tmp_path)
    assert path.read_text(encoding="utf-8") == '{"conflicting": true}'
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []


def test_publication_race_accepts_identical_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    value = _version()
    path = recommendation_governance_path(value.schema_version, value.governance_id, repo=tmp_path)

    def racing_link(src: Path, dst: Path) -> None:
        destination = Path(dst)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(Path(src).read_text(encoding="utf-8"), encoding="utf-8")
        raise FileExistsError

    monkeypatch.setattr("gridiron_edge.market.recommendation_governance_store.os.link", racing_link)
    assert write_recommendation_governance(value, repo=tmp_path) == path
    assert read_recommendation_governance(path) == value
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []


def test_pre_publication_failure_leaves_no_destination_or_temporary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    value = _version()
    path = recommendation_governance_path(value.schema_version, value.governance_id, repo=tmp_path)

    def failing_link(src: Path, dst: Path) -> None:
        raise OSError("simulated pre-publication failure")

    monkeypatch.setattr(
        "gridiron_edge.market.recommendation_governance_store.os.link", failing_link
    )
    with pytest.raises(OSError, match="simulated pre-publication failure"):
        write_recommendation_governance(value, repo=tmp_path)
    assert not path.exists()
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []
