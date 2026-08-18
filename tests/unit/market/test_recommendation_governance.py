"""Tests for explicit immutable recommendation governance."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from gridiron_edge.market.recommendation_governance import (
    create_recommendation_governance,
    recommendation_governance_id,
    validate_recommendation_governance,
)
from gridiron_edge.market.recommendation_policy import StakeRoundingMode

NOW = datetime(2026, 8, 18, 15, 30, tzinfo=UTC)


def _version(**overrides):
    values = {
        "created_at": NOW,
        "fractional_kelly_multiplier": 0.25,
        "minimum_actionable_stake": 5.0,
        "stake_increment": 1.0,
        "stake_rounding": StakeRoundingMode.DOWN,
        "maximum_candidate_bankroll_fraction": 0.02,
        "maximum_game_bankroll_fraction": 0.05,
        "maximum_portfolio_bankroll_fraction": 0.20,
        "prohibit_opposing_positions": True,
        "correlation_check_mandatory": True,
        "exposure_eligible_statuses": ("open",),
    }
    values.update(overrides)
    return create_recommendation_governance(**values)


def test_identity_excludes_created_at_and_changes_with_governed_values() -> None:
    first = _version()
    later = _version(created_at=datetime(2026, 8, 19, tzinfo=UTC))
    changed = _version(fractional_kelly_multiplier=0.10)
    assert first.governance_id == later.governance_id
    assert first.governance_id != changed.governance_id
    assert first.governance_id == recommendation_governance_id(first.governance)


def test_validation_rejects_identity_mismatch() -> None:
    with pytest.raises(ValueError, match="ID"):
        validate_recommendation_governance(replace(_version(), governance_id="0" * 64))


def test_domain_reuses_policy_governance_validation() -> None:
    with pytest.raises(ValueError, match="ordered"):
        _version(maximum_candidate_bankroll_fraction=0.10)
