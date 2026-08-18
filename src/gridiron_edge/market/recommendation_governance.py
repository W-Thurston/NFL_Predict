"""Immutable ownership of explicit recommendation-governance inputs."""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime, timedelta
from hashlib import sha256
import json
from typing import Final

from gridiron_edge.market.recommendation_policy import (
    PolicyValueSource,
    RecommendationPolicyGovernance,
    StakeRoundingMode,
    validate_recommendation_policy_governance,
)

RECOMMENDATION_GOVERNANCE_SCHEMA_VERSION: Final[int] = 1


@dataclass(frozen=True, slots=True)
class RecommendationGovernanceVersion:
    """One immutable version of explicitly governed recommendation inputs."""

    schema_version: int
    governance_id: str
    created_at: datetime
    governance: RecommendationPolicyGovernance


def recommendation_governance_id(
    governance: RecommendationPolicyGovernance,
) -> str:
    """Return deterministic governed-content identity, excluding created_at."""
    validate_recommendation_policy_governance(governance)
    payload = {
        field.name: _canonical(getattr(governance, field.name)) for field in fields(governance)
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return sha256(encoded).hexdigest()


def create_recommendation_governance(
    *,
    created_at: datetime,
    fractional_kelly_multiplier: float,
    minimum_actionable_stake: float,
    stake_increment: float,
    stake_rounding: StakeRoundingMode,
    maximum_candidate_bankroll_fraction: float,
    maximum_game_bankroll_fraction: float,
    maximum_portfolio_bankroll_fraction: float,
    prohibit_opposing_positions: bool,
    correlation_check_mandatory: bool,
    exposure_eligible_statuses: tuple[str, ...],
) -> RecommendationGovernanceVersion:
    """Create one version from explicit values without supplying defaults."""
    created = _require_utc(created_at, "created_at")
    governance = RecommendationPolicyGovernance(
        fractional_kelly_multiplier=fractional_kelly_multiplier,
        minimum_actionable_stake=minimum_actionable_stake,
        stake_increment=stake_increment,
        stake_rounding=stake_rounding,
        maximum_candidate_bankroll_fraction=maximum_candidate_bankroll_fraction,
        maximum_game_bankroll_fraction=maximum_game_bankroll_fraction,
        maximum_portfolio_bankroll_fraction=maximum_portfolio_bankroll_fraction,
        prohibit_opposing_positions=prohibit_opposing_positions,
        correlation_check_mandatory=correlation_check_mandatory,
        exposure_eligible_statuses=exposure_eligible_statuses,
        source=PolicyValueSource.GOVERNED_POLICY_INPUT,
    )
    validate_recommendation_policy_governance(governance)
    result = RecommendationGovernanceVersion(
        RECOMMENDATION_GOVERNANCE_SCHEMA_VERSION,
        recommendation_governance_id(governance),
        created,
        governance,
    )
    validate_recommendation_governance(result)
    return result


def validate_recommendation_governance(
    version: RecommendationGovernanceVersion,
) -> None:
    """Validate schema, UTC provenance, governed values, and identity."""
    if version.schema_version != RECOMMENDATION_GOVERNANCE_SCHEMA_VERSION:
        raise ValueError("Unsupported recommendation-governance schema version.")
    _require_utc(version.created_at, "created_at")
    validate_recommendation_policy_governance(version.governance)
    if version.governance_id != recommendation_governance_id(version.governance):
        raise ValueError("Recommendation-governance ID does not match governed content.")


def _canonical(value: object) -> object:
    if isinstance(value, StakeRoundingMode | PolicyValueSource):
        return value.value
    if isinstance(value, tuple):
        return list(value)
    if value is None or isinstance(value, bool | int | float | str):
        return value
    raise TypeError(f"Unsupported recommendation-governance value: {type(value).__name__}")


def _require_utc(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must be timezone-aware UTC.")
    return value
