# src/gridiron_edge/market/recommendation_governance_store.py

"""Immutable JSON persistence for recommendation-governance versions."""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import os
from pathlib import Path
from typing import cast
from uuid import uuid4

from gridiron_edge.core.settings import get_settings
from gridiron_edge.market.recommendation_governance import (
    RECOMMENDATION_GOVERNANCE_SCHEMA_VERSION,
    RecommendationGovernanceVersion,
    validate_recommendation_governance,
)
from gridiron_edge.market.recommendation_policy import (
    PolicyValueSource,
    RecommendationPolicyGovernance,
    StakeRoundingMode,
)


def recommendation_governance_root(repo: Path | None = None) -> Path:
    """Return the immutable recommendation-governance root."""
    root = repo or get_settings().repo_root
    return root / "data/output/recommendation_governance"


def recommendation_governance_path(
    schema_version: int,
    governance_id: str,
    *,
    repo: Path | None = None,
) -> Path:
    """Return the identity-addressed path for one governance version."""
    if schema_version != RECOMMENDATION_GOVERNANCE_SCHEMA_VERSION:
        raise ValueError("Unsupported recommendation-governance schema version.")
    identity = _digest(governance_id, "governance_id")
    return (
        recommendation_governance_root(repo)
        / f"schema={schema_version}"
        / "versions"
        / f"{identity}.json"
    )


def write_recommendation_governance(
    version: RecommendationGovernanceVersion,
    *,
    repo: Path | None = None,
) -> Path:
    """Persist one immutable version or accept an exact replay."""
    validate_recommendation_governance(version)
    path: Path = recommendation_governance_path(
        version.schema_version,
        version.governance_id,
        repo=repo,
    )
    encoded: str = json.dumps(_payload(version), indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise ValueError(
                "Recommendation-governance identity cannot be reused with different content."
            )
        return path
    temporary: Path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(encoded, encoding="utf-8")
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_text(encoding="utf-8") != encoded:
                raise ValueError(
                    "Recommendation-governance identity cannot be reused with different content."
                ) from None
    finally:
        temporary.unlink(missing_ok=True)
    return path


def read_recommendation_governance(path: Path) -> RecommendationGovernanceVersion:
    """Read and strictly validate one exact governance version."""
    raw = _object(json.loads(path.read_text(encoding="utf-8")), "governance artifact")
    expected = {
        "schema_version",
        "governance_id",
        "created_at",
        "fractional_kelly_multiplier",
        "minimum_actionable_stake",
        "stake_increment",
        "stake_rounding",
        "maximum_candidate_bankroll_fraction",
        "maximum_game_bankroll_fraction",
        "maximum_portfolio_bankroll_fraction",
        "prohibit_opposing_positions",
        "correlation_check_mandatory",
        "exposure_eligible_statuses",
        "source",
    }
    if set(raw) != expected:
        raise ValueError("Recommendation-governance artifact keys do not match the schema.")
    governance = RecommendationPolicyGovernance(
        fractional_kelly_multiplier=_number(
            raw["fractional_kelly_multiplier"], "fractional_kelly_multiplier"
        ),
        minimum_actionable_stake=_number(
            raw["minimum_actionable_stake"], "minimum_actionable_stake"
        ),
        stake_increment=_number(raw["stake_increment"], "stake_increment"),
        stake_rounding=StakeRoundingMode(_text(raw["stake_rounding"], "stake_rounding")),
        maximum_candidate_bankroll_fraction=_number(
            raw["maximum_candidate_bankroll_fraction"],
            "maximum_candidate_bankroll_fraction",
        ),
        maximum_game_bankroll_fraction=_number(
            raw["maximum_game_bankroll_fraction"],
            "maximum_game_bankroll_fraction",
        ),
        maximum_portfolio_bankroll_fraction=_number(
            raw["maximum_portfolio_bankroll_fraction"],
            "maximum_portfolio_bankroll_fraction",
        ),
        prohibit_opposing_positions=_boolean(
            raw["prohibit_opposing_positions"], "prohibit_opposing_positions"
        ),
        correlation_check_mandatory=_boolean(
            raw["correlation_check_mandatory"], "correlation_check_mandatory"
        ),
        exposure_eligible_statuses=tuple(
            _text(value, "exposure_eligible_status")
            for value in _list(raw["exposure_eligible_statuses"], "exposure_eligible_statuses")
        ),
        source=PolicyValueSource(_text(raw["source"], "source")),
    )
    version = RecommendationGovernanceVersion(
        schema_version=_integer(raw["schema_version"], "schema_version"),
        governance_id=_digest(_text(raw["governance_id"], "governance_id"), "governance_id"),
        created_at=_datetime(raw["created_at"], "created_at"),
        governance=governance,
    )
    validate_recommendation_governance(version)
    expected_path = recommendation_governance_path(
        version.schema_version,
        version.governance_id,
        repo=_artifact_repo(path),
    )
    if path.resolve() != expected_path.resolve():
        raise ValueError("Recommendation-governance path and embedded identity disagree.")
    return version


def _payload(version: RecommendationGovernanceVersion) -> dict[str, object]:
    governance = version.governance
    return {
        "schema_version": version.schema_version,
        "governance_id": version.governance_id,
        "created_at": version.created_at.isoformat(),
        "fractional_kelly_multiplier": governance.fractional_kelly_multiplier,
        "minimum_actionable_stake": governance.minimum_actionable_stake,
        "stake_increment": governance.stake_increment,
        "stake_rounding": governance.stake_rounding.value,
        "maximum_candidate_bankroll_fraction": governance.maximum_candidate_bankroll_fraction,
        "maximum_game_bankroll_fraction": governance.maximum_game_bankroll_fraction,
        "maximum_portfolio_bankroll_fraction": governance.maximum_portfolio_bankroll_fraction,
        "prohibit_opposing_positions": governance.prohibit_opposing_positions,
        "correlation_check_mandatory": governance.correlation_check_mandatory,
        "exposure_eligible_statuses": list(governance.exposure_eligible_statuses),
        "source": governance.source.value,
    }


def _artifact_repo(path: Path) -> Path:
    resolved = path.resolve()
    marker = ("data", "output", "recommendation_governance")
    parts = resolved.parts
    for index in range(len(parts) - len(marker) + 1):
        if tuple(parts[index : index + len(marker)]) == marker:
            return Path(*parts[:index])
    raise ValueError("Recommendation-governance path is outside the canonical store.")


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{label} must be a JSON object with string keys.")
    return cast(dict[str, object], value)


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list.")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonempty string.")
    return value


def _integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer.")
    return value


def _number(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{label} must be numeric.")
    return float(value)


def _boolean(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{label} must be boolean.")
    return value


def _datetime(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be an ISO timestamp string.")
    result = datetime.fromisoformat(value)
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must be timezone-aware UTC.")
    return result


def _digest(value: str, label: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest.")
    return value
