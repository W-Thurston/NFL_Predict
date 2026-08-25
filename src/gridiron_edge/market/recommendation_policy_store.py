# src/gridiron_edge/market/recommendation_policy_store.py

"""Immutable JSON persistence for versioned recommendation policies."""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import os
from pathlib import Path
from typing import cast
from uuid import uuid4

from gridiron_edge.core.settings import get_settings
from gridiron_edge.market.recommendation_policy import (
    EmpiricalQualificationThresholds,
    MarketFamilyRecommendationPolicy,
    PolicyDerivationReason,
    PolicyDerivationStatus,
    PolicyValueSource,
    RecommendationPolicy,
    RecommendationPolicyGovernance,
    StakeRoundingMode,
    validate_recommendation_policy,
)


def recommendation_policy_root(repo: Path | None = None) -> Path:
    """Return the immutable recommendation-policy storage root."""
    root = repo or get_settings().repo_root
    return root / "data" / "output" / "recommendation_policies"


def recommendation_policy_path(
    schema_version: int,
    policy_id: str,
    *,
    repo: Path | None = None,
) -> Path:
    """Return the identity-addressed path for one policy version."""
    if schema_version < 1:
        raise ValueError("schema_version must be positive.")
    identity = _require_digest(policy_id, "policy_id")
    return recommendation_policy_root(repo) / f"schema={schema_version}" / f"{identity}.json"


def write_recommendation_policy(
    policy: RecommendationPolicy,
    *,
    repo: Path | None = None,
) -> Path:
    """Create one policy or accept an exact idempotent replay."""
    validate_recommendation_policy(policy)
    path: Path = recommendation_policy_path(policy.schema_version, policy.policy_id, repo=repo)
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded: str = json.dumps(_payload(policy), indent=2, sort_keys=True) + "\n"
    if path.exists():
        existing: RecommendationPolicy = read_recommendation_policy(path)
        if existing != policy:
            raise ValueError(
                "Recommendation policy ID cannot be reused with different content: "
                f"{policy.policy_id}"
            )
        return path
    temporary: Path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(encoded, encoding="utf-8")
        try:
            os.link(temporary, path)
        except FileExistsError:
            existing = read_recommendation_policy(path)
            if existing != policy:
                raise ValueError(
                    "Recommendation policy ID cannot be reused with different content: "
                    f"{policy.policy_id}"
                ) from None
    finally:
        temporary.unlink(missing_ok=True)
    return path


def read_recommendation_policy(path: Path) -> RecommendationPolicy:
    """Read and validate one exact immutable policy artifact."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Recommendation policy artifact must contain a JSON object.")
    expected = {
        "schema_version",
        "policy_id",
        "created_at",
        "source_evidence_fingerprint",
        "governance_fingerprint",
        "derivation_method",
        "moneyline",
        "spread",
        "total",
        "governance",
    }
    if set(raw) != expected:
        raise ValueError("Recommendation policy artifact keys do not match the schema.")
    policy = RecommendationPolicy(
        schema_version=_integer(raw["schema_version"], "schema_version"),
        policy_id=_text(raw["policy_id"], "policy_id"),
        created_at=_datetime(raw["created_at"]),
        source_evidence_fingerprint=_text(
            raw["source_evidence_fingerprint"], "source_evidence_fingerprint"
        ),
        governance_fingerprint=_text(raw["governance_fingerprint"], "governance_fingerprint"),
        derivation_method=_text(raw["derivation_method"], "derivation_method"),
        moneyline=_family(raw["moneyline"]),
        spread=_family(raw["spread"]),
        total=_family(raw["total"]),
        governance=_governance(raw["governance"]),
    )
    validate_recommendation_policy(policy)
    expected_path = recommendation_policy_path(
        policy.schema_version, policy.policy_id, repo=_artifact_repo(path)
    )
    if path.resolve() != expected_path.resolve():
        raise ValueError("Recommendation policy path and embedded identity disagree.")
    return policy


def _artifact_repo(path: Path) -> Path:
    """Recover the repository root from the canonical policy artifact path."""
    resolved = path.resolve()
    parts = resolved.parts
    marker = ("data", "output", "recommendation_policies")
    for index in range(len(parts) - len(marker) + 1):
        if tuple(parts[index : index + len(marker)]) == marker:
            return Path(*parts[:index])
    raise ValueError("Recommendation policy path is outside the canonical store.")


def _payload(policy: RecommendationPolicy) -> dict[str, object]:
    return {
        "schema_version": policy.schema_version,
        "policy_id": policy.policy_id,
        "created_at": policy.created_at.isoformat(),
        "source_evidence_fingerprint": policy.source_evidence_fingerprint,
        "governance_fingerprint": policy.governance_fingerprint,
        "derivation_method": policy.derivation_method,
        "moneyline": _family_payload(policy.moneyline),
        "spread": _family_payload(policy.spread),
        "total": _family_payload(policy.total),
        "governance": _governance_payload(policy.governance),
    }


def _family_payload(family: MarketFamilyRecommendationPolicy) -> dict[str, object]:
    thresholds = family.thresholds
    return {
        "market": family.market,
        "status": family.status.value,
        "reason": family.reason.value,
        "candidate_count": family.candidate_count,
        "outcome_available_count": family.outcome_available_count,
        "clv_available_count": family.clv_available_count,
        "return_available_count": family.return_available_count,
        "evidence_statuses": [list(item) for item in family.evidence_statuses],
        "thresholds": (
            None
            if thresholds is None
            else {
                "minimum_expected_value": thresholds.minimum_expected_value,
                "maximum_quote_age_seconds": thresholds.maximum_quote_age_seconds,
                "minimum_observation_count": thresholds.minimum_observation_count,
                "minimum_distinct_fetch_count": thresholds.minimum_distinct_fetch_count,
            }
        ),
        "source": family.source.value,
    }


def _governance_payload(
    governance: RecommendationPolicyGovernance,
) -> dict[str, object]:
    return {
        "fractional_kelly_multiplier": governance.fractional_kelly_multiplier,
        "minimum_actionable_stake": governance.minimum_actionable_stake,
        "stake_increment": governance.stake_increment,
        "stake_rounding": governance.stake_rounding.value,
        "maximum_candidate_bankroll_fraction": (governance.maximum_candidate_bankroll_fraction),
        "maximum_game_bankroll_fraction": governance.maximum_game_bankroll_fraction,
        "maximum_portfolio_bankroll_fraction": (governance.maximum_portfolio_bankroll_fraction),
        "prohibit_opposing_positions": governance.prohibit_opposing_positions,
        "correlation_check_mandatory": governance.correlation_check_mandatory,
        "exposure_eligible_statuses": list(governance.exposure_eligible_statuses),
        "source": governance.source.value,
    }


def _family(value: object) -> MarketFamilyRecommendationPolicy:
    data = _object(value, "market-family policy")
    expected = {
        "market",
        "status",
        "reason",
        "candidate_count",
        "outcome_available_count",
        "clv_available_count",
        "return_available_count",
        "evidence_statuses",
        "thresholds",
        "source",
    }
    _exact_keys(data, expected, "Market-family policy")
    statuses_raw = _list(data["evidence_statuses"], "evidence_statuses")
    statuses: list[tuple[str, str]] = []
    for item in statuses_raw:
        pair = _list(item, "evidence status")
        if len(pair) != 2:
            raise ValueError("Each evidence status must contain exactly two values.")
        statuses.append((_text(pair[0], "evidence key"), _text(pair[1], "evidence value")))
    return MarketFamilyRecommendationPolicy(
        market=_text(data["market"], "market"),
        status=PolicyDerivationStatus(_text(data["status"], "status")),
        reason=PolicyDerivationReason(_text(data["reason"], "reason")),
        candidate_count=_integer(data["candidate_count"], "candidate_count"),
        outcome_available_count=_integer(
            data["outcome_available_count"], "outcome_available_count"
        ),
        clv_available_count=_integer(data["clv_available_count"], "clv_available_count"),
        return_available_count=_integer(data["return_available_count"], "return_available_count"),
        evidence_statuses=tuple(statuses),
        thresholds=_thresholds(data["thresholds"]),
        source=PolicyValueSource(_text(data["source"], "source")),
    )


def _thresholds(value: object) -> EmpiricalQualificationThresholds | None:
    if value is None:
        return None
    data = _object(value, "thresholds")
    expected = {
        "minimum_expected_value",
        "maximum_quote_age_seconds",
        "minimum_observation_count",
        "minimum_distinct_fetch_count",
    }
    _exact_keys(data, expected, "Threshold")
    return EmpiricalQualificationThresholds(
        _optional_float(data["minimum_expected_value"], "minimum_expected_value"),
        _optional_float(data["maximum_quote_age_seconds"], "maximum_quote_age_seconds"),
        _optional_integer(data["minimum_observation_count"], "minimum_observation_count"),
        _optional_integer(data["minimum_distinct_fetch_count"], "minimum_distinct_fetch_count"),
    )


def _governance(value: object) -> RecommendationPolicyGovernance:
    data = _object(value, "governance")
    expected = {
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
    _exact_keys(data, expected, "Governance")
    statuses = _list(data["exposure_eligible_statuses"], "exposure_eligible_statuses")
    return RecommendationPolicyGovernance(
        fractional_kelly_multiplier=_number(
            data["fractional_kelly_multiplier"], "fractional_kelly_multiplier"
        ),
        minimum_actionable_stake=_number(
            data["minimum_actionable_stake"], "minimum_actionable_stake"
        ),
        stake_increment=_number(data["stake_increment"], "stake_increment"),
        stake_rounding=StakeRoundingMode(_text(data["stake_rounding"], "stake_rounding")),
        maximum_candidate_bankroll_fraction=_number(
            data["maximum_candidate_bankroll_fraction"],
            "maximum_candidate_bankroll_fraction",
        ),
        maximum_game_bankroll_fraction=_number(
            data["maximum_game_bankroll_fraction"],
            "maximum_game_bankroll_fraction",
        ),
        maximum_portfolio_bankroll_fraction=_number(
            data["maximum_portfolio_bankroll_fraction"],
            "maximum_portfolio_bankroll_fraction",
        ),
        prohibit_opposing_positions=_boolean(
            data["prohibit_opposing_positions"], "prohibit_opposing_positions"
        ),
        correlation_check_mandatory=_boolean(
            data["correlation_check_mandatory"], "correlation_check_mandatory"
        ),
        exposure_eligible_statuses=tuple(_text(item, "exposure status") for item in statuses),
        source=PolicyValueSource(_text(data["source"], "source")),
    )


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{label} must be a JSON object with string keys.")
    return cast(dict[str, object], value)


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a JSON list.")
    return cast(list[object], value)


def _exact_keys(data: dict[str, object], expected: set[str], label: str) -> None:
    if set(data) != expected:
        raise ValueError(f"{label} keys do not match the current schema.")


def _text(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a string.")
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


def _optional_float(value: object, label: str) -> float | None:
    return None if value is None else _number(value, label)


def _optional_integer(value: object, label: str) -> int | None:
    return None if value is None else _integer(value, label)


def _datetime(value: object) -> datetime:
    text = _text(value, "created_at")
    result = datetime.fromisoformat(text)
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("Recommendation policy timestamp must be timezone-aware UTC.")
    return result


def _require_digest(value: str, label: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest.")
    return value
