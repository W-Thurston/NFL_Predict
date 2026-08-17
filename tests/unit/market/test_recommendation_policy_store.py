"""Tests for immutable recommendation-policy JSON persistence."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
import json
from pathlib import Path

import pytest

from gridiron_edge.market.recommendation_policy import (
    RECOMMENDATION_POLICY_DERIVATION_METHOD,
    RECOMMENDATION_POLICY_SCHEMA_VERSION,
    MarketFamilyRecommendationPolicy,
    PolicyDerivationReason,
    PolicyDerivationStatus,
    PolicyValueSource,
    RecommendationPolicy,
    RecommendationPolicyGovernance,
    StakeRoundingMode,
    governance_fingerprint,
    recommendation_policy_id,
)
from gridiron_edge.market.recommendation_policy_store import (
    read_recommendation_policy,
    recommendation_policy_path,
    write_recommendation_policy,
)


def _family(market: str) -> MarketFamilyRecommendationPolicy:
    return MarketFamilyRecommendationPolicy(
        market,
        PolicyDerivationStatus.INSUFFICIENT_EVIDENCE,
        PolicyDerivationReason.NO_VALIDATED_THRESHOLD_SELECTION_METHOD,
        10,
        8,
        7,
        6,
        (("expected_value_cohorts", "available"),),
        None,
        PolicyValueSource.EMPIRICAL_MARKET_FAMILY_EVIDENCE,
    )


def _policy() -> RecommendationPolicy:
    governance = RecommendationPolicyGovernance(
        0.25,
        5.0,
        1.0,
        StakeRoundingMode.DOWN,
        0.02,
        0.05,
        0.20,
        True,
        True,
        ("open",),
    )
    policy = RecommendationPolicy(
        RECOMMENDATION_POLICY_SCHEMA_VERSION,
        "0" * 64,
        datetime(2026, 8, 17, tzinfo=UTC),
        "a" * 64,
        governance_fingerprint(governance),
        RECOMMENDATION_POLICY_DERIVATION_METHOD,
        _family("moneyline"),
        _family("spread"),
        _family("total"),
        governance,
    )
    return replace(policy, policy_id=recommendation_policy_id(policy))


def test_round_trip(tmp_path: Path) -> None:
    policy = _policy()
    path = write_recommendation_policy(policy, repo=tmp_path)
    assert path == recommendation_policy_path(
        policy.schema_version, policy.policy_id, repo=tmp_path
    )
    assert read_recommendation_policy(path) == policy


def test_exact_replay_is_idempotent(tmp_path: Path) -> None:
    policy = _policy()
    first = write_recommendation_policy(policy, repo=tmp_path)
    content = first.read_text()
    second = write_recommendation_policy(policy, repo=tmp_path)
    assert second == first
    assert second.read_text() == content


def test_conflicting_replay_is_rejected(tmp_path: Path) -> None:
    policy = _policy()
    path = write_recommendation_policy(policy, repo=tmp_path)
    payload = json.loads(path.read_text())
    payload["created_at"] = "2026-08-18T00:00:00+00:00"
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError):
        write_recommendation_policy(policy, repo=tmp_path)


def test_malformed_top_level_and_nested_keys_are_rejected(tmp_path: Path) -> None:
    policy = _policy()
    path = write_recommendation_policy(policy, repo=tmp_path)
    valid_content = path.read_text()
    payload = json.loads(valid_content)
    payload["unexpected"] = True
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="keys"):
        read_recommendation_policy(path)

    path.write_text(valid_content)
    payload = json.loads(valid_content)
    payload["governance"]["unexpected"] = True
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="Governance keys"):
        read_recommendation_policy(path)


def test_unsupported_schema_and_identity_mismatch_are_rejected(tmp_path: Path) -> None:
    policy = _policy()
    path = write_recommendation_policy(policy, repo=tmp_path)
    valid_content = path.read_text()
    payload = json.loads(valid_content)
    payload["schema_version"] = 999
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="schema version"):
        read_recommendation_policy(path)

    path.write_text(valid_content)
    payload = json.loads(valid_content)
    payload["governance"]["fractional_kelly_multiplier"] = 0.10
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match=r"fingerprint|ID"):
        read_recommendation_policy(path)


def test_filename_and_policy_id_mismatch_is_rejected(tmp_path: Path) -> None:
    policy = _policy()
    path = write_recommendation_policy(policy, repo=tmp_path)
    other = path.with_name(f"{'f' * 64}.json")
    other.write_text(path.read_text())
    with pytest.raises(ValueError, match="path"):
        read_recommendation_policy(other)


def test_unsafe_identity_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="SHA-256"):
        recommendation_policy_path(1, "../escape", repo=tmp_path)


def test_store_has_no_current_selection_or_request_path_dependency() -> None:
    source = Path("src/gridiron_edge/market/recommendation_policy_store.py").read_text()
    for forbidden in (
        "current.json",
        "gridiron_edge.api",
        "gridiron_edge.cli",
        "gridiron_edge.betting.bankroll",
        "gridiron_edge.betting.ledger",
    ):
        assert forbidden not in source
