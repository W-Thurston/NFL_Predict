# tests/unit/market/test_recommendation_policy.py
"""Tests for immutable recommendation-policy contracts and derivation."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from gridiron_edge.market.market_family_evaluation import (
    EmpiricalMarketFamilyEvaluation,
    EvaluationEvidenceStatus,
    MarketFamilyCoverage,
    MarketFamilyEvaluation,
    MetricEstimate,
    NumericCohortSet,
    ObservationDepthEvaluation,
    QuoteAgeEvaluation,
    RealizedReturnEvaluation,
    ReliabilityEvaluation,
)
from gridiron_edge.market.recommendation_policy import (
    PolicyDerivationReason,
    PolicyDerivationStatus,
    RecommendationPolicyGovernance,
    StakeRoundingMode,
    derive_recommendation_policy,
    empirical_evidence_fingerprint,
    validate_recommendation_policy,
)


def _metric() -> MetricEstimate:
    return MetricEstimate(EvaluationEvidenceStatus.AVAILABLE, 4, 0.2, None)


def _cohorts(kind: str, market: str) -> NumericCohortSet:
    del market
    return NumericCohortSet(kind, EvaluationEvidenceStatus.AVAILABLE, 4, 4, 4, 0, (), None)


def _family(market: str) -> MarketFamilyEvaluation:
    coverage = MarketFamilyCoverage(
        market, 4, 4, 0, 0, 4, 4, 4, 0, 0, 0, 4, 4, 4, 4, 0, 4, 0, 0, ()
    )
    return MarketFamilyEvaluation(
        market=market,
        coverage=coverage,
        reliability=ReliabilityEvaluation(4, 4, 3, 1, 0, 0, 0, _metric(), _metric(), _metric()),
        quote_age=QuoteAgeEvaluation(
            EvaluationEvidenceStatus.AVAILABLE, 4, 4, 0, 10.0, 20.0, 30.0, None
        ),
        observation_depth=ObservationDepthEvaluation(
            EvaluationEvidenceStatus.AVAILABLE, 4, 4, 0, 0, 2, 3.0, 4, 2, 3.0, 4, None
        ),
        sportsbook_cohorts=(),
        market_side_cohorts=(),
        expected_value_cohorts=_cohorts("expected_value", market),
        clv_cohorts=_cohorts("clv", market),
        quote_age_cohorts=_cohorts("quote_age_seconds", market),
        observation_count_cohorts=_cohorts("observation_count", market),
        distinct_fetch_count_cohorts=_cohorts("distinct_fetch_count", market),
        realized_return=RealizedReturnEvaluation(
            EvaluationEvidenceStatus.AVAILABLE, 4, 4, 0, 0, 400.0, 40.0, 0.1, 0.1, None
        ),
    )


def _evaluation() -> EmpiricalMarketFamilyEvaluation:
    return EmpiricalMarketFamilyEvaluation(
        _family("moneyline"), _family("spread"), _family("total")
    )


def _governance() -> RecommendationPolicyGovernance:
    return RecommendationPolicyGovernance(
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


def test_complete_descriptive_evidence_does_not_invent_thresholds() -> None:
    policy = derive_recommendation_policy(
        evaluation=_evaluation(),
        governance=_governance(),
        created_at=datetime(2026, 8, 17, tzinfo=UTC),
    )
    for family in (policy.moneyline, policy.spread, policy.total):
        assert family.status is PolicyDerivationStatus.INSUFFICIENT_EVIDENCE
        assert family.reason is PolicyDerivationReason.NO_VALIDATED_THRESHOLD_SELECTION_METHOD
        assert family.thresholds is None


def test_created_at_does_not_change_policy_identity() -> None:
    first = derive_recommendation_policy(
        evaluation=_evaluation(),
        governance=_governance(),
        created_at=datetime(2026, 8, 17, tzinfo=UTC),
    )
    second = derive_recommendation_policy(
        evaluation=_evaluation(),
        governance=_governance(),
        created_at=datetime(2026, 8, 18, tzinfo=UTC),
    )
    assert first.policy_id == second.policy_id
    assert first.source_evidence_fingerprint == second.source_evidence_fingerprint


def test_governance_change_changes_policy_identity() -> None:
    first = derive_recommendation_policy(
        evaluation=_evaluation(),
        governance=_governance(),
        created_at=datetime(2026, 8, 17, tzinfo=UTC),
    )
    changed = replace(_governance(), fractional_kelly_multiplier=0.10)
    second = derive_recommendation_policy(
        evaluation=_evaluation(), governance=changed, created_at=datetime(2026, 8, 17, tzinfo=UTC)
    )
    assert first.policy_id != second.policy_id


def test_evidence_fingerprint_is_deterministic() -> None:
    assert empirical_evidence_fingerprint(_evaluation()) == empirical_evidence_fingerprint(
        _evaluation()
    )


def test_policy_identity_mismatch_is_rejected() -> None:
    policy = derive_recommendation_policy(
        evaluation=_evaluation(),
        governance=_governance(),
        created_at=datetime(2026, 8, 17, tzinfo=UTC),
    )
    changed = replace(policy, policy_id="0" * 64)
    with pytest.raises(ValueError, match="ID"):
        validate_recommendation_policy(changed)


def test_governance_requires_ordered_exposure_limits() -> None:
    governance = replace(_governance(), maximum_candidate_bankroll_fraction=0.10)
    with pytest.raises(ValueError, match="ordered"):
        derive_recommendation_policy(
            evaluation=_evaluation(),
            governance=governance,
            created_at=datetime(2026, 8, 17, tzinfo=UTC),
        )


def test_policy_module_has_no_mutable_or_request_path_dependency() -> None:
    source = Path("src/gridiron_edge/market/recommendation_policy.py").read_text()
    for forbidden in (
        "gridiron_edge.api",
        "gridiron_edge.cli",
        "gridiron_edge.betting.bankroll",
        "gridiron_edge.betting.ledger",
    ):
        assert forbidden not in source
