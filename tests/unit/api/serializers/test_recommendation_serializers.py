"""Tests for mechanical persisted-recommendation API serialization."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from tests.fixtures.recommended_bet_results import evaluation

from gridiron_edge.api.serializers.recommendations import serialize_recommendation_result
from gridiron_edge.market.recommendation_policy import PolicyCheckStatus


def test_serializer_preserves_persisted_result_without_recalculation() -> None:
    value = evaluation()
    result = value.results[0]
    response = serialize_recommendation_result(result, evaluation_id=value.evaluation_id)
    assert response.result_id == result.result_id
    assert response.evaluation_id == value.evaluation_id
    assert response.result_state == result.result_state
    assert response.decision_state == result.decision_state
    assert response.suggested_stake == result.sizing.actionable_stake
    assert response.sizing.actionable_stake == result.sizing.actionable_stake
    assert response.offer_provenance.candidate_reference_id == result.candidate_reference_id
    assert response.policy_provenance.policy_id == result.policy_id
    assert response.forecast_provenance.product_id == result.product_id


def test_serializer_categories_are_mechanical_views_of_complete_checks() -> None:
    result = evaluation().results[0]
    response = serialize_recommendation_result(result, evaluation_id=None)
    assert response.checks
    assert all(value.status is PolicyCheckStatus.PASSED for value in response.supporting_checks)
    assert all(value.status is PolicyCheckStatus.FAILED for value in response.failed_checks)
    assert all(
        value.status in {PolicyCheckStatus.UNAVAILABLE, PolicyCheckStatus.CONFLICTING_EVIDENCE}
        for value in response.unavailable_checks
    )


def test_suggested_stake_is_direct_persisted_actionable_stake_alias() -> None:
    result = evaluation().results[0]
    changed_sizing = replace(result.sizing, actionable_stake=7.0)
    changed = replace(result, sizing=changed_sizing)
    response = serialize_recommendation_result(changed, evaluation_id=None)
    assert response.suggested_stake == 7.0


def test_serializer_has_no_policy_evaluation_or_mutable_dependency() -> None:
    source = Path("src/gridiron_edge/api/serializers/recommendations.py").read_text(
        encoding="utf-8"
    )
    for forbidden in (
        "evaluate_recommendation_candidate",
        "evaluate_recommendation_issuance",
        "gridiron_edge.market.kelly",
        "gridiron_edge.market.weekly_edge_service",
        "gridiron_edge.betting.bankroll",
        "gridiron_edge.betting.ledger",
    ):
        assert forbidden not in source
