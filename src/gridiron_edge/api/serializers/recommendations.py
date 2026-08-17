"""Mechanical serialization of persisted recommended-bet results."""

from __future__ import annotations

from gridiron_edge.api.schemas.recommendations import (
    RecommendationBankrollBasisResponse,
    RecommendationCheckResponse,
    RecommendationForecastProvenanceResponse,
    RecommendationOfferProvenanceResponse,
    RecommendationPolicyProvenanceResponse,
    RecommendationPresentation,
    RecommendationSizingResponse,
)
from gridiron_edge.market.recommendation_policy import PolicyCheckResult, PolicyCheckStatus
from gridiron_edge.market.recommended_bet_result import RecommendedBetResult


def _check(value: PolicyCheckResult) -> RecommendationCheckResponse:
    return RecommendationCheckResponse(
        check_id=value.check_id,
        mandatory=value.mandatory,
        status=value.status,
        reason=value.reason,
        observed_value=value.observed_value,
        required_value=value.required_value,
    )


def serialize_recommendation_result(
    result: RecommendedBetResult,
    *,
    evaluation_id: str | None,
) -> RecommendationPresentation:
    """Serialize persisted evidence without deriving qualification or sizing."""
    checks = tuple(_check(value) for value in result.checks)
    supporting = tuple(value for value in checks if value.status is PolicyCheckStatus.PASSED)
    failed = tuple(value for value in checks if value.status is PolicyCheckStatus.FAILED)
    unavailable = tuple(
        value
        for value in checks
        if value.status in {PolicyCheckStatus.UNAVAILABLE, PolicyCheckStatus.CONFLICTING_EVIDENCE}
    )
    bankroll = result.bankroll_basis
    return RecommendationPresentation(
        result_id=result.result_id,
        evaluation_id=evaluation_id,
        result_state=result.result_state,
        decision_state=result.decision_state,
        recommendation_eligible=result.recommendation_eligible,
        evaluated_at=result.evaluated_at,
        issuance_quote_age_seconds=result.issuance_quote_age_seconds,
        decision_quote_age_seconds=result.decision_quote_age_seconds,
        checks=checks,
        supporting_checks=supporting,
        failed_checks=failed,
        unavailable_checks=unavailable,
        sizing=RecommendationSizingResponse(
            full_kelly_fraction=result.sizing.full_kelly_fraction,
            fractional_kelly_fraction=result.sizing.fractional_kelly_fraction,
            raw_stake=result.sizing.raw_stake,
            constrained_stake=result.sizing.constrained_stake,
            rounded_stake=result.sizing.rounded_stake,
            actionable_stake=result.sizing.actionable_stake,
        ),
        suggested_stake=result.sizing.actionable_stake,
        bankroll_basis=(
            None
            if bankroll is None
            else RecommendationBankrollBasisResponse(
                amount=bankroll.amount,
                observed_at=bankroll.observed_at,
                source_kind=bankroll.source_kind,
                source_id=bankroll.source_id,
            )
        ),
        portfolio_snapshot_id=result.portfolio_snapshot_id,
        portfolio_observed_at=result.portfolio_observed_at,
        offer_provenance=RecommendationOfferProvenanceResponse(
            issuance_id=result.issuance_id,
            candidate_reference_id=result.candidate_reference_id,
            game_id=result.game_id,
            market=result.market,
            side=result.side,
            provider=result.provider,
            provider_event_id=result.provider_event_id,
            sportsbook=result.sportsbook,
            fetched_at=result.fetched_at,
            sportsbook_updated_at=result.sportsbook_updated_at,
            kickoff=result.kickoff,
            is_live=result.is_live,
            american_price=result.american_price,
            line=result.line,
        ),
        forecast_provenance=RecommendationForecastProvenanceResponse(
            product_id=result.product_id,
            product_run_id=result.product_run_id,
            product_generated_at=result.product_generated_at,
            forecast_event_id=result.forecast_event_id,
            forecast_run_id=result.forecast_run_id,
            forecast_role=result.forecast_role,
            forecast_generated_at=result.forecast_generated_at,
            model_name=result.model_name,
            model_type=result.model_type,
            model_probability=result.model_probability,
            expected_value=result.expected_value,
        ),
        policy_provenance=RecommendationPolicyProvenanceResponse(
            policy_id=result.policy_id,
            policy_schema_version=result.policy_schema_version,
            source_evidence_fingerprint=result.source_evidence_fingerprint,
            governance_fingerprint=result.governance_fingerprint,
            derivation_method=result.derivation_method,
        ),
    )
