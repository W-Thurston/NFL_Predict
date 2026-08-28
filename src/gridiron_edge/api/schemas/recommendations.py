"""Read-only API schemas for persisted recommendation results."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict

from gridiron_edge.market.recommendation_policy import (
    PolicyCheckStatus,
    PortfolioAllocationReason,
    PortfolioAllocationState,
    RecommendationDecisionState,
)
from gridiron_edge.market.recommended_bet_result import RecommendedBetResultState


class RecommendationCheckResponse(BaseModel):
    """One persisted policy check without request-time reinterpretation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    check_id: str
    mandatory: bool
    status: PolicyCheckStatus
    reason: str
    observed_value: float | str | None = None
    required_value: float | str | None = None


class RecommendationSizingResponse(BaseModel):
    """Persisted Kelly and stake evidence from the original decision."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    full_kelly_fraction: float | None = None
    fractional_kelly_fraction: float | None = None
    raw_stake: float | None = None
    constrained_stake: float | None = None
    rounded_stake: float | None = None
    actionable_stake: float | None = None


class RecommendationAllocationResponse(BaseModel):
    """Mechanical projection of one persisted portfolio-allocation outcome."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    state: PortfolioAllocationState
    reason: PortfolioAllocationReason
    allocated_stake: float | None = None


class RecommendationBankrollBasisResponse(BaseModel):
    """Persisted bankroll basis used by the original policy evaluation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    amount: float
    observed_at: datetime
    source_kind: str
    source_id: str


class RecommendationOfferProvenanceResponse(BaseModel):
    """Exact immutable offer evidence associated with one result."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    issuance_id: str
    candidate_reference_id: str
    candidate_reference_derivation_version: int
    game_id: str
    market: str
    side: str
    provider: str
    provider_event_id: str | None = None
    sportsbook: str | None = None
    fetched_at: datetime
    sportsbook_updated_at: datetime | None = None
    kickoff: datetime | None = None
    is_live: bool
    american_price: int | None = None
    line: float | None = None


class RecommendationForecastProvenanceResponse(BaseModel):
    """Selected-product and forecast evidence associated with one result."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    product_id: str
    product_run_id: str
    product_generated_at: datetime
    forecast_event_id: str | None = None
    forecast_run_id: str | None = None
    forecast_role: str | None = None
    forecast_generated_at: datetime | None = None
    model_name: str | None = None
    model_type: str | None = None
    model_probability: float | None = None
    expected_value: float | None = None


class RecommendationPolicyProvenanceResponse(BaseModel):
    """Exact persisted policy provenance associated with one result."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    policy_id: str
    policy_schema_version: int
    source_evidence_fingerprint: str
    governance_fingerprint: str
    derivation_method: str


class RecommendationPresentation(BaseModel):
    """Mechanical read-only presentation of one persisted recommendation result."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    result_id: str
    evaluation_id: str | None = None
    result_state: RecommendedBetResultState
    decision_state: RecommendationDecisionState
    recommendation_eligible: bool
    evaluated_at: datetime
    issuance_quote_age_seconds: float
    decision_quote_age_seconds: float
    checks: tuple[RecommendationCheckResponse, ...]
    supporting_checks: tuple[RecommendationCheckResponse, ...]
    failed_checks: tuple[RecommendationCheckResponse, ...]
    unavailable_checks: tuple[RecommendationCheckResponse, ...]
    sizing: RecommendationSizingResponse
    allocation: RecommendationAllocationResponse
    suggested_stake: float | None = None
    bankroll_basis: RecommendationBankrollBasisResponse | None = None
    portfolio_snapshot_id: str | None = None
    portfolio_observed_at: datetime | None = None
    offer_provenance: RecommendationOfferProvenanceResponse
    forecast_provenance: RecommendationForecastProvenanceResponse
    policy_provenance: RecommendationPolicyProvenanceResponse
