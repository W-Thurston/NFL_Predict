"""Immutable recommended-bet results and deterministic issuance evaluation."""

from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timedelta
from enum import Enum, StrEnum
from hashlib import sha256
import json
import math
from typing import Final

from gridiron_edge.market.candidate_issuance import (
    CandidateIssuance,
    CandidateIssuanceRow,
    CandidateIssuanceState,
    candidate_issuance_row_id,
)
from gridiron_edge.market.recommendation_policy import (
    BankrollBasis,
    CorrelationExposureEvidence,
    PolicyCheckResult,
    PolicyCheckStatus,
    PortfolioExposureSnapshot,
    RecommendationDecisionState,
    RecommendationPolicy,
    RecommendationPolicyDecision,
    RecommendationSizingResult,
    evaluate_recommendation_candidate,
    validate_recommendation_policy,
)

RECOMMENDED_BET_RESULT_SCHEMA_VERSION: Final[int] = 1


class RecommendedBetResultState(StrEnum):
    """Persistable lifecycle summary of one exact policy decision."""

    QUALIFIED = "qualified"
    RECOMMENDED = "recommended"
    FAILED = "failed"
    UNAVAILABLE = "unavailable"
    CONFLICTING = "conflicting"


@dataclass(frozen=True, slots=True)
class RecommendedBetResult:
    """Complete immutable result for one evaluated exact candidate offer."""

    schema_version: int
    result_id: str
    issuance_schema_version: int
    issuance_id: str
    candidate_reference_id: str
    product_id: str
    product_run_id: str
    product_generated_at: datetime
    season: str
    week: int
    game_id: str
    market: str
    side: str
    provider: str
    provider_event_id: str | None
    sportsbook: str | None
    fetched_at: datetime
    sportsbook_updated_at: datetime | None
    kickoff: datetime | None
    is_live: bool
    american_price: int | None
    line: float | None
    forecast_event_id: str | None
    forecast_run_id: str | None
    forecast_role: str | None
    forecast_generated_at: datetime | None
    model_name: str | None
    model_type: str | None
    model_probability: float | None
    expected_value: float | None
    policy_schema_version: int
    policy_id: str
    source_evidence_fingerprint: str
    governance_fingerprint: str
    derivation_method: str
    evaluated_at: datetime
    issuance_quote_age_seconds: float
    decision_quote_age_seconds: float
    decision_state: RecommendationDecisionState
    result_state: RecommendedBetResultState
    recommendation_eligible: bool
    checks: tuple[PolicyCheckResult, ...]
    sizing: RecommendationSizingResult
    bankroll_basis: BankrollBasis | None
    portfolio_snapshot_id: str | None
    portfolio_observed_at: datetime | None
    correlation_evidence: CorrelationExposureEvidence | None


@dataclass(frozen=True, slots=True)
class RecommendedBetEvaluation:
    """One deterministic evaluation invocation over all issued candidates."""

    schema_version: int
    evaluation_id: str
    issuance_id: str
    policy_id: str
    evaluated_at: datetime
    results: tuple[RecommendedBetResult, ...]


def build_recommended_bet_result(
    *,
    issuance: CandidateIssuance,
    policy: RecommendationPolicy,
    decision: RecommendationPolicyDecision,
    bankroll: BankrollBasis | None,
    portfolio: PortfolioExposureSnapshot | None,
    correlation: CorrelationExposureEvidence | None,
) -> RecommendedBetResult:
    """Freeze one existing decision with its exact immutable provenance."""
    validate_recommendation_policy(policy)
    row = _resolve_row(issuance, decision.candidate_reference_id)
    if decision.policy_id != policy.policy_id or decision.market != row.market:
        raise ValueError("Decision provenance does not match policy or candidate.")
    expected_issuance_age = (issuance.evaluated_at - row.fetched_at).total_seconds()
    expected_decision_age = (decision.decision_at - row.fetched_at).total_seconds()
    if (
        decision.issuance_quote_age_seconds != expected_issuance_age
        or decision.decision_quote_age_seconds != expected_decision_age
    ):
        raise ValueError("Decision quote-age evidence disagrees with issuance evidence.")
    state = _result_state(decision)
    result = RecommendedBetResult(
        RECOMMENDED_BET_RESULT_SCHEMA_VERSION,
        "0" * 64,
        issuance.schema_version,
        issuance.issuance_id,
        decision.candidate_reference_id,
        issuance.product_id,
        issuance.product_run_id,
        issuance.product_generated_at,
        issuance.season,
        issuance.week,
        row.game_id,
        row.market,
        row.side,
        row.provider,
        row.provider_event_id,
        row.sportsbook,
        row.fetched_at,
        row.sportsbook_updated_at,
        row.kickoff,
        row.is_live,
        row.american_price,
        row.line,
        row.forecast_event_id,
        row.forecast_run_id,
        row.forecast_role,
        row.forecast_generated_at,
        row.model_name,
        row.model_type,
        row.model_probability,
        row.expected_value,
        policy.schema_version,
        policy.policy_id,
        policy.source_evidence_fingerprint,
        policy.governance_fingerprint,
        policy.derivation_method,
        decision.decision_at,
        decision.issuance_quote_age_seconds,
        decision.decision_quote_age_seconds,
        decision.state,
        state,
        decision.recommendation_eligible,
        decision.checks,
        decision.sizing,
        bankroll,
        None if portfolio is None else portfolio.snapshot_id,
        None if portfolio is None else portfolio.observed_at,
        correlation,
    )
    result = _with_result_id(result)
    validate_recommended_bet_result(result)
    return result


def evaluate_recommendation_issuance(
    *,
    policy: RecommendationPolicy,
    issuance: CandidateIssuance,
    decision_at: datetime,
    bankroll: BankrollBasis | None = None,
    portfolio: PortfolioExposureSnapshot | None = None,
    correlations: tuple[CorrelationExposureEvidence, ...] = (),
) -> RecommendedBetEvaluation:
    """Evaluate every issued candidate exactly once in canonical issuance order."""
    decision = _require_utc(decision_at, "decision_at")
    results: list[RecommendedBetResult] = []
    for row in issuance.rows:
        if row.state is not CandidateIssuanceState.CANDIDATE:
            continue
        reference = candidate_issuance_row_id(issuance.issuance_id, row)
        matching = tuple(
            evidence for evidence in correlations if reference in evidence.member_reference_ids
        )
        if len(matching) > 1:
            raise ValueError("Candidate belongs to multiple correlation evidence groups.")
        correlation = matching[0] if matching else None
        policy_decision = evaluate_recommendation_candidate(
            policy=policy,
            issuance=issuance,
            candidate_reference_id=reference,
            decision_at=decision,
            bankroll=bankroll,
            portfolio=portfolio,
            correlation=correlation,
        )
        results.append(
            build_recommended_bet_result(
                issuance=issuance,
                policy=policy,
                decision=policy_decision,
                bankroll=bankroll,
                portfolio=portfolio,
                correlation=correlation,
            )
        )
    ordered = tuple(results)
    if len({result.result_id for result in ordered}) != len(ordered):
        raise ValueError("Recommended-bet evaluation contains duplicate result identities.")
    identity = {
        "schema_version": RECOMMENDED_BET_RESULT_SCHEMA_VERSION,
        "issuance_id": issuance.issuance_id,
        "policy_id": policy.policy_id,
        "evaluated_at": decision.isoformat(),
        "result_ids": [result.result_id for result in ordered],
    }
    return RecommendedBetEvaluation(
        RECOMMENDED_BET_RESULT_SCHEMA_VERSION,
        _digest(identity),
        issuance.issuance_id,
        policy.policy_id,
        decision,
        ordered,
    )


def recommended_bet_result_id(result: RecommendedBetResult) -> str:
    """Return the deterministic identity of complete immutable result evidence."""
    payload = _canonical(result)
    assert isinstance(payload, dict)
    payload["result_id"] = ""
    return _digest(payload)


def validate_recommended_bet_result(result: RecommendedBetResult) -> None:
    """Validate identity, provenance, lifecycle, timestamps, and check ordering."""
    if result.schema_version != RECOMMENDED_BET_RESULT_SCHEMA_VERSION:
        raise ValueError("Unsupported recommended-bet result schema version.")
    for value in (
        result.product_generated_at,
        result.fetched_at,
        result.evaluated_at,
    ):
        _require_utc(value, "recommended-bet timestamp")
    for value in (
        result.sportsbook_updated_at,
        result.kickoff,
        result.forecast_generated_at,
        None if result.bankroll_basis is None else result.bankroll_basis.observed_at,
        result.portfolio_observed_at,
    ):
        if value is not None:
            _require_utc(value, "optional recommended-bet timestamp")
    if result.candidate_reference_id != candidate_issuance_row_id(
        result.issuance_id, _result_row(result)
    ):
        raise ValueError("Recommended-bet candidate identity does not match offer evidence.")
    if result.policy_id == "" or result.result_id != recommended_bet_result_id(result):
        raise ValueError("Recommended-bet result ID does not match canonical content.")
    if len({check.check_id for check in result.checks}) != len(result.checks):
        raise ValueError("Recommended-bet checks contain duplicate identifiers.")
    if result.recommendation_eligible != (
        result.decision_state is RecommendationDecisionState.RECOMMENDATION_ELIGIBLE
    ):
        raise ValueError("Recommendation eligibility disagrees with decision state.")
    if result.result_state is RecommendedBetResultState.RECOMMENDED and (
        result.sizing.actionable_stake is None or not result.recommendation_eligible
    ):
        raise ValueError("Recommended result requires actionable stake and eligibility.")
    if result.portfolio_snapshot_id is None and result.portfolio_observed_at is not None:
        raise ValueError("Portfolio observation requires a snapshot identity.")


def _result_state(decision: RecommendationPolicyDecision) -> RecommendedBetResultState:
    if decision.state is RecommendationDecisionState.RECOMMENDATION_ELIGIBLE:
        return RecommendedBetResultState.RECOMMENDED
    if decision.state is RecommendationDecisionState.QUALIFIED_OPPORTUNITY:
        return RecommendedBetResultState.QUALIFIED
    if decision.state is RecommendationDecisionState.UNQUALIFIED:
        return RecommendedBetResultState.FAILED
    if any(
        check.mandatory and check.status is PolicyCheckStatus.CONFLICTING_EVIDENCE
        for check in decision.checks
    ):
        return RecommendedBetResultState.CONFLICTING
    return RecommendedBetResultState.UNAVAILABLE


def _resolve_row(issuance: CandidateIssuance, reference: str) -> CandidateIssuanceRow:
    matches = tuple(
        row
        for row in issuance.rows
        if candidate_issuance_row_id(issuance.issuance_id, row) == reference
    )
    if len(matches) != 1:
        raise ValueError("Result candidate reference must resolve exactly once.")
    return matches[0]


def _with_result_id(result: RecommendedBetResult) -> RecommendedBetResult:
    values = {field.name: getattr(result, field.name) for field in fields(result)}
    values["result_id"] = recommended_bet_result_id(result)
    return RecommendedBetResult(**values)


def _result_row(result: RecommendedBetResult) -> CandidateIssuanceRow:
    return CandidateIssuanceRow(
        result.game_id,
        result.market,
        result.side,
        result.provider,
        result.provider_event_id,
        result.sportsbook,
        result.line,
        result.american_price,
        result.fetched_at,
        result.sportsbook_updated_at,
        result.kickoff,
        result.is_live,
        result.forecast_event_id,
        result.forecast_run_id,
        result.forecast_role,
        result.forecast_generated_at,
        result.model_name,
        result.model_type,
        result.model_probability,
        result.expected_value,
        CandidateIssuanceState.CANDIDATE,
        __import__(
            "gridiron_edge.market.candidate_issuance", fromlist=["CandidateIssuanceReason"]
        ).CandidateIssuanceReason.POSITIVE_EXPECTED_VALUE,
    )


def _canonical(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _canonical(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return _require_utc(value, "canonical timestamp").isoformat()
    if isinstance(value, tuple | list):
        return [_canonical(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Recommended-bet evidence cannot contain non-finite numbers.")
    if value is None or isinstance(value, bool | int | float | str):
        return value
    raise TypeError(f"Unsupported recommended-bet value: {type(value).__name__}")


def _digest(value: object) -> str:
    return sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _require_utc(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must be timezone-aware UTC.")
    return value
