"""Retrospective decision-quality evaluation for one persisted result.

Decision quality evaluates whether valid decision-time artifacts agree
with each other and with a replay of their owning rules. Realized
outcome is recorded beside that assessment, but never participates in
deciding whether the original decision was justified.

Invalid individual artifacts (malformed identity, schema, or internal
structure) raise and produce no evaluation -- this module never
references a result, policy, or issuance whose own identity is not
trustworthy. INCONSISTENT is reserved for valid, individually-correct
artifacts that disagree across a required boundary (e.g. the result
claims a policy that, once actually resolved and validated, does not
match).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum
from hashlib import sha256
import json
from typing import Final

from gridiron_edge.market.candidate_issuance import (
    CANDIDATE_ISSUANCE_SCHEMA_VERSION,
    CandidateIssuance,
    candidate_issuance_row_id,
)
from gridiron_edge.market.candidate_outcome import CandidateOutcome
from gridiron_edge.market.recommendation_policy import (
    RECOMMENDATION_POLICY_SCHEMA_VERSION,
    CorrelationExposureEvidence,
    PortfolioExposureSnapshot,
    RecommendationPolicy,
    evaluate_recommendation_candidate,
    validate_recommendation_policy,
)
from gridiron_edge.market.recommended_bet_result import (
    RecommendedBetEvaluation,
    RecommendedBetResult,
    validate_recommended_bet_evaluation,
    validate_recommended_bet_result,
)

DECISION_QUALITY_SCHEMA_VERSION: Final[int] = 1
DECISION_QUALITY_CHECK_IDS: Final[tuple[str, ...]] = (
    "result_integrity",
    "recommendation_evaluation_reference",
    "policy_reference",
    "candidate_reference",
    "allocation_recomputation",
)
_DECISION_QUALITY_MANDATORY_BY_CHECK_ID: Final[dict[str, bool]] = {
    "result_integrity": True,
    "recommendation_evaluation_reference": True,
    "policy_reference": True,
    "candidate_reference": True,
    "allocation_recomputation": False,
}


class DecisionQualityStatus(StrEnum):
    """Result of one retrospective cross-artifact consistency check.

    CONFLICTING_EVIDENCE is intentionally not defined. No concrete,
    testable first-unit input topology produces non-unique evidence
    resolution -- add it in a later unit only when a real input shape
    requires it.
    """

    CONSISTENT = "consistent"
    INCONSISTENT = "inconsistent"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class DecisionQualityCheck:
    """One named, ordered retrospective consistency check.

    `mandatory=False` means: its own absence of evidence does not lower
    the overall conclusion. It does NOT mean a completed, contradictory
    result is ignored -- `_overall_status` treats any completed
    INCONSISTENT check, mandatory or not, as decisive.
    """

    check_id: str
    mandatory: bool
    status: DecisionQualityStatus
    reason: str


@dataclass(frozen=True, slots=True)
class DecisionQualityEvaluation:
    """Immutable retrospective assessment of one recommendation decision.

    Cross-artifact consistency, independent of realized outcome. Every
    supplied evidence identity that could affect a check is recorded, so
    evaluation identity changes if that evidence changes -- without
    duplicating the underlying artifacts themselves.
    """

    schema_version: int
    evaluation_id: str
    evaluated_at: datetime
    result_id: str
    recommendation_evaluation_id: str
    candidate_reference_id: str
    policy_id: str
    policy_schema_version: int
    issuance_id: str
    portfolio_snapshot_id: str | None
    correlation_evidence_fingerprint: str | None
    checks: tuple[DecisionQualityCheck, ...]
    decision_status: DecisionQualityStatus
    realized_outcome: CandidateOutcome


def evaluate_decision_quality(
    *,
    result_id: str,
    recommendation_evaluation: RecommendedBetEvaluation,
    policy: RecommendationPolicy,
    issuance: CandidateIssuance,
    portfolio: PortfolioExposureSnapshot | None = None,
    correlation: CorrelationExposureEvidence | None = None,
    outcome: CandidateOutcome = CandidateOutcome.UNAVAILABLE,
    evaluated_at: datetime,
) -> DecisionQualityEvaluation:
    """Evaluate one recommendation decision's cross-artifact consistency.

    Against its exact referenced policy and issuance, and optionally
    against supplied portfolio/correlation evidence for full allocation
    replay.

    Raises:
        ValueError: If `recommendation_evaluation`, `policy`, or
            `issuance` is individually invalid, or if `result_id` does
            not occur exactly once inside `recommendation_evaluation.results`.
            This function never references an artifact whose own
            identity, or parent relationship, is not trustworthy.
    """
    validate_recommended_bet_evaluation(recommendation_evaluation)
    result = _resolve_result(recommendation_evaluation, result_id)
    validate_recommended_bet_result(result)
    validate_recommendation_policy(policy)
    _validate_candidate_issuance(issuance)
    evaluated = _require_utc(evaluated_at, label="evaluated_at")

    checks: list[DecisionQualityCheck] = []
    checks.append(
        DecisionQualityCheck(
            "result_integrity",
            True,
            DecisionQualityStatus.CONSISTENT,
            "recommended_bet_result_validated",
        )
    )
    checks.append(_recommendation_evaluation_reference_check(result, recommendation_evaluation))
    checks.append(_policy_reference_check(result, policy))
    checks.append(_candidate_reference_check(result, issuance))
    replay_check, correlation_fingerprint = _allocation_recomputation_check(
        result=result,
        policy=policy,
        issuance=issuance,
        portfolio=portfolio,
        correlation=correlation,
    )
    checks.append(replay_check)

    decision_status = _overall_status(checks)

    evaluation = DecisionQualityEvaluation(
        schema_version=DECISION_QUALITY_SCHEMA_VERSION,
        evaluation_id="0" * 64,
        evaluated_at=evaluated,
        result_id=result.result_id,
        recommendation_evaluation_id=recommendation_evaluation.evaluation_id,
        candidate_reference_id=result.candidate_reference_id,
        policy_id=policy.policy_id,
        policy_schema_version=policy.schema_version,
        issuance_id=issuance.issuance_id,
        portfolio_snapshot_id=portfolio.snapshot_id if portfolio is not None else None,
        correlation_evidence_fingerprint=correlation_fingerprint,
        checks=tuple(checks),
        decision_status=decision_status,
        realized_outcome=outcome,
    )
    evaluation = replace(evaluation, evaluation_id=decision_quality_evaluation_id(evaluation))
    validate_decision_quality_evaluation(evaluation)
    return evaluation


def decision_quality_evaluation_id(evaluation: DecisionQualityEvaluation) -> str:
    """Return the canonical identity of one decision-quality evaluation.

    The single owner of the identity payload's shape -- callers must
    never independently construct this dict; every caller supplies the
    evaluation object and receives the identity back.
    """
    payload: dict[str, object] = {
        "schema_version": evaluation.schema_version,
        "evaluated_at": evaluation.evaluated_at.isoformat(),
        "result_id": evaluation.result_id,
        "recommendation_evaluation_id": evaluation.recommendation_evaluation_id,
        "candidate_reference_id": evaluation.candidate_reference_id,
        "policy_id": evaluation.policy_id,
        "policy_schema_version": evaluation.policy_schema_version,
        "issuance_id": evaluation.issuance_id,
        "portfolio_snapshot_id": evaluation.portfolio_snapshot_id,
        "correlation_evidence_fingerprint": evaluation.correlation_evidence_fingerprint,
        "checks": [
            {
                "check_id": c.check_id,
                "mandatory": c.mandatory,
                "status": c.status.value,
                "reason": c.reason,
            }
            for c in evaluation.checks
        ],
        "decision_status": evaluation.decision_status.value,
        "realized_outcome": evaluation.realized_outcome.value,
    }
    return _digest(payload)


def validate_decision_quality_evaluation(evaluation: DecisionQualityEvaluation) -> None:
    """Own semantic validation and canonical identity for one evaluation.

    Both evaluate_decision_quality's caller and the store must rely on
    this -- not reimplement it.
    """
    if evaluation.schema_version != DECISION_QUALITY_SCHEMA_VERSION:
        raise ValueError("Unsupported decision-quality evaluation schema version.")
    _require_digest(evaluation.evaluation_id, "evaluation_id")
    _require_digest(evaluation.result_id, "result_id")
    _require_digest(evaluation.recommendation_evaluation_id, "recommendation_evaluation_id")
    _require_candidate_reference_id(evaluation.candidate_reference_id, "candidate_reference_id")
    _require_digest(evaluation.policy_id, "policy_id")
    _require_digest(evaluation.issuance_id, "issuance_id")
    if evaluation.portfolio_snapshot_id is not None:
        _require_digest(evaluation.portfolio_snapshot_id, "portfolio_snapshot_id")
    if evaluation.correlation_evidence_fingerprint is not None:
        _require_digest(
            evaluation.correlation_evidence_fingerprint, "correlation_evidence_fingerprint"
        )
    _require_utc(evaluation.evaluated_at, label="evaluated_at")

    if not evaluation.checks:
        raise ValueError("Decision-quality evaluation must contain at least one check.")
    check_ids = [c.check_id for c in evaluation.checks]
    if len(set(check_ids)) != len(check_ids):
        raise ValueError("Decision-quality evaluation contains duplicate check IDs.")
    if evaluation.policy_schema_version != RECOMMENDATION_POLICY_SCHEMA_VERSION:
        raise ValueError("Unsupported recommendation-policy schema version.")
    actual_check_ids = tuple(check.check_id for check in evaluation.checks)
    if actual_check_ids != DECISION_QUALITY_CHECK_IDS:
        raise ValueError(
            "Decision-quality evaluation does not use the exact schema-1 check set and order."
        )
    for check in evaluation.checks:
        expected_mandatory = _DECISION_QUALITY_MANDATORY_BY_CHECK_ID[check.check_id]
        if check.mandatory != expected_mandatory:
            raise ValueError(f"{check.check_id} has an incorrect mandatory flag for schema 1.")
        _require_nonempty(check.check_id, "check.check_id")
        _require_nonempty(check.reason, "check.reason")

    recomputed_status = _overall_status(list(evaluation.checks))
    if recomputed_status is not evaluation.decision_status:
        raise ValueError("decision_status does not agree with the recorded checks.")

    expected_id = decision_quality_evaluation_id(evaluation)
    if evaluation.evaluation_id != expected_id:
        raise ValueError("Decision-quality evaluation ID does not match canonical content.")


def _resolve_result(evaluation: RecommendedBetEvaluation, result_id: str) -> RecommendedBetResult:
    """Resolve result_id to exactly one result in the trusted parent manifest.

    This is what makes recommendation_evaluation_id a verified
    relationship, not a caller-supplied assertion.
    """
    matches = [r for r in evaluation.results if r.result_id == result_id]
    if len(matches) != 1:
        raise ValueError(
            "result_id does not resolve to exactly one result in the parent evaluation."
        )
    result = matches[0]
    if (
        result.issuance_id != evaluation.issuance_id
        or result.policy_id != evaluation.policy_id
        or result.evaluated_at != evaluation.evaluated_at
    ):
        raise ValueError(
            "Resolved result provenance disagrees with its parent evaluation manifest."
        )
    return result


def _validate_candidate_issuance(issuance: CandidateIssuance) -> None:
    """Bounded issuance identity and timestamp checks for this evaluator.

    Not full canonical issuance validation. If a public validator is
    later added to candidate_issuance.py, this should call it instead.
    """
    if issuance.schema_version != CANDIDATE_ISSUANCE_SCHEMA_VERSION:
        raise ValueError("Unsupported candidate issuance schema version.")
    _require_digest(issuance.issuance_id, "issuance.issuance_id")
    _require_utc(issuance.evaluated_at, label="issuance.evaluated_at")
    _require_utc(issuance.product_generated_at, label="issuance.product_generated_at")


def _recommendation_evaluation_reference_check(
    result: RecommendedBetResult, evaluation: RecommendedBetEvaluation
) -> DecisionQualityCheck:
    """Whether the result genuinely belongs to the supplied parent evaluation.

    Already enforced by _resolve_result raising on disagreement; this
    records that confirmed relationship visibly in the persisted
    evaluation's own check list.
    """
    return DecisionQualityCheck(
        "recommendation_evaluation_reference",
        True,
        DecisionQualityStatus.CONSISTENT,
        "result_resolves_from_parent_evaluation_with_agreeing_provenance",
    )


def _policy_reference_check(
    result: RecommendedBetResult, policy: RecommendationPolicy
) -> DecisionQualityCheck:
    """Whether persisted policy references agree with the actual policy.

    Compares against the actual, already-validated policy object -- not
    a string-only comparison.
    """
    agrees = (
        result.policy_id == policy.policy_id
        and result.policy_schema_version == policy.schema_version
        and result.source_evidence_fingerprint == policy.source_evidence_fingerprint
        and result.governance_fingerprint == policy.governance_fingerprint
    )
    if agrees:
        return DecisionQualityCheck(
            "policy_reference", True, DecisionQualityStatus.CONSISTENT, "policy_reference_matches"
        )
    return DecisionQualityCheck(
        "policy_reference",
        True,
        DecisionQualityStatus.INCONSISTENT,
        "policy_reference_disagrees_with_supplied_policy",
    )


def _candidate_reference_check(
    result: RecommendedBetResult, issuance: CandidateIssuance
) -> DecisionQualityCheck:
    """Whether the result's claimed candidate resolves to exactly one row.

    Also whether that row's materialized offer/model evidence agrees
    with what the result recorded.
    """
    if result.issuance_id != issuance.issuance_id:
        return DecisionQualityCheck(
            "candidate_reference",
            True,
            DecisionQualityStatus.INCONSISTENT,
            "result_issuance_id_disagrees_with_supplied_issuance",
        )
    matches = [
        row
        for row in issuance.rows
        if candidate_issuance_row_id(issuance.issuance_id, row) == result.candidate_reference_id
    ]
    if len(matches) != 1:
        return DecisionQualityCheck(
            "candidate_reference",
            True,
            DecisionQualityStatus.INCONSISTENT,
            "candidate_reference_does_not_resolve_to_exactly_one_row",
        )
    row = matches[0]
    offer_agrees = (
        row.game_id == result.game_id
        and row.market == result.market
        and row.side == result.side
        and row.provider == result.provider
        and row.provider_event_id == result.provider_event_id
        and row.sportsbook == result.sportsbook
        and row.american_price == result.american_price
        and row.line == result.line
        and row.fetched_at == result.fetched_at
        and row.sportsbook_updated_at == result.sportsbook_updated_at
        and row.kickoff == result.kickoff
        and row.is_live == result.is_live
    )
    forecast_agrees = (
        row.forecast_event_id == result.forecast_event_id
        and row.forecast_run_id == result.forecast_run_id
        and row.forecast_role == result.forecast_role
        and row.forecast_generated_at == result.forecast_generated_at
    )
    analytical_agrees = (
        row.model_name == result.model_name
        and row.model_type == result.model_type
        and row.model_probability == result.model_probability
        and row.expected_value == result.expected_value
    )
    if offer_agrees and forecast_agrees and analytical_agrees:
        return DecisionQualityCheck(
            "candidate_reference",
            True,
            DecisionQualityStatus.CONSISTENT,
            "candidate_reference_resolves_and_agrees",
        )
    return DecisionQualityCheck(
        "candidate_reference",
        True,
        DecisionQualityStatus.INCONSISTENT,
        "resolved_candidate_row_disagrees_with_result_evidence",
    )


def _allocation_recomputation_check(
    *,
    result: RecommendedBetResult,
    policy: RecommendationPolicy,
    issuance: CandidateIssuance,
    portfolio: PortfolioExposureSnapshot | None,
    correlation: CorrelationExposureEvidence | None,
) -> tuple[DecisionQualityCheck, str | None]:
    """Replay evaluate_recommendation_candidate against supplied evidence.

    Compares the replay against the persisted result. Never duplicates
    Kelly, capacity, duplicate, or opposing-position logic locally --
    reuses the actual owning domain evaluator.

    Non-mandatory: UNAVAILABLE here does not affect overall
    decision_status.
    """
    correlation_fingerprint = (
        None if correlation is None else _digest(_correlation_payload(correlation))
    )

    if portfolio is None:
        return (
            DecisionQualityCheck(
                "allocation_recomputation",
                False,
                DecisionQualityStatus.UNAVAILABLE,
                "original_portfolio_evidence_not_supplied",
            ),
            correlation_fingerprint,
        )
    if portfolio.snapshot_id != result.portfolio_snapshot_id:
        return (
            DecisionQualityCheck(
                "allocation_recomputation",
                False,
                DecisionQualityStatus.UNAVAILABLE,
                "supplied_portfolio_snapshot_id_disagrees_with_result",
            ),
            correlation_fingerprint,
        )
    if portfolio.observed_at != result.portfolio_observed_at:
        return (
            DecisionQualityCheck(
                "allocation_recomputation",
                False,
                DecisionQualityStatus.UNAVAILABLE,
                "supplied_portfolio_observed_at_disagrees_with_result",
            ),
            correlation_fingerprint,
        )
    if result.bankroll_basis is None:
        return (
            DecisionQualityCheck(
                "allocation_recomputation",
                False,
                DecisionQualityStatus.UNAVAILABLE,
                "result_has_no_recorded_bankroll_basis",
            ),
            correlation_fingerprint,
        )

    replayed = evaluate_recommendation_candidate(
        policy=policy,
        issuance=issuance,
        candidate_reference_id=result.candidate_reference_id,
        decision_at=result.evaluated_at,
        bankroll=result.bankroll_basis,
        portfolio=portfolio,
        correlation=correlation,
    )
    replay_agrees = (
        replayed.state is result.decision_state
        and replayed.recommendation_eligible == result.recommendation_eligible
        and replayed.checks == result.checks
        and replayed.allocation == result.allocation
        and replayed.sizing == result.sizing
    )
    if replay_agrees:
        return (
            DecisionQualityCheck(
                "allocation_recomputation",
                False,
                DecisionQualityStatus.CONSISTENT,
                "replay_against_supplied_evidence_agrees",
            ),
            correlation_fingerprint,
        )
    return (
        DecisionQualityCheck(
            "allocation_recomputation",
            False,
            DecisionQualityStatus.INCONSISTENT,
            "replay_against_supplied_evidence_disagrees",
        ),
        correlation_fingerprint,
    )


def _correlation_payload(correlation: CorrelationExposureEvidence) -> dict[str, object]:
    return {
        "group_id": correlation.group_id,
        "member_reference_ids": list(correlation.member_reference_ids),
        "existing_stake": correlation.existing_stake,
    }


def _overall_status(checks: list[DecisionQualityCheck]) -> DecisionQualityStatus:
    """Determine overall decision_status from all checks.

    Any completed INCONSISTENT check -- mandatory or not -- is decisive.
    `mandatory` only governs whether an UNAVAILABLE check lowers the
    conclusion; it never permits ignoring a real, completed disagreement.
    """
    if any(check.status is DecisionQualityStatus.INCONSISTENT for check in checks):
        return DecisionQualityStatus.INCONSISTENT
    mandatory = tuple(check for check in checks if check.mandatory)
    if any(check.status is DecisionQualityStatus.UNAVAILABLE for check in mandatory):
        return DecisionQualityStatus.UNAVAILABLE
    return DecisionQualityStatus.CONSISTENT


def _digest(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return sha256(encoded).hexdigest()


def _require_digest(value: str, label: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest.")
    return value


def _require_candidate_reference_id(value: str, label: str) -> str:
    """Validate the composite candidate-reference identity format.

    '{issuance_id_digest}:{row_digest}', matching
    candidate_issuance_row_id's real, confirmed output shape -- not a
    bare SHA-256 digest.
    """
    parts = value.split(":")
    if len(parts) != 2 or any(_is_valid_digest_format(part) is False for part in parts):
        raise ValueError(f"{label} must be a colon-separated pair of lowercase SHA-256 digests.")
    return value


def _is_valid_digest_format(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _require_nonempty(value: str, label: str) -> str:
    if not value.strip():
        raise ValueError(f"{label} must not be empty.")
    return value


def _require_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must be timezone-aware UTC.")
    return value
