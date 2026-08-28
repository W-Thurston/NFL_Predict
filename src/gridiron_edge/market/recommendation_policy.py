# src/gridiron_edge/market/recommendation_policy.py

"""Immutable versioned recommendation-policy contracts and derivation.

This module is pure. It fingerprints empirical market-family evidence, records
governed operational inputs, and derives independent market-family policy
states. It does not load mutable storage, emit recommendations, or mutate
betting state.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass, replace
from datetime import datetime, timedelta
from enum import Enum, StrEnum
from hashlib import sha256
import json
import math
from typing import Final, cast

from gridiron_edge.market.candidate_issuance import (
    CandidateIssuance,
    CandidateIssuanceReason,
    CandidateIssuanceRow,
    CandidateIssuanceState,
    candidate_issuance_row_id,
)
from gridiron_edge.market.kelly import kelly_fraction
from gridiron_edge.market.market_family_evaluation import (
    EmpiricalMarketFamilyEvaluation,
    EvaluationEvidenceStatus,
    MarketFamilyEvaluation,
)

RECOMMENDATION_POLICY_SCHEMA_VERSION: Final[int] = 1
RECOMMENDATION_POLICY_DERIVATION_METHOD: Final[str] = "market_family_empirical_evidence_v1"
_MARKETS: Final[tuple[str, ...]] = ("moneyline", "spread", "total")


class PolicyDerivationStatus(StrEnum):
    """Availability of one independently derived market-family policy."""

    ACTIVE = "active"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    UNAVAILABLE = "unavailable"
    CONFLICTING_EVIDENCE = "conflicting_evidence"


class PolicyDerivationReason(StrEnum):
    """Stable reason for a market-family derivation state."""

    DERIVED = "derived"
    NO_CANDIDATES = "no_candidates"
    REQUIRED_EVIDENCE_UNAVAILABLE = "required_evidence_unavailable"
    REQUIRED_EVIDENCE_CONFLICTING = "required_evidence_conflicting"
    NO_VALIDATED_THRESHOLD_SELECTION_METHOD = "no_validated_threshold_selection_method"


class PolicyValueSource(StrEnum):
    """Provenance class for one policy value."""

    EMPIRICAL_MARKET_FAMILY_EVIDENCE = "empirical_market_family_evidence"
    GOVERNED_POLICY_INPUT = "governed_policy_input"


class StakeRoundingMode(StrEnum):
    """Permitted deterministic stake rounding behavior."""

    DOWN = "down"
    NEAREST = "nearest"


@dataclass(frozen=True, slots=True)
class RecommendationPolicyGovernance:
    """Explicit non-empirical operational policy inputs."""

    fractional_kelly_multiplier: float
    minimum_actionable_stake: float
    stake_increment: float
    stake_rounding: StakeRoundingMode
    maximum_candidate_bankroll_fraction: float
    maximum_game_bankroll_fraction: float
    maximum_portfolio_bankroll_fraction: float
    prohibit_opposing_positions: bool
    correlation_check_mandatory: bool
    exposure_eligible_statuses: tuple[str, ...]
    source: PolicyValueSource = PolicyValueSource.GOVERNED_POLICY_INPUT


@dataclass(frozen=True, slots=True)
class EmpiricalQualificationThresholds:
    """Empirically supported thresholds, absent until derivation is defensible."""

    minimum_expected_value: float | None
    maximum_quote_age_seconds: float | None
    minimum_observation_count: int | None
    minimum_distinct_fetch_count: int | None


@dataclass(frozen=True, slots=True)
class MarketFamilyRecommendationPolicy:
    """One independently derived market-family policy section."""

    market: str
    status: PolicyDerivationStatus
    reason: PolicyDerivationReason
    candidate_count: int
    outcome_available_count: int
    clv_available_count: int
    return_available_count: int
    evidence_statuses: tuple[tuple[str, str], ...]
    thresholds: EmpiricalQualificationThresholds | None
    source: PolicyValueSource


@dataclass(frozen=True, slots=True)
class RecommendationPolicy:
    """One immutable versioned recommendation-policy artifact."""

    schema_version: int
    policy_id: str
    created_at: datetime
    source_evidence_fingerprint: str
    governance_fingerprint: str
    derivation_method: str
    moneyline: MarketFamilyRecommendationPolicy
    spread: MarketFamilyRecommendationPolicy
    total: MarketFamilyRecommendationPolicy
    governance: RecommendationPolicyGovernance


def empirical_evidence_fingerprint(
    evaluation: EmpiricalMarketFamilyEvaluation,
) -> str:
    """Return a canonical SHA-256 fingerprint of complete empirical evidence."""
    return _digest(_canonical_value(evaluation))


def governance_fingerprint(governance: RecommendationPolicyGovernance) -> str:
    """Return a canonical SHA-256 fingerprint of governed policy inputs."""
    validate_recommendation_policy_governance(governance)
    return _digest(_canonical_value(governance))


def recommendation_policy_id(policy: RecommendationPolicy) -> str:
    """Return deterministic identity, intentionally excluding created_at."""
    identity = {
        "schema_version": policy.schema_version,
        "source_evidence_fingerprint": policy.source_evidence_fingerprint,
        "governance_fingerprint": policy.governance_fingerprint,
        "derivation_method": policy.derivation_method,
        "moneyline": _canonical_value(policy.moneyline),
        "spread": _canonical_value(policy.spread),
        "total": _canonical_value(policy.total),
        "governance": _canonical_value(policy.governance),
    }
    return _digest(identity)


def derive_recommendation_policy(
    *,
    evaluation: EmpiricalMarketFamilyEvaluation,
    governance: RecommendationPolicyGovernance,
    created_at: datetime,
) -> RecommendationPolicy:
    """Derive independent family states without inventing empirical cutoffs."""
    created = _require_utc(created_at, label="created_at")
    validate_recommendation_policy_governance(governance)
    evidence_fingerprint = empirical_evidence_fingerprint(evaluation)
    governed_fingerprint = governance_fingerprint(governance)
    policy = RecommendationPolicy(
        schema_version=RECOMMENDATION_POLICY_SCHEMA_VERSION,
        policy_id="0" * 64,
        created_at=created,
        source_evidence_fingerprint=evidence_fingerprint,
        governance_fingerprint=governed_fingerprint,
        derivation_method=RECOMMENDATION_POLICY_DERIVATION_METHOD,
        moneyline=_derive_family(evaluation.moneyline),
        spread=_derive_family(evaluation.spread),
        total=_derive_family(evaluation.total),
        governance=governance,
    )
    policy = replace(policy, policy_id=recommendation_policy_id(policy))
    validate_recommendation_policy(policy)
    return policy


def validate_recommendation_policy_governance(
    governance: RecommendationPolicyGovernance,
) -> None:
    """Validate explicit governed values without supplying hidden defaults."""
    if governance.source is not PolicyValueSource.GOVERNED_POLICY_INPUT:
        raise ValueError("Recommendation governance requires governed-policy provenance.")
    _fraction(governance.fractional_kelly_multiplier, "fractional_kelly_multiplier")
    _nonnegative(governance.minimum_actionable_stake, "minimum_actionable_stake")
    if not math.isfinite(governance.stake_increment) or governance.stake_increment <= 0:
        raise ValueError("stake_increment must be finite and positive.")
    _fraction(
        governance.maximum_candidate_bankroll_fraction,
        "maximum_candidate_bankroll_fraction",
    )
    _fraction(
        governance.maximum_game_bankroll_fraction,
        "maximum_game_bankroll_fraction",
    )
    _fraction(
        governance.maximum_portfolio_bankroll_fraction,
        "maximum_portfolio_bankroll_fraction",
    )
    if (
        governance.maximum_candidate_bankroll_fraction > governance.maximum_game_bankroll_fraction
        or governance.maximum_game_bankroll_fraction
        > governance.maximum_portfolio_bankroll_fraction
    ):
        raise ValueError("Exposure fractions must be ordered candidate <= game <= portfolio.")
    if not governance.exposure_eligible_statuses:
        raise ValueError("At least one exposure-eligible status is required.")
    statuses = governance.exposure_eligible_statuses
    if statuses != tuple(sorted(set(statuses))):
        raise ValueError("Exposure-eligible statuses must be unique and sorted.")
    if any(not value.strip() for value in statuses):
        raise ValueError("Exposure-eligible statuses must be nonempty.")


def validate_recommendation_policy(policy: RecommendationPolicy) -> None:
    """Validate schema, identity, provenance, ordering, and family invariants."""
    if policy.schema_version != RECOMMENDATION_POLICY_SCHEMA_VERSION:
        raise ValueError("Unsupported recommendation policy schema version.")
    _require_digest(policy.source_evidence_fingerprint, "source_evidence_fingerprint")
    _require_digest(policy.governance_fingerprint, "governance_fingerprint")
    _require_utc(policy.created_at, label="created_at")
    if policy.derivation_method != RECOMMENDATION_POLICY_DERIVATION_METHOD:
        raise ValueError("Unsupported recommendation policy derivation method.")
    validate_recommendation_policy_governance(policy.governance)
    if policy.governance_fingerprint != governance_fingerprint(policy.governance):
        raise ValueError("Governance fingerprint does not match governance content.")
    for market, family in zip(
        _MARKETS, (policy.moneyline, policy.spread, policy.total), strict=True
    ):
        _validate_family(family, expected_market=market)
    if policy.policy_id != recommendation_policy_id(policy):
        raise ValueError("Recommendation policy ID does not match canonical content.")


def _derive_family(family: MarketFamilyEvaluation) -> MarketFamilyRecommendationPolicy:
    coverage = family.coverage
    statuses = tuple(
        sorted(
            (
                ("clv_cohorts", family.clv_cohorts.status.value),
                ("distinct_fetch_count_cohorts", family.distinct_fetch_count_cohorts.status.value),
                ("expected_value_cohorts", family.expected_value_cohorts.status.value),
                ("observation_count_cohorts", family.observation_count_cohorts.status.value),
                ("quote_age_cohorts", family.quote_age_cohorts.status.value),
                ("realized_return", family.realized_return.status.value),
            )
        )
    )
    evidence_values = {value for _, value in statuses}
    if coverage.candidate_count == 0:
        status = PolicyDerivationStatus.UNAVAILABLE
        reason = PolicyDerivationReason.NO_CANDIDATES
    elif EvaluationEvidenceStatus.CONFLICTING_EVIDENCE.value in evidence_values:
        status = PolicyDerivationStatus.CONFLICTING_EVIDENCE
        reason = PolicyDerivationReason.REQUIRED_EVIDENCE_CONFLICTING
    elif EvaluationEvidenceStatus.UNAVAILABLE.value in evidence_values:
        status = PolicyDerivationStatus.UNAVAILABLE
        reason = PolicyDerivationReason.REQUIRED_EVIDENCE_UNAVAILABLE
    else:
        status = PolicyDerivationStatus.INSUFFICIENT_EVIDENCE
        reason = PolicyDerivationReason.NO_VALIDATED_THRESHOLD_SELECTION_METHOD
    return MarketFamilyRecommendationPolicy(
        market=family.market,
        status=status,
        reason=reason,
        candidate_count=coverage.candidate_count,
        outcome_available_count=coverage.outcome_available_count,
        clv_available_count=coverage.clv_available_count,
        return_available_count=coverage.return_available_count,
        evidence_statuses=statuses,
        thresholds=None,
        source=PolicyValueSource.EMPIRICAL_MARKET_FAMILY_EVIDENCE,
    )


def _validate_family(
    family: MarketFamilyRecommendationPolicy,
    *,
    expected_market: str,
) -> None:
    if family.market != expected_market:
        raise ValueError("Market-family policy is stored under the wrong family.")
    if (
        min(
            family.candidate_count,
            family.outcome_available_count,
            family.clv_available_count,
            family.return_available_count,
        )
        < 0
    ):
        raise ValueError("Market-family policy counts must be nonnegative.")
    if family.evidence_statuses != tuple(sorted(set(family.evidence_statuses))):
        raise ValueError("Evidence statuses must be unique and sorted.")
    if family.source is not PolicyValueSource.EMPIRICAL_MARKET_FAMILY_EVIDENCE:
        raise ValueError("Market-family policy requires empirical provenance.")
    if family.status is PolicyDerivationStatus.ACTIVE:
        if family.reason is not PolicyDerivationReason.DERIVED or family.thresholds is None:
            raise ValueError("Active family policy requires derived thresholds.")
    elif family.thresholds is not None:
        raise ValueError("Inactive family policy cannot contain qualification thresholds.")


def _canonical_value(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        result = {
            field.name: _canonical_value(getattr(value, field.name)) for field in fields(value)
        }
    elif isinstance(value, Enum):
        result = value.value
    elif isinstance(value, datetime):
        result = _require_utc(value, label="canonical timestamp").isoformat()
    elif isinstance(value, tuple | list):
        result = [_canonical_value(item) for item in value]
    elif isinstance(value, dict):
        result = {
            str(key): _canonical_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    else:
        result = _canonical_scalar(value)
    return result


def _canonical_scalar(value: object) -> object:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Canonical policy evidence cannot contain non-finite numbers.")
    if value is None or isinstance(value, bool | int | float | str):
        return value
    raise TypeError(f"Unsupported canonical policy value: {type(value).__name__}")


def _digest(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return sha256(encoded).hexdigest()


def _require_digest(value: str, label: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest.")
    return value


def _require_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must be timezone-aware UTC.")
    return value


def _fraction(value: float, label: str) -> None:
    if not math.isfinite(value) or not 0 <= value <= 1:
        raise ValueError(f"{label} must be finite and in [0, 1].")


def _nonnegative(value: float, label: str) -> None:
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{label} must be finite and nonnegative.")


class PolicyCheckStatus(StrEnum):
    """Result of one stable recommendation-policy check."""

    PASSED = "passed"
    FAILED = "failed"
    UNAVAILABLE = "unavailable"
    CONFLICTING_EVIDENCE = "conflicting_evidence"
    NOT_APPLICABLE = "not_applicable"


class RecommendationDecisionState(StrEnum):
    """Final non-mutating candidate decision state."""

    UNQUALIFIED = "unqualified"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    QUALIFIED_OPPORTUNITY = "qualified_opportunity"
    RECOMMENDATION_ELIGIBLE = "recommendation_eligible"


@dataclass(frozen=True, slots=True)
class BankrollBasis:
    """Immutable supplied bankroll evidence."""

    amount: float
    observed_at: datetime
    source_kind: str
    source_id: str


@dataclass(frozen=True, slots=True)
class PortfolioExposureRow:
    """Narrow immutable wager evidence used by policy evaluation."""

    bet_id: str
    game_id: str
    placed_at: datetime
    market: str
    side: str
    stake: float
    status: str
    reference_id: str | None


@dataclass(frozen=True, slots=True)
class PortfolioExposureSnapshot:
    """Canonical supplied portfolio state at one observation time."""

    snapshot_id: str
    observed_at: datetime
    rows: tuple[PortfolioExposureRow, ...]


@dataclass(frozen=True, slots=True)
class CorrelationExposureEvidence:
    """Explicit correlation membership supplied outside the evaluator."""

    group_id: str
    member_reference_ids: tuple[str, ...]
    existing_stake: float


@dataclass(frozen=True, slots=True)
class PolicyCheckResult:
    """One ordered check with explicit evidence state."""

    check_id: str
    mandatory: bool
    status: PolicyCheckStatus
    reason: str
    observed_value: float | str | None = None
    required_value: float | str | None = None


@dataclass(frozen=True, slots=True)
class RecommendationSizingResult:
    """Auditable Kelly and constrained stake evidence."""

    full_kelly_fraction: float | None
    fractional_kelly_fraction: float | None
    raw_stake: float | None
    constrained_stake: float | None
    rounded_stake: float | None
    actionable_stake: float | None


class PortfolioAllocationState(StrEnum):
    """Whether portfolio allocation was evaluated, and if so, its outcome."""

    NOT_EVALUATED = "not_evaluated"
    ALLOCATED = "allocated"
    ZERO_ALLOCATION = "zero_allocation"


class PortfolioAllocationReason(StrEnum):
    """Stable, machine-readable cause for the allocation state."""

    RECOMMENDATION_INELIGIBLE = "recommendation_ineligible"
    ALLOCATION_EVIDENCE_UNAVAILABLE = "allocation_evidence_unavailable"
    ALLOCATED = "allocated"
    EXACT_DUPLICATE_FOUND = "exact_duplicate_found"
    OPPOSING_POSITION_FOUND = "opposing_position_found"
    CANDIDATE_CAPACITY_EXHAUSTED = "candidate_capacity_exhausted"
    GAME_CAPACITY_EXHAUSTED = "game_capacity_exhausted"
    PORTFOLIO_CAPACITY_EXHAUSTED = "portfolio_capacity_exhausted"
    CORRELATION_CAPACITY_EXHAUSTED = "correlation_capacity_exhausted"
    BELOW_MINIMUM_ACTIONABLE_STAKE = "below_minimum_actionable_stake"


@dataclass(frozen=True, slots=True)
class PortfolioAllocationResult:
    """Final portfolio-allocation outcome, independent of recommendation eligibility."""

    state: PortfolioAllocationState
    reason: PortfolioAllocationReason
    allocated_stake: float | None


@dataclass(frozen=True, slots=True)
class RecommendationPolicyDecision:
    """Deterministic immutable policy evaluation result."""

    policy_id: str
    candidate_reference_id: str
    market: str
    decision_at: datetime
    issuance_quote_age_seconds: float
    decision_quote_age_seconds: float
    state: RecommendationDecisionState
    recommendation_eligible: bool
    checks: tuple[PolicyCheckResult, ...]
    sizing: RecommendationSizingResult
    allocation: PortfolioAllocationResult


def portfolio_exposure_snapshot(
    *, observed_at: datetime, rows: tuple[PortfolioExposureRow, ...]
) -> PortfolioExposureSnapshot:
    """Validate, canonically order, and identify supplied exposure evidence."""
    observed = _require_utc(observed_at, label="portfolio observed_at")
    ordered = tuple(sorted(rows, key=lambda row: row.bet_id))
    if len({row.bet_id for row in ordered}) != len(ordered):
        raise ValueError("Portfolio exposure contains duplicate bet IDs.")
    for row in ordered:
        _validate_exposure_row(row, observed_at=observed)
    identity = {"observed_at": observed.isoformat(), "rows": _canonical_value(ordered)}
    return PortfolioExposureSnapshot(_digest(identity), observed, ordered)


def evaluate_recommendation_candidate(
    *,
    policy: RecommendationPolicy,
    issuance: CandidateIssuance,
    candidate_reference_id: str,
    decision_at: datetime,
    bankroll: BankrollBasis | None = None,
    portfolio: PortfolioExposureSnapshot | None = None,
    correlation: CorrelationExposureEvidence | None = None,
) -> RecommendationPolicyDecision:
    """Evaluate one exact immutable candidate without loading or mutating state.

    Recommendation eligibility (Stage 1) is determined independently of
    portfolio allocation (Stage 2). A zero allocation never demotes or
    erases an established recommendation eligibility.
    """
    validate_recommendation_policy(policy)
    decision = _require_utc(decision_at, label="decision_at")
    row = _resolve_candidate(issuance, candidate_reference_id)
    issuance_age = (issuance.evaluated_at - row.fetched_at).total_seconds()
    decision_age = (decision - row.fetched_at).total_seconds()
    family = _family_policy(policy, row.market)

    empty_sizing = RecommendationSizingResult(None, None, None, None, None, None)
    not_evaluated = PortfolioAllocationResult(
        PortfolioAllocationState.NOT_EVALUATED,
        PortfolioAllocationReason.RECOMMENDATION_INELIGIBLE,
        None,
    )

    # --- Stage 1: qualification and recommendation eligibility ---
    checks: list[PolicyCheckResult] = [_issuance_check(row), _family_check(family)]
    if any(check.status is not PolicyCheckStatus.PASSED for check in checks):
        return RecommendationPolicyDecision(
            policy.policy_id,
            candidate_reference_id,
            row.market,
            decision,
            issuance_age,
            decision_age,
            RecommendationDecisionState.INSUFFICIENT_EVIDENCE,
            False,
            tuple(checks),
            empty_sizing,
            not_evaluated,
        )

    thresholds = cast(EmpiricalQualificationThresholds, family.thresholds)
    checks.extend(_candidate_evidence_checks(row, issuance, decision, thresholds))

    recommendation_check_ids = {
        "candidate_issuance",
        "market_family_policy",
        "quote_freshness",
        "expected_value_threshold",
    }
    recommendation_eligible = all(
        check.status is PolicyCheckStatus.PASSED
        for check in checks
        if check.check_id in recommendation_check_ids
    )
    if not recommendation_eligible:
        has_failure = any(
            check.status is PolicyCheckStatus.FAILED
            for check in checks
            if check.check_id in recommendation_check_ids
        )
        state = (
            RecommendationDecisionState.UNQUALIFIED
            if has_failure
            else RecommendationDecisionState.INSUFFICIENT_EVIDENCE
        )
        return RecommendationPolicyDecision(
            policy.policy_id,
            candidate_reference_id,
            row.market,
            decision,
            issuance_age,
            decision_age,
            state,
            False,
            tuple(checks),
            empty_sizing,
            not_evaluated,
        )

    # Recommendation eligibility is now established and FROZEN. Nothing
    # below this point may change `state` away from RECOMMENDATION_ELIGIBLE
    # or `recommendation_eligible` away from True.

    # --- Stage 2: portfolio evidence and allocation ---
    portfolio_check = _portfolio_check(portfolio, decision)
    duplicate_check = _duplicate_check(candidate_reference_id, portfolio)
    opposing_check = _opposing_check(row, portfolio, policy.governance)
    bankroll_check = _bankroll_check(bankroll, decision)
    correlation_check = _correlation_check(correlation, policy.governance)
    checks.extend(
        [portfolio_check, duplicate_check, opposing_check, bankroll_check, correlation_check]
    )

    # Missing or conflicting evidence prevents any allocation conclusion
    # from being reached at all -- distinct from a FAILED check below,
    # which is a real, completed policy conclusion, not missing evidence.
    evidence_gap_checks = (portfolio_check, bankroll_check, correlation_check)
    allocation_evidence_ready = all(
        check.status is PolicyCheckStatus.PASSED for check in evidence_gap_checks if check.mandatory
    )
    if not allocation_evidence_ready:
        allocation = PortfolioAllocationResult(
            PortfolioAllocationState.NOT_EVALUATED,
            PortfolioAllocationReason.ALLOCATION_EVIDENCE_UNAVAILABLE,
            None,
        )
        return RecommendationPolicyDecision(
            policy.policy_id,
            candidate_reference_id,
            row.market,
            decision,
            issuance_age,
            decision_age,
            RecommendationDecisionState.RECOMMENDATION_ELIGIBLE,
            True,
            tuple(checks),
            empty_sizing,
            allocation,
        )

    # Valid, available evidence, but the portfolio policy actively rejects
    # this candidate -- a completed zero allocation with a specific,
    # real reason, not a missing decision.
    if duplicate_check.status is PolicyCheckStatus.FAILED:
        allocation = PortfolioAllocationResult(
            PortfolioAllocationState.ZERO_ALLOCATION,
            PortfolioAllocationReason.EXACT_DUPLICATE_FOUND,
            0.0,
        )
        return RecommendationPolicyDecision(
            policy.policy_id,
            candidate_reference_id,
            row.market,
            decision,
            issuance_age,
            decision_age,
            RecommendationDecisionState.RECOMMENDATION_ELIGIBLE,
            True,
            tuple(checks),
            empty_sizing,
            allocation,
        )
    if opposing_check.status is PolicyCheckStatus.FAILED:
        allocation = PortfolioAllocationResult(
            PortfolioAllocationState.ZERO_ALLOCATION,
            PortfolioAllocationReason.OPPOSING_POSITION_FOUND,
            0.0,
        )
        return RecommendationPolicyDecision(
            policy.policy_id,
            candidate_reference_id,
            row.market,
            decision,
            issuance_age,
            decision_age,
            RecommendationDecisionState.RECOMMENDATION_ELIGIBLE,
            True,
            tuple(checks),
            empty_sizing,
            allocation,
        )

    sizing, allocation, sizing_checks = _size_candidate(
        row=row,
        governance=policy.governance,
        bankroll=cast(BankrollBasis, bankroll),
        portfolio=cast(PortfolioExposureSnapshot, portfolio),
        correlation=correlation,
    )
    checks.extend(sizing_checks)

    return RecommendationPolicyDecision(
        policy.policy_id,
        candidate_reference_id,
        row.market,
        decision,
        issuance_age,
        decision_age,
        RecommendationDecisionState.RECOMMENDATION_ELIGIBLE,
        True,
        tuple(checks),
        sizing,
        allocation,
    )


def _resolve_candidate(issuance: CandidateIssuance, reference_id: str) -> CandidateIssuanceRow:
    matches = [
        row
        for row in issuance.rows
        if candidate_issuance_row_id(issuance.issuance_id, row) == reference_id
    ]
    if len(matches) != 1:
        raise ValueError("Candidate reference must resolve to exactly one issuance row.")
    return matches[0]


def _family_policy(policy: RecommendationPolicy, market: str) -> MarketFamilyRecommendationPolicy:
    if market not in _MARKETS:
        raise ValueError(f"Unsupported candidate market: {market}")
    return cast(MarketFamilyRecommendationPolicy, getattr(policy, market))


def _issuance_check(row: CandidateIssuanceRow) -> PolicyCheckResult:
    passed = (
        row.state is CandidateIssuanceState.CANDIDATE
        and row.reason is CandidateIssuanceReason.POSITIVE_EXPECTED_VALUE
    )
    return PolicyCheckResult(
        "candidate_issuance",
        True,
        PolicyCheckStatus.PASSED if passed else PolicyCheckStatus.FAILED,
        "issued_candidate" if passed else "issuance_not_candidate",
    )


def _family_check(family: MarketFamilyRecommendationPolicy) -> PolicyCheckResult:
    status = {
        PolicyDerivationStatus.ACTIVE: PolicyCheckStatus.PASSED,
        PolicyDerivationStatus.INSUFFICIENT_EVIDENCE: PolicyCheckStatus.UNAVAILABLE,
        PolicyDerivationStatus.UNAVAILABLE: PolicyCheckStatus.UNAVAILABLE,
        PolicyDerivationStatus.CONFLICTING_EVIDENCE: PolicyCheckStatus.CONFLICTING_EVIDENCE,
    }[family.status]
    return PolicyCheckResult("market_family_policy", True, status, family.reason.value)


def _candidate_evidence_checks(
    row: CandidateIssuanceRow,
    issuance: CandidateIssuance,
    decision: datetime,
    thresholds: EmpiricalQualificationThresholds,
) -> tuple[PolicyCheckResult, ...]:
    temporal_valid = (
        row.kickoff is not None
        and not row.is_live
        and row.fetched_at < row.kickoff
        and issuance.evaluated_at <= decision < row.kickoff
        and row.fetched_at <= decision
    )
    age = (decision - row.fetched_at).total_seconds()
    freshness_available = thresholds.maximum_quote_age_seconds is not None
    freshness_passed = (
        temporal_valid
        and freshness_available
        and age <= cast(float, thresholds.maximum_quote_age_seconds)
    )
    ev_available = row.expected_value is not None and thresholds.minimum_expected_value is not None
    ev_passed = ev_available and cast(float, row.expected_value) >= cast(
        float, thresholds.minimum_expected_value
    )
    return (
        PolicyCheckResult(
            "quote_freshness",
            True,
            PolicyCheckStatus.PASSED
            if freshness_passed
            else (
                PolicyCheckStatus.FAILED if freshness_available else PolicyCheckStatus.UNAVAILABLE
            ),
            "fresh" if freshness_passed else "quote_freshness_not_satisfied",
            age,
            thresholds.maximum_quote_age_seconds,
        ),
        PolicyCheckResult(
            "expected_value_threshold",
            True,
            PolicyCheckStatus.PASSED
            if ev_passed
            else (PolicyCheckStatus.FAILED if ev_available else PolicyCheckStatus.UNAVAILABLE),
            "expected_value_satisfied" if ev_passed else "expected_value_not_satisfied",
            row.expected_value,
            thresholds.minimum_expected_value,
        ),
    )


def _portfolio_check(
    portfolio: PortfolioExposureSnapshot | None, decision: datetime
) -> PolicyCheckResult:
    if portfolio is None:
        return PolicyCheckResult(
            "portfolio_snapshot", True, PolicyCheckStatus.UNAVAILABLE, "portfolio_missing"
        )
    valid = portfolio.observed_at <= decision and portfolio.snapshot_id == _digest(
        {"observed_at": portfolio.observed_at.isoformat(), "rows": _canonical_value(portfolio.rows)}
    )
    return PolicyCheckResult(
        "portfolio_snapshot",
        True,
        PolicyCheckStatus.PASSED if valid else PolicyCheckStatus.CONFLICTING_EVIDENCE,
        "portfolio_valid" if valid else "portfolio_identity_or_time_conflict",
    )


def _duplicate_check(
    reference_id: str, portfolio: PortfolioExposureSnapshot | None
) -> PolicyCheckResult:
    if portfolio is None:
        return PolicyCheckResult(
            "exact_duplicate", True, PolicyCheckStatus.UNAVAILABLE, "portfolio_missing"
        )
    duplicates = [row for row in portfolio.rows if row.reference_id == reference_id]
    return PolicyCheckResult(
        "exact_duplicate",
        True,
        PolicyCheckStatus.FAILED if duplicates else PolicyCheckStatus.PASSED,
        "exact_duplicate_found" if duplicates else "no_exact_duplicate",
    )


def _opposing_check(
    candidate: CandidateIssuanceRow,
    portfolio: PortfolioExposureSnapshot | None,
    governance: RecommendationPolicyGovernance,
) -> PolicyCheckResult:
    if not governance.prohibit_opposing_positions:
        return PolicyCheckResult(
            "opposing_position", False, PolicyCheckStatus.NOT_APPLICABLE, "not_prohibited"
        )
    if portfolio is None:
        return PolicyCheckResult(
            "opposing_position", True, PolicyCheckStatus.UNAVAILABLE, "portfolio_missing"
        )
    opposing = {"home": "away", "away": "home", "over": "under", "under": "over"}[candidate.side]
    found = any(
        row.game_id == candidate.game_id and row.market == candidate.market and row.side == opposing
        for row in portfolio.rows
    )
    return PolicyCheckResult(
        "opposing_position",
        True,
        PolicyCheckStatus.FAILED if found else PolicyCheckStatus.PASSED,
        "opposing_position_found" if found else "no_opposing_position",
    )


def _bankroll_check(bankroll: BankrollBasis | None, decision: datetime) -> PolicyCheckResult:
    if bankroll is None:
        return PolicyCheckResult(
            "bankroll_basis", True, PolicyCheckStatus.UNAVAILABLE, "bankroll_missing"
        )
    valid = (
        math.isfinite(bankroll.amount)
        and bankroll.amount > 0
        and bankroll.observed_at <= decision
        and bool(bankroll.source_kind.strip())
        and bool(bankroll.source_id.strip())
    )
    try:
        _require_utc(bankroll.observed_at, label="bankroll observed_at")
    except ValueError:
        valid = False
    return PolicyCheckResult(
        "bankroll_basis",
        True,
        PolicyCheckStatus.PASSED if valid else PolicyCheckStatus.CONFLICTING_EVIDENCE,
        "bankroll_valid" if valid else "bankroll_invalid",
        bankroll.amount,
    )


def _correlation_check(
    correlation: CorrelationExposureEvidence | None,
    governance: RecommendationPolicyGovernance,
) -> PolicyCheckResult:
    if correlation is None:
        return PolicyCheckResult(
            "correlation_evidence",
            governance.correlation_check_mandatory,
            PolicyCheckStatus.UNAVAILABLE
            if governance.correlation_check_mandatory
            else PolicyCheckStatus.NOT_APPLICABLE,
            "correlation_evidence_missing",
        )
    valid = (
        bool(correlation.group_id.strip())
        and correlation.member_reference_ids == tuple(sorted(set(correlation.member_reference_ids)))
        and math.isfinite(correlation.existing_stake)
        and correlation.existing_stake >= 0
    )
    return PolicyCheckResult(
        "correlation_evidence",
        governance.correlation_check_mandatory,
        PolicyCheckStatus.PASSED if valid else PolicyCheckStatus.CONFLICTING_EVIDENCE,
        "correlation_valid" if valid else "correlation_invalid",
    )


def _size_candidate(
    *,
    row: CandidateIssuanceRow,
    governance: RecommendationPolicyGovernance,
    bankroll: BankrollBasis,
    portfolio: PortfolioExposureSnapshot,
    correlation: CorrelationExposureEvidence | None,
) -> tuple[RecommendationSizingResult, PortfolioAllocationResult, tuple[PolicyCheckResult, ...]]:
    if row.model_probability is None or row.american_price is None:
        unavailable = PolicyCheckResult(
            "kelly_sizing", True, PolicyCheckStatus.UNAVAILABLE, "kelly_inputs_missing"
        )
        empty_sizing = RecommendationSizingResult(None, None, None, None, None, None)
        allocation = PortfolioAllocationResult(
            PortfolioAllocationState.NOT_EVALUATED,
            PortfolioAllocationReason.ALLOCATION_EVIDENCE_UNAVAILABLE,
            None,
        )
        return empty_sizing, allocation, (unavailable,)

    full = kelly_fraction(row.model_probability, row.american_price)
    fractional = full * governance.fractional_kelly_multiplier
    raw = bankroll.amount * fractional
    active = tuple(
        row_ for row_ in portfolio.rows if row_.status in governance.exposure_eligible_statuses
    )
    game_stake = sum(row_.stake for row_ in active if row_.game_id == row.game_id)
    portfolio_stake = sum(row_.stake for row_ in active)
    correlation_stake = 0.0 if correlation is None else correlation.existing_stake

    candidate_capacity = bankroll.amount * governance.maximum_candidate_bankroll_fraction
    game_capacity = max(
        bankroll.amount * governance.maximum_game_bankroll_fraction - game_stake, 0.0
    )
    portfolio_capacity = max(
        bankroll.amount * governance.maximum_portfolio_bankroll_fraction - portfolio_stake, 0.0
    )
    correlation_capacity = max(
        bankroll.amount * governance.maximum_game_bankroll_fraction - correlation_stake, 0.0
    )
    capacities = (candidate_capacity, game_capacity, portfolio_capacity, correlation_capacity)

    constrained = min(raw, *capacities)
    increment = governance.stake_increment
    if governance.stake_rounding is StakeRoundingMode.DOWN:
        rounded = math.floor(constrained / increment) * increment
    else:
        rounded = math.floor(constrained / increment + 0.5) * increment

    if rounded >= governance.minimum_actionable_stake and rounded > 0:
        allocation_reason = PortfolioAllocationReason.ALLOCATED
        allocation_state = PortfolioAllocationState.ALLOCATED
        allocated_stake = rounded
    else:
        allocation_state = PortfolioAllocationState.ZERO_ALLOCATION
        allocated_stake = 0.0
        # Identify the specific binding cause, in the same precedence
        # order the capacities were computed above -- the first exhausted
        # capacity that actually constrained `raw` below the minimum.
        if candidate_capacity < governance.minimum_actionable_stake and candidate_capacity == min(
            capacities
        ):
            allocation_reason = PortfolioAllocationReason.CANDIDATE_CAPACITY_EXHAUSTED
        elif (
            game_capacity == min(capacities) and game_capacity < governance.minimum_actionable_stake
        ):
            allocation_reason = PortfolioAllocationReason.GAME_CAPACITY_EXHAUSTED
        elif (
            portfolio_capacity == min(capacities)
            and portfolio_capacity < governance.minimum_actionable_stake
        ):
            allocation_reason = PortfolioAllocationReason.PORTFOLIO_CAPACITY_EXHAUSTED
        elif (
            correlation_capacity == min(capacities)
            and correlation_capacity < governance.minimum_actionable_stake
        ):
            allocation_reason = PortfolioAllocationReason.CORRELATION_CAPACITY_EXHAUSTED
        else:
            allocation_reason = PortfolioAllocationReason.BELOW_MINIMUM_ACTIONABLE_STAKE

    # kelly_sizing now reports whether allocation completed at all (not
    # whether it was positive) -- a real zero is a completed decision,
    # not a failed check.
    checks = (
        PolicyCheckResult(
            "kelly_sizing",
            True,
            PolicyCheckStatus.PASSED,
            "allocation_evaluated",
            raw,
            governance.minimum_actionable_stake,
        ),
        PolicyCheckResult(
            "candidate_exposure",
            True,
            PolicyCheckStatus.PASSED,
            "candidate_capacity_applied",
            constrained,
            candidate_capacity,
        ),
        PolicyCheckResult(
            "game_exposure",
            True,
            PolicyCheckStatus.PASSED,
            "game_capacity_applied",
            game_stake + constrained,
            bankroll.amount * governance.maximum_game_bankroll_fraction,
        ),
        PolicyCheckResult(
            "portfolio_exposure",
            True,
            PolicyCheckStatus.PASSED,
            "portfolio_capacity_applied",
            portfolio_stake + constrained,
            bankroll.amount * governance.maximum_portfolio_bankroll_fraction,
        ),
        PolicyCheckResult(
            "correlation_exposure",
            governance.correlation_check_mandatory,
            PolicyCheckStatus.PASSED,
            "correlation_capacity_applied",
            correlation_stake + constrained,
            bankroll.amount * governance.maximum_game_bankroll_fraction,
        ),
    )
    sizing = RecommendationSizingResult(
        full, fractional, raw, constrained, rounded, allocated_stake
    )
    allocation = PortfolioAllocationResult(allocation_state, allocation_reason, allocated_stake)
    return sizing, allocation, checks


def _validate_exposure_row(row: PortfolioExposureRow, *, observed_at: datetime) -> None:
    if not row.bet_id.strip() or not row.game_id.strip():
        raise ValueError("Portfolio exposure identities must be nonempty.")
    _require_utc(row.placed_at, label="portfolio placed_at")
    if row.placed_at > observed_at:
        raise ValueError("Portfolio exposure row cannot be later than the snapshot.")
    if not math.isfinite(row.stake) or row.stake <= 0:
        raise ValueError("Portfolio exposure stake must be finite and positive.")
    if row.market not in _MARKETS:
        raise ValueError("Portfolio exposure market is unsupported.")
