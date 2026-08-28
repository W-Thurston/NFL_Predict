# tests/unit/market/test_recommendation_policy_evaluation.py
"""Focused tests for pure recommendation-policy evaluation mechanics."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from gridiron_edge.market.candidate_issuance import (
    CANDIDATE_ISSUANCE_SCHEMA_VERSION,
    CandidateIssuance,
    CandidateIssuanceReason,
    CandidateIssuanceRow,
    CandidateIssuanceState,
    candidate_issuance_row_id,
)
from gridiron_edge.market.recommendation_policy import (
    RECOMMENDATION_POLICY_DERIVATION_METHOD,
    RECOMMENDATION_POLICY_SCHEMA_VERSION,
    BankrollBasis,
    CorrelationExposureEvidence,
    EmpiricalQualificationThresholds,
    MarketFamilyRecommendationPolicy,
    PolicyCheckStatus,
    PolicyDerivationReason,
    PolicyDerivationStatus,
    PolicyValueSource,
    PortfolioAllocationReason,
    PortfolioAllocationState,
    PortfolioExposureRow,
    RecommendationDecisionState,
    RecommendationPolicy,
    RecommendationPolicyGovernance,
    StakeRoundingMode,
    evaluate_recommendation_candidate,
    governance_fingerprint,
    portfolio_exposure_snapshot,
    recommendation_policy_id,
)

FETCHED = datetime(2026, 9, 1, 12, tzinfo=UTC)
EVALUATED = datetime(2026, 9, 1, 12, 5, tzinfo=UTC)
DECISION = datetime(2026, 9, 1, 12, 10, tzinfo=UTC)
KICKOFF = datetime(2026, 9, 1, 20, tzinfo=UTC)


def _governance(*, correlation_mandatory: bool = True) -> RecommendationPolicyGovernance:
    return RecommendationPolicyGovernance(
        fractional_kelly_multiplier=0.25,
        minimum_actionable_stake=5.0,
        stake_increment=1.0,
        stake_rounding=StakeRoundingMode.DOWN,
        maximum_candidate_bankroll_fraction=0.02,
        maximum_game_bankroll_fraction=0.05,
        maximum_portfolio_bankroll_fraction=0.20,
        prohibit_opposing_positions=True,
        correlation_check_mandatory=correlation_mandatory,
        exposure_eligible_statuses=("open",),
    )


def _family(market: str, *, active: bool) -> MarketFamilyRecommendationPolicy:
    return MarketFamilyRecommendationPolicy(
        market=market,
        status=(
            PolicyDerivationStatus.ACTIVE
            if active
            else PolicyDerivationStatus.INSUFFICIENT_EVIDENCE
        ),
        reason=(
            PolicyDerivationReason.DERIVED
            if active
            else PolicyDerivationReason.NO_VALIDATED_THRESHOLD_SELECTION_METHOD
        ),
        candidate_count=20,
        outcome_available_count=20,
        clv_available_count=20,
        return_available_count=20,
        evidence_statuses=(("synthetic_test_evidence", "available"),),
        thresholds=(EmpiricalQualificationThresholds(0.01, 900.0, None, None) if active else None),
        source=PolicyValueSource.EMPIRICAL_MARKET_FAMILY_EVIDENCE,
    )


def _policy(*, active: bool, correlation_mandatory: bool = True) -> RecommendationPolicy:
    governance = _governance(correlation_mandatory=correlation_mandatory)
    policy = RecommendationPolicy(
        schema_version=RECOMMENDATION_POLICY_SCHEMA_VERSION,
        policy_id="0" * 64,
        created_at=datetime(2026, 8, 17, tzinfo=UTC),
        source_evidence_fingerprint="a" * 64,
        governance_fingerprint=governance_fingerprint(governance),
        derivation_method=RECOMMENDATION_POLICY_DERIVATION_METHOD,
        moneyline=_family("moneyline", active=active),
        spread=_family("spread", active=active),
        total=_family("total", active=active),
        governance=governance,
    )
    return replace(policy, policy_id=recommendation_policy_id(policy))


def _row(**changes: object) -> CandidateIssuanceRow:
    row = CandidateIssuanceRow(
        game_id="2026_01_KC_LAC",
        market="moneyline",
        side="home",
        provider="the_odds_api",
        provider_event_id="event-1",
        sportsbook="draftkings",
        line=None,
        american_price=100,
        fetched_at=FETCHED,
        sportsbook_updated_at=FETCHED,
        kickoff=KICKOFF,
        is_live=False,
        forecast_event_id="forecast-1",
        forecast_run_id="run-1",
        forecast_role="champion",
        forecast_generated_at=FETCHED,
        model_name="win_prob",
        model_type="random_forest",
        model_probability=0.60,
        expected_value=0.20,
        state=CandidateIssuanceState.CANDIDATE,
        reason=CandidateIssuanceReason.POSITIVE_EXPECTED_VALUE,
    )
    return replace(row, **changes)


def _issuance(row: CandidateIssuanceRow | None = None) -> CandidateIssuance:
    return CandidateIssuance(
        CANDIDATE_ISSUANCE_SCHEMA_VERSION,
        "b" * 64,
        "product-1",
        "run-1",
        FETCHED,
        "2026",
        1,
        EVALUATED,
        (_row() if row is None else row,),
    )


def _bankroll() -> BankrollBasis:
    return BankrollBasis(1000.0, EVALUATED, "transaction_snapshot", "bankroll-1")


def _portfolio(*rows: PortfolioExposureRow):
    return portfolio_exposure_snapshot(observed_at=EVALUATED, rows=tuple(rows))


def _correlation(existing_stake: float = 0.0) -> CorrelationExposureEvidence:
    return CorrelationExposureEvidence("game-risk", (), existing_stake)


def _evaluate(
    *,
    policy: RecommendationPolicy | None = None,
    issuance: CandidateIssuance | None = None,
    portfolio=None,
    bankroll: BankrollBasis | None = None,
    correlation: CorrelationExposureEvidence | None = None,
    decision_at: datetime = DECISION,
):
    issued = _issuance() if issuance is None else issuance
    reference = candidate_issuance_row_id(issued.issuance_id, issued.rows[0])
    return evaluate_recommendation_candidate(
        policy=_policy(active=True) if policy is None else policy,
        issuance=issued,
        candidate_reference_id=reference,
        decision_at=decision_at,
        bankroll=_bankroll() if bankroll is None else bankroll,
        portfolio=_portfolio() if portfolio is None else portfolio,
        correlation=_correlation() if correlation is None else correlation,
    )


def test_inactive_family_stops_before_sizing() -> None:
    result = _evaluate(policy=_policy(active=False))
    assert result.state is RecommendationDecisionState.INSUFFICIENT_EVIDENCE
    assert [check.check_id for check in result.checks] == [
        "candidate_issuance",
        "market_family_policy",
    ]
    assert result.sizing.actionable_stake is None


def test_active_synthetic_policy_produces_auditable_fractional_kelly_stake() -> None:
    result = _evaluate()
    assert result.state is RecommendationDecisionState.RECOMMENDATION_ELIGIBLE
    assert result.recommendation_eligible
    assert result.sizing.full_kelly_fraction == pytest.approx(0.20)
    assert result.sizing.fractional_kelly_fraction == pytest.approx(0.05)
    assert result.sizing.raw_stake == pytest.approx(50.0)
    assert result.sizing.constrained_stake == pytest.approx(20.0)
    assert result.sizing.rounded_stake == pytest.approx(20.0)
    assert result.sizing.actionable_stake == pytest.approx(20.0)


def test_unknown_candidate_reference_is_rejected() -> None:
    issued = _issuance()
    with pytest.raises(ValueError, match="exactly one"):
        evaluate_recommendation_candidate(
            policy=_policy(active=True),
            issuance=issued,
            candidate_reference_id="unknown",
            decision_at=DECISION,
        )


def test_decision_time_controls_freshness() -> None:
    result = _evaluate(decision_at=datetime(2026, 9, 1, 12, 20, tzinfo=UTC))
    freshness = next(check for check in result.checks if check.check_id == "quote_freshness")
    assert freshness.status is PolicyCheckStatus.FAILED
    assert result.state is RecommendationDecisionState.UNQUALIFIED


def test_missing_mandatory_correlation_evidence_prevents_allocation() -> None:
    """Missing correlation evidence is a Stage 2 (allocation) gap. It must
    not affect Stage 1 recommendation eligibility, which is independent."""
    issued = _issuance()
    reference = candidate_issuance_row_id(issued.issuance_id, issued.rows[0])
    result = evaluate_recommendation_candidate(
        policy=_policy(active=True),
        issuance=issued,
        candidate_reference_id=reference,
        decision_at=DECISION,
        bankroll=_bankroll(),
        portfolio=_portfolio(),
        correlation=None,
    )
    check = next(item for item in result.checks if item.check_id == "correlation_evidence")
    assert check.status is PolicyCheckStatus.UNAVAILABLE
    assert result.state is RecommendationDecisionState.RECOMMENDATION_ELIGIBLE
    assert result.recommendation_eligible
    assert result.allocation.state is PortfolioAllocationState.NOT_EVALUATED
    assert result.allocation.reason is PortfolioAllocationReason.ALLOCATION_EVIDENCE_UNAVAILABLE
    assert result.allocation.allocated_stake is None


def test_exact_duplicate_prevents_allocation_not_eligibility() -> None:
    """An exact duplicate is a Stage 2 (allocation) gap -- it must not
    retroactively erase an already-established recommendation eligibility."""
    issued = _issuance()
    reference = candidate_issuance_row_id(issued.issuance_id, issued.rows[0])
    duplicate = PortfolioExposureRow(
        "bet-1", issued.rows[0].game_id, EVALUATED, "moneyline", "home", 10.0, "open", reference
    )
    result = _evaluate(issuance=issued, portfolio=_portfolio(duplicate))
    check = next(item for item in result.checks if item.check_id == "exact_duplicate")
    assert check.status is PolicyCheckStatus.FAILED
    assert result.state is RecommendationDecisionState.RECOMMENDATION_ELIGIBLE
    assert result.recommendation_eligible
    assert result.allocation.state is PortfolioAllocationState.NOT_EVALUATED
    assert result.allocation.reason is PortfolioAllocationReason.ALLOCATION_EVIDENCE_UNAVAILABLE


def test_opposing_position_fails() -> None:
    opposing = PortfolioExposureRow(
        "bet-1", "2026_01_KC_LAC", EVALUATED, "moneyline", "away", 10.0, "open", None
    )
    result = _evaluate(portfolio=_portfolio(opposing))
    check = next(item for item in result.checks if item.check_id == "opposing_position")
    assert check.status is PolicyCheckStatus.FAILED


def test_game_capacity_constrains_stake() -> None:
    existing = PortfolioExposureRow(
        "bet-1", "2026_01_KC_LAC", EVALUATED, "total", "over", 45.0, "open", None
    )
    result = _evaluate(portfolio=_portfolio(existing))
    assert result.sizing.raw_stake == pytest.approx(50.0)
    assert result.sizing.constrained_stake == pytest.approx(5.0)
    assert result.sizing.actionable_stake == pytest.approx(5.0)


def test_below_minimum_constrained_stake_is_zero_allocation_not_ineligibility() -> None:
    """Real portfolio/correlation capacity exhaustion produces an
    independently eligible recommendation with an explained zero
    allocation -- not a demoted recommendation state. This is the seed
    proof for Unit 2's core obligation."""
    result = _evaluate(correlation=_correlation(existing_stake=49.0))
    assert result.recommendation_eligible
    assert result.state is RecommendationDecisionState.RECOMMENDATION_ELIGIBLE
    assert result.sizing.constrained_stake == pytest.approx(1.0)
    assert result.allocation.state is PortfolioAllocationState.ZERO_ALLOCATION
    assert result.allocation.reason is PortfolioAllocationReason.CORRELATION_CAPACITY_EXHAUSTED
    assert result.allocation.allocated_stake == pytest.approx(0.0)


def test_repeated_evaluation_is_deterministic_and_inputs_are_unchanged() -> None:
    issued = _issuance()
    policy = _policy(active=True)
    portfolio = _portfolio()
    bankroll = _bankroll()
    before = (issued, policy, portfolio, bankroll)
    first = _evaluate(policy=policy, issuance=issued, portfolio=portfolio, bankroll=bankroll)
    second = _evaluate(policy=policy, issuance=issued, portfolio=portfolio, bankroll=bankroll)
    assert first == second
    assert before == (issued, policy, portfolio, bankroll)
    assert [check.check_id for check in first.checks] == [
        "candidate_issuance",
        "market_family_policy",
        "quote_freshness",
        "expected_value_threshold",
        "portfolio_snapshot",
        "exact_duplicate",
        "opposing_position",
        "bankroll_basis",
        "correlation_evidence",
        "kelly_sizing",
        "candidate_exposure",
        "game_exposure",
        "portfolio_exposure",
        "correlation_exposure",
    ]


def test_portfolio_snapshot_rejects_duplicate_ids_and_future_rows() -> None:
    row = PortfolioExposureRow(
        "bet-1", "2026_01_KC_LAC", EVALUATED, "moneyline", "home", 10.0, "open", None
    )
    with pytest.raises(ValueError, match="duplicate"):
        _portfolio(row, row)
    future = replace(row, placed_at=DECISION)
    with pytest.raises(ValueError, match="later"):
        portfolio_exposure_snapshot(observed_at=EVALUATED, rows=(future,))


def test_qualified_opportunity_requires_all_qualification_checks() -> None:
    stale = _evaluate(decision_at=datetime(2026, 9, 1, 12, 20, tzinfo=UTC))
    assert stale.state is RecommendationDecisionState.UNQUALIFIED
    assert not stale.recommendation_eligible
