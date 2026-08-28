"""Shared synthetic evidence builders for recommended-bet result tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

from gridiron_edge.market.candidate_issuance import (
    CANDIDATE_ISSUANCE_SCHEMA_VERSION,
    CandidateIssuance,
    CandidateIssuanceReason,
    CandidateIssuanceRow,
    CandidateIssuanceState,
)
from gridiron_edge.market.recommendation_policy import (
    RECOMMENDATION_POLICY_DERIVATION_METHOD,
    RECOMMENDATION_POLICY_SCHEMA_VERSION,
    BankrollBasis,
    EmpiricalQualificationThresholds,
    MarketFamilyRecommendationPolicy,
    PolicyDerivationReason,
    PolicyDerivationStatus,
    PolicyValueSource,
    RecommendationPolicy,
    RecommendationPolicyGovernance,
    StakeRoundingMode,
    governance_fingerprint,
    portfolio_exposure_snapshot,
    recommendation_policy_id,
)
from gridiron_edge.market.recommended_bet_result import (
    RecommendedBetEvaluation,
    evaluate_recommendation_issuance,
)

FETCHED = datetime(2026, 9, 1, 12, tzinfo=UTC)
EVALUATED = datetime(2026, 9, 1, 12, 5, tzinfo=UTC)
DECISION = datetime(2026, 9, 1, 12, 10, tzinfo=UTC)
KICKOFF = datetime(2026, 9, 1, 20, tzinfo=UTC)


def governance() -> RecommendationPolicyGovernance:
    """Return deterministic governed recommendation inputs for tests."""
    return RecommendationPolicyGovernance(
        0.25,
        5.0,
        1.0,
        StakeRoundingMode.DOWN,
        0.02,
        0.05,
        0.20,
        True,
        False,
        ("open",),
    )


def family(market: str, *, active: bool) -> MarketFamilyRecommendationPolicy:
    """Return one synthetic active or inactive market-family policy."""
    return MarketFamilyRecommendationPolicy(
        market,
        PolicyDerivationStatus.ACTIVE if active else PolicyDerivationStatus.INSUFFICIENT_EVIDENCE,
        PolicyDerivationReason.DERIVED
        if active
        else PolicyDerivationReason.NO_VALIDATED_THRESHOLD_SELECTION_METHOD,
        2,
        2,
        2,
        2,
        (("synthetic_test_evidence", "available"),),
        EmpiricalQualificationThresholds(0.01, 900.0, None, None) if active else None,
        PolicyValueSource.EMPIRICAL_MARKET_FAMILY_EVIDENCE,
    )


def policy(*, active: bool = True) -> RecommendationPolicy:
    """Return one internally valid synthetic policy."""
    governed = governance()
    value = RecommendationPolicy(
        RECOMMENDATION_POLICY_SCHEMA_VERSION,
        "0" * 64,
        datetime(2026, 8, 17, tzinfo=UTC),
        "a" * 64,
        governance_fingerprint(governed),
        RECOMMENDATION_POLICY_DERIVATION_METHOD,
        family("moneyline", active=active),
        family("spread", active=active),
        family("total", active=active),
        governed,
    )
    return replace(value, policy_id=recommendation_policy_id(value))


def row(
    *,
    game_id: str = "2026_01_KC_LAC",
    sportsbook: str = "draftkings",
    state: CandidateIssuanceState = CandidateIssuanceState.CANDIDATE,
) -> CandidateIssuanceRow:
    """Return one exact synthetic issuance row."""
    reason = (
        CandidateIssuanceReason.POSITIVE_EXPECTED_VALUE
        if state is CandidateIssuanceState.CANDIDATE
        else CandidateIssuanceReason.EXPECTED_VALUE_NOT_POSITIVE
    )
    return CandidateIssuanceRow(
        game_id,
        "moneyline",
        "home",
        "the_odds_api",
        f"event-{game_id}",
        sportsbook,
        None,
        100,
        FETCHED,
        FETCHED,
        KICKOFF,
        False,
        f"forecast-{game_id}",
        "forecast-run",
        "champion",
        FETCHED,
        "win_prob",
        "random_forest",
        0.60,
        0.20,
        state,
        reason,
    )


def issuance(*rows: CandidateIssuanceRow) -> CandidateIssuance:
    """Return one deterministic synthetic candidate issuance."""
    return CandidateIssuance(
        CANDIDATE_ISSUANCE_SCHEMA_VERSION,
        "b" * 64,
        "product-1",
        "product-run-1",
        FETCHED,
        "2026",
        1,
        EVALUATED,
        rows or (row(),),
    )


def bankroll() -> BankrollBasis:
    """Return one immutable synthetic bankroll basis."""
    return BankrollBasis(1000.0, EVALUATED, "transaction_snapshot", "bankroll-1")


def evaluation(*, active: bool = True) -> RecommendedBetEvaluation:
    """Return one deterministic issuance-wide recommendation evaluation."""
    return evaluate_recommendation_issuance(
        policy=policy(active=active),
        issuance=issuance(row()),
        decision_at=DECISION,
        bankroll=bankroll() if active else None,
        portfolio=(portfolio_exposure_snapshot(observed_at=EVALUATED, rows=()) if active else None),
    )
