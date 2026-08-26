"""Tests for immutable recommended-bet result contracts."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from gridiron_edge.market.candidate_issuance import (
    CANDIDATE_ISSUANCE_SCHEMA_VERSION,
    CURRENT_CANDIDATE_REFERENCE_DERIVATION_VERSION,
    CandidateIssuance,
    CandidateIssuanceReason,
    CandidateIssuanceRow,
    CandidateIssuanceState,
    UnsupportedCandidateReferenceVersionError,
    candidate_issuance_row_id,
)
from gridiron_edge.market.recommendation_policy import (
    RECOMMENDATION_POLICY_DERIVATION_METHOD,
    RECOMMENDATION_POLICY_SCHEMA_VERSION,
    BankrollBasis,
    CorrelationExposureEvidence,
    EmpiricalQualificationThresholds,
    MarketFamilyRecommendationPolicy,
    PolicyDerivationReason,
    PolicyDerivationStatus,
    PolicyValueSource,
    RecommendationDecisionState,
    RecommendationPolicy,
    RecommendationPolicyGovernance,
    StakeRoundingMode,
    evaluate_recommendation_candidate,
    governance_fingerprint,
    portfolio_exposure_snapshot,
    recommendation_policy_id,
)
from gridiron_edge.market.recommended_bet_result import (
    RecommendedBetResultState,
    build_recommended_bet_result,
    evaluate_recommendation_issuance,
    recommended_bet_result_id,
    validate_recommended_bet_result,
)


def test_result_state_enum_preserves_required_lifecycle_states() -> None:
    assert {state.value for state in RecommendedBetResultState} == {
        "qualified",
        "recommended",
        "failed",
        "unavailable",
        "conflicting",
    }


def test_unit24_module_has_no_request_or_mutation_dependency() -> None:
    source = Path("src/gridiron_edge/market/recommended_bet_result.py").read_text(encoding="utf-8")
    for forbidden in (
        "gridiron_edge.api",
        "gridiron_edge.cli",
        "gridiron_edge.betting.ledger",
        "gridiron_edge.betting.bankroll",
    ):
        assert forbidden not in source


FETCHED = datetime(2026, 9, 1, 12, tzinfo=UTC)
EVALUATED = datetime(2026, 9, 1, 12, 5, tzinfo=UTC)
DECISION = datetime(2026, 9, 1, 12, 10, tzinfo=UTC)
KICKOFF = datetime(2026, 9, 1, 20, tzinfo=UTC)


def _governance() -> RecommendationPolicyGovernance:
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


def _family(market: str, *, active: bool) -> MarketFamilyRecommendationPolicy:
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


def _policy(*, active: bool = True) -> RecommendationPolicy:
    governance = _governance()
    policy = RecommendationPolicy(
        RECOMMENDATION_POLICY_SCHEMA_VERSION,
        "0" * 64,
        datetime(2026, 8, 17, tzinfo=UTC),
        "a" * 64,
        governance_fingerprint(governance),
        RECOMMENDATION_POLICY_DERIVATION_METHOD,
        _family("moneyline", active=active),
        _family("spread", active=active),
        _family("total", active=active),
        governance,
    )
    return replace(policy, policy_id=recommendation_policy_id(policy))


def _row(
    *,
    game_id: str = "2026_01_KC_LAC",
    sportsbook: str = "draftkings",
    state: CandidateIssuanceState = CandidateIssuanceState.CANDIDATE,
) -> CandidateIssuanceRow:
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


def _issuance(*rows: CandidateIssuanceRow) -> CandidateIssuance:
    return CandidateIssuance(
        CANDIDATE_ISSUANCE_SCHEMA_VERSION,
        "b" * 64,
        "product-1",
        "product-run-1",
        FETCHED,
        "2026",
        1,
        EVALUATED,
        rows or (_row(),),
    )


def _bankroll() -> BankrollBasis:
    return BankrollBasis(1000.0, EVALUATED, "transaction_snapshot", "bankroll-1")


def test_batch_preserves_every_candidate_and_provenance() -> None:
    rows = (
        _row(game_id="2026_01_KC_LAC", sportsbook="draftkings"),
        _row(game_id="2026_01_BUF_NYJ", sportsbook="fanduel"),
        _row(game_id="2026_01_DEN_LV", state=CandidateIssuanceState.NOT_CANDIDATE),
    )
    issuance = _issuance(*rows)
    portfolio = portfolio_exposure_snapshot(observed_at=EVALUATED, rows=())
    evaluation = evaluate_recommendation_issuance(
        policy=_policy(),
        issuance=issuance,
        decision_at=DECISION,
        bankroll=_bankroll(),
        portfolio=portfolio,
    )
    assert len(evaluation.results) == 2
    assert [result.game_id for result in evaluation.results] == [
        "2026_01_KC_LAC",
        "2026_01_BUF_NYJ",
    ]
    first = evaluation.results[0]
    assert first.product_id == issuance.product_id
    assert first.forecast_run_id == "forecast-run"
    assert first.policy_id == evaluation.policy_id
    assert first.bankroll_basis == _bankroll()
    assert first.portfolio_snapshot_id == portfolio.snapshot_id
    assert first.result_state is RecommendedBetResultState.RECOMMENDED


def test_inactive_policy_persists_unavailable_result_without_sizing() -> None:
    evaluation = evaluate_recommendation_issuance(
        policy=_policy(active=False),
        issuance=_issuance(_row()),
        decision_at=DECISION,
    )
    result = evaluation.results[0]
    assert result.result_state is RecommendedBetResultState.UNAVAILABLE
    assert result.decision_state is RecommendationDecisionState.INSUFFICIENT_EVIDENCE
    assert result.sizing.actionable_stake is None


def test_result_identity_is_deterministic_and_changes_with_decision_time() -> None:
    issuance = _issuance(_row())
    policy = _policy()
    portfolio = portfolio_exposure_snapshot(observed_at=EVALUATED, rows=())
    first = evaluate_recommendation_issuance(
        policy=policy,
        issuance=issuance,
        decision_at=DECISION,
        bankroll=_bankroll(),
        portfolio=portfolio,
    )
    replay = evaluate_recommendation_issuance(
        policy=policy,
        issuance=issuance,
        decision_at=DECISION,
        bankroll=_bankroll(),
        portfolio=portfolio,
    )
    later = evaluate_recommendation_issuance(
        policy=policy,
        issuance=issuance,
        decision_at=datetime(2026, 9, 1, 12, 11, tzinfo=UTC),
        bankroll=_bankroll(),
        portfolio=portfolio,
    )
    assert first == replay
    assert first.evaluation_id == replay.evaluation_id
    assert first.results[0].result_id != later.results[0].result_id


def test_result_builder_rejects_policy_and_quote_age_mismatch() -> None:
    issuance = _issuance(_row())
    policy = _policy()
    reference = candidate_issuance_row_id(issuance.issuance_id, issuance.rows[0])
    portfolio = portfolio_exposure_snapshot(observed_at=EVALUATED, rows=())
    decision = evaluate_recommendation_candidate(
        policy=policy,
        issuance=issuance,
        candidate_reference_id=reference,
        decision_at=DECISION,
        bankroll=_bankroll(),
        portfolio=portfolio,
    )
    other_governance = replace(
        policy.governance,
        fractional_kelly_multiplier=0.10,
    )
    other_policy = replace(
        policy,
        policy_id="0" * 64,
        governance=other_governance,
        governance_fingerprint=governance_fingerprint(other_governance),
    )
    other_policy = replace(
        other_policy,
        policy_id=recommendation_policy_id(other_policy),
    )
    with pytest.raises(ValueError, match="provenance"):
        build_recommended_bet_result(
            issuance=issuance,
            policy=other_policy,
            decision=decision,
            bankroll=_bankroll(),
            portfolio=portfolio,
            correlation=None,
        )
    changed = replace(decision, decision_quote_age_seconds=999.0)
    with pytest.raises(ValueError, match="quote-age"):
        build_recommended_bet_result(
            issuance=issuance,
            policy=policy,
            decision=changed,
            bankroll=_bankroll(),
            portfolio=portfolio,
            correlation=None,
        )


def test_ambiguous_correlation_membership_is_rejected() -> None:
    issuance = _issuance(_row())
    reference = candidate_issuance_row_id(issuance.issuance_id, issuance.rows[0])
    correlations = (
        CorrelationExposureEvidence("group-a", (reference,), 0.0),
        CorrelationExposureEvidence("group-b", (reference,), 0.0),
    )
    with pytest.raises(ValueError, match="multiple correlation"):
        evaluate_recommendation_issuance(
            policy=_policy(),
            issuance=issuance,
            decision_at=DECISION,
            bankroll=_bankroll(),
            portfolio=portfolio_exposure_snapshot(observed_at=EVALUATED, rows=()),
            correlations=correlations,
        )


def test_batch_evaluation_does_not_mutate_inputs() -> None:
    issuance = _issuance(_row())
    policy = _policy(active=False)
    before = (issuance, policy)
    evaluate_recommendation_issuance(
        policy=policy,
        issuance=issuance,
        decision_at=DECISION,
    )
    assert before == (issuance, policy)


def test_new_result_records_current_derivation_version() -> None:
    evaluation = evaluate_recommendation_issuance(
        policy=_policy(),
        issuance=_issuance(_row()),
        decision_at=DECISION,
        bankroll=_bankroll(),
        portfolio=portfolio_exposure_snapshot(observed_at=EVALUATED, rows=()),
    )
    result = evaluation.results[0]
    assert (
        result.candidate_reference_derivation_version
        == CURRENT_CANDIDATE_REFERENCE_DERIVATION_VERSION
    )


def test_derivation_version_participates_in_result_identity() -> None:
    """Pure identity-function test: an unsupported version tag still changes
    result_id, since identity canonicalization does not validate the field
    -- this does NOT assert the tagged object is a valid result."""
    evaluation = evaluate_recommendation_issuance(
        policy=_policy(),
        issuance=_issuance(_row()),
        decision_at=DECISION,
        bankroll=_bankroll(),
        portfolio=portfolio_exposure_snapshot(observed_at=EVALUATED, rows=()),
    )
    result = evaluation.results[0]
    retagged = replace(result, candidate_reference_derivation_version=2, result_id="0" * 64)
    assert recommended_bet_result_id(retagged) != result.result_id


def test_derivation_version_field_participates_in_result_identity_only() -> None:
    """The derivation-version field is canonicalized into result_id (proven
    directly, as a pure identity function). Evaluation identity's inclusion
    of result_ids (proven separately by
    test_result_identity_is_deterministic_and_changes_with_decision_time)
    means any result_id change propagates -- this test isolates the first
    link in that chain without re-testing the second."""
    evaluation = evaluate_recommendation_issuance(
        policy=_policy(),
        issuance=_issuance(_row()),
        decision_at=DECISION,
        bankroll=_bankroll(),
        portfolio=portfolio_exposure_snapshot(observed_at=EVALUATED, rows=()),
    )
    result = evaluation.results[0]
    retagged = replace(result, candidate_reference_derivation_version=2, result_id="0" * 64)
    assert recommended_bet_result_id(retagged) != result.result_id


def test_unsupported_recorded_version_raises_distinct_from_corruption() -> None:
    """A result whose recorded derivation version has no implementation
    raises the propagated UnsupportedCandidateReferenceVersionError -- NOT
    the ordinary content-corruption ValueError -- and this is not described
    as a valid v2 result."""
    evaluation = evaluate_recommendation_issuance(
        policy=_policy(),
        issuance=_issuance(_row()),
        decision_at=DECISION,
        bankroll=_bankroll(),
        portfolio=portfolio_exposure_snapshot(observed_at=EVALUATED, rows=()),
    )
    result = evaluation.results[0]
    tagged = replace(result, candidate_reference_derivation_version=2)
    tagged = replace(tagged, result_id=recommended_bet_result_id(tagged))
    with pytest.raises(UnsupportedCandidateReferenceVersionError):
        validate_recommended_bet_result(tagged)


def test_supported_version_with_altered_evidence_still_raises_corruption_message() -> None:
    """A supported (v1) recorded version with tampered offer evidence still
    raises today's exact existing corruption message, unchanged."""
    evaluation = evaluate_recommendation_issuance(
        policy=_policy(),
        issuance=_issuance(_row()),
        decision_at=DECISION,
        bankroll=_bankroll(),
        portfolio=portfolio_exposure_snapshot(observed_at=EVALUATED, rows=()),
    )
    result = evaluation.results[0]
    tampered = replace(result, american_price=(result.american_price or 0) + 5)
    with pytest.raises(ValueError, match="does not match offer evidence"):
        validate_recommended_bet_result(tampered)
