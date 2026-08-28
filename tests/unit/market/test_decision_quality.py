"""Tests for retrospective decision-quality evaluation."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
import json as _json

import pytest

from gridiron_edge.market.candidate_issuance import (
    CANDIDATE_ISSUANCE_SCHEMA_VERSION,
    CandidateIssuance,
    CandidateIssuanceReason,
    CandidateIssuanceRow,
    CandidateIssuanceState,
    candidate_issuance_id,
    candidate_issuance_row_id,
)
from gridiron_edge.market.candidate_outcome import CandidateOutcome, grade_candidate_outcome
import gridiron_edge.market.decision_quality as decision_quality_module
from gridiron_edge.market.decision_quality import (
    DecisionQualityStatus,
    evaluate_decision_quality,
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
    PortfolioExposureSnapshot,
    RecommendationPolicy,
    RecommendationPolicyGovernance,
    StakeRoundingMode,
    evaluate_recommendation_candidate,
    governance_fingerprint,
    portfolio_exposure_snapshot,
    recommendation_policy_id,
)
from gridiron_edge.market.recommended_bet_result import (
    RECOMMENDED_BET_RESULT_SCHEMA_VERSION,
    RecommendedBetEvaluation,
    build_recommended_bet_result,
)

# --- Canonical game-spread fixture -----------------------------------------
# home -3.5, away 20, home 27 -> 27 + (-3.5) = 23.5 > 20 -> WIN.
# Integer NFL scores cannot push against a half-point line; push is
# proven separately in test_candidate_outcome.py using a whole-number line.

_FETCHED = datetime(2026, 9, 1, 12, tzinfo=UTC)
_EVALUATED = datetime(2026, 9, 1, 12, 5, tzinfo=UTC)
_DECISION = datetime(2026, 9, 1, 12, 10, tzinfo=UTC)
_KICKOFF = datetime(2026, 9, 1, 20, tzinfo=UTC)


def _spread_governance(**overrides: object) -> RecommendationPolicyGovernance:
    base: dict[str, object] = {
        "fractional_kelly_multiplier": 0.25,
        "minimum_actionable_stake": 5.0,
        "stake_increment": 1.0,
        "stake_rounding": StakeRoundingMode.DOWN,
        "maximum_candidate_bankroll_fraction": 0.02,
        "maximum_game_bankroll_fraction": 0.05,
        "maximum_portfolio_bankroll_fraction": 0.20,
        "prohibit_opposing_positions": True,
        "correlation_check_mandatory": False,
        "exposure_eligible_statuses": ("open",),
    }
    base.update(overrides)
    return RecommendationPolicyGovernance(**base)


def _family(market: str, *, active: bool) -> MarketFamilyRecommendationPolicy:
    """Market-aware family builder -- each family is stored under its
    own matching market, per validate_recommendation_policy's
    requirement. Only the target market should be active for a clean
    canonical proof unless a test specifically needs otherwise."""
    return MarketFamilyRecommendationPolicy(
        market=market,
        status=PolicyDerivationStatus.ACTIVE
        if active
        else PolicyDerivationStatus.INSUFFICIENT_EVIDENCE,
        reason=(
            PolicyDerivationReason.DERIVED
            if active
            else PolicyDerivationReason.NO_VALIDATED_THRESHOLD_SELECTION_METHOD
        ),
        candidate_count=2,
        outcome_available_count=2,
        clv_available_count=2,
        return_available_count=2,
        evidence_statuses=(("synthetic_test_evidence", "available"),),
        thresholds=(EmpiricalQualificationThresholds(0.01, 900.0, None, None) if active else None),
        source=PolicyValueSource.EMPIRICAL_MARKET_FAMILY_EVIDENCE,
    )


def spread_policy(
    *, governance: RecommendationPolicyGovernance | None = None
) -> RecommendationPolicy:
    """The persisted policy referenced by the canonical spread proof.
    Only the spread family is active; moneyline and total are correctly
    inactive, each stored under its own matching market."""
    resolved_governance = governance or _spread_governance()
    policy = RecommendationPolicy(
        schema_version=RECOMMENDATION_POLICY_SCHEMA_VERSION,
        policy_id="0" * 64,
        created_at=datetime(2026, 8, 17, tzinfo=UTC),
        source_evidence_fingerprint="a" * 64,
        governance_fingerprint=governance_fingerprint(resolved_governance),
        derivation_method=RECOMMENDATION_POLICY_DERIVATION_METHOD,
        moneyline=_family("moneyline", active=False),
        spread=_family("spread", active=True),
        total=_family("total", active=False),
        governance=resolved_governance,
    )
    return replace(policy, policy_id=recommendation_policy_id(policy))


def spread_candidate_row(*, line: float = -3.5) -> CandidateIssuanceRow:
    """The canonical game-spread candidate: home -3.5 at -110."""
    return CandidateIssuanceRow(
        game_id="2026_01_KC_LAC",
        market="spread",
        side="home",
        provider="the_odds_api",
        provider_event_id="event-1",
        sportsbook="draftkings",
        line=line,
        american_price=-110,
        fetched_at=_FETCHED,
        sportsbook_updated_at=_FETCHED,
        kickoff=_KICKOFF,
        is_live=False,
        forecast_event_id="forecast-1",
        forecast_run_id="run-1",
        forecast_role="champion",
        forecast_generated_at=_FETCHED,
        model_name="spread_model",
        model_type="random_forest",
        model_probability=0.60,
        expected_value=0.20,
        state=CandidateIssuanceState.CANDIDATE,
        reason=CandidateIssuanceReason.POSITIVE_EXPECTED_VALUE,
    )


def spread_issuance(*, row: CandidateIssuanceRow | None = None) -> CandidateIssuance:
    resolved_row = row or spread_candidate_row()
    issuance_id = candidate_issuance_id(
        product_id="product-1",
        product_run_id="run-1",
        season="2026-2027",
        week=1,
        evaluated_at=_EVALUATED,
    )
    return CandidateIssuance(
        schema_version=CANDIDATE_ISSUANCE_SCHEMA_VERSION,
        issuance_id=issuance_id,
        product_id="product-1",
        product_run_id="run-1",
        product_generated_at=_FETCHED,
        season="2026-2027",
        week=1,
        evaluated_at=_EVALUATED,
        rows=(resolved_row,),
    )


def spread_bankroll() -> BankrollBasis:
    return BankrollBasis(
        amount=1000.0,
        observed_at=_EVALUATED,
        source_kind="transaction_snapshot",
        source_id="bankroll-1",
    )


def spread_portfolio() -> PortfolioExposureSnapshot:
    return portfolio_exposure_snapshot(observed_at=_EVALUATED, rows=())


def eligible_spread_evaluation(
    *, policy: RecommendationPolicy | None = None
) -> tuple[RecommendedBetEvaluation, RecommendationPolicy, CandidateIssuance]:
    issuance = spread_issuance()
    resolved_policy = policy or spread_policy()
    reference = candidate_issuance_row_id(issuance.issuance_id, issuance.rows[0])
    decision = evaluate_recommendation_candidate(
        policy=resolved_policy,
        issuance=issuance,
        candidate_reference_id=reference,
        decision_at=_DECISION,
        bankroll=spread_bankroll(),
        portfolio=spread_portfolio(),
        correlation=None,
    )
    result = build_recommended_bet_result(
        issuance=issuance,
        policy=resolved_policy,
        decision=decision,
        bankroll=spread_bankroll(),
        portfolio=spread_portfolio(),
        correlation=None,
    )
    identity = {
        "schema_version": RECOMMENDED_BET_RESULT_SCHEMA_VERSION,
        "issuance_id": issuance.issuance_id,
        "policy_id": resolved_policy.policy_id,
        "evaluated_at": result.evaluated_at.isoformat(),
        "result_ids": [result.result_id],
    }

    evaluation_id = sha256(
        _json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    evaluation = RecommendedBetEvaluation(
        schema_version=RECOMMENDED_BET_RESULT_SCHEMA_VERSION,
        evaluation_id=evaluation_id,
        issuance_id=issuance.issuance_id,
        policy_id=resolved_policy.policy_id,
        evaluated_at=result.evaluated_at,
        results=(result,),
    )
    return evaluation, resolved_policy, issuance


# --- Tests -------------------------------------------------------------


def test_consistent_decision_before_outcome_is_known() -> None:
    evaluation, policy, issuance = eligible_spread_evaluation()
    result_id = evaluation.results[0].result_id
    dq = evaluate_decision_quality(
        result_id=result_id,
        recommendation_evaluation=evaluation,
        policy=policy,
        issuance=issuance,
        outcome=CandidateOutcome.UNAVAILABLE,
        evaluated_at=_DECISION,
    )
    assert dq.decision_status is DecisionQualityStatus.CONSISTENT
    assert dq.realized_outcome is CandidateOutcome.UNAVAILABLE
    assert dq.recommendation_evaluation_id == evaluation.evaluation_id


def test_pre_and_post_outcome_separation() -> None:
    """The central proof: the same decision-quality conclusion holds
    before and after the game result is known -- and the post-outcome
    grade is produced by the shared, real grading function against real
    final scores, not an ungrounded enum literal."""
    evaluation, policy, issuance = eligible_spread_evaluation()
    result_id = evaluation.results[0].result_id
    row = spread_candidate_row()

    pre_outcome = evaluate_decision_quality(
        result_id=result_id,
        recommendation_evaluation=evaluation,
        policy=policy,
        issuance=issuance,
        outcome=grade_candidate_outcome(row, None),
        evaluated_at=_DECISION,
    )

    final_scores = (20.0, 27.0)  # away, home -- home covers -3.5
    graded_outcome = grade_candidate_outcome(row, final_scores)
    assert graded_outcome is CandidateOutcome.WIN

    post_outcome = evaluate_decision_quality(
        result_id=result_id,
        recommendation_evaluation=evaluation,
        policy=policy,
        issuance=issuance,
        outcome=graded_outcome,
        evaluated_at=_DECISION,
    )

    assert pre_outcome.checks == post_outcome.checks
    assert pre_outcome.decision_status == post_outcome.decision_status
    assert pre_outcome.realized_outcome is CandidateOutcome.UNAVAILABLE
    assert post_outcome.realized_outcome is CandidateOutcome.WIN
    assert pre_outcome.evaluation_id != post_outcome.evaluation_id


def test_invalid_policy_identity_is_rejected_before_evaluation() -> None:
    """A policy object whose own policy_id no longer matches its
    canonical content is a malformed input artifact -- rejected by
    validate_recommendation_policy, not scored as INCONSISTENT."""
    evaluation, policy, issuance = eligible_spread_evaluation()
    result_id = evaluation.results[0].result_id
    malformed_policy = replace(policy, policy_id="f" * 64)
    with pytest.raises(ValueError):
        evaluate_decision_quality(
            result_id=result_id,
            recommendation_evaluation=evaluation,
            policy=malformed_policy,
            issuance=issuance,
            evaluated_at=_DECISION,
        )


def test_true_policy_mismatch_is_inconsistent() -> None:
    """A second, independently valid policy -- not the one the result
    actually references -- must produce a real INCONSISTENT conclusion,
    not a raised exception. This is the genuine cross-artifact-
    disagreement case, distinct from the malformed-input case above."""
    evaluation, original_policy, issuance = eligible_spread_evaluation()
    result_id = evaluation.results[0].result_id

    changed_governance = _spread_governance(fractional_kelly_multiplier=0.10)
    other_policy = spread_policy(governance=changed_governance)
    assert other_policy.policy_id != original_policy.policy_id  # sanity check on the fixture

    dq = evaluate_decision_quality(
        result_id=result_id,
        recommendation_evaluation=evaluation,
        policy=other_policy,
        issuance=issuance,
        evaluated_at=_DECISION,
    )
    assert dq.decision_status is DecisionQualityStatus.INCONSISTENT
    policy_check = next(c for c in dq.checks if c.check_id == "policy_reference")
    assert policy_check.status is DecisionQualityStatus.INCONSISTENT


def test_missing_original_evidence_is_unavailable_not_inconsistent() -> None:
    evaluation, policy, issuance = eligible_spread_evaluation()
    result_id = evaluation.results[0].result_id
    dq = evaluate_decision_quality(
        result_id=result_id,
        recommendation_evaluation=evaluation,
        policy=policy,
        issuance=issuance,
        portfolio=None,
        evaluated_at=_DECISION,
    )
    replay_check = next(c for c in dq.checks if c.check_id == "allocation_recomputation")
    assert replay_check.status is DecisionQualityStatus.UNAVAILABLE
    assert replay_check.mandatory is False
    assert dq.decision_status is DecisionQualityStatus.CONSISTENT


def test_full_replay_confirms_consistency_when_evidence_supplied() -> None:
    evaluation, policy, issuance = eligible_spread_evaluation()
    result_id = evaluation.results[0].result_id
    dq = evaluate_decision_quality(
        result_id=result_id,
        recommendation_evaluation=evaluation,
        policy=policy,
        issuance=issuance,
        portfolio=spread_portfolio(),
        correlation=None,
        evaluated_at=_DECISION,
    )
    replay_check = next(c for c in dq.checks if c.check_id == "allocation_recomputation")
    assert replay_check.status is DecisionQualityStatus.CONSISTENT


def test_public_replay_seam_returning_a_disagreeing_decision_flips_status(monkeypatch) -> None:
    """Proves the public evaluator's replay branch (not merely the
    private aggregator) propagates a disagreement to INCONSISTENT, via a
    monkeypatched replay seam. This deliberately mutates one field on an
    otherwise-real replayed decision to force disagreement -- it is not
    a claim that a second independently-valid domain replay was
    constructed."""
    evaluation, policy, issuance = eligible_spread_evaluation()
    result_id = evaluation.results[0].result_id
    real_result = evaluation.results[0]

    disagreeing_decision = replace(
        evaluate_recommendation_candidate(
            policy=policy,
            issuance=issuance,
            candidate_reference_id=real_result.candidate_reference_id,
            decision_at=_DECISION,
            bankroll=spread_bankroll(),
            portfolio=spread_portfolio(),
            correlation=None,
        ),
        allocation=replace(real_result.allocation, allocated_stake=999.0),
    )

    monkeypatch.setattr(
        decision_quality_module,
        "evaluate_recommendation_candidate",
        lambda **_kwargs: disagreeing_decision,
    )

    dq = evaluate_decision_quality(
        result_id=result_id,
        recommendation_evaluation=evaluation,
        policy=policy,
        issuance=issuance,
        portfolio=spread_portfolio(),
        correlation=None,
        evaluated_at=_DECISION,
    )
    replay_check = next(c for c in dq.checks if c.check_id == "allocation_recomputation")
    assert replay_check.status is DecisionQualityStatus.INCONSISTENT
    assert dq.decision_status is DecisionQualityStatus.INCONSISTENT
