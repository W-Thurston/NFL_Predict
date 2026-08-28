"""End-to-end proof of the ten first-slice obligations for one spread."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from pandas import DataFrame
import pytest
from tests.fixtures.spread_vertical_slice import (  # pyrefly: ignore [missing-import]
    FORECAST_EVENT_ID,
    GAME_ID,
    PRODUCT_ID,
    SEASON,
    T1,
    T1_FETCHED_AT,
    T2,
    T2_FETCHED_AT,
    WEEK,
    bankroll,
    empty_portfolio,
    forecast_events,
    recommendation_policy,
    t1_quotes,
    t2_quotes,
    weekly_product,
    weekly_product_identity,
)

from gridiron_edge.evaluation.forecast_store import (
    load_forecast_events,
    write_forecast_events,
)
from gridiron_edge.ingest.odds.as_known import as_known_at
from gridiron_edge.ingest.odds.store import (
    append_to_odds_ledger,
    load_odds_ledger,
    odds_history_partition_path,
)
from gridiron_edge.market.candidate_issuance import (
    CandidateIssuance,
    CandidateIssuanceReason,
    CandidateIssuanceRow,
    CandidateIssuanceState,
    candidate_issuance_row_id,
    issue_pregame_candidates,
)
from gridiron_edge.market.candidate_issuance_store import (
    candidate_issuance_path,
    read_candidate_issuance,
    write_candidate_issuance,
)
from gridiron_edge.market.candidate_outcome import (
    CandidateOutcome,
    grade_candidate_outcome,
)
from gridiron_edge.market.decision_quality import (
    DecisionQualityEvaluation,
    DecisionQualityStatus,
    evaluate_decision_quality,
)
from gridiron_edge.market.decision_quality_store import (
    decision_quality_evaluation_path,
    read_decision_quality_evaluation,
    write_decision_quality_evaluation,
)
from gridiron_edge.market.recommendation_policy import (
    CorrelationExposureEvidence,
    PortfolioAllocationReason,
    PortfolioAllocationState,
    RecommendationDecisionState,
    RecommendationPolicy,
)
from gridiron_edge.market.recommendation_policy_store import (
    read_recommendation_policy,
    recommendation_policy_path,
    write_recommendation_policy,
)
from gridiron_edge.market.recommended_bet_result import (
    RecommendedBetEvaluation,
    RecommendedBetResult,
    RecommendedBetResultState,
    evaluate_recommendation_issuance,
)
from gridiron_edge.market.recommended_bet_result_store import (
    read_recommended_bet_evaluation,
    recommended_bet_evaluation_path,
    recommended_bet_result_path,
    write_recommended_bet_evaluation,
)
from gridiron_edge.models.game_prediction.weekly_product_store import (
    get_current_weekly_product_selection,
    load_current_weekly_product,
    select_current_weekly_product,
    weekly_product_artifact_path,
    write_weekly_product,
)


@dataclass(frozen=True)
class SpreadProof:
    """All persisted and computed evidence for the canonical spread slice."""

    repo: Path
    ledger_t1: DataFrame
    ledger_t2: DataFrame
    product: DataFrame
    forecasts: DataFrame
    issuance_t1: CandidateIssuance
    issuance_t2: CandidateIssuance
    t1_row: CandidateIssuanceRow
    t2_row: CandidateIssuanceRow
    candidate_reference_id: str
    active_policy: RecommendationPolicy
    abstaining_policy: RecommendationPolicy
    positive_evaluation: RecommendedBetEvaluation
    positive_result: RecommendedBetResult
    abstaining_evaluation: RecommendedBetEvaluation
    abstaining_result: RecommendedBetResult
    zero_result: RecommendedBetResult
    zero_evaluation: RecommendedBetEvaluation
    zero_evaluation_path: Path
    pre_outcome: DecisionQualityEvaluation
    post_outcome: DecisionQualityEvaluation
    ledger_path: Path
    product_path: Path
    issuance_t1_path: Path
    issuance_t2_path: Path
    active_policy_path: Path
    positive_evaluation_path: Path
    pre_outcome_path: Path
    post_outcome_path: Path


def _latest_row(
    issuance: CandidateIssuance,
) -> CandidateIssuanceRow:
    matching = tuple(
        row
        for row in issuance.rows
        if (
            row.game_id == GAME_ID
            and row.market == "spread"
            and row.side == "home"
            and row.provider == "the_odds_api"
            and row.provider_event_id == "provider-event-1"
            and row.sportsbook == "draftkings"
        )
    )
    assert matching
    return max(matching, key=lambda row: row.fetched_at)


@pytest.fixture
def proof(tmp_path: Path) -> SpreadProof:
    identity = weekly_product_identity()

    product_path = write_weekly_product(
        weekly_product(),
        identity=identity,
        repo=tmp_path,
    )
    select_current_weekly_product(
        identity.product_id,
        season=identity.season,
        week=identity.week,
        selected_at=T1,
        repo=tmp_path,
    )
    product = load_current_weekly_product(
        season=SEASON,
        week=WEEK,
        repo=tmp_path,
    )

    write_forecast_events(
        forecast_events(),
        repo=tmp_path,
    )
    forecasts = load_forecast_events(
        event_id=FORECAST_EVENT_ID,
        repo=tmp_path,
    )

    ledger_path = append_to_odds_ledger(
        t1_quotes(),
        repo=tmp_path,
    )
    ledger_t1 = load_odds_ledger(
        season=SEASON,
        week=WEEK,
        market="spread",
        repo=tmp_path,
    )
    visible_t1 = as_known_at(ledger_t1, T1)

    issuance_t1 = issue_pregame_candidates(
        product=product,
        forecast_events=forecasts,
        quotes=visible_t1,
        evaluated_at=T1,
    )
    issuance_t1_path = write_candidate_issuance(
        issuance_t1,
        repo=tmp_path,
    )
    t1_row = issuance_t1.rows[0]
    candidate_reference_id = candidate_issuance_row_id(
        issuance_t1.issuance_id,
        t1_row,
    )

    active_policy = recommendation_policy(
        spread_active=True,
    )
    active_policy_path = write_recommendation_policy(
        active_policy,
        repo=tmp_path,
    )
    abstaining_policy = recommendation_policy(
        spread_active=False,
    )
    write_recommendation_policy(
        abstaining_policy,
        repo=tmp_path,
    )

    positive_evaluation = evaluate_recommendation_issuance(
        policy=active_policy,
        issuance=issuance_t1,
        decision_at=T1,
        bankroll=bankroll(),
        portfolio=empty_portfolio(),
    )
    positive_evaluation_path = write_recommended_bet_evaluation(
        positive_evaluation,
        repo=tmp_path,
    )
    positive_result = positive_evaluation.results[0]

    abstaining_evaluation = evaluate_recommendation_issuance(
        policy=abstaining_policy,
        issuance=issuance_t1,
        decision_at=T1,
        bankroll=bankroll(),
        portfolio=empty_portfolio(),
    )
    write_recommended_bet_evaluation(
        abstaining_evaluation,
        repo=tmp_path,
    )
    abstaining_result = abstaining_evaluation.results[0]

    zero_correlation = CorrelationExposureEvidence(
        group_id="spread-game-risk",
        member_reference_ids=(candidate_reference_id,),
        existing_stake=49.0,
    )
    zero_evaluation = evaluate_recommendation_issuance(
        policy=active_policy,
        issuance=issuance_t1,
        decision_at=T1,
        bankroll=bankroll(),
        portfolio=empty_portfolio(),
        correlations=(zero_correlation,),
    )
    zero_evaluation_path = write_recommended_bet_evaluation(
        zero_evaluation,
        repo=tmp_path,
    )
    zero_result = zero_evaluation.results[0]
    zero_evaluation = evaluate_recommendation_issuance(
        policy=active_policy,
        issuance=issuance_t1,
        decision_at=T1,
        bankroll=bankroll(),
        portfolio=empty_portfolio(),
        correlations=(
            CorrelationExposureEvidence(
                group_id="spread-game-risk",
                member_reference_ids=(candidate_reference_id,),
                existing_stake=49.0,
            ),
        ),
    )
    zero_evaluation_path = write_recommended_bet_evaluation(
        zero_evaluation,
        repo=tmp_path,
    )
    zero_result = zero_evaluation.results[0]

    pre_outcome = evaluate_decision_quality(
        result_id=positive_result.result_id,
        recommendation_evaluation=positive_evaluation,
        policy=active_policy,
        issuance=issuance_t1,
        portfolio=empty_portfolio(),
        correlation=None,
        outcome=CandidateOutcome.UNAVAILABLE,
        evaluated_at=T1,
    )
    pre_outcome_path = write_decision_quality_evaluation(
        pre_outcome,
        repo=tmp_path,
    )

    final_scores = (20.0, 27.0)
    realized_outcome = grade_candidate_outcome(
        t1_row,
        final_scores,
    )
    assert realized_outcome is CandidateOutcome.WIN

    post_outcome = evaluate_decision_quality(
        result_id=positive_result.result_id,
        recommendation_evaluation=positive_evaluation,
        policy=active_policy,
        issuance=issuance_t1,
        portfolio=empty_portfolio(),
        correlation=None,
        outcome=realized_outcome,
        evaluated_at=T1,
    )
    post_outcome_path = write_decision_quality_evaluation(
        post_outcome,
        repo=tmp_path,
    )

    t1_bytes = issuance_t1_path.read_bytes()
    append_to_odds_ledger(
        t2_quotes(),
        repo=tmp_path,
    )
    ledger_t2 = load_odds_ledger(
        season=SEASON,
        week=WEEK,
        market="spread",
        repo=tmp_path,
    )
    visible_t2 = as_known_at(ledger_t2, T2)
    issuance_t2 = issue_pregame_candidates(
        product=product,
        forecast_events=forecasts,
        quotes=visible_t2,
        evaluated_at=T2,
    )
    issuance_t2_path = write_candidate_issuance(
        issuance_t2,
        repo=tmp_path,
    )
    assert issuance_t1_path.read_bytes() == t1_bytes
    t2_row = _latest_row(issuance_t2)

    return SpreadProof(
        repo=tmp_path,
        ledger_t1=ledger_t1,
        ledger_t2=ledger_t2,
        product=product,
        forecasts=forecasts,
        issuance_t1=issuance_t1,
        issuance_t2=issuance_t2,
        t1_row=t1_row,
        t2_row=t2_row,
        candidate_reference_id=candidate_reference_id,
        active_policy=active_policy,
        abstaining_policy=abstaining_policy,
        positive_evaluation=positive_evaluation,
        positive_result=positive_result,
        abstaining_evaluation=abstaining_evaluation,
        abstaining_result=abstaining_result,
        zero_result=zero_result,
        zero_evaluation=zero_evaluation,
        zero_evaluation_path=zero_evaluation_path,
        pre_outcome=pre_outcome,
        post_outcome=post_outcome,
        ledger_path=ledger_path,
        product_path=product_path,
        issuance_t1_path=issuance_t1_path,
        issuance_t2_path=issuance_t2_path,
        active_policy_path=active_policy_path,
        positive_evaluation_path=positive_evaluation_path,
        pre_outcome_path=pre_outcome_path,
        post_outcome_path=post_outcome_path,
    )


def test_source_observations_are_preserved_without_overwrite(
    proof: SpreadProof,
) -> None:
    assert proof.ledger_path == odds_history_partition_path(
        season=SEASON,
        week=WEEK,
        repo=proof.repo,
    )
    assert len(proof.ledger_t1) == 1
    assert len(proof.ledger_t2) == 2
    assert set(proof.ledger_t2["line"]) == {-1.0, -9.5}
    assert set(proof.ledger_t2["fetched_at"]) == {
        pd.Timestamp(T1_FETCHED_AT),
        pd.Timestamp(T2_FETCHED_AT),
    }


def test_time_valid_claim_consumes_an_exact_source_version(
    proof: SpreadProof,
) -> None:
    visible = as_known_at(proof.ledger_t2, T1)
    assert len(visible) == 1
    assert proof.t1_row.fetched_at == T1_FETCHED_AT
    assert proof.t1_row.line == -1.0
    assert proof.positive_result.candidate_reference_id == (proof.candidate_reference_id)
    assert proof.positive_result.forecast_event_id == FORECAST_EVENT_ID
    assert len(proof.forecasts) == 1


def test_estimated_output_preserves_uncertainty_or_limitation(
    proof: SpreadProof,
) -> None:
    assert proof.product["spread_status"].iloc[0] == "available"
    assert proof.product["spread_uncertainty"].iloc[0] == pytest.approx(13.5)
    assert proof.t1_row.model_probability is not None
    assert proof.t1_row.expected_value is not None


def test_market_price_remains_separate_from_prediction(
    proof: SpreadProof,
) -> None:
    assert proof.positive_result.model_probability == (proof.t1_row.model_probability)
    assert proof.positive_result.line == -1.0
    assert proof.positive_result.american_price == -110
    assert proof.positive_result.model_probability != -110


def test_positive_edge_does_not_automatically_become_a_recommendation(
    proof: SpreadProof,
) -> None:
    assert proof.t1_row.state is CandidateIssuanceState.CANDIDATE
    assert proof.t1_row.expected_value is not None
    assert proof.t1_row.expected_value > 0.0

    assert proof.abstaining_result.result_state is (RecommendedBetResultState.UNAVAILABLE)
    assert proof.abstaining_result.decision_state is (
        RecommendationDecisionState.INSUFFICIENT_EVIDENCE
    )
    assert not proof.abstaining_result.recommendation_eligible


def test_policy_can_recommend_and_abstain(
    proof: SpreadProof,
) -> None:
    freshness = next(
        check for check in proof.positive_result.checks if check.check_id == "quote_freshness"
    )
    assert freshness.status.value == "passed"
    assert freshness.observed_value == pytest.approx(1800.0)
    assert freshness.required_value == pytest.approx(3600.0)

    assert proof.positive_result.result_state is (RecommendedBetResultState.RECOMMENDED)
    assert proof.positive_result.recommendation_eligible
    assert proof.positive_result.allocation.state is (PortfolioAllocationState.ALLOCATED)
    assert proof.positive_result.allocation.allocated_stake is not None
    assert proof.positive_result.allocation.allocated_stake > 0.0

    assert proof.abstaining_result.result_state is (RecommendedBetResultState.UNAVAILABLE)
    assert not proof.abstaining_result.recommendation_eligible


def test_eligible_recommendation_can_receive_zero_allocation(
    proof: SpreadProof,
) -> None:
    result = proof.zero_result
    assert result.decision_state is (RecommendationDecisionState.RECOMMENDATION_ELIGIBLE)
    assert result.recommendation_eligible
    assert result.allocation.state is (PortfolioAllocationState.ZERO_ALLOCATION)
    assert result.allocation.reason is (PortfolioAllocationReason.CORRELATION_CAPACITY_EXHAUSTED)
    assert result.allocation.allocated_stake == pytest.approx(0.0)


def test_later_observation_changes_the_recomputed_outcome(
    proof: SpreadProof,
) -> None:
    assert proof.t1_row.state is CandidateIssuanceState.CANDIDATE
    assert proof.t1_row.reason is (CandidateIssuanceReason.POSITIVE_EXPECTED_VALUE)
    assert proof.t1_row.expected_value is not None
    assert proof.t1_row.expected_value > 0.0

    assert proof.t2_row.fetched_at == T2_FETCHED_AT
    assert proof.t2_row.state is CandidateIssuanceState.NOT_CANDIDATE
    assert proof.t2_row.reason is (CandidateIssuanceReason.EXPECTED_VALUE_NOT_POSITIVE)
    assert proof.t2_row.expected_value is not None
    assert proof.t2_row.expected_value <= 0.0


def test_original_decision_remains_reproducible(
    proof: SpreadProof,
) -> None:
    assert read_candidate_issuance(proof.issuance_t1_path) == proof.issuance_t1
    assert read_candidate_issuance(proof.issuance_t2_path) == proof.issuance_t2

    assert read_recommendation_policy(proof.active_policy_path) == proof.active_policy

    assert (
        read_recommended_bet_evaluation(proof.positive_evaluation_path) == proof.positive_evaluation
    )

    repeated = evaluate_recommendation_issuance(
        policy=proof.active_policy,
        issuance=proof.issuance_t1,
        decision_at=T1,
        bankroll=bankroll(),
        portfolio=empty_portfolio(),
    )
    assert repeated == proof.positive_evaluation


def test_realized_outcome_remains_separate_from_decision_quality(
    proof: SpreadProof,
) -> None:
    assert proof.pre_outcome.decision_status is (DecisionQualityStatus.CONSISTENT)
    assert proof.post_outcome.decision_status is (DecisionQualityStatus.CONSISTENT)
    assert proof.pre_outcome.checks == proof.post_outcome.checks
    assert proof.pre_outcome.realized_outcome is (CandidateOutcome.UNAVAILABLE)
    assert proof.post_outcome.realized_outcome is CandidateOutcome.WIN
    assert proof.pre_outcome.evaluation_id != (proof.post_outcome.evaluation_id)

    assert read_decision_quality_evaluation(proof.pre_outcome_path) == proof.pre_outcome
    assert read_decision_quality_evaluation(proof.post_outcome_path) == proof.post_outcome


def test_canonical_paths_agree_with_embedded_identities(
    proof: SpreadProof,
) -> None:
    assert proof.product_path == weekly_product_artifact_path(
        PRODUCT_ID,
        repo=proof.repo,
    )
    selection = get_current_weekly_product_selection(
        season=SEASON,
        week=WEEK,
        repo=proof.repo,
    )
    assert selection.product_id == PRODUCT_ID
    assert proof.issuance_t1_path == candidate_issuance_path(
        proof.issuance_t1.issuance_id,
        repo=proof.repo,
    )
    assert proof.active_policy_path == recommendation_policy_path(
        proof.active_policy.schema_version,
        proof.active_policy.policy_id,
        repo=proof.repo,
    )
    assert proof.positive_evaluation_path == (
        recommended_bet_evaluation_path(
            proof.positive_evaluation.schema_version,
            proof.positive_evaluation.evaluation_id,
            repo=proof.repo,
        )
    )
    assert recommended_bet_result_path(
        proof.positive_result.schema_version,
        proof.positive_result.result_id,
        repo=proof.repo,
    ).exists()
    assert proof.pre_outcome_path == decision_quality_evaluation_path(
        proof.pre_outcome.schema_version,
        proof.pre_outcome.evaluation_id,
        repo=proof.repo,
    )
    assert proof.post_outcome_path == decision_quality_evaluation_path(
        proof.post_outcome.schema_version,
        proof.post_outcome.evaluation_id,
        repo=proof.repo,
    )
    assert proof.zero_evaluation_path == (
        recommended_bet_evaluation_path(
            proof.zero_evaluation.schema_version,
            proof.zero_evaluation.evaluation_id,
            repo=proof.repo,
        )
    )
