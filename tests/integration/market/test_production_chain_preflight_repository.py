"""Real-repository readiness assessment for Market Unit 26."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from gridiron_edge.market.production_chain_preflight import (
    ProductionMarketFamily,
    ProofComponentState,
    assess_production_chain_preflight,
)


def test_current_week_one_evidence_is_classified_truthfully() -> None:
    repo_root = Path(__file__).resolve().parents[3]

    result = assess_production_chain_preflight(
        repo=repo_root,
        season="2026-2027",
        week=1,
        assessed_at=datetime(2026, 8, 17, 18, 57, tzinfo=UTC),
    )

    expected_markets = (
        ProductionMarketFamily.MONEYLINE,
        ProductionMarketFamily.SPREAD,
        ProductionMarketFamily.TOTAL,
    )
    families = (result.moneyline, result.spread, result.total)
    assert tuple(family.market for family in families) == expected_markets

    for family in families:
        assert family.component("selected_product").state is ProofComponentState.AVAILABLE
        assert family.component("forecast_provenance").state is ProofComponentState.AVAILABLE
        assert family.component("quote_snapshot").state is ProofComponentState.AVAILABLE
        history = family.component("repeated_quote_history")
        assert history.state is ProofComponentState.AVAILABLE
        assert history.distinct_timestamp_count == 2
        assert family.component("selected_collection_plan").state is ProofComponentState.AVAILABLE
        assert (
            family.component("collection_execution").state is ProofComponentState.NOT_YET_ELIGIBLE
        )
        candidate = family.component("candidate_issuance")
        assert candidate.state is ProofComponentState.AVAILABLE
        assert candidate.evidence_ids == (
            "278d60da4e2dc089ff7eb973620f49050f83de336034cbff0c8c1a097401ccff",
        )
        assert candidate.observation_count == 1680
        policy = family.component("recommendation_policy")
        assert policy.state is ProofComponentState.AVAILABLE
        assert policy.evidence_ids == (
            "9e2cc3363656366eae76ec0935f01ff201ce9c9784e2736936fd0af9ab0ab024",
        )
        recommendation = family.component("recommendation_result")
        assert recommendation.state is ProofComponentState.AVAILABLE
        assert recommendation.evidence_ids == (
            "8301fb74e1eaa10437376ff3b616aaa1efc3477944d1a8da0df94abd55de073c",
            "9e2cc3363656366eae76ec0935f01ff201ce9c9784e2736936fd0af9ab0ab024",
        )
        assert recommendation.observation_count == 698
        assert family.component("backend_serialization").state is ProofComponentState.AVAILABLE
        assert family.component("frontend_presentation").state is ProofComponentState.AVAILABLE
        assert family.component("completed_outcome").state is ProofComponentState.NOT_YET_ELIGIBLE
        assert family.component("market_closeout").state is ProofComponentState.NOT_YET_ELIGIBLE
        assert family.component("clv").state is ProofComponentState.NOT_YET_ELIGIBLE
        assert (
            family.component("realized_performance").state is ProofComponentState.NOT_YET_ELIGIBLE
        )

    assert not result.all_families_proven
