"""Tests for pregame candidate issuance identity and state contracts."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from gridiron_edge.market.candidate_issuance import (
    CandidateIssuanceReason,
    CandidateIssuanceRow,
    CandidateIssuanceState,
    candidate_issuance_id,
    candidate_issuance_row_id,
)


def test_identity_is_deterministic() -> None:
    evaluated = datetime(2026, 9, 1, 12, tzinfo=UTC)
    first = candidate_issuance_id(
        product_id="product-1",
        product_run_id="run-1",
        season="2026-2027",
        week=1,
        evaluated_at=evaluated,
    )
    second = candidate_issuance_id(
        product_id="product-1",
        product_run_id="run-1",
        season="2026-2027",
        week=1,
        evaluated_at=evaluated,
    )
    assert first == second
    assert len(first) == 64


def test_evaluation_time_is_part_of_identity() -> None:
    evaluated = datetime(2026, 9, 1, 12, tzinfo=UTC)
    first = candidate_issuance_id(
        product_id="product-1",
        product_run_id="run-1",
        season="2026-2027",
        week=1,
        evaluated_at=evaluated,
    )
    second = candidate_issuance_id(
        product_id="product-1",
        product_run_id="run-1",
        season="2026-2027",
        week=1,
        evaluated_at=evaluated + timedelta(seconds=1),
    )
    assert first != second


def test_identity_rejects_non_utc_time() -> None:
    with pytest.raises(ValueError, match="UTC"):
        candidate_issuance_id(
            product_id="product-1",
            product_run_id="run-1",
            season="2026-2027",
            week=1,
            evaluated_at=datetime(2026, 9, 1, 12),
        )


def test_candidate_issuance_row_id_is_stable_and_exact() -> None:
    timestamp = datetime(2026, 9, 1, 12, tzinfo=UTC)
    row = CandidateIssuanceRow(
        game_id="2026_01_KC_LAC",
        market="moneyline",
        side="home",
        provider="the_odds_api",
        provider_event_id="event-1",
        sportsbook="draftkings",
        line=None,
        american_price=-110,
        fetched_at=timestamp,
        sportsbook_updated_at=timestamp,
        kickoff=datetime(2026, 9, 1, 20, tzinfo=UTC),
        is_live=False,
        forecast_event_id="forecast-1",
        forecast_run_id="run-1",
        forecast_role="champion",
        forecast_generated_at=timestamp,
        model_name="win_prob",
        model_type="random_forest",
        model_probability=0.55,
        expected_value=0.05,
        state=CandidateIssuanceState.CANDIDATE,
        reason=CandidateIssuanceReason.POSITIVE_EXPECTED_VALUE,
    )
    issuance_id = "a" * 64

    first = candidate_issuance_row_id(issuance_id, row)
    second = candidate_issuance_row_id(issuance_id, row)

    assert first == second
    assert first.startswith(f"{issuance_id}:")
    assert len(first.removeprefix(f"{issuance_id}:")) == 64
    assert candidate_issuance_row_id(issuance_id, replace(row, american_price=-105)) != first


def test_candidate_issuance_row_id_requires_nonempty_issuance_id() -> None:
    timestamp = datetime(2026, 9, 1, 12, tzinfo=UTC)
    row = CandidateIssuanceRow(
        "game",
        "moneyline",
        "home",
        "provider",
        None,
        "book",
        None,
        -110,
        timestamp,
        None,
        datetime(2026, 9, 1, 20, tzinfo=UTC),
        False,
        None,
        None,
        None,
        None,
        None,
        None,
        0.55,
        0.05,
        CandidateIssuanceState.CANDIDATE,
        CandidateIssuanceReason.POSITIVE_EXPECTED_VALUE,
    )
    with pytest.raises(ValueError, match="issuance_id"):
        candidate_issuance_row_id("", row)
