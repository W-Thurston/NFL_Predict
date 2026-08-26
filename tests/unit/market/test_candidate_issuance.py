"""Tests for pregame candidate issuance identity and state contracts."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from gridiron_edge.market.candidate_issuance import (
    CANDIDATE_REFERENCE_DERIVATION_VERSION_V1,
    CandidateIssuanceReason,
    CandidateIssuanceRow,
    CandidateIssuanceState,
    UnsupportedCandidateReferenceVersionError,
    candidate_issuance_id,
    candidate_issuance_row_id,
)


def _canonical_row(**overrides: object) -> CandidateIssuanceRow:
    """Build one canonical candidate row; override any field for focused tests."""
    timestamp = datetime(2026, 9, 1, 12, tzinfo=UTC)
    base: dict[str, object] = {
        "game_id": "2026_01_KC_LAC",
        "market": "moneyline",
        "side": "home",
        "provider": "the_odds_api",
        "provider_event_id": "event-1",
        "sportsbook": "draftkings",
        "line": None,
        "american_price": -110,
        "fetched_at": timestamp,
        "sportsbook_updated_at": timestamp,
        "kickoff": datetime(2026, 9, 1, 20, tzinfo=UTC),
        "is_live": False,
        "forecast_event_id": "forecast-1",
        "forecast_run_id": "run-1",
        "forecast_role": "champion",
        "forecast_generated_at": timestamp,
        "model_name": "win_prob",
        "model_type": "random_forest",
        "model_probability": 0.55,
        "expected_value": 0.05,
        "state": CandidateIssuanceState.CANDIDATE,
        "reason": CandidateIssuanceReason.POSITIVE_EXPECTED_VALUE,
    }
    base.update(overrides)
    return CandidateIssuanceRow(**base)  # pyrefly: ignore [bad-argument-type]


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


def test_candidate_reference_is_stable_and_exact_over_observation_identity() -> None:
    row = _canonical_row()
    issuance_id = "a" * 64
    first = candidate_issuance_row_id(issuance_id, row)
    assert candidate_issuance_row_id(issuance_id, row) == first
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


def test_candidate_reference_distinguishes_source_update_time() -> None:
    row = _canonical_row()
    reference = candidate_issuance_row_id("a" * 64, row)
    changed = replace(row, sportsbook_updated_at=datetime(2026, 9, 1, 13, tzinfo=UTC))
    assert candidate_issuance_row_id("a" * 64, changed) != reference


def test_candidate_reference_distinguishes_live_state() -> None:
    row = _canonical_row()
    reference = candidate_issuance_row_id("a" * 64, row)
    assert candidate_issuance_row_id("a" * 64, replace(row, is_live=True)) != reference


def test_candidate_reference_distinguishes_missing_source_update_time() -> None:
    row = _canonical_row()
    reference = candidate_issuance_row_id("a" * 64, row)
    assert (
        candidate_issuance_row_id("a" * 64, replace(row, sportsbook_updated_at=None)) != reference
    )


def test_default_version_matches_explicit_v1() -> None:
    row = _canonical_row()
    issuance_id = "a" * 64
    assert candidate_issuance_row_id(issuance_id, row) == candidate_issuance_row_id(
        issuance_id, row, version=CANDIDATE_REFERENCE_DERIVATION_VERSION_V1
    )


def test_v1_output_is_pinned_exactly() -> None:
    """Regression-pin the exact v1 algorithm output for one fixed input,
    proving preservation of the historical digest -- not merely
    determinism or field-sensitivity, which the tests above already cover.
    """
    row = _canonical_row()
    reference = candidate_issuance_row_id(
        "a" * 64, row, version=CANDIDATE_REFERENCE_DERIVATION_VERSION_V1
    )
    assert reference == (
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:"
        "c8d47252ecf1558be0a79c405b1d37b11a8424af3f86ebb45f326274c656cc7a"
    )


def test_unrecognized_version_raises_unsupported_error() -> None:
    row = _canonical_row()
    with pytest.raises(UnsupportedCandidateReferenceVersionError, match="not supported"):
        candidate_issuance_row_id("a" * 64, row, version=2)


@pytest.mark.parametrize("invalid_version", [True, 0, -1, 1.0, "1", None])
def test_invalid_version_raises_unsupported_error(invalid_version: object) -> None:
    row = _canonical_row()
    with pytest.raises(UnsupportedCandidateReferenceVersionError, match="invalid"):
        candidate_issuance_row_id(
            "a" * 64,
            row,
            version=invalid_version,  # pyrefly: ignore [bad-argument-type]
        )
