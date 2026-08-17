"""Tests for pregame candidate issuance identity and state contracts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from gridiron_edge.market.candidate_issuance import candidate_issuance_id


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
