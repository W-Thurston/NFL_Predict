"""Tests for unavailable metadata on empty Portfolio responses."""

from __future__ import annotations

import pandas as pd

from gridiron_edge.api.meta import BlockedStatus
from gridiron_edge.api.serializers.portfolio import (
    serialize_portfolio_summary,
    serialize_splits,
)


def test_empty_summary_documents_percentage_fields() -> None:
    result = serialize_portfolio_summary(pd.DataFrame(), 1000.0, {})

    assert result.response_meta is not None
    statuses = result.response_meta.field_status
    win_status = statuses["win_pct"]
    roi_status = statuses["roi_pct"]
    assert isinstance(win_status, BlockedStatus)
    assert isinstance(roi_status, BlockedStatus)
    assert win_status.blocker == "no_settled_bets"
    assert roi_status.blocker == "no_settled_bets"


def test_empty_splits_documents_items() -> None:
    result = serialize_splits(pd.DataFrame(), "market_type")

    assert result.response_meta is not None
    status = result.response_meta.field_status["items"]
    assert isinstance(status, BlockedStatus)
    assert status.blocker == "no_split_data"
