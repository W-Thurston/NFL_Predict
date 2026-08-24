# tests/unit/ingest/odds/test_as_known.py
"""Cutoff-visible quote evidence retrieval: as_known_at."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pandas as pd
import pytest

from gridiron_edge.ingest.odds.as_known import CutoffError, as_known_at
from gridiron_edge.ingest.odds.store import QUOTE_COLUMNS


def _ts(y: int, mo: int, d: int, h: int, mi: int) -> pd.Timestamp:
    return pd.Timestamp(datetime(y, mo, d, h, mi, tzinfo=UTC))


def _row(
    fetched_at,
    *,
    game_id="G1",
    market="spread",
    side="home",
    sportsbook="dk",
    is_live=False,
    commence=None,
    odds=-110.0,
    line=3.5,
):
    commence = commence if commence is not None else _ts(2026, 9, 7, 20, 0)
    return {
        "fetched_at": fetched_at,
        "provider": "the_odds_api",
        "provider_event_id": "e1",
        "sportsbook": sportsbook,
        "sportsbook_updated_at": fetched_at,
        "commence_time": commence,
        "is_live": is_live,
        "season": "2026-2027",
        "week": 1,
        "game_id": game_id,
        "game_date": _ts(2026, 9, 7, 0, 0),
        "away_team": "AwayTeam",
        "home_team": "HomeTeam",
        "market": market,
        "side": side,
        "odds": odds,
        "line": line,
    }


def _frame(rows):
    return pd.DataFrame(rows, columns=list(QUOTE_COLUMNS))


CUTOFF = _ts(2026, 9, 5, 12, 0)


def test_before_cutoff_is_included():
    assert len(as_known_at(_frame([_row(_ts(2026, 9, 5, 11, 0))]), CUTOFF)) == 1


def test_exactly_at_cutoff_is_included():
    assert len(as_known_at(_frame([_row(CUTOFF)]), CUTOFF)) == 1


def test_after_cutoff_is_excluded():
    assert len(as_known_at(_frame([_row(_ts(2026, 9, 5, 13, 0))]), CUTOFF)) == 0


def test_empty_input_returns_canonical_empty_frame():
    out = as_known_at(_frame([]), CUTOFF)
    assert len(out) == 0
    assert list(out.columns) == list(QUOTE_COLUMNS)


def test_cutoff_before_all_returns_canonical_empty_frame():
    df = _frame([_row(_ts(2026, 9, 6, 10, 0)), _row(_ts(2026, 9, 7, 10, 0))])
    out = as_known_at(df, _ts(2026, 9, 1, 0, 0))
    assert len(out) == 0
    assert list(out.columns) == list(QUOTE_COLUMNS)


def test_input_frame_is_not_mutated():
    df = _frame([_row(_ts(2026, 9, 5, 11, 0)), _row(_ts(2026, 9, 5, 13, 0))])
    before = df.copy(deep=True)
    as_known_at(df, CUTOFF)
    assert df.equals(before)


def test_output_preserves_canonical_schema():
    out = as_known_at(_frame([_row(_ts(2026, 9, 5, 11, 0))]), CUTOFF)
    assert list(out.columns) == list(QUOTE_COLUMNS)


def test_output_ordering_is_deterministic():
    rows = [
        _row(_ts(2026, 9, 5, 11, 0), game_id="G2", side="away"),
        _row(_ts(2026, 9, 5, 11, 0), game_id="G1", side="home"),
        _row(_ts(2026, 9, 5, 11, 0), game_id="G1", market="total", side="over"),
    ]
    a = as_known_at(_frame(rows), CUTOFF).reset_index(drop=True)
    b = as_known_at(_frame(list(reversed(rows))), CUTOFF).reset_index(drop=True)
    assert a.equals(b)


def test_naive_cutoff_is_rejected():
    with pytest.raises(CutoffError):
        as_known_at(_frame([_row(_ts(2026, 9, 5, 11, 0))]), datetime(2026, 9, 5, 12, 0))


def test_non_utc_cutoff_is_rejected():
    non_utc = datetime(2026, 9, 5, 12, 0, tzinfo=timezone(timedelta(hours=-6)))
    with pytest.raises(CutoffError):
        as_known_at(_frame([_row(_ts(2026, 9, 5, 11, 0))]), non_utc)


def test_identities_appearing_only_after_cutoff_are_absent():
    rows = [
        _row(_ts(2026, 9, 5, 11, 0), game_id="G1"),
        _row(_ts(2026, 9, 5, 13, 0), game_id="G_LATE"),
    ]
    assert set(as_known_at(_frame(rows), CUTOFF)["game_id"]) == {"G1"}


def test_live_observation_before_cutoff_remains_visible():
    out = as_known_at(_frame([_row(_ts(2026, 9, 5, 11, 0), is_live=True)]), CUTOFF)
    assert len(out) == 1
    assert bool(out.iloc[0]["is_live"]) is True


def test_incomplete_quote_schema_is_rejected():
    observations = pd.DataFrame({"fetched_at": [CUTOFF]})
    with pytest.raises(ValueError, match="Invalid quote schema"):
        as_known_at(observations, CUTOFF)


def test_naive_observation_timestamp_is_rejected():
    df = _frame([_row(_ts(2026, 9, 5, 11, 0))])
    df["fetched_at"] = [datetime(2026, 9, 5, 11, 0)]
    with pytest.raises(ValueError, match="timezone-aware UTC"):
        as_known_at(df, CUTOFF)
