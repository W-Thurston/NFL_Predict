# tests/unit/ingest/odds/test_as_known_composition.py
"""Cutoff-visible retrieval composes with observed history boundaries."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from gridiron_edge.ingest.odds.as_known import as_known_at
from gridiron_edge.ingest.odds.store import QUOTE_COLUMNS
from gridiron_edge.market.history_boundaries import (
    QuoteBoundaryStatus,
    select_quote_history_boundaries,
)


def _ts(y: int, mo: int, d: int, h: int, mi: int) -> pd.Timestamp:
    return pd.Timestamp(datetime(y, mo, d, h, mi, tzinfo=UTC))


def _row(fetched_at, *, is_live=False, commence=None, line=3.5):
    commence = commence if commence is not None else _ts(2026, 9, 7, 20, 0)
    return {
        "fetched_at": fetched_at,
        "provider": "the_odds_api",
        "provider_event_id": "e1",
        "sportsbook": "dk",
        "sportsbook_updated_at": fetched_at,
        "commence_time": commence,
        "is_live": is_live,
        "season": "2026-2027",
        "week": 1,
        "game_id": "G1",
        "game_date": _ts(2026, 9, 7, 0, 0),
        "away_team": "AwayTeam",
        "home_team": "HomeTeam",
        "market": "spread",
        "side": "home",
        "odds": -110.0,
        "line": line,
    }


def _frame(rows):
    return pd.DataFrame(rows, columns=list(QUOTE_COLUMNS))


CUTOFF = _ts(2026, 9, 5, 12, 0)


def test_between_fetch_cutoff_exposes_only_the_earlier_observation():
    df = _frame([_row(_ts(2026, 9, 5, 11, 0), line=3.5), _row(_ts(2026, 9, 6, 11, 0), line=4.0)])
    boundaries = select_quote_history_boundaries(as_known_at(df, CUTOFF))
    assert len(boundaries) == 1
    assert boundaries[0].observation_count == 1
    assert boundaries[0].latest_eligible_pregame.line == 3.5


def test_post_cutoff_fetches_do_not_affect_counts_or_repeated_evidence():
    df = _frame([_row(_ts(2026, 9, 5, 11, 0)), _row(_ts(2026, 9, 6, 11, 0))])
    full = select_quote_history_boundaries(df)[0]
    visible = select_quote_history_boundaries(as_known_at(df, CUTOFF))[0]
    assert full.distinct_fetch_count == 2
    assert full.repeated_observation_evidence_available is True
    assert visible.distinct_fetch_count == 1
    assert visible.repeated_observation_evidence_available is False


def test_post_cutoff_kickoff_conflict_absent_from_cutoff_view():
    df = _frame(
        [
            _row(_ts(2026, 9, 5, 11, 0), commence=_ts(2026, 9, 7, 20, 0)),
            _row(_ts(2026, 9, 6, 11, 0), commence=_ts(2026, 9, 7, 23, 0)),
        ]
    )
    assert select_quote_history_boundaries(df)[0].status is QuoteBoundaryStatus.KICKOFF_CONFLICT
    visible = select_quote_history_boundaries(as_known_at(df, CUTOFF))[0]
    assert visible.status is QuoteBoundaryStatus.AVAILABLE


def test_inclusive_visibility_with_strict_kickoff_eligibility():
    kickoff = _ts(2026, 9, 7, 20, 0)
    df = _frame([_row(kickoff, commence=kickoff)])
    visible = as_known_at(df, kickoff)
    boundary = select_quote_history_boundaries(visible)[0]
    assert len(visible) == 1  # inclusive visibility keeps the at-cutoff row
    assert boundary.status is QuoteBoundaryStatus.NO_ELIGIBLE_PREGAME_OBSERVATION
    assert boundary.latest_eligible_pregame is None  # strict kickoff excludes it


def test_visible_live_observation_is_kept_but_not_selected_as_latest_eligible():
    df = _frame(
        [
            _row(_ts(2026, 9, 5, 10, 0), is_live=False, line=3.5),
            _row(_ts(2026, 9, 5, 11, 0), is_live=True, line=9.9),
        ]
    )
    visible = as_known_at(df, CUTOFF)
    boundary = select_quote_history_boundaries(visible)[0]
    assert len(visible) == 2
    assert boundary.observation_count == 2
    assert boundary.latest_eligible_pregame.line == 3.5
    assert boundary.latest_eligible_pregame.is_live is False
