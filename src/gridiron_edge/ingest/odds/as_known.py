# src/gridiron_edge/ingest/odds/as_known.py

"""As-known-at-cutoff visibility over the quote observation ledger.

Given a loaded quote-ledger frame and a decision cutoff, return the cutoff-visible
evidence set -- every observation whose system-known time (``fetched_at``) is at or
before the cutoff -- before any identity, count, selection, or eligibility is
derived by a consumer.

System-known visibility is governed by ``fetched_at`` and is inclusive
(``fetched_at <= cutoff``). ``sportsbook_updated_at`` is source-update metadata and
``commence_time`` is the event-start boundary; neither governs system-known
visibility. Visibility is a separate concern from pregame eligibility
(``is_live is False and fetched_at < commence_time``): this operation owns only
visibility, and downstream consumers apply their own eligibility or selection over
the cutoff-visible frame. The two predicates are never fused into a single
``min(cutoff, kickoff)`` bound.

The input is the canonical quote-observation contract. Rows are validated and
normalized through ``validate_quote_rows`` before filtering, so this operation
enforces -- rather than weakens -- the same schema, UTC timestamp, numeric, and
live-state guarantees as every other quote consumer. Naive or non-UTC observation
timestamps are rejected rather than coerced.

Contract:
- The cutoff must be a timezone-aware UTC instant; naive or non-UTC cutoffs are
  rejected with :class:`CutoffError`.
- The input frame is never mutated; the result is a fresh copy.
- Ordering is the canonical observation ordering.
- An empty result is a first-class outcome returned as the canonical empty quote
  frame; this operation does not invent identity-specific "no observation by
  cutoff" statuses, because identities appearing only after the cutoff are, by
  definition, not visible here.
- Live rows known by the cutoff remain visible (merely ineligible for a later
  pregame selection); they are never silently filtered from the evidence layer.

This is a read-only operation. It never mutates the input, the ledger, or the
current snapshot.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
from pandas import DataFrame

from gridiron_edge.ingest.odds.store import (
    OBSERVATION_SORT_COLUMNS,
    empty_quote_frame,
    validate_quote_rows,
)


class CutoffError(ValueError):
    """Raised when a decision cutoff is not a timezone-aware UTC instant."""


def _require_utc_cutoff(cutoff: datetime) -> None:
    """Validate that ``cutoff`` is a timezone-aware UTC datetime.

    Rejects naive datetimes and tz-aware datetimes whose offset is not UTC. An
    explicit UTC cutoff is required rather than silently coercing an ambiguous
    value.
    """
    if not isinstance(cutoff, datetime):
        raise CutoffError("cutoff must be a datetime.")
    offset = cutoff.utcoffset()
    if offset is None:
        raise CutoffError("cutoff must be timezone-aware; naive datetimes are rejected.")
    if offset != timedelta(0):
        raise CutoffError("cutoff must use UTC.")


def as_known_at(observations: DataFrame, cutoff: datetime) -> DataFrame:
    """Return canonical quote observations visible by an inclusive UTC cutoff.

    Validates and normalizes ``observations`` through the canonical quote contract,
    then returns the fresh, deterministically ordered subset whose ``fetched_at`` is
    at or before ``cutoff`` (inclusive). An empty result is the canonical empty quote
    frame -- a first-class outcome, not an error. The input is never mutated.

    Parameters
    ----------
    observations:
        A canonical quote-ledger frame (for example, from ``load_odds_ledger``).
    cutoff:
        A timezone-aware UTC decision cutoff. Naive or non-UTC values are rejected
        with :class:`CutoffError`.
    """
    _require_utc_cutoff(cutoff)
    rows = validate_quote_rows(observations)
    if rows.empty:
        return empty_quote_frame()
    visible = rows.loc[rows["fetched_at"].le(pd.Timestamp(cutoff)), :].copy()
    if visible.empty:
        return empty_quote_frame()
    return visible.sort_values(
        list(OBSERVATION_SORT_COLUMNS),
        kind="stable",
        na_position="first",
    ).reset_index(drop=True)
