# src/gridiron_edge/market/candidate_outcome.py
"""Realized-outcome grading for one exact issued market side.

This module owns exactly one operation: given an already-resolved
candidate row and its exact final scores, grade the realized result.
It does not load games, search by game_id, read files, resolve
recommendation results, calculate CLV, calculate return, or evaluate
policy. Callers own resolving which scores belong to which game.
"""

from __future__ import annotations

from enum import StrEnum
import math

from gridiron_edge.market.candidate_issuance import CandidateIssuanceRow


class CandidateOutcome(StrEnum):
    """Realized result of one exact issued market side."""

    WIN = "win"
    LOSS = "loss"
    PUSH = "push"
    UNAVAILABLE = "unavailable"
    CONFLICT = "conflict"


def grade_candidate_outcome(
    row: CandidateIssuanceRow,
    scores: tuple[float, float] | None,
) -> CandidateOutcome:
    """Grade one issued market side from an already-resolved row and scores.

    Args:
        row: The exact candidate issuance row being graded. Only its
            market, side, and line are consulted -- the row is not
            re-validated or re-resolved here.
        scores: The exact final (away, home) scores for this row's game,
            or None if the game has not yet completed.

    Returns:
        WIN, LOSS, or PUSH when scores are available and finite;
        UNAVAILABLE when no scores are supplied; CONFLICT when supplied
        scores are non-finite (a malformed or contradictory game record).

    Raises:
        ValueError: If row.market or the row.market/row.side pair is
            unsupported. This check runs before scores are inspected --
            an invalid grading contract is rejected regardless of
            whether outcome evidence happens to be available yet.
    """
    valid_sides = {
        "moneyline": {"away", "home"},
        "spread": {"away", "home"},
        "total": {"over", "under"},
    }
    if row.market not in valid_sides:
        raise ValueError(f"Unsupported candidate market: {row.market!r}.")
    if row.side not in valid_sides[row.market]:
        raise ValueError(f"Unsupported candidate market-side pair: {row.market}/{row.side}.")
    if scores is None:
        return CandidateOutcome.UNAVAILABLE
    away, home = scores
    if not math.isfinite(away) or not math.isfinite(home):
        return CandidateOutcome.CONFLICT
    if row.market == "moneyline":
        selected, other = (home, away) if row.side == "home" else (away, home)
    elif row.market == "spread":
        if row.line is None:
            return CandidateOutcome.UNAVAILABLE
        selected, other = (home + row.line, away) if row.side == "home" else (away + row.line, home)
    else:
        if row.line is None:
            return CandidateOutcome.UNAVAILABLE
        total = home + away
        selected, other = (total, row.line) if row.side == "over" else (row.line, total)
    if selected == other:
        return CandidateOutcome.PUSH
    return CandidateOutcome.WIN if selected > other else CandidateOutcome.LOSS
