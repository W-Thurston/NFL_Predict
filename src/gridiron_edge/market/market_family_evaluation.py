"""Pure empirical evaluation of issued candidates by market family.

The evaluator reports evidence and descriptive metrics. It does not define
qualification, recommendation, staking, exposure, or policy thresholds.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from itertools import pairwise
import math
from statistics import median
from typing import Any, cast

import pandas as pd
from pandas import DataFrame, Series

from gridiron_edge.evaluation.metrics import brier_score, log_loss
from gridiron_edge.market.candidate_issuance import (
    CandidateIssuance,
    CandidateIssuanceRow,
    CandidateIssuanceState,
    candidate_issuance_row_id,
)
from gridiron_edge.market.history_boundaries import QuoteHistoryBoundary
from gridiron_edge.market.market_closeout import (
    MarketCloseoutReferenceKind,
    MarketCloseoutResult,
    MarketCloseoutStatus,
)

_MARKETS = ("moneyline", "spread", "total")
_GAME_COLUMNS = frozenset({"GAME_ID", "AWAY_SCORE", "HOME_SCORE"})


class EvaluationEvidenceStatus(StrEnum):
    """Availability of one empirical evidence calculation."""

    AVAILABLE = "available"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    UNAVAILABLE = "unavailable"
    CONFLICTING_EVIDENCE = "conflicting_evidence"


class CandidateOutcome(StrEnum):
    """Realized result of one exact issued market side."""

    WIN = "win"
    LOSS = "loss"
    PUSH = "push"
    UNAVAILABLE = "unavailable"
    CONFLICT = "conflict"


@dataclass(frozen=True, slots=True)
class MetricEstimate:
    """One descriptive metric with explicit evidence availability."""

    status: EvaluationEvidenceStatus
    sample_size: int
    value: float | None
    reason: str | None


@dataclass(frozen=True, slots=True)
class MarketFamilyCoverage:
    """Coverage counts for one independently evaluated market family."""

    market: str
    issued_count: int
    candidate_count: int
    not_candidate_count: int
    unavailable_count: int
    probability_available_count: int
    expected_value_available_count: int
    outcome_available_count: int
    outcome_push_count: int
    outcome_unavailable_count: int
    outcome_conflict_count: int
    closeout_available_count: int
    clv_available_count: int
    history_depth_available_count: int
    quote_age_available_count: int
    quote_age_conflict_count: int
    return_available_count: int
    return_unavailable_count: int
    return_conflict_count: int
    closeout_status_counts: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class ReliabilityEvaluation:
    """Binary candidate reliability with outcome and sample accounting."""

    candidate_count: int
    evaluable_count: int
    win_count: int
    loss_count: int
    push_count: int
    unavailable_count: int
    conflict_count: int
    brier: MetricEstimate
    log_loss: MetricEstimate
    accuracy: MetricEstimate


@dataclass(frozen=True, slots=True)
class QuoteAgeEvaluation:
    """Descriptive quote-age evidence for issued candidates."""

    status: EvaluationEvidenceStatus
    candidate_count: int
    available_count: int
    conflict_count: int
    minimum_seconds: float | None
    median_seconds: float | None
    maximum_seconds: float | None
    reason: str | None


@dataclass(frozen=True, slots=True)
class ObservationDepthEvaluation:
    """Historical observation-depth evidence for issued candidates."""

    status: EvaluationEvidenceStatus
    candidate_count: int
    available_count: int
    unavailable_count: int
    conflict_count: int
    minimum_observation_count: int | None
    median_observation_count: float | None
    maximum_observation_count: int | None
    minimum_distinct_fetch_count: int | None
    median_distinct_fetch_count: float | None
    maximum_distinct_fetch_count: int | None
    reason: str | None


@dataclass(frozen=True, slots=True)
class CategoricalCohortEvaluation:
    """Descriptive counts for one categorical candidate cohort."""

    cohort_kind: str
    cohort_key: str
    candidate_count: int
    outcome_evaluable_count: int
    closeout_available_count: int
    clv_available_count: int


@dataclass(frozen=True, slots=True)
class NumericCohortEvaluation:
    """One neutral empirically bounded numeric cohort."""

    cohort_kind: str
    market: str
    cohort_index: int
    lower_bound: float
    upper_bound: float
    candidate_count: int
    outcome_evaluable_count: int
    win_count: int
    loss_count: int
    push_count: int
    closeout_available_count: int
    clv_available_count: int
    mean_expected_value: float | None
    mean_clv: float | None
    mean_realized_return: float | None
    aggregate_realized_return: float | None


@dataclass(frozen=True, slots=True)
class NumericCohortSet:
    """Empirical cohorts or an explicit unavailable evidence state."""

    cohort_kind: str
    status: EvaluationEvidenceStatus
    candidate_count: int
    value_available_count: int
    distinct_value_count: int
    conflict_count: int
    cohorts: tuple[NumericCohortEvaluation, ...]
    reason: str | None


@dataclass(frozen=True, slots=True)
class RealizedReturnEvaluation:
    """Settled-wager return evidence uniquely attributed to issued candidates."""

    status: EvaluationEvidenceStatus
    candidate_count: int
    available_count: int
    unavailable_count: int
    conflict_count: int
    total_stake: float | None
    total_pnl: float | None
    mean_per_wager_return: float | None
    aggregate_return: float | None
    reason: str | None


@dataclass(frozen=True, slots=True)
class MarketFamilyEvaluation:
    """Empirical evidence report for one market family."""

    market: str
    coverage: MarketFamilyCoverage
    reliability: ReliabilityEvaluation
    quote_age: QuoteAgeEvaluation
    observation_depth: ObservationDepthEvaluation
    sportsbook_cohorts: tuple[CategoricalCohortEvaluation, ...]
    market_side_cohorts: tuple[CategoricalCohortEvaluation, ...]
    expected_value_cohorts: NumericCohortSet
    clv_cohorts: NumericCohortSet
    quote_age_cohorts: NumericCohortSet
    observation_count_cohorts: NumericCohortSet
    distinct_fetch_count_cohorts: NumericCohortSet
    realized_return: RealizedReturnEvaluation


@dataclass(frozen=True, slots=True)
class EmpiricalMarketFamilyEvaluation:
    """Separate empirical reports for Moneyline, Spread, and Total."""

    moneyline: MarketFamilyEvaluation
    spread: MarketFamilyEvaluation
    total: MarketFamilyEvaluation


@dataclass(frozen=True, slots=True)
class _EvaluatedCandidate:
    row: CandidateIssuanceRow
    outcome: CandidateOutcome
    closeout: MarketCloseoutResult | None
    closeout_join_conflict: bool
    history: QuoteHistoryBoundary | None
    history_join_conflict: bool
    quote_age_seconds: float | None
    quote_age_conflict: bool
    return_stake: float | None
    return_pnl: float | None
    realized_return: float | None
    return_conflict: bool


def evaluate_market_families(
    *,
    issuance: CandidateIssuance,
    closeouts: Sequence[MarketCloseoutResult],
    games: DataFrame,
    history_boundaries: Sequence[QuoteHistoryBoundary],
    wagers: DataFrame | None = None,
) -> EmpiricalMarketFamilyEvaluation:
    """Evaluate immutable issuance evidence independently by market family."""
    outcomes = _validated_game_outcomes(games)
    wager_rows = _validated_wagers(wagers)
    evaluations = tuple(
        _evaluate_row(
            issuance,
            row,
            closeouts=closeouts,
            outcomes=outcomes,
            history_boundaries=history_boundaries,
            wagers=wager_rows,
        )
        for row in issuance.rows
    )
    families = {
        market: _evaluate_family(
            market,
            issuance.rows,
            tuple(item for item in evaluations if item.row.market == market),
        )
        for market in _MARKETS
    }
    return EmpiricalMarketFamilyEvaluation(
        moneyline=families["moneyline"],
        spread=families["spread"],
        total=families["total"],
    )


def _validated_game_outcomes(games: DataFrame) -> dict[str, tuple[float, float] | None]:
    missing = sorted(_GAME_COLUMNS - set(games.columns))
    if missing:
        raise ValueError("Completed games are missing columns: " + ", ".join(missing))
    if games["GAME_ID"].astype(str).duplicated().any():
        raise ValueError("Completed games contain duplicate GAME_ID values.")
    outcomes: dict[str, tuple[float, float] | None] = {}
    for row in games.loc[:, ["GAME_ID", "AWAY_SCORE", "HOME_SCORE"]].itertuples(index=False):
        game_id = str(row.GAME_ID)
        if pd.isna(cast(Any, row.AWAY_SCORE)) or pd.isna(cast(Any, row.HOME_SCORE)):
            outcomes[game_id] = None
            continue
        try:
            away = float(cast(float | int | str, row.AWAY_SCORE))
            home = float(cast(float | int | str, row.HOME_SCORE))
        except (TypeError, ValueError):
            outcomes[game_id] = (math.nan, math.nan)
            continue
        outcomes[game_id] = (
            (away, home) if math.isfinite(away) and math.isfinite(home) else (math.nan, math.nan)
        )
    return outcomes


def _evaluate_row(
    issuance: CandidateIssuance,
    row: CandidateIssuanceRow,
    *,
    closeouts: Sequence[MarketCloseoutResult],
    outcomes: dict[str, tuple[float, float] | None],
    history_boundaries: Sequence[QuoteHistoryBoundary],
    wagers: DataFrame,
) -> _EvaluatedCandidate:
    closeout_matches = [item for item in closeouts if _closeout_matches(issuance, row, item)]
    history_matches = [item for item in history_boundaries if _history_matches(row, item)]
    age = (issuance.evaluated_at - row.fetched_at).total_seconds()
    return_stake, return_pnl, realized_return, return_conflict = _wager_return_for_row(row, wagers)
    return _EvaluatedCandidate(
        row=row,
        outcome=_grade_outcome(row, outcomes.get(row.game_id)),
        closeout=closeout_matches[0] if len(closeout_matches) == 1 else None,
        closeout_join_conflict=len(closeout_matches) > 1,
        history=history_matches[0] if len(history_matches) == 1 else None,
        history_join_conflict=len(history_matches) > 1,
        quote_age_seconds=age if age >= 0 else None,
        quote_age_conflict=age < 0,
        return_stake=return_stake,
        return_pnl=return_pnl,
        realized_return=realized_return,
        return_conflict=return_conflict,
    )


def _closeout_matches(
    issuance: CandidateIssuance,
    row: CandidateIssuanceRow,
    result: MarketCloseoutResult,
) -> bool:
    reference = result.reference
    return (
        reference.reference_kind is MarketCloseoutReferenceKind.CANDIDATE_ISSUANCE
        and reference.reference_id == candidate_issuance_row_id(issuance.issuance_id, row)
        and reference.reference_kickoff == row.kickoff
    )


def _history_matches(row: CandidateIssuanceRow, boundary: QuoteHistoryBoundary) -> bool:
    return all(
        (
            boundary.provider == row.provider,
            boundary.provider_event_id == row.provider_event_id,
            boundary.sportsbook == row.sportsbook,
            boundary.game_id == row.game_id,
            boundary.market == row.market,
            boundary.side == row.side,
        )
    )


def _grade_outcome(
    row: CandidateIssuanceRow,
    scores: tuple[float, float] | None,
) -> CandidateOutcome:
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


def _evaluate_family(
    market: str,
    all_rows: Sequence[CandidateIssuanceRow],
    evaluations: Sequence[_EvaluatedCandidate],
) -> MarketFamilyEvaluation:
    family_rows = tuple(row for row in all_rows if row.market == market)
    candidates = tuple(
        item for item in evaluations if item.row.state is CandidateIssuanceState.CANDIDATE
    )
    coverage = _coverage(market, family_rows, candidates)
    return MarketFamilyEvaluation(
        market=market,
        coverage=coverage,
        reliability=_reliability(candidates),
        quote_age=_quote_age(candidates),
        observation_depth=_observation_depth(candidates),
        sportsbook_cohorts=_categorical_cohorts(candidates, kind="sportsbook"),
        market_side_cohorts=_categorical_cohorts(candidates, kind="market_side"),
        expected_value_cohorts=_numeric_cohort_set(
            candidates, market=market, kind="expected_value"
        ),
        clv_cohorts=_numeric_cohort_set(candidates, market=market, kind="clv"),
        quote_age_cohorts=_numeric_cohort_set(candidates, market=market, kind="quote_age_seconds"),
        observation_count_cohorts=_numeric_cohort_set(
            candidates, market=market, kind="observation_count"
        ),
        distinct_fetch_count_cohorts=_numeric_cohort_set(
            candidates, market=market, kind="distinct_fetch_count"
        ),
        realized_return=_realized_return(candidates),
    )


def _validated_wagers(wagers: DataFrame | None) -> DataFrame:
    """Copy and validate the narrow immutable wager-return input contract."""
    required = {
        "bet_id",
        "game_id",
        "market_type",
        "side",
        "reference_provider",
        "reference_provider_event_id",
        "reference_sportsbook",
        "reference_market_fetched_at",
        "reference_sportsbook_updated_at",
        "reference_commence_time",
        "reference_american_odds",
        "reference_line",
        "status",
        "stake",
        "pnl",
    }
    if wagers is None:
        return DataFrame(columns=sorted(required))
    missing = sorted(required - set(wagers.columns))
    if missing:
        raise ValueError("Recorded wagers are missing columns: " + ", ".join(missing))
    rows = wagers.loc[:, sorted(required)].copy(deep=True)
    if rows["bet_id"].isna().any() or rows["bet_id"].astype(str).str.strip().eq("").any():
        raise ValueError("Recorded wager bet_id values must be nonempty.")
    if rows["bet_id"].astype(str).duplicated().any():
        raise ValueError("Recorded wagers contain duplicate bet_id values.")
    return rows


def _wager_return_for_row(
    row: CandidateIssuanceRow,
    wagers: DataFrame,
) -> tuple[float | None, float | None, float | None, bool]:
    """Return exact settled stake, PnL, return, and conflict state."""
    result: tuple[float | None, float | None, float | None, bool]
    if wagers.empty:
        result = (None, None, None, False)
    else:
        matches = cast(
            DataFrame,
            wagers.loc[
                wagers["reference_provider"].eq(row.provider)
                & _nullable_identity_mask(
                    wagers["reference_provider_event_id"],
                    row.provider_event_id,
                )
                & _nullable_identity_mask(
                    wagers["reference_sportsbook"],
                    row.sportsbook,
                )
                & wagers["game_id"].astype(str).eq(row.game_id)
                & wagers["market_type"].astype(str).eq(row.market)
                & wagers["side"].astype(str).eq(row.side)
                & _datetime_identity_mask(
                    wagers["reference_market_fetched_at"],
                    row.fetched_at,
                )
                & _datetime_identity_mask(
                    wagers["reference_sportsbook_updated_at"],
                    row.sportsbook_updated_at,
                )
                & _datetime_identity_mask(
                    wagers["reference_commence_time"],
                    row.kickoff,
                )
                & _numeric_identity_mask(
                    wagers["reference_american_odds"],
                    row.american_price,
                )
                & _numeric_identity_mask(
                    wagers["reference_line"],
                    row.line,
                ),
                :,
            ],
        )
        if len(matches) > 1:
            result = (None, None, None, True)
        elif matches.empty:
            result = (None, None, None, False)
        else:
            match = cast(Series, matches.iloc[0])
            if str(match["status"]) not in {"won", "lost", "push"}:
                result = (None, None, None, False)
            else:
                try:
                    stake = float(cast(float | int | str, match["stake"]))
                    pnl = float(cast(float | int | str, match["pnl"]))
                except (TypeError, ValueError):
                    result = (None, None, None, True)
                else:
                    if not math.isfinite(stake) or stake <= 0 or not math.isfinite(pnl):
                        result = (None, None, None, True)
                    else:
                        result = (stake, pnl, pnl / stake, False)
    return result


def _nullable_identity_mask(values: Series, expected: str | None) -> Series:
    """Return exact null-aware text identity equality."""
    return values.isna() if expected is None else values.astype("string").eq(expected)


def _datetime_identity_mask(values: Series, expected: object) -> Series:
    """Return exact UTC instant equality without inventing missing evidence."""
    if expected is None:
        return values.isna()
    normalized = pd.to_datetime(values, utc=True, errors="coerce")
    return normalized.eq(pd.Timestamp(cast(Any, expected)))


def _numeric_identity_mask(values: Series, expected: float | int | None) -> Series:
    """Return exact null-aware numeric identity equality."""
    if expected is None:
        return values.isna()
    numeric = pd.to_numeric(values, errors="coerce")
    return numeric.eq(float(expected))


def _realized_return(candidates: Sequence[_EvaluatedCandidate]) -> RealizedReturnEvaluation:
    """Summarize uniquely attributed settled return evidence."""
    available = [
        item
        for item in candidates
        if item.realized_return is not None
        and item.return_stake is not None
        and item.return_pnl is not None
        and not item.return_conflict
    ]
    conflicts = sum(item.return_conflict for item in candidates)
    unavailable = len(candidates) - len(available) - conflicts
    if not candidates:
        status = EvaluationEvidenceStatus.UNAVAILABLE
        reason = "no_candidates"
    elif available:
        status = EvaluationEvidenceStatus.AVAILABLE
        reason = None
    elif conflicts:
        status = EvaluationEvidenceStatus.CONFLICTING_EVIDENCE
        reason = "all_return_evidence_conflicting"
    else:
        status = EvaluationEvidenceStatus.UNAVAILABLE
        reason = "no_settled_wager_evidence"
    stakes = [cast(float, item.return_stake) for item in available]
    pnls = [cast(float, item.return_pnl) for item in available]
    returns = [cast(float, item.realized_return) for item in available]
    total_stake = None if not stakes else float(sum(stakes))
    total_pnl = None if not pnls else float(sum(pnls))
    return RealizedReturnEvaluation(
        status=status,
        candidate_count=len(candidates),
        available_count=len(available),
        unavailable_count=unavailable,
        conflict_count=conflicts,
        total_stake=total_stake,
        total_pnl=total_pnl,
        mean_per_wager_return=(None if not returns else float(sum(returns) / len(returns))),
        aggregate_return=(
            None if total_stake is None or total_pnl is None else float(total_pnl / total_stake)
        ),
        reason=reason,
    )


def _numeric_cohort_set(
    candidates: Sequence[_EvaluatedCandidate],
    *,
    market: str,
    kind: str,
    requested_partitions: int = 4,
) -> NumericCohortSet:
    """Build neutral cohorts from observed quantiles without policy cutoffs."""
    valued: list[tuple[float, _EvaluatedCandidate]] = []
    conflicts = 0
    for item in candidates:
        value, conflict = _cohort_value(item, kind=kind, market=market)
        if conflict:
            conflicts += 1
        elif value is not None:
            valued.append((value, item))
    values = [value for value, _ in valued]
    distinct = len(set(values))
    if not values:
        status = (
            EvaluationEvidenceStatus.CONFLICTING_EVIDENCE
            if conflicts
            else EvaluationEvidenceStatus.UNAVAILABLE
        )
        reason = "all_values_conflicting" if conflicts else "no_available_values"
        cohorts: tuple[NumericCohortEvaluation, ...] = ()
    elif distinct < 2:
        status = EvaluationEvidenceStatus.INSUFFICIENT_EVIDENCE
        reason = "fewer_than_two_distinct_values"
        cohorts = ()
    else:
        partitions = min(requested_partitions, distinct)
        probabilities = [index / partitions for index in range(partitions + 1)]
        series = Series(values, dtype="float64")
        boundaries = sorted({float(series.quantile(value)) for value in probabilities})
        grouped = _assign_empirical_groups(valued, boundaries)
        if len(grouped) < 2 or any(not rows for _, _, rows in grouped):
            status = EvaluationEvidenceStatus.INSUFFICIENT_EVIDENCE
            reason = "empirical_partitions_not_distinct"
            cohorts = ()
        else:
            status = EvaluationEvidenceStatus.AVAILABLE
            reason = None
            cohorts = tuple(
                _numeric_cohort(
                    kind=kind,
                    market=market,
                    index=index,
                    lower=lower,
                    upper=upper,
                    items=rows,
                )
                for index, (lower, upper, rows) in enumerate(grouped, start=1)
            )
    return NumericCohortSet(
        cohort_kind=kind,
        status=status,
        candidate_count=len(candidates),
        value_available_count=len(values),
        distinct_value_count=distinct,
        conflict_count=conflicts,
        cohorts=cohorts,
        reason=reason,
    )


def _cohort_value(
    item: _EvaluatedCandidate,
    *,
    kind: str,
    market: str,
) -> tuple[float | None, bool]:
    """Return one finite immutable evidence value and conflict diagnostic."""
    value: float | int | None
    conflict = False
    if kind == "expected_value":
        value = item.row.expected_value
    elif kind == "clv":
        closeout = item.closeout
        expected_kind = {
            "moneyline": "moneyline_price",
            "spread": "spread_points",
            "total": "total_points",
        }[market]
        if closeout is None or closeout.clv is None:
            value = None
        elif closeout.clv_kind is None or closeout.clv_kind.value != expected_kind:
            value = None
            conflict = True
        else:
            value = closeout.clv
    elif kind == "quote_age_seconds":
        value = item.quote_age_seconds
        conflict = item.quote_age_conflict
    elif kind == "observation_count":
        value = None if item.history is None else item.history.observation_count
        conflict = item.history_join_conflict
    else:
        value = None if item.history is None else item.history.distinct_fetch_count
        conflict = item.history_join_conflict
    if value is not None and not math.isfinite(float(value)):
        return None, True
    return None if value is None else float(value), conflict


def _assign_empirical_groups(
    valued: Sequence[tuple[float, _EvaluatedCandidate]],
    boundaries: Sequence[float],
) -> list[tuple[float, float, list[_EvaluatedCandidate]]]:
    """Assign every observation exactly once to ordered empirical intervals."""
    if len(boundaries) < 3:
        return []
    groups = [(lower, upper, []) for lower, upper in pairwise(boundaries)]
    for value, item in valued:
        for index, (lower, upper, rows) in enumerate(groups):
            final = index == len(groups) - 1
            if lower <= value < upper or (final and lower <= value <= upper):
                rows.append(item)
                break
    return [group for group in groups if group[2]]


def _numeric_cohort(
    *,
    kind: str,
    market: str,
    index: int,
    lower: float,
    upper: float,
    items: Sequence[_EvaluatedCandidate],
) -> NumericCohortEvaluation:
    """Summarize one empirical group without evaluative labels."""
    outcomes = Counter(item.outcome.value for item in items)
    expected_values = [
        item.row.expected_value
        for item in items
        if item.row.expected_value is not None and math.isfinite(item.row.expected_value)
    ]
    clv_values = [
        item.closeout.clv
        for item in items
        if item.closeout is not None
        and item.closeout.clv is not None
        and item.closeout.clv_kind is not None
    ]
    return_items = [
        item
        for item in items
        if item.realized_return is not None
        and item.return_stake is not None
        and item.return_pnl is not None
        and not item.return_conflict
    ]
    realized_returns = [cast(float, item.realized_return) for item in return_items]
    return_stakes = [cast(float, item.return_stake) for item in return_items]
    return_pnls = [cast(float, item.return_pnl) for item in return_items]
    total_return_stake = float(sum(return_stakes)) if return_stakes else None
    total_return_pnl = float(sum(return_pnls)) if return_pnls else None
    return NumericCohortEvaluation(
        cohort_kind=kind,
        market=market,
        cohort_index=index,
        lower_bound=lower,
        upper_bound=upper,
        candidate_count=len(items),
        outcome_evaluable_count=(
            outcomes[CandidateOutcome.WIN.value] + outcomes[CandidateOutcome.LOSS.value]
        ),
        win_count=outcomes[CandidateOutcome.WIN.value],
        loss_count=outcomes[CandidateOutcome.LOSS.value],
        push_count=outcomes[CandidateOutcome.PUSH.value],
        closeout_available_count=sum(
            item.closeout is not None and item.closeout.status is MarketCloseoutStatus.AVAILABLE
            for item in items
        ),
        clv_available_count=len(clv_values),
        mean_expected_value=(
            None if not expected_values else float(sum(expected_values) / len(expected_values))
        ),
        mean_clv=(None if not clv_values else float(sum(clv_values) / len(clv_values))),
        mean_realized_return=(
            None if not realized_returns else float(sum(realized_returns) / len(realized_returns))
        ),
        aggregate_realized_return=(
            None
            if total_return_stake is None or total_return_pnl is None
            else float(total_return_pnl / total_return_stake)
        ),
    )


def _coverage(
    market: str,
    rows: Sequence[CandidateIssuanceRow],
    candidates: Sequence[_EvaluatedCandidate],
) -> MarketFamilyCoverage:
    """Summarize issuance, outcome, closeout, depth, age, and return coverage."""
    outcomes = Counter(item.outcome.value for item in candidates)
    statuses: Counter[str] = Counter()
    for item in candidates:
        if item.closeout_join_conflict:
            statuses["evaluation_closeout_conflict"] += 1
        elif item.closeout is None:
            statuses["evaluation_closeout_missing"] += 1
        else:
            statuses[item.closeout.status.value] += 1
    return MarketFamilyCoverage(
        market=market,
        issued_count=len(rows),
        candidate_count=sum(row.state is CandidateIssuanceState.CANDIDATE for row in rows),
        not_candidate_count=sum(row.state is CandidateIssuanceState.NOT_CANDIDATE for row in rows),
        unavailable_count=sum(row.state is CandidateIssuanceState.UNAVAILABLE for row in rows),
        probability_available_count=sum(row.model_probability is not None for row in rows),
        expected_value_available_count=sum(row.expected_value is not None for row in rows),
        outcome_available_count=(
            outcomes[CandidateOutcome.WIN.value] + outcomes[CandidateOutcome.LOSS.value]
        ),
        outcome_push_count=outcomes[CandidateOutcome.PUSH.value],
        outcome_unavailable_count=outcomes[CandidateOutcome.UNAVAILABLE.value],
        outcome_conflict_count=outcomes[CandidateOutcome.CONFLICT.value],
        closeout_available_count=sum(
            item.closeout is not None
            and not item.closeout_join_conflict
            and item.closeout.status is MarketCloseoutStatus.AVAILABLE
            for item in candidates
        ),
        clv_available_count=sum(
            item.closeout is not None
            and item.closeout.clv is not None
            and item.closeout.clv_kind is not None
            for item in candidates
        ),
        history_depth_available_count=sum(
            item.history is not None and not item.history_join_conflict for item in candidates
        ),
        quote_age_available_count=sum(item.quote_age_seconds is not None for item in candidates),
        quote_age_conflict_count=sum(item.quote_age_conflict for item in candidates),
        return_available_count=sum(
            item.realized_return is not None and not item.return_conflict for item in candidates
        ),
        return_unavailable_count=sum(
            item.realized_return is None and not item.return_conflict for item in candidates
        ),
        return_conflict_count=sum(item.return_conflict for item in candidates),
        closeout_status_counts=tuple(sorted(statuses.items())),
    )


def _reliability(candidates: Sequence[_EvaluatedCandidate]) -> ReliabilityEvaluation:
    usable: list[tuple[float, float]] = []
    probability_conflicts = 0
    for item in candidates:
        probability = item.row.model_probability
        if item.outcome not in {CandidateOutcome.WIN, CandidateOutcome.LOSS} or probability is None:
            continue
        if not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
            probability_conflicts += 1
            continue
        usable.append((probability, 1.0 if item.outcome is CandidateOutcome.WIN else 0.0))
    outcomes = Counter(item.outcome.value for item in candidates)
    if usable:
        probabilities = Series([value[0] for value in usable], dtype="float64")
        actuals = Series([value[1] for value in usable], dtype="float64")
        brier = _metric(float(brier_score(probabilities, actuals)), len(usable))
        loss = _metric(float(log_loss(probabilities, actuals)), len(usable))
        acc = _metric(float(((probabilities >= 0.5) == actuals.astype(bool)).mean()), len(usable))
    else:
        brier = _missing_metric("no_evaluable_binary_outcomes")
        loss = _missing_metric("no_evaluable_binary_outcomes")
        acc = _missing_metric("no_evaluable_binary_outcomes")
    return ReliabilityEvaluation(
        candidate_count=len(candidates),
        evaluable_count=len(usable),
        win_count=outcomes[CandidateOutcome.WIN.value],
        loss_count=outcomes[CandidateOutcome.LOSS.value],
        push_count=outcomes[CandidateOutcome.PUSH.value],
        unavailable_count=outcomes[CandidateOutcome.UNAVAILABLE.value],
        conflict_count=outcomes[CandidateOutcome.CONFLICT.value] + probability_conflicts,
        brier=brier,
        log_loss=loss,
        accuracy=acc,
    )


def _metric(value: float, count: int) -> MetricEstimate:
    return MetricEstimate(EvaluationEvidenceStatus.AVAILABLE, count, value, None)


def _missing_metric(reason: str) -> MetricEstimate:
    return MetricEstimate(EvaluationEvidenceStatus.UNAVAILABLE, 0, None, reason)


def _quote_age(candidates: Sequence[_EvaluatedCandidate]) -> QuoteAgeEvaluation:
    values = [item.quote_age_seconds for item in candidates if item.quote_age_seconds is not None]
    conflicts = sum(item.quote_age_conflict for item in candidates)
    status, reason = _summary_status(len(candidates), len(values), conflicts)
    return QuoteAgeEvaluation(
        status=status,
        candidate_count=len(candidates),
        available_count=len(values),
        conflict_count=conflicts,
        minimum_seconds=None if not values else min(values),
        median_seconds=None if not values else float(median(values)),
        maximum_seconds=None if not values else max(values),
        reason=reason,
    )


def _observation_depth(candidates: Sequence[_EvaluatedCandidate]) -> ObservationDepthEvaluation:
    available = [
        item.history
        for item in candidates
        if item.history is not None and not item.history_join_conflict
    ]
    conflicts = sum(item.history_join_conflict for item in candidates)
    missing = len(candidates) - len(available) - conflicts
    status, reason = _summary_status(len(candidates), len(available), conflicts)
    observations = [item.observation_count for item in available]
    fetches = [item.distinct_fetch_count for item in available]
    return ObservationDepthEvaluation(
        status=status,
        candidate_count=len(candidates),
        available_count=len(available),
        unavailable_count=missing,
        conflict_count=conflicts,
        minimum_observation_count=None if not observations else min(observations),
        median_observation_count=None if not observations else float(median(observations)),
        maximum_observation_count=None if not observations else max(observations),
        minimum_distinct_fetch_count=None if not fetches else min(fetches),
        median_distinct_fetch_count=None if not fetches else float(median(fetches)),
        maximum_distinct_fetch_count=None if not fetches else max(fetches),
        reason=reason,
    )


def _summary_status(
    total: int, available: int, conflicts: int
) -> tuple[EvaluationEvidenceStatus, str | None]:
    if total == 0:
        return EvaluationEvidenceStatus.UNAVAILABLE, "no_candidates"
    if available > 0:
        return EvaluationEvidenceStatus.AVAILABLE, None
    if conflicts > 0:
        return EvaluationEvidenceStatus.CONFLICTING_EVIDENCE, "all_evidence_conflicting"
    return EvaluationEvidenceStatus.UNAVAILABLE, "no_available_evidence"


def _categorical_cohorts(
    candidates: Sequence[_EvaluatedCandidate],
    *,
    kind: str,
) -> tuple[CategoricalCohortEvaluation, ...]:
    grouped: dict[str, list[_EvaluatedCandidate]] = {}
    for item in candidates:
        if kind == "sportsbook":
            key = f"{item.row.provider}/{item.row.sportsbook or '<null>'}"
        else:
            key = f"{item.row.market}/{item.row.side}"
        grouped.setdefault(key, []).append(item)
    rows = []
    for key, items in sorted(grouped.items()):
        rows.append(
            CategoricalCohortEvaluation(
                cohort_kind=kind,
                cohort_key=key,
                candidate_count=len(items),
                outcome_evaluable_count=sum(
                    item.outcome in {CandidateOutcome.WIN, CandidateOutcome.LOSS} for item in items
                ),
                closeout_available_count=sum(
                    item.closeout is not None
                    and item.closeout.status is MarketCloseoutStatus.AVAILABLE
                    for item in items
                ),
                clv_available_count=sum(
                    item.closeout is not None
                    and item.closeout.clv is not None
                    and item.closeout.clv_kind is not None
                    for item in items
                ),
            )
        )
    return tuple(rows)
