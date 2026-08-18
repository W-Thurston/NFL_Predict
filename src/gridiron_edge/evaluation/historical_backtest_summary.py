"""Pure summaries and chart series for historical walk-forward evidence."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import math
from typing import Final, cast

import pandas as pd
from pandas import DataFrame, Series

_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "selection_id",
        "season",
        "week",
        "game_id",
        "game_date",
        "moneyline_evaluable",
        "moneyline_correct",
        "moneyline_squared_error",
        "moneyline_log_loss",
        "total_evaluable",
        "total_outcome",
        "total_absolute_error",
        "total_error",
        "total_unit_return",
        "total_assumed_american_price",
    }
)
ROLLING_DECISION_WINDOW: Final[int] = 100


class HistoricalPriceEvidenceStatus(StrEnum):
    """Availability of retained historical price evidence."""

    UNAVAILABLE = "unavailable"
    ASSUMED = "assumed"


@dataclass(frozen=True, slots=True)
class MoneylineBacktestSummary:
    """Aggregate current-champion Moneyline prediction quality."""

    evaluated_count: int
    win_count: int
    loss_count: int
    net_wins: int
    accuracy: float | None
    brier: float | None
    log_loss: float | None
    unit_return_status: HistoricalPriceEvidenceStatus
    unit_return_reason: str


@dataclass(frozen=True, slots=True)
class TotalBacktestSummary:
    """Aggregate current-champion Total quality and assumed-price return."""

    decision_count: int
    win_count: int
    loss_count: int
    push_count: int
    no_bet_count: int
    net_wins: int
    hit_rate_excluding_pushes: float | None
    mae: float | None
    rmse: float | None
    bias: float | None
    net_units: float | None
    roi_per_unit_staked: float | None
    price_evidence_status: HistoricalPriceEvidenceStatus
    assumed_american_price: int | None
    methodology: str


@dataclass(frozen=True, slots=True)
class HistoricalBacktestSummary:
    """Aggregate report summary for one exact run selection."""

    selection_id: str
    first_season: str
    last_season: str
    evidence_row_count: int
    moneyline: MoneylineBacktestSummary
    total: TotalBacktestSummary


def summarize_historical_backtest(evidence: DataFrame) -> HistoricalBacktestSummary:
    """Summarize validated historical evidence without mutating the input."""
    rows = _validated_evidence(evidence)
    selection_id = _single_text(rows, "selection_id")
    moneyline = rows.loc[rows["moneyline_evaluable"].eq(True), :].copy()
    moneyline_wins = int(moneyline["moneyline_correct"].eq(True).sum())
    moneyline_losses = int(moneyline["moneyline_correct"].eq(False).sum())

    total_graded = rows.loc[rows["total_outcome"].notna(), :].copy()
    total_wins = int(total_graded["total_outcome"].eq("win").sum())
    total_losses = int(total_graded["total_outcome"].eq("loss").sum())
    total_pushes = int(total_graded["total_outcome"].eq("push").sum())
    total_decisions = rows.loc[rows["total_unit_return"].notna(), :].copy()
    no_bets = int(rows["total_evaluable"].eq(True).sum()) - len(total_graded)
    non_pushes = total_wins + total_losses

    return HistoricalBacktestSummary(
        selection_id=selection_id,
        first_season=str(rows["season"].min()),
        last_season=str(rows["season"].max()),
        evidence_row_count=len(rows),
        moneyline=MoneylineBacktestSummary(
            evaluated_count=len(moneyline),
            win_count=moneyline_wins,
            loss_count=moneyline_losses,
            net_wins=moneyline_wins - moneyline_losses,
            accuracy=_mean(moneyline["moneyline_correct"]),
            brier=_mean(moneyline["moneyline_squared_error"]),
            log_loss=_mean(moneyline["moneyline_log_loss"]),
            unit_return_status=HistoricalPriceEvidenceStatus.UNAVAILABLE,
            unit_return_reason="Historical Moneyline prices were not retained.",
        ),
        total=TotalBacktestSummary(
            decision_count=len(total_decisions),
            win_count=total_wins,
            loss_count=total_losses,
            push_count=total_pushes,
            no_bet_count=no_bets,
            net_wins=total_wins - total_losses,
            hit_rate_excluding_pushes=(total_wins / non_pushes if non_pushes > 0 else None),
            mae=_mean(rows["total_absolute_error"]),
            rmse=_rmse(rows["total_error"]),
            bias=_mean(rows["total_error"]),
            net_units=_sum_or_none(total_decisions["total_unit_return"]),
            roi_per_unit_staked=_mean(total_decisions["total_unit_return"]),
            price_evidence_status=HistoricalPriceEvidenceStatus.ASSUMED,
            assumed_american_price=_single_nullable_int(
                total_decisions,
                "total_assumed_american_price",
            ),
            methodology=(
                "Historical consensus-total line; one unit per eligible decision; "
                "assumed -110 pricing."
            ),
        ),
    )


def build_historical_backtest_series(evidence: DataFrame) -> DataFrame:
    """Build deterministic cumulative net-win, accuracy, and unit chart points."""
    rows = (
        _validated_evidence(evidence)
        .sort_values(
            ["season", "week", "game_date", "game_id"],
            kind="stable",
        )
        .reset_index(drop=True)
    )
    moneyline_score = rows["moneyline_correct"].map({True: 1.0, False: -1.0})
    total_score = rows["total_outcome"].map({"win": 1.0, "loss": -1.0, "push": 0.0})
    result = rows.loc[:, ["selection_id", "season", "week", "game_id", "game_date"]].copy()
    result["moneyline_decision_score"] = moneyline_score
    result["moneyline_cumulative_net_wins"] = moneyline_score.fillna(0.0).cumsum()
    result["moneyline_cumulative_accuracy"] = _expanding_accuracy(rows["moneyline_correct"])
    result["moneyline_rolling_accuracy_100"] = _rolling_accuracy(rows["moneyline_correct"])
    result["total_decision_score"] = total_score
    result["total_cumulative_net_wins"] = total_score.fillna(0.0).cumsum()
    result["total_cumulative_accuracy"] = _expanding_accuracy(
        rows["total_outcome"].map({"win": True, "loss": False})
    )
    result["total_rolling_accuracy_100"] = _rolling_accuracy(
        rows["total_outcome"].map({"win": True, "loss": False})
    )
    result["total_cumulative_units"] = rows["total_unit_return"].fillna(0.0).cumsum()
    return result


def _validated_evidence(evidence: DataFrame) -> DataFrame:
    missing = sorted(_REQUIRED_COLUMNS - set(evidence.columns))
    if missing:
        raise ValueError("Historical evidence is missing columns: " + ", ".join(missing))
    rows = evidence.copy(deep=True)
    if rows.empty:
        raise ValueError("Historical evidence must contain at least one row.")
    if rows["game_id"].astype(str).duplicated().any():
        raise ValueError("Historical evidence contains duplicate game IDs.")
    _single_text(rows, "selection_id")
    return rows


def _single_text(rows: DataFrame, column: str) -> str:
    values = rows[column].dropna().astype(str).unique().tolist()
    if len(values) != 1 or not values[0].strip():
        raise ValueError(f"Historical evidence must contain one nonempty {column} value.")
    return values[0]


def _single_nullable_int(rows: DataFrame, column: str) -> int | None:
    numeric = cast(Series, pd.to_numeric(rows[column], errors="coerce"))
    values = numeric.dropna().astype(int).unique().tolist()
    if not values:
        return None
    if len(values) != 1:
        raise ValueError(f"Historical evidence contains conflicting {column} values.")
    return int(values[0])


def _mean(values: Series) -> float | None:
    converted = cast(Series, pd.to_numeric(values, errors="coerce"))
    numeric = converted.dropna().astype(float)
    return None if numeric.empty else float(numeric.mean())


def _sum_or_none(values: Series) -> float | None:
    converted = cast(Series, pd.to_numeric(values, errors="coerce"))
    numeric = converted.dropna().astype(float)
    return None if numeric.empty else float(numeric.sum())


def _rmse(values: Series) -> float | None:
    converted = cast(Series, pd.to_numeric(values, errors="coerce"))
    numeric = converted.dropna().astype(float)
    return None if numeric.empty else math.sqrt(float((numeric**2).mean()))


def _expanding_accuracy(values: Series) -> Series:
    numeric = values.map({True: 1.0, False: 0.0})
    cumulative_correct = numeric.fillna(0.0).cumsum()
    cumulative_count = numeric.notna().astype(int).cumsum().replace(0, pd.NA)
    return cast(Series, cumulative_correct / cumulative_count)


def _rolling_accuracy(values: Series) -> Series:
    numeric = values.map({True: 1.0, False: 0.0})
    compact = numeric.dropna()
    rolling = compact.rolling(
        window=ROLLING_DECISION_WINDOW,
        min_periods=ROLLING_DECISION_WINDOW,
    ).mean()
    return rolling.reindex(values.index)
