"""Strict loading for stored and explicitly selected historical reports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import cast

from pandas import DataFrame

from gridiron_edge.evaluation.historical_backtest_report import (
    HISTORICAL_BACKTEST_REPORT_SCHEMA_VERSION,
    BacktestFrameReference,
    HistoricalBacktestReport,
    validate_historical_backtest_report,
)
from gridiron_edge.evaluation.historical_backtest_report_selection import (
    get_current_historical_backtest_report_selection,
    historical_backtest_report_path,
)
from gridiron_edge.evaluation.historical_backtest_report_store import (
    HISTORICAL_BACKTEST_REPORT_STORE_SCHEMA_VERSION,
    verify_historical_backtest_report,
)
from gridiron_edge.evaluation.historical_backtest_summary import (
    HistoricalBacktestSummary,
    HistoricalPriceEvidenceStatus,
    MoneylineBacktestSummary,
    TotalBacktestSummary,
)


@dataclass(frozen=True, slots=True)
class CurrentHistoricalBacktestReport:
    """Explicitly selected report and its verified persisted frames."""

    report: HistoricalBacktestReport
    evidence: DataFrame
    series: DataFrame
    selected_at: datetime


def read_historical_backtest_report(path: Path) -> HistoricalBacktestReport:
    """Strictly deserialize and validate one identity-addressed report manifest."""
    raw = _object(json.loads(path.read_text(encoding="utf-8")), "report artifact")
    _exact_keys(
        raw,
        {"store_schema_version", "report_id", "report"},
        "Report artifact",
    )
    store_version = _integer(raw["store_schema_version"], "store_schema_version")
    if store_version != HISTORICAL_BACKTEST_REPORT_STORE_SCHEMA_VERSION:
        raise ValueError("Unsupported historical backtest report store schema version.")
    embedded_id = _digest(_text(raw["report_id"], "report_id"), "report_id")
    report = _report(raw["report"])
    if embedded_id != report.report_id:
        raise ValueError("Stored report identity does not match report content.")
    expected_path = historical_backtest_report_path(
        report.report_id,
        repo=_artifact_repo(path),
    )
    if path.resolve() != expected_path.resolve():
        raise ValueError("Historical backtest report path and embedded identity disagree.")
    return report


def load_current_historical_backtest_report(
    *,
    repo: Path | None = None,
) -> CurrentHistoricalBacktestReport:
    """Load the explicitly selected report and verify both persisted frames."""
    selection = get_current_historical_backtest_report_selection(repo=repo)
    path = historical_backtest_report_path(selection.report_id, repo=repo)
    report = read_historical_backtest_report(path)
    evidence, series = verify_historical_backtest_report(report, repo=repo)
    return CurrentHistoricalBacktestReport(
        report=report,
        evidence=evidence,
        series=series,
        selected_at=selection.selected_at,
    )


def _report(value: object) -> HistoricalBacktestReport:
    data = _object(value, "report")
    _exact_keys(
        data,
        {
            "schema_version",
            "report_id",
            "generated_at",
            "run_selection_id",
            "win_model_type",
            "win_run_id",
            "total_model_type",
            "total_run_id",
            "summary",
            "evidence",
            "series",
            "rolling_decision_window",
        },
        "Report",
    )
    schema_version = _integer(data["schema_version"], "schema_version")
    if schema_version != HISTORICAL_BACKTEST_REPORT_SCHEMA_VERSION:
        raise ValueError("Unsupported historical backtest report schema version.")
    report = HistoricalBacktestReport(
        schema_version=schema_version,
        report_id=_digest(_text(data["report_id"], "report_id"), "report_id"),
        generated_at=_datetime(data["generated_at"], "generated_at"),
        run_selection_id=_digest(
            _text(data["run_selection_id"], "run_selection_id"),
            "run_selection_id",
        ),
        win_model_type=_text(data["win_model_type"], "win_model_type"),
        win_run_id=_text(data["win_run_id"], "win_run_id"),
        total_model_type=_text(data["total_model_type"], "total_model_type"),
        total_run_id=_text(data["total_run_id"], "total_run_id"),
        summary=_summary(data["summary"]),
        evidence=_frame_reference(data["evidence"], "evidence"),
        series=_frame_reference(data["series"], "series"),
        rolling_decision_window=_integer(
            data["rolling_decision_window"],
            "rolling_decision_window",
        ),
    )
    validate_historical_backtest_report(report)
    return report


def _summary(value: object) -> HistoricalBacktestSummary:
    data = _object(value, "summary")
    _exact_keys(
        data,
        {
            "selection_id",
            "first_season",
            "last_season",
            "evidence_row_count",
            "moneyline",
            "total",
        },
        "Summary",
    )
    return HistoricalBacktestSummary(
        selection_id=_digest(
            _text(data["selection_id"], "summary.selection_id"),
            "summary.selection_id",
        ),
        first_season=_text(data["first_season"], "first_season"),
        last_season=_text(data["last_season"], "last_season"),
        evidence_row_count=_integer(data["evidence_row_count"], "evidence_row_count"),
        moneyline=_moneyline_summary(data["moneyline"]),
        total=_total_summary(data["total"]),
    )


def _moneyline_summary(value: object) -> MoneylineBacktestSummary:
    data = _object(value, "moneyline summary")
    _exact_keys(
        data,
        {
            "evaluated_count",
            "win_count",
            "loss_count",
            "net_wins",
            "accuracy",
            "brier",
            "log_loss",
            "unit_return_status",
            "unit_return_reason",
        },
        "Moneyline summary",
    )
    return MoneylineBacktestSummary(
        evaluated_count=_integer(data["evaluated_count"], "evaluated_count"),
        win_count=_integer(data["win_count"], "win_count"),
        loss_count=_integer(data["loss_count"], "loss_count"),
        net_wins=_integer(data["net_wins"], "net_wins"),
        accuracy=_optional_number(data["accuracy"], "accuracy"),
        brier=_optional_number(data["brier"], "brier"),
        log_loss=_optional_number(data["log_loss"], "log_loss"),
        unit_return_status=HistoricalPriceEvidenceStatus(
            _text(data["unit_return_status"], "unit_return_status")
        ),
        unit_return_reason=_text(data["unit_return_reason"], "unit_return_reason"),
    )


def _total_summary(value: object) -> TotalBacktestSummary:
    data = _object(value, "total summary")
    _exact_keys(
        data,
        {
            "decision_count",
            "win_count",
            "loss_count",
            "push_count",
            "no_bet_count",
            "net_wins",
            "hit_rate_excluding_pushes",
            "mae",
            "rmse",
            "bias",
            "net_units",
            "roi_per_unit_staked",
            "price_evidence_status",
            "assumed_american_price",
            "methodology",
        },
        "Total summary",
    )
    return TotalBacktestSummary(
        decision_count=_integer(data["decision_count"], "decision_count"),
        win_count=_integer(data["win_count"], "win_count"),
        loss_count=_integer(data["loss_count"], "loss_count"),
        push_count=_integer(data["push_count"], "push_count"),
        no_bet_count=_integer(data["no_bet_count"], "no_bet_count"),
        net_wins=_integer(data["net_wins"], "net_wins"),
        hit_rate_excluding_pushes=_optional_number(
            data["hit_rate_excluding_pushes"],
            "hit_rate_excluding_pushes",
        ),
        mae=_optional_number(data["mae"], "mae"),
        rmse=_optional_number(data["rmse"], "rmse"),
        bias=_optional_number(data["bias"], "bias"),
        net_units=_optional_number(data["net_units"], "net_units"),
        roi_per_unit_staked=_optional_number(
            data["roi_per_unit_staked"],
            "roi_per_unit_staked",
        ),
        price_evidence_status=HistoricalPriceEvidenceStatus(
            _text(data["price_evidence_status"], "price_evidence_status")
        ),
        assumed_american_price=_optional_integer(
            data["assumed_american_price"],
            "assumed_american_price",
        ),
        methodology=_text(data["methodology"], "methodology"),
    )


def _frame_reference(value: object, label: str) -> BacktestFrameReference:
    data = _object(value, f"{label} reference")
    _exact_keys(
        data,
        {"artifact", "row_count", "columns", "content_digest"},
        f"{label.title()} reference",
    )
    columns = _list(data["columns"], f"{label}.columns")
    return BacktestFrameReference(
        artifact=_text(data["artifact"], f"{label}.artifact"),
        row_count=_integer(data["row_count"], f"{label}.row_count"),
        columns=tuple(_text(column, f"{label}.column") for column in columns),
        content_digest=_digest(
            _text(data["content_digest"], f"{label}.content_digest"),
            f"{label}.content_digest",
        ),
    )


def _artifact_repo(path: Path) -> Path:
    resolved = path.resolve()
    marker = ("data", "output", "model_performance")
    parts = resolved.parts
    for index in range(len(parts) - len(marker) + 1):
        if tuple(parts[index : index + len(marker)]) == marker:
            return Path(*parts[:index])
    raise ValueError("Historical report path is outside the canonical store.")


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{label} must be a JSON object with string keys.")
    return cast(dict[str, object], value)


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list.")
    return value


def _exact_keys(
    data: dict[str, object],
    expected: set[str],
    label: str,
) -> None:
    if set(data) != expected:
        raise ValueError(f"{label} keys do not match the current schema.")


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonempty string.")
    return value


def _integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer.")
    return value


def _optional_integer(value: object, label: str) -> int | None:
    return None if value is None else _integer(value, label)


def _optional_number(value: object, label: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{label} must be numeric or null.")
    return float(value)


def _datetime(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be an ISO timestamp string.")
    result = datetime.fromisoformat(value)
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must be timezone-aware UTC.")
    return result


def _digest(value: str, label: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest.")
    return value
