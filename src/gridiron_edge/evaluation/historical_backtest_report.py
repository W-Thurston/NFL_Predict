"""Immutable historical backtest report contracts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
from typing import Final

from pandas import DataFrame

from gridiron_edge.evaluation.historical_backtest_summary import (
    ROLLING_DECISION_WINDOW,
    HistoricalBacktestSummary,
)

HISTORICAL_BACKTEST_REPORT_SCHEMA_VERSION: Final[int] = 1


@dataclass(frozen=True, slots=True)
class BacktestFrameReference:
    """One content-addressed Parquet frame owned by a report."""

    artifact: str
    row_count: int
    columns: tuple[str, ...]
    content_digest: str


@dataclass(frozen=True, slots=True)
class HistoricalBacktestReport:
    """Frozen summary and exact row-level artifact references."""

    schema_version: int
    report_id: str
    generated_at: datetime
    run_selection_id: str
    win_model_type: str
    win_run_id: str
    total_model_type: str
    total_run_id: str
    summary: HistoricalBacktestSummary
    evidence: BacktestFrameReference
    series: BacktestFrameReference
    rolling_decision_window: int


def create_historical_backtest_report(
    *,
    generated_at: datetime,
    run_selection_id: str,
    win_model_type: str,
    win_run_id: str,
    total_model_type: str,
    total_run_id: str,
    summary: HistoricalBacktestSummary,
    evidence: DataFrame,
    series: DataFrame,
    evidence_artifact: str,
    series_artifact: str,
) -> HistoricalBacktestReport:
    """Create a deterministic report contract from validated report inputs."""
    _utc(generated_at)
    if summary.selection_id != run_selection_id:
        raise ValueError("Summary selection_id does not match run selection.")
    evidence_ref = _frame_reference(evidence, evidence_artifact)
    series_ref = _frame_reference(series, series_artifact)
    core = _identity_payload(
        schema_version=HISTORICAL_BACKTEST_REPORT_SCHEMA_VERSION,
        generated_at=generated_at,
        run_selection_id=_digest(run_selection_id, "run_selection_id"),
        win_model_type=_text(win_model_type, "win_model_type"),
        win_run_id=_text(win_run_id, "win_run_id"),
        total_model_type=_text(total_model_type, "total_model_type"),
        total_run_id=_text(total_run_id, "total_run_id"),
        summary=summary,
        evidence=evidence_ref,
        series=series_ref,
        rolling_decision_window=ROLLING_DECISION_WINDOW,
    )
    report = HistoricalBacktestReport(
        schema_version=HISTORICAL_BACKTEST_REPORT_SCHEMA_VERSION,
        report_id=sha256(_canonical(core)).hexdigest(),
        generated_at=generated_at,
        run_selection_id=run_selection_id,
        win_model_type=win_model_type,
        win_run_id=win_run_id,
        total_model_type=total_model_type,
        total_run_id=total_run_id,
        summary=summary,
        evidence=evidence_ref,
        series=series_ref,
        rolling_decision_window=ROLLING_DECISION_WINDOW,
    )
    validate_historical_backtest_report(report)
    return report


def validate_historical_backtest_report(report: HistoricalBacktestReport) -> None:
    """Validate report identity and internal cross-field invariants."""
    if report.schema_version != HISTORICAL_BACKTEST_REPORT_SCHEMA_VERSION:
        raise ValueError("Unsupported historical backtest report schema version.")
    _digest(report.report_id, "report_id")
    _digest(report.run_selection_id, "run_selection_id")
    _utc(report.generated_at)
    if report.summary.selection_id != report.run_selection_id:
        raise ValueError("Summary selection_id does not match run selection.")
    if report.rolling_decision_window != ROLLING_DECISION_WINDOW:
        raise ValueError("Historical backtest rolling window does not match schema.")
    for label, value in (
        ("win_model_type", report.win_model_type),
        ("win_run_id", report.win_run_id),
        ("total_model_type", report.total_model_type),
        ("total_run_id", report.total_run_id),
    ):
        _text(value, label)
    for reference in (report.evidence, report.series):
        _validate_frame_reference(reference)
    expected_id = sha256(
        _canonical(
            _identity_payload(
                schema_version=report.schema_version,
                generated_at=report.generated_at,
                run_selection_id=report.run_selection_id,
                win_model_type=report.win_model_type,
                win_run_id=report.win_run_id,
                total_model_type=report.total_model_type,
                total_run_id=report.total_run_id,
                summary=report.summary,
                evidence=report.evidence,
                series=report.series,
                rolling_decision_window=report.rolling_decision_window,
            )
        )
    ).hexdigest()
    if report.report_id != expected_id:
        raise ValueError("report_id does not match canonical report content.")


def frame_content_digest(frame: DataFrame) -> str:
    """Return a stable digest of canonical frame values and schema."""
    payload = frame.to_json(
        orient="table",
        date_format="iso",
        index=False,
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _frame_reference(
    frame: DataFrame,
    artifact: str,
) -> BacktestFrameReference:
    reference = BacktestFrameReference(
        artifact=artifact,
        row_count=len(frame),
        columns=tuple(frame.columns),
        content_digest=frame_content_digest(frame),
    )
    _validate_frame_reference(reference)
    return reference


def _validate_frame_reference(reference: BacktestFrameReference) -> None:
    path = Path(reference.artifact)
    if not reference.artifact.strip() or path.is_absolute() or ".." in path.parts:
        raise ValueError("Report artifact must be a safe relative path.")
    if reference.row_count < 0:
        raise ValueError("Report frame row_count must be nonnegative.")
    if not reference.columns:
        raise ValueError("Report frame columns must not be empty.")
    _digest(reference.content_digest, "content_digest")


def _identity_payload(
    *,
    schema_version: int,
    generated_at: datetime,
    run_selection_id: str,
    win_model_type: str,
    win_run_id: str,
    total_model_type: str,
    total_run_id: str,
    summary: HistoricalBacktestSummary,
    evidence: BacktestFrameReference,
    series: BacktestFrameReference,
    rolling_decision_window: int,
) -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "generated_at": generated_at.isoformat(),
        "run_selection_id": run_selection_id,
        "win_model_type": win_model_type,
        "win_run_id": win_run_id,
        "total_model_type": total_model_type,
        "total_run_id": total_run_id,
        "summary": asdict(summary),
        "evidence": asdict(evidence),
        "series": asdict(series),
        "rolling_decision_window": rolling_decision_window,
    }


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    ).encode()


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("generated_at must be timezone-aware UTC.")


def _text(value: str, label: str) -> str:
    if not value.strip():
        raise ValueError(f"{label} must not be empty.")
    return value


def _digest(value: str, label: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest.")
    return value
