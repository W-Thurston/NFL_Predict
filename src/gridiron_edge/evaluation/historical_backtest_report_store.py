"""Immutable JSON plus Parquet storage for historical backtest reports."""

from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
from uuid import uuid4

import pandas as pd
from pandas import DataFrame

from gridiron_edge.core.settings import get_settings
from gridiron_edge.evaluation.historical_backtest_report import (
    BacktestFrameReference,
    HistoricalBacktestReport,
    frame_content_digest,
    validate_historical_backtest_report,
)

HISTORICAL_BACKTEST_REPORT_STORE_SCHEMA_VERSION = 1


def historical_backtest_report_root(repo: Path | None = None) -> Path:
    """Return the canonical report store root."""
    return (repo or get_settings().repo_root) / "data/output/model_performance"


def write_historical_backtest_report(
    report: HistoricalBacktestReport,
    *,
    evidence: DataFrame,
    series: DataFrame,
    repo: Path | None = None,
) -> Path:
    """Persist exact report frames and manifest or accept an exact replay."""
    validate_historical_backtest_report(report)
    root = historical_backtest_report_root(repo)
    _validate_frame(report.evidence, evidence, label="evidence")
    _validate_frame(report.series, series, label="series")
    evidence_path = _resolved(root, report.evidence.artifact)
    series_path = _resolved(root, report.series.artifact)
    manifest = root / f"schema={report.schema_version}" / "reports" / f"{report.report_id}.json"
    _write_parquet(evidence_path, evidence)
    _write_parquet(series_path, series)
    encoded = (
        json.dumps(
            {
                "store_schema_version": (HISTORICAL_BACKTEST_REPORT_STORE_SCHEMA_VERSION),
                "report_id": report.report_id,
                "report": asdict(report),
            },
            indent=2,
            sort_keys=True,
            default=str,
            allow_nan=False,
        )
        + "\n"
    )
    manifest.parent.mkdir(parents=True, exist_ok=True)
    if manifest.exists():
        if manifest.read_text(encoding="utf-8") != encoded:
            raise ValueError(
                "Historical backtest report identity cannot be reused with different content."
            )
        return manifest
    temporary = manifest.with_name(f".{manifest.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(encoded, encoding="utf-8")
        if manifest.exists():
            if manifest.read_text(encoding="utf-8") != encoded:
                raise ValueError(
                    "Historical backtest report identity cannot be reused with different content."
                )
        else:
            temporary.replace(manifest)
    finally:
        temporary.unlink(missing_ok=True)
    return manifest


def verify_historical_backtest_report(
    report: HistoricalBacktestReport,
    *,
    repo: Path | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Load and verify both exact frames without recomputing analytics."""
    validate_historical_backtest_report(report)
    root = historical_backtest_report_root(repo)
    evidence_path = _resolved(root, report.evidence.artifact)
    series_path = _resolved(root, report.series.artifact)
    if not evidence_path.exists():
        raise FileNotFoundError(f"Historical evidence artifact is missing: {evidence_path}")
    if not series_path.exists():
        raise FileNotFoundError(f"Historical series artifact is missing: {series_path}")
    evidence = pd.read_parquet(evidence_path)
    series = pd.read_parquet(series_path)
    _validate_frame(report.evidence, evidence, label="evidence")
    _validate_frame(report.series, series, label="series")
    return evidence, series


def _validate_frame(
    reference: BacktestFrameReference,
    frame: DataFrame,
    *,
    label: str,
) -> None:
    if len(frame) != reference.row_count:
        raise ValueError(f"{label} row count does not match report.")
    if tuple(frame.columns) != reference.columns:
        raise ValueError(f"{label} columns do not match report.")
    if frame_content_digest(frame) != reference.content_digest:
        raise ValueError(f"{label} content digest does not match report.")


def _resolved(root: Path, artifact: str) -> Path:
    resolved_root = root.resolve()
    path = (root / artifact).resolve()
    try:
        path.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError("Report artifact escapes storage root.") from exc
    return path


def _write_parquet(path: Path, frame: DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = pd.read_parquet(path)
        if not existing.equals(frame):
            raise ValueError("Report frame identity cannot be reused with different content.")
        return
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        frame.to_parquet(temporary, index=False)
        normalized = pd.read_parquet(temporary)
        if not normalized.equals(frame):
            raise ValueError("Serialized report frame does not replay exactly.")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
