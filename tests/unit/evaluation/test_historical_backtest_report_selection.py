"""Tests for explicit current historical backtest report selection."""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path

import pytest

from gridiron_edge.evaluation.historical_backtest_report_selection import (
    get_current_historical_backtest_report_selection,
    historical_backtest_report_path,
    historical_backtest_report_root,
    select_current_historical_backtest_report,
)

REPORT_ONE = "1" * 64
REPORT_TWO = "2" * 64
SELECTED_AT = datetime(2026, 8, 18, 21, tzinfo=UTC)


def _store_report(tmp_path: Path, report_id: str) -> Path:
    path = historical_backtest_report_path(report_id, repo=tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}\n", encoding="utf-8")
    return path


def _current_path(tmp_path: Path) -> Path:
    return historical_backtest_report_root(tmp_path) / "current.json"


def test_explicit_selection_round_trips(tmp_path: Path) -> None:
    _store_report(tmp_path, REPORT_ONE)

    selected = select_current_historical_backtest_report(
        REPORT_ONE,
        selected_at=SELECTED_AT,
        repo=tmp_path,
    )

    assert selected.report_id == REPORT_ONE
    assert selected.selected_at == SELECTED_AT
    assert get_current_historical_backtest_report_selection(repo=tmp_path) == selected


def test_selection_can_be_changed_explicitly(tmp_path: Path) -> None:
    _store_report(tmp_path, REPORT_ONE)
    _store_report(tmp_path, REPORT_TWO)
    select_current_historical_backtest_report(
        REPORT_ONE,
        selected_at=SELECTED_AT,
        repo=tmp_path,
    )

    changed = select_current_historical_backtest_report(
        REPORT_TWO,
        selected_at=datetime(2026, 8, 18, 22, tzinfo=UTC),
        repo=tmp_path,
    )

    assert changed.report_id == REPORT_TWO
    assert get_current_historical_backtest_report_selection(repo=tmp_path) == changed


def test_storing_newer_report_does_not_change_current(tmp_path: Path) -> None:
    _store_report(tmp_path, REPORT_ONE)
    selected = select_current_historical_backtest_report(
        REPORT_ONE,
        selected_at=SELECTED_AT,
        repo=tmp_path,
    )
    _store_report(tmp_path, REPORT_TWO)

    assert get_current_historical_backtest_report_selection(repo=tmp_path) == selected


def test_missing_current_selection_fails_clearly(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="No current"):
        get_current_historical_backtest_report_selection(repo=tmp_path)


def test_selection_requires_existing_report(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="not stored"):
        select_current_historical_backtest_report(
            REPORT_ONE,
            selected_at=SELECTED_AT,
            repo=tmp_path,
        )


def test_selection_requires_valid_report_id(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="SHA-256"):
        select_current_historical_backtest_report(
            "not-a-digest",
            selected_at=SELECTED_AT,
            repo=tmp_path,
        )


def test_selection_requires_utc_timestamp(tmp_path: Path) -> None:
    _store_report(tmp_path, REPORT_ONE)

    with pytest.raises(ValueError, match="timezone-aware UTC"):
        select_current_historical_backtest_report(
            REPORT_ONE,
            selected_at=datetime(2026, 8, 18, 21),
            repo=tmp_path,
        )


def test_malformed_current_selection_is_rejected(tmp_path: Path) -> None:
    root = historical_backtest_report_root(tmp_path)
    root.mkdir(parents=True)
    _current_path(tmp_path).write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_id": REPORT_ONE,
                "selected_at": SELECTED_AT.isoformat(),
                "extra": True,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="keys"):
        get_current_historical_backtest_report_selection(repo=tmp_path)


def test_current_schema_mismatch_is_rejected(tmp_path: Path) -> None:
    root = historical_backtest_report_root(tmp_path)
    root.mkdir(parents=True)
    _current_path(tmp_path).write_text(
        json.dumps(
            {
                "schema_version": 999,
                "report_id": REPORT_ONE,
                "selected_at": SELECTED_AT.isoformat(),
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="selection schema version"):
        get_current_historical_backtest_report_selection(repo=tmp_path)


def test_selected_report_must_remain_present(tmp_path: Path) -> None:
    report_path = _store_report(tmp_path, REPORT_ONE)
    select_current_historical_backtest_report(
        REPORT_ONE,
        selected_at=SELECTED_AT,
        repo=tmp_path,
    )
    report_path.unlink()

    with pytest.raises(FileNotFoundError, match="Selected historical"):
        get_current_historical_backtest_report_selection(repo=tmp_path)
