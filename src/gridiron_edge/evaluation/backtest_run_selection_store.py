"""Immutable JSON persistence for historical backtest run selections."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
import json
from pathlib import Path
from typing import cast
from uuid import uuid4

from gridiron_edge.core.settings import get_settings
from gridiron_edge.evaluation.backtest_run_selection import (
    BACKTEST_RUN_SELECTION_SCHEMA_VERSION,
    BacktestRunComponent,
    BacktestRunSelection,
    validate_backtest_run_selection,
)


def backtest_run_selection_root(repo: Path | None = None) -> Path:
    """Return the immutable backtest-run selection root."""
    root = repo or get_settings().repo_root
    return root / "data/output/model_performance/run_selections"


def backtest_run_selection_path(
    selection_id: str,
    *,
    repo: Path | None = None,
) -> Path:
    """Return the identity-addressed path for one exact selection."""
    _digest(selection_id, "selection_id")
    return (
        backtest_run_selection_root(repo)
        / f"schema={BACKTEST_RUN_SELECTION_SCHEMA_VERSION}"
        / "selections"
        / f"{selection_id}.json"
    )


def write_backtest_run_selection(
    selection: BacktestRunSelection,
    *,
    repo: Path | None = None,
) -> Path:
    """Persist one immutable selection or accept an exact replay."""
    validate_backtest_run_selection(selection)
    path = backtest_run_selection_path(selection.selection_id, repo=repo)
    encoded = json.dumps(_payload(selection), indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise ValueError(
                "Backtest-run selection identity cannot be reused with different content."
            )
        return path
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(encoded, encoding="utf-8")
        if path.exists():
            if path.read_text(encoding="utf-8") != encoded:
                raise ValueError(
                    "Backtest-run selection identity cannot be reused with different content."
                )
        else:
            temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def read_backtest_run_selection(path: Path) -> BacktestRunSelection:
    """Read and strictly validate one exact run selection."""
    raw = _object(json.loads(path.read_text(encoding="utf-8")), "selection artifact")
    expected = {"schema_version", "selection_id", "created_at", "win", "total"}
    if set(raw) != expected:
        raise ValueError("Backtest-run selection artifact keys do not match the schema.")
    selection = BacktestRunSelection(
        schema_version=_integer(raw["schema_version"], "schema_version"),
        selection_id=_digest(_text(raw["selection_id"], "selection_id"), "selection_id"),
        created_at=_datetime(raw["created_at"], "created_at"),
        win=_component(raw["win"], "win"),
        total=_component(raw["total"], "total"),
    )
    validate_backtest_run_selection(selection)
    expected_path = backtest_run_selection_path(
        selection.selection_id,
        repo=_artifact_repo(path),
    )
    if path.resolve() != expected_path.resolve():
        raise ValueError("Backtest-run selection path and embedded identity disagree.")
    return selection


def _payload(selection: BacktestRunSelection) -> dict[str, object]:
    return {
        "schema_version": selection.schema_version,
        "selection_id": selection.selection_id,
        "created_at": selection.created_at.isoformat(),
        "win": asdict(selection.win),
        "total": asdict(selection.total),
    }


def _component(value: object, label: str) -> BacktestRunComponent:
    raw = _object(value, label)
    expected = {"model_name", "model_type", "run_id", "event_count", "first_season", "last_season"}
    if set(raw) != expected:
        raise ValueError(f"{label} component keys do not match the schema.")
    return BacktestRunComponent(
        model_name=_text(raw["model_name"], f"{label}.model_name"),
        model_type=_text(raw["model_type"], f"{label}.model_type"),
        run_id=_text(raw["run_id"], f"{label}.run_id"),
        event_count=_integer(raw["event_count"], f"{label}.event_count"),
        first_season=_text(raw["first_season"], f"{label}.first_season"),
        last_season=_text(raw["last_season"], f"{label}.last_season"),
    )


def _artifact_repo(path: Path) -> Path:
    resolved = path.resolve()
    marker = ("data", "output", "model_performance", "run_selections")
    parts = resolved.parts
    for index in range(len(parts) - len(marker) + 1):
        if tuple(parts[index : index + len(marker)]) == marker:
            return Path(*parts[:index])
    raise ValueError("Backtest-run selection path is outside the canonical store.")


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{label} must be a JSON object with string keys.")
    return cast(dict[str, object], value)


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonempty string.")
    return value


def _integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer.")
    return value


def _datetime(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be an ISO timestamp string.")
    result = datetime.fromisoformat(value)
    offset = result.utcoffset()
    if result.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{label} must be timezone-aware UTC.")
    return result


def _digest(value: str, label: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest.")
    return value
