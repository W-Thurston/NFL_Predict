"""Exact immutable forecast-run selection for historical backtests."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from hashlib import sha256
import json
from typing import Final

from pandas import DataFrame

BACKTEST_RUN_SELECTION_SCHEMA_VERSION: Final[int] = 1
_REQUIRED_EVENT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "event_id",
        "run_id",
        "role",
        "season",
        "week",
        "game_id",
        "model_name",
        "model_type",
    }
)


@dataclass(frozen=True, slots=True)
class BacktestRunComponent:
    """One exact immutable backfilled forecast run selected for a model family."""

    model_name: str
    model_type: str
    run_id: str
    event_count: int
    first_season: str
    last_season: str


@dataclass(frozen=True, slots=True)
class BacktestRunSelection:
    """Exact champion-algorithm runs selected for one historical backtest."""

    schema_version: int
    selection_id: str
    created_at: datetime
    win: BacktestRunComponent
    total: BacktestRunComponent


def create_backtest_run_selection(
    *,
    events: DataFrame,
    champion_models: dict[str, str],
    win_run_id: str,
    total_run_id: str,
    created_at: datetime,
) -> BacktestRunSelection:
    """Create a content-addressed selection from two caller-named exact runs."""
    _validate_created_at(created_at)
    missing = sorted(_REQUIRED_EVENT_COLUMNS - set(events.columns))
    if missing:
        raise ValueError("Forecast events are missing required columns: " + ", ".join(missing))
    if events["event_id"].astype(str).duplicated().any():
        raise ValueError("Forecast events contain duplicate event IDs.")
    expected_champions = {"win_prob", "total"}
    if set(champion_models) != expected_champions:
        raise ValueError("Champion models must contain exactly win_prob and total.")

    win = _component(
        events,
        model_name="win_prob",
        model_type=champion_models["win_prob"],
        run_id=win_run_id,
    )
    total = _component(
        events,
        model_name="total",
        model_type=champion_models["total"],
        run_id=total_run_id,
    )
    payload = {
        "schema_version": BACKTEST_RUN_SELECTION_SCHEMA_VERSION,
        "created_at": created_at.isoformat(),
        "win": asdict(win),
        "total": asdict(total),
    }
    selection_id = sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    selection = BacktestRunSelection(
        schema_version=BACKTEST_RUN_SELECTION_SCHEMA_VERSION,
        selection_id=selection_id,
        created_at=created_at,
        win=win,
        total=total,
    )
    validate_backtest_run_selection(selection)
    return selection


def validate_backtest_run_selection(selection: BacktestRunSelection) -> None:
    """Validate one selection contract independently of repository state."""
    if selection.schema_version != BACKTEST_RUN_SELECTION_SCHEMA_VERSION:
        raise ValueError("Unsupported backtest-run selection schema version.")
    if len(selection.selection_id) != 64 or any(
        character not in "0123456789abcdef" for character in selection.selection_id
    ):
        raise ValueError("selection_id must be a lowercase SHA-256 digest.")
    _validate_created_at(selection.created_at)
    for label, component in (("win", selection.win), ("total", selection.total)):
        for field_name, value in (
            ("model_name", component.model_name),
            ("model_type", component.model_type),
            ("run_id", component.run_id),
            ("first_season", component.first_season),
            ("last_season", component.last_season),
        ):
            if not value.strip():
                raise ValueError(f"{label}.{field_name} must not be empty.")
        if component.event_count < 1:
            raise ValueError(f"{label}.event_count must be positive.")
        if component.first_season > component.last_season:
            raise ValueError(f"{label} season range is reversed.")
    if selection.win.model_name != "win_prob":
        raise ValueError("Win component must use model_name='win_prob'.")
    if selection.total.model_name != "total":
        raise ValueError("Total component must use model_name='total'.")

    expected_payload = {
        "schema_version": selection.schema_version,
        "created_at": selection.created_at.isoformat(),
        "win": asdict(selection.win),
        "total": asdict(selection.total),
    }
    expected_id = sha256(
        json.dumps(expected_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if selection.selection_id != expected_id:
        raise ValueError("selection_id does not match selection content.")


def _component(
    events: DataFrame,
    *,
    model_name: str,
    model_type: str,
    run_id: str,
) -> BacktestRunComponent:
    if not run_id.strip():
        raise ValueError(f"{model_name} run_id must not be empty.")
    rows = events.loc[events["run_id"].astype(str).eq(run_id), :].copy()
    if rows.empty:
        raise ValueError(f"Selected backfill run is missing: {run_id!r}.")
    if set(rows["role"].astype(str)) != {"backfilled"}:
        raise ValueError(f"Selected run {run_id!r} must contain only backfilled events.")
    if set(rows["model_name"].astype(str)) != {model_name}:
        raise ValueError(f"Selected run {run_id!r} has the wrong model_name.")
    if set(rows["model_type"].astype(str)) != {model_type}:
        raise ValueError(f"Selected run {run_id!r} does not match the champion model_type.")
    if rows["game_id"].astype(str).duplicated().any():
        raise ValueError(f"Selected run {run_id!r} contains duplicate game IDs.")
    seasons = sorted(rows["season"].astype(str).unique().tolist())
    if not seasons:
        raise ValueError(f"Selected run {run_id!r} contains no season values.")
    return BacktestRunComponent(
        model_name=model_name,
        model_type=model_type,
        run_id=run_id,
        event_count=len(rows),
        first_season=seasons[0],
        last_season=seasons[-1],
    )


def _validate_created_at(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("created_at must be timezone-aware UTC.")
