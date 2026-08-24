"""Tests for production-chain CLI reporting."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
from typer.testing import CliRunner

from gridiron_edge.cli.main import app
from gridiron_edge.ingest.odds.store import QUOTE_COLUMNS

runner = CliRunner()


def _canonical_quote_row(
    fetched_at: datetime,
    *,
    sportsbook: str,
    market: str = "spread",
    side: str = "home",
    line: float | None = 1.5,
) -> dict:
    """Build one canonical quote-observation row for CLI fixtures."""
    kickoff = datetime(2026, 9, 10, tzinfo=UTC)
    return {
        "fetched_at": fetched_at,
        "provider": "provider",
        "provider_event_id": "event",
        "sportsbook": sportsbook,
        "sportsbook_updated_at": fetched_at,
        "commence_time": kickoff,
        "is_live": False,
        "season": "2026-2027",
        "week": 1,
        "game_id": "2026_01_A_B",
        "game_date": datetime(2026, 9, 10, tzinfo=UTC),
        "away_team": "A",
        "home_team": "B",
        "market": market,
        "side": side,
        "odds": -110.0,
        "line": line,
    }


def _canonical_quotes(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=list(QUOTE_COLUMNS))


def test_help_registers_production_chain_group() -> None:
    result = runner.invoke(app, ["production-chain", "--help"])
    assert result.exit_code == 0
    assert "assess" in result.stdout
    assert "verify" in result.stdout


def test_assess_renders_independent_states(monkeypatch) -> None:
    from tests.unit.market.test_production_chain_preflight_store import _preflight

    monkeypatch.setattr(
        "gridiron_edge.market.production_chain_preflight.assess_production_chain_preflight",
        lambda **_kwargs: _preflight(),
    )
    result = runner.invoke(
        app,
        [
            "production-chain",
            "assess",
            "--season",
            "2026-2027",
            "--week",
            "1",
            "--assessed-at",
            datetime(2026, 8, 17, 18, 57, tzinfo=UTC).isoformat(),
        ],
    )
    assert result.exit_code == 0
    assert "MONEYLINE" in result.stdout
    assert "SPREAD" in result.stdout
    assert "TOTAL" in result.stdout
    assert "UNAVAILABLE" in result.stdout
    assert "PROOF INCOMPLETE" in result.stdout


def test_require_ready_exits_nonzero(monkeypatch) -> None:
    from tests.unit.market.test_production_chain_preflight_store import _preflight

    monkeypatch.setattr(
        "gridiron_edge.market.production_chain_preflight.assess_production_chain_preflight",
        lambda **_kwargs: _preflight(),
    )
    result = runner.invoke(
        app,
        ["production-chain", "assess", "--season", "2026-2027", "--week", "1", "--require-ready"],
    )
    assert result.exit_code == 1


def test_issue_candidates_requires_explicit_utc_timestamp() -> None:
    result = runner.invoke(
        app,
        [
            "production-chain",
            "issue-candidates",
            "--season",
            "2026-2027",
            "--week",
            "1",
            "--evaluated-at",
            "2026-08-18T14:45:00",
        ],
    )
    assert result.exit_code == 2
    assert "timezone-aware UTC" in result.stderr


def test_issue_candidates_uses_selected_product_events_and_history(monkeypatch) -> None:
    from datetime import datetime

    import pandas as pd

    from gridiron_edge.market.candidate_issuance import (
        CANDIDATE_ISSUANCE_SCHEMA_VERSION,
        CandidateIssuance,
        CandidateIssuanceReason,
        CandidateIssuanceRow,
        CandidateIssuanceState,
        candidate_issuance_id,
    )

    evaluated = datetime(2026, 8, 18, 14, 45, tzinfo=UTC)
    issuance_id = candidate_issuance_id(
        product_id="product-1",
        product_run_id="run-1",
        season="2026-2027",
        week=1,
        evaluated_at=evaluated,
    )
    rows = tuple(
        CandidateIssuanceRow(
            game_id="2026_01_A_B",
            market=market,
            side=side,
            provider="provider",
            provider_event_id="event",
            sportsbook=f"book-{index}",
            line=None if market == "moneyline" else 1.5,
            american_price=-110,
            fetched_at=evaluated,
            sportsbook_updated_at=evaluated,
            kickoff=datetime(2026, 9, 10, tzinfo=UTC),
            is_live=False,
            forecast_event_id=f"forecast-{index}",
            forecast_run_id="run-1",
            forecast_role="live",
            forecast_generated_at=evaluated,
            model_name="model",
            model_type="type",
            model_probability=0.6,
            expected_value=0.1,
            state=state,
            reason=reason,
        )
        for index, (market, side, state, reason) in enumerate(
            (
                (
                    "moneyline",
                    "home",
                    CandidateIssuanceState.CANDIDATE,
                    CandidateIssuanceReason.POSITIVE_EXPECTED_VALUE,
                ),
                (
                    "spread",
                    "away",
                    CandidateIssuanceState.NOT_CANDIDATE,
                    CandidateIssuanceReason.EXPECTED_VALUE_NOT_POSITIVE,
                ),
                (
                    "total",
                    "over",
                    CandidateIssuanceState.UNAVAILABLE,
                    CandidateIssuanceReason.MODEL_UNAVAILABLE,
                ),
            )
        )
    )
    issuance = CandidateIssuance(
        CANDIDATE_ISSUANCE_SCHEMA_VERSION,
        issuance_id,
        "product-1",
        "run-1",
        evaluated,
        "2026-2027",
        1,
        evaluated,
        rows,
    )
    product = pd.DataFrame({"product_run_id": ["run-1"]})
    events = pd.DataFrame({"event_id": ["forecast-1"]})
    before = datetime(2026, 8, 18, 11, 0, tzinfo=UTC)
    after = datetime(2026, 8, 18, 18, 0, tzinfo=UTC)
    quotes = _canonical_quotes(
        [
            _canonical_quote_row(before, sportsbook="dk"),
            _canonical_quote_row(before, sportsbook="fd"),
            _canonical_quote_row(evaluated, sportsbook="dk"),
            _canonical_quote_row(after, sportsbook="dk"),
        ]
    )
    observed: dict[str, object] = {}

    monkeypatch.setattr(
        "gridiron_edge.datasets.loaders.load_current_weekly_product",
        lambda repo, **kwargs: observed.update(product_scope=(repo, kwargs)) or product,
    )
    monkeypatch.setattr(
        "gridiron_edge.evaluation.forecast_store.load_forecast_events",
        lambda **kwargs: observed.update(event_scope=kwargs) or events,
    )
    monkeypatch.setattr(
        "gridiron_edge.ingest.odds.store.load_odds_ledger",
        lambda **kwargs: observed.update(quote_scope=kwargs) or quotes,
    )
    monkeypatch.setattr(
        "gridiron_edge.market.candidate_issuance.issue_pregame_candidates",
        lambda **kwargs: observed.update(issue_args=kwargs) or issuance,
    )

    result = runner.invoke(
        app,
        [
            "production-chain",
            "issue-candidates",
            "--season",
            "2026-2027",
            "--week",
            "1",
            "--evaluated-at",
            evaluated.isoformat(),
        ],
    )
    assert result.exit_code == 0
    assert observed["event_scope"]["run_id"] == "run-1"
    assert observed["quote_scope"]["season"] == "2026-2027"
    assert observed["quote_scope"]["week"] == 1
    assert observed["issue_args"]["product"] is product
    assert observed["issue_args"]["forecast_events"] is events
    assert observed["issue_args"]["evaluated_at"] == evaluated
    visible = observed["issue_args"]["quotes"]
    assert list(visible["fetched_at"]) == [before, before, evaluated]
    assert list(visible.columns) == list(QUOTE_COLUMNS)
    assert "MONEYLINE" in result.stdout
    assert "candidate         1" in result.stdout
    assert "SPREAD" in result.stdout
    assert "not candidate     1" in result.stdout
    assert "TOTAL" in result.stdout
    assert "unavailable       1" in result.stdout
    assert issuance_id in result.stdout


def test_issue_candidates_accepts_no_quotes_visible_by_evaluation_time(monkeypatch) -> None:
    from gridiron_edge.market.candidate_issuance import (
        CANDIDATE_ISSUANCE_SCHEMA_VERSION,
        CandidateIssuance,
        candidate_issuance_id,
    )

    evaluated = datetime(2026, 8, 18, 14, 45, tzinfo=UTC)
    after = datetime(2026, 8, 18, 18, 0, tzinfo=UTC)
    issuance_id = candidate_issuance_id(
        product_id="product-1",
        product_run_id="run-1",
        season="2026-2027",
        week=1,
        evaluated_at=evaluated,
    )
    issuance = CandidateIssuance(
        CANDIDATE_ISSUANCE_SCHEMA_VERSION,
        issuance_id,
        "product-1",
        "run-1",
        evaluated,
        "2026-2027",
        1,
        evaluated,
        (),  # zero rows
    )

    product = pd.DataFrame({"product_run_id": ["run-1"]})
    events = pd.DataFrame({"event_id": ["forecast-1"]})
    quotes = _canonical_quotes([_canonical_quote_row(after, sportsbook="dk")])
    observed: dict[str, object] = {}

    monkeypatch.setattr(
        "gridiron_edge.datasets.loaders.load_current_weekly_product",
        lambda repo, **kwargs: product,
    )
    monkeypatch.setattr(
        "gridiron_edge.evaluation.forecast_store.load_forecast_events",
        lambda **kwargs: events,
    )
    monkeypatch.setattr(
        "gridiron_edge.ingest.odds.store.load_odds_ledger",
        lambda **kwargs: quotes,
    )
    monkeypatch.setattr(
        "gridiron_edge.market.candidate_issuance.issue_pregame_candidates",
        lambda **kwargs: observed.update(issue_args=kwargs) or issuance,
    )

    result = runner.invoke(
        app,
        [
            "production-chain",
            "issue-candidates",
            "--season",
            "2026-2027",
            "--week",
            "1",
            "--evaluated-at",
            evaluated.isoformat(),
        ],
    )

    assert result.exit_code == 0
    visible = observed["issue_args"]["quotes"]
    assert visible.empty
    assert list(visible.columns) == list(QUOTE_COLUMNS)
    assert observed["issue_args"]["evaluated_at"] == evaluated
    assert issuance_id in result.stdout


def test_issue_candidates_write_persists_exact_issuance(monkeypatch, tmp_path) -> None:
    import pandas as pd
    from tests.unit.market.test_candidate_issuance_store import _issuance

    from gridiron_edge.ingest.odds.store import empty_quote_frame

    issuance = _issuance()
    stored = tmp_path / "issuance.json"
    monkeypatch.setattr(
        "gridiron_edge.datasets.loaders.load_current_weekly_product",
        lambda *_args, **_kwargs: pd.DataFrame({"product_run_id": ["run-1"]}),
    )
    monkeypatch.setattr(
        "gridiron_edge.evaluation.forecast_store.load_forecast_events",
        lambda **_kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        "gridiron_edge.ingest.odds.store.load_odds_ledger",
        lambda **_kwargs: empty_quote_frame(),
    )
    monkeypatch.setattr(
        "gridiron_edge.market.candidate_issuance.issue_pregame_candidates",
        lambda **_kwargs: issuance,
    )
    monkeypatch.setattr(
        "gridiron_edge.market.candidate_issuance_store.write_candidate_issuance",
        lambda value, **_kwargs: stored if value == issuance else None,
    )

    result = runner.invoke(
        app,
        [
            "production-chain",
            "issue-candidates",
            "--season",
            "2026-2027",
            "--week",
            "1",
            "--evaluated-at",
            issuance.evaluated_at.isoformat(),
            "--write",
        ],
    )
    assert result.exit_code == 0
    assert f"stored              {stored}" in result.stdout


def test_help_registers_governance_commands() -> None:
    result = runner.invoke(app, ["production-chain", "--help"])
    assert result.exit_code == 0
    assert "create-governance" in result.stdout
    assert "verify-governance" in result.stdout


def test_help_registers_policy_and_evaluation_commands() -> None:
    result = runner.invoke(app, ["production-chain", "--help"])
    assert result.exit_code == 0
    assert "derive-policy" in result.stdout
    assert "evaluate-recommendations" in result.stdout


def test_evaluate_recommendations_requires_exact_utc_decision_time() -> None:
    result = runner.invoke(
        app,
        [
            "production-chain",
            "evaluate-recommendations",
            "--issuance-id",
            "a" * 64,
            "--policy-id",
            "b" * 64,
            "--decision-at",
            "2026-08-18T16:00:00",
        ],
    )
    assert result.exit_code == 2
