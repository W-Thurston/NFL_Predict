# tests/unit/betting/test_recording.py
"""Tests for rollback-safe recorded-wager orchestration."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
import threading
import time

import pandas as pd
import pytest

from gridiron_edge.betting.ledger import load_bets
from gridiron_edge.betting.recording import (
    RecommendationRecordingEvidence,
    RecordWagerCommand,
    record_wager,
)


def command() -> RecordWagerCommand:
    return RecordWagerCommand(
        game_id="2026_01_KC_LAC",
        market_type="moneyline",
        side="away",
        odds=175,
        stake=25.0,
        book="fanduel",
    )


def test_records_ledger_and_bankroll_together(tmp_path: Path) -> None:
    recorded = record_wager(command(), repo=tmp_path)
    row = load_bets(repo=tmp_path).iloc[0]
    txns = pd.read_parquet(tmp_path / "data/betting/bankroll_txn.parquet")
    assert row["bet_id"] == recorded.bet_id
    assert txns.iloc[0]["reference_id"] == recorded.bet_id
    assert txns.iloc[0]["txn_id"] == recorded.bankroll_transaction_id


def test_preserves_recorded_and_reference_terms(tmp_path: Path) -> None:
    evidence = RecommendationRecordingEvidence(
        result_id="result-1",
        evaluation_id="evaluation-1",
        candidate_reference_id="candidate-1",
        policy_id="policy-1",
        game_id="2026_01_KC_LAC",
        market_type="moneyline",
        side="away",
        provider="the_odds_api",
        provider_event_id="event-1",
        sportsbook="draftkings",
        fetched_at=datetime(2026, 9, 1, 12, tzinfo=UTC),
        sportsbook_updated_at=None,
        commence_time=None,
        american_odds=170,
        line=None,
        model_name="win_prob",
        model_type="random_forest",
        model_probability=0.42,
        expected_value=0.08,
    )
    record_wager(
        replace(command(), recommendation=evidence),
        repo=tmp_path,
    )
    row = load_bets(repo=tmp_path).iloc[0]
    assert row["odds"] == 175
    assert row["book"] == "fanduel"
    assert row["reference_american_odds"] == 170
    assert row["reference_sportsbook"] == "draftkings"
    assert row["recommended_bet_result_id"] == "result-1"


def test_invalid_command_creates_neither_artifact(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="positive"):
        record_wager(
            replace(command(), stake=0.0),
            repo=tmp_path,
        )
    assert not (tmp_path / "data/betting/bet_ledger.parquet").exists()
    assert not (tmp_path / "data/betting/bankroll_txn.parquet").exists()


def test_bankroll_failure_removes_new_ledger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(*_args: object, **_kwargs: object) -> str:
        raise RuntimeError("bankroll write failed")

    monkeypatch.setattr("gridiron_edge.betting.recording.record_bet_placed", fail)
    with pytest.raises(RuntimeError, match="bankroll write failed"):
        record_wager(command(), repo=tmp_path)
    assert not (tmp_path / "data/betting/bet_ledger.parquet").exists()
    assert not (tmp_path / "data/betting/bankroll_txn.parquet").exists()


def test_concurrent_write_survives_a_failed_recorded_wager_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A record_wager rollback must not discard a concurrent completed write."""
    from gridiron_edge.betting.ledger import log_bet

    entered_critical_section = threading.Event()

    def failing_record_bet_placed(*_args: object, **_kwargs: object) -> str:
        entered_critical_section.set()
        time.sleep(0.05)
        raise RuntimeError("simulated bankroll failure")

    monkeypatch.setattr(
        "gridiron_edge.betting.recording.record_bet_placed",
        failing_record_bet_placed,
    )

    errors: list[Exception] = []
    concurrent_bet_id: dict[str, str] = {}

    def concurrent_writer() -> None:
        entered_critical_section.wait()
        try:
            concurrent_bet_id["id"] = log_bet(
                game_id="2026_02_EEE_FFF",
                market_type="moneyline",
                side="home",
                odds=-120,
                stake=10.0,
                book="draftkings",
                repo=tmp_path,
            )
        except Exception as exc:
            errors.append(exc)

    writer_thread = threading.Thread(target=concurrent_writer)
    writer_thread.start()

    with pytest.raises(RuntimeError, match="simulated bankroll failure"):
        record_wager(command(), repo=tmp_path)

    writer_thread.join()

    assert not errors
    assert "id" in concurrent_bet_id
    df = load_bets(repo=tmp_path)
    assert list(df["bet_id"]) == [concurrent_bet_id["id"]]
