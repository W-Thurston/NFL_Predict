"""Integration tests for recorded Portfolio wagers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from fastapi.testclient import TestClient
import pandas as pd
import pytest

from gridiron_edge.api.app import create_app
from gridiron_edge.api.deps import settings_dependency


@dataclass
class _Settings:
    repo_root: Path


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    app = create_app()
    app.dependency_overrides[settings_dependency] = lambda: _Settings(tmp_path)
    return TestClient(app)


def test_records_manual_wager_without_fabricated_provenance(
    client: TestClient,
    tmp_path: Path,
) -> None:
    response = client.post(
        "/portfolio/bets",
        json={
            "game_id": "2026_01_KC_LAC",
            "market_type": "moneyline",
            "side": "away",
            "odds": 175,
            "stake": 25.0,
            "book": "fanduel",
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["bet"]["recommended_bet_result_id"] is None
    assert body["bankroll_transaction_id"]
    assert body["message"] == ("Wager recorded in Gridiron Edge. No sportsbook wager was placed.")
    txns = pd.read_parquet(tmp_path / "data/betting/bankroll_txn.parquet")
    assert txns.iloc[0]["reference_id"] == body["bet"]["bet_id"]


def test_rejects_partial_recommendation_identity(client: TestClient) -> None:
    response = client.post(
        "/portfolio/bets",
        json={
            "game_id": "2026_01_KC_LAC",
            "market_type": "moneyline",
            "side": "away",
            "odds": 175,
            "stake": 25.0,
            "book": "fanduel",
            "recommended_bet_result_id": "result-1",
        },
    )
    assert response.status_code == 422
