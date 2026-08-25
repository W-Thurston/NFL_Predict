"""Tests for immutable production-chain preflight persistence."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
import json
from pathlib import Path

import pytest

from gridiron_edge.market.production_chain_preflight import (
    PRODUCTION_CHAIN_COMPONENT_IDS,
    PRODUCTION_CHAIN_PREFLIGHT_SCHEMA_VERSION,
    MarketFamilyProductionPreflight,
    ProductionChainComponent,
    ProductionChainPreflight,
    ProductionMarketFamily,
    ProofComponentState,
)
from gridiron_edge.market.production_chain_preflight_store import (
    production_chain_preflight_id,
    production_chain_preflight_path,
    read_production_chain_preflight,
    write_production_chain_preflight,
)

NOW = datetime(2026, 8, 17, 18, 57, tzinfo=UTC)


def _family(market: ProductionMarketFamily) -> MarketFamilyProductionPreflight:
    return MarketFamilyProductionPreflight(
        market,
        tuple(
            ProductionChainComponent(value, ProofComponentState.UNAVAILABLE, "Absent.")
            for value in PRODUCTION_CHAIN_COMPONENT_IDS
        ),
    )


def _preflight() -> ProductionChainPreflight:
    return ProductionChainPreflight(
        PRODUCTION_CHAIN_PREFLIGHT_SCHEMA_VERSION,
        "2026-2027",
        1,
        NOW,
        _family(ProductionMarketFamily.MONEYLINE),
        _family(ProductionMarketFamily.SPREAD),
        _family(ProductionMarketFamily.TOTAL),
    )


def test_identity_is_deterministic_and_evidence_sensitive() -> None:
    value = _preflight()
    assert production_chain_preflight_id(value) == production_chain_preflight_id(value)
    changed = replace(value, week=2)
    assert production_chain_preflight_id(changed) != production_chain_preflight_id(value)


def test_exact_round_trip_and_idempotent_replay(tmp_path: Path) -> None:
    value = _preflight()
    path = write_production_chain_preflight(value, repo=tmp_path)
    first = path.read_bytes()
    assert read_production_chain_preflight(path) == value
    assert write_production_chain_preflight(value, repo=tmp_path) == path
    assert path.read_bytes() == first


def test_conflicting_content_and_filename_mismatch_are_rejected(tmp_path: Path) -> None:
    value = _preflight()
    path = write_production_chain_preflight(value, repo=tmp_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["preflight"]["week"] = 2
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="identity"):
        read_production_chain_preflight(path)


def test_malformed_schema_is_rejected(tmp_path: Path) -> None:
    path = write_production_chain_preflight(_preflight(), repo=tmp_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["extra"] = True
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="keys"):
        read_production_chain_preflight(path)


def test_read_does_not_assess_repository(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = write_production_chain_preflight(_preflight(), repo=tmp_path)
    monkeypatch.setattr(
        "gridiron_edge.market.production_chain_preflight.assess_production_chain_preflight",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not assess")),
    )
    assert read_production_chain_preflight(path) == _preflight()


def test_publication_race_rejects_conflicting_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    value = _preflight()
    identity = production_chain_preflight_id(value)
    path = production_chain_preflight_path(value.schema_version, identity, repo=tmp_path)

    def racing_link(src: Path, dst: Path) -> None:
        destination = Path(dst)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text('{"conflicting": true}', encoding="utf-8")
        raise FileExistsError

    monkeypatch.setattr(
        "gridiron_edge.market.production_chain_preflight_store.os.link", racing_link
    )
    with pytest.raises(
        ValueError, match="Preflight identity cannot be reused with different content"
    ):
        write_production_chain_preflight(value, repo=tmp_path)
    assert path.read_text(encoding="utf-8") == '{"conflicting": true}'
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []


def test_publication_race_accepts_identical_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    value = _preflight()
    identity = production_chain_preflight_id(value)
    path = production_chain_preflight_path(value.schema_version, identity, repo=tmp_path)

    def racing_link(src: Path, dst: Path) -> None:
        destination = Path(dst)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(Path(src).read_text(encoding="utf-8"), encoding="utf-8")
        raise FileExistsError

    monkeypatch.setattr(
        "gridiron_edge.market.production_chain_preflight_store.os.link", racing_link
    )
    assert write_production_chain_preflight(value, repo=tmp_path) == path
    assert read_production_chain_preflight(path) == value
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []


def test_pre_publication_failure_leaves_no_destination_or_temporary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    value = _preflight()

    def failing_link(src: Path, dst: Path) -> None:
        raise OSError("simulated pre-publication failure")

    monkeypatch.setattr(
        "gridiron_edge.market.production_chain_preflight_store.os.link", failing_link
    )
    with pytest.raises(OSError, match="simulated pre-publication failure"):
        write_production_chain_preflight(value, repo=tmp_path)
    identity = production_chain_preflight_id(value)
    path = production_chain_preflight_path(value.schema_version, identity, repo=tmp_path)
    assert not path.exists()
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []
