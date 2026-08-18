"""Immutable JSON persistence for production-chain preflight assessments."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
from typing import Final, cast
from uuid import uuid4

from gridiron_edge.core.settings import get_settings
from gridiron_edge.market.production_chain_preflight import (
    MarketFamilyProductionPreflight,
    ProductionChainComponent,
    ProductionChainPreflight,
    ProductionClvKind,
    ProductionMarketFamily,
    ProofComponentState,
    validate_production_chain_preflight,
)

PREFLIGHT_STORE_SCHEMA_VERSION: Final[int] = 1


def production_chain_preflight_root(repo: Path | None = None) -> Path:
    """Return the immutable production-chain preflight storage root."""
    root = repo or get_settings().repo_root
    return root / "data/output/production_chain_preflight"


def production_chain_preflight_id(preflight: ProductionChainPreflight) -> str:
    """Return the deterministic identity of complete preflight evidence."""
    validate_production_chain_preflight(preflight)
    return sha256(_canonical_bytes(_payload(preflight))).hexdigest()


def production_chain_preflight_path(
    schema_version: int,
    preflight_id: str,
    *,
    repo: Path | None = None,
) -> Path:
    """Return the identity-addressed path for one preflight assessment."""
    if schema_version != PREFLIGHT_STORE_SCHEMA_VERSION:
        raise ValueError("Unsupported production-chain preflight store schema version.")
    identity = _digest(preflight_id, "preflight_id")
    return (
        production_chain_preflight_root(repo)
        / f"schema={schema_version}"
        / "assessments"
        / f"{identity}.json"
    )


def write_production_chain_preflight(
    preflight: ProductionChainPreflight,
    *,
    repo: Path | None = None,
) -> Path:
    """Persist one immutable assessment or accept an exact replay."""
    validate_production_chain_preflight(preflight)
    identity = production_chain_preflight_id(preflight)
    path = production_chain_preflight_path(PREFLIGHT_STORE_SCHEMA_VERSION, identity, repo=repo)
    encoded = (
        json.dumps(
            {
                "store_schema_version": PREFLIGHT_STORE_SCHEMA_VERSION,
                "preflight_id": identity,
                "preflight": _payload(preflight),
            },
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise ValueError("Preflight identity cannot be reused with different content.")
        return path
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(encoded, encoding="utf-8")
        if path.exists():
            if path.read_text(encoding="utf-8") != encoded:
                raise ValueError("Preflight identity cannot be reused with different content.")
        else:
            temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def read_production_chain_preflight(path: Path) -> ProductionChainPreflight:
    """Read and strictly validate one exact stored preflight assessment."""
    raw = _object(json.loads(path.read_text(encoding="utf-8")), "preflight artifact")
    _exact_keys(raw, {"store_schema_version", "preflight_id", "preflight"}, "Preflight artifact")
    version = _integer(raw["store_schema_version"], "store_schema_version")
    if version != PREFLIGHT_STORE_SCHEMA_VERSION:
        raise ValueError("Unsupported production-chain preflight store schema version.")
    identity = _digest(_text(raw["preflight_id"], "preflight_id"), "preflight_id")
    preflight = _preflight(raw["preflight"])
    if identity != production_chain_preflight_id(preflight):
        raise ValueError("Stored preflight identity does not match canonical content.")
    expected = production_chain_preflight_path(version, identity, repo=_artifact_repo(path))
    if path.resolve() != expected.resolve():
        raise ValueError("Preflight path and embedded identity disagree.")
    return preflight


def _payload(preflight: ProductionChainPreflight) -> dict[str, object]:
    return {
        "schema_version": preflight.schema_version,
        "season": preflight.season,
        "week": preflight.week,
        "assessed_at": preflight.assessed_at.isoformat(),
        "moneyline": _family_payload(preflight.moneyline),
        "spread": _family_payload(preflight.spread),
        "total": _family_payload(preflight.total),
    }


def _family_payload(family: MarketFamilyProductionPreflight) -> dict[str, object]:
    return {
        "market": family.market.value,
        "components": [_component_payload(value) for value in family.components],
    }


def _component_payload(component: ProductionChainComponent) -> dict[str, object]:
    return {
        "component_id": component.component_id,
        "state": component.state.value,
        "reason": component.reason,
        "evidence_ids": list(component.evidence_ids),
        "timestamps": [value.isoformat() for value in component.timestamps],
        "observation_count": component.observation_count,
        "distinct_timestamp_count": component.distinct_timestamp_count,
        "provider": component.provider,
        "sportsbook": component.sportsbook,
        "kickoff": None if component.kickoff is None else component.kickoff.isoformat(),
        "clv_kind": None if component.clv_kind is None else component.clv_kind.value,
    }


def _preflight(value: object) -> ProductionChainPreflight:
    data = _object(value, "preflight")
    _exact_keys(
        data,
        {"schema_version", "season", "week", "assessed_at", "moneyline", "spread", "total"},
        "Preflight",
    )
    result = ProductionChainPreflight(
        _integer(data["schema_version"], "schema_version"),
        _text(data["season"], "season"),
        _integer(data["week"], "week"),
        _datetime(data["assessed_at"], "assessed_at"),
        _family(data["moneyline"]),
        _family(data["spread"]),
        _family(data["total"]),
    )
    validate_production_chain_preflight(result)
    return result


def _family(value: object) -> MarketFamilyProductionPreflight:
    data = _object(value, "family")
    _exact_keys(data, {"market", "components"}, "Family")
    components = _list(data["components"], "components")
    return MarketFamilyProductionPreflight(
        ProductionMarketFamily(_text(data["market"], "market")),
        tuple(_component(item) for item in components),
    )


def _component(value: object) -> ProductionChainComponent:
    data = _object(value, "component")
    expected = set(asdict(_empty_component()))
    _exact_keys(data, expected, "Component")
    return ProductionChainComponent(
        component_id=_text(data["component_id"], "component_id"),
        state=ProofComponentState(_text(data["state"], "state")),
        reason=_text(data["reason"], "reason"),
        evidence_ids=tuple(
            _text(item, "evidence_id") for item in _list(data["evidence_ids"], "evidence_ids")
        ),
        timestamps=tuple(
            _datetime(item, "timestamp") for item in _list(data["timestamps"], "timestamps")
        ),
        observation_count=_optional_integer(data["observation_count"], "observation_count"),
        distinct_timestamp_count=_optional_integer(
            data["distinct_timestamp_count"], "distinct_timestamp_count"
        ),
        provider=_optional_text(data["provider"], "provider"),
        sportsbook=_optional_text(data["sportsbook"], "sportsbook"),
        kickoff=None if data["kickoff"] is None else _datetime(data["kickoff"], "kickoff"),
        clv_kind=None
        if data["clv_kind"] is None
        else ProductionClvKind(_text(data["clv_kind"], "clv_kind")),
    )


def _empty_component() -> ProductionChainComponent:
    return ProductionChainComponent("", ProofComponentState.UNAVAILABLE, "x")


def _artifact_repo(path: Path) -> Path:
    resolved = path.resolve()
    marker = ("data", "output", "production_chain_preflight")
    parts = resolved.parts
    for index in range(len(parts) - len(marker) + 1):
        if tuple(parts[index : index + len(marker)]) == marker:
            return Path(*parts[:index])
    raise ValueError("Preflight path is outside the canonical store.")


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _digest(value: str, label: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest.")
    return value


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{label} must be a JSON object with string keys.")
    return cast(dict[str, object], value)


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list.")
    return value


def _exact_keys(data: dict[str, object], expected: set[str], label: str) -> None:
    if set(data) != expected:
        raise ValueError(f"{label} keys do not match the current schema.")


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonempty string.")
    return value


def _optional_text(value: object, label: str) -> str | None:
    return None if value is None else _text(value, label)


def _integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer.")
    return value


def _optional_integer(value: object, label: str) -> int | None:
    return None if value is None else _integer(value, label)


def _datetime(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be an ISO timestamp string.")
    result = datetime.fromisoformat(value)
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must be timezone-aware UTC.")
    return result
