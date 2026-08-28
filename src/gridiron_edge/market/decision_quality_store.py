# src/gridiron_edge/market/decision_quality_store.py

"""Immutable JSON persistence for decision-quality evaluations.

Mirrors the architectural pattern established by
recommended_bet_result_store.py (schema-versioned identity paths, exact
JSON key validation, strict enum/UTC decoding, immutable create-or-
exact-replay, temp-file-plus-hard-link publication, path-versus-identity
verification) as an independent implementation of the same pattern --
this store does not import that module's private helpers. Semantic
validation and canonical identity are owned by decision_quality.py, not
reimplemented here.
"""

from __future__ import annotations

from dataclasses import fields
from datetime import datetime
import json
import os
from pathlib import Path
from uuid import uuid4

from gridiron_edge.core.settings import get_settings
from gridiron_edge.market.candidate_outcome import CandidateOutcome
from gridiron_edge.market.decision_quality import (
    DecisionQualityCheck,
    DecisionQualityEvaluation,
    DecisionQualityStatus,
    validate_decision_quality_evaluation,
)


def decision_quality_root(repo: Path | None = None) -> Path:
    """Return the immutable decision-quality evaluation storage root."""
    root = repo or get_settings().repo_root
    return root / "data" / "output" / "decision_quality_evaluations"


def decision_quality_evaluation_path(
    schema_version: int, evaluation_id: str, *, repo: Path | None = None
) -> Path:
    """Return the identity-addressed path for one persisted evaluation."""
    return (
        decision_quality_root(repo)
        / f"schema={_schema(schema_version)}"
        / "evaluations"
        / f"{_digest_format(evaluation_id, 'evaluation_id')}.json"
    )


def write_decision_quality_evaluation(
    evaluation: DecisionQualityEvaluation, *, repo: Path | None = None
) -> Path:
    """Persist one immutable decision-quality evaluation.

    Or accept an exact replay at the same identity. Raises if the
    identity already exists with different content.
    """
    validate_decision_quality_evaluation(evaluation)
    path = decision_quality_evaluation_path(
        evaluation.schema_version, evaluation.evaluation_id, repo=repo
    )
    payload = _encode(evaluation)
    _immutable_write(path, payload, label="Decision-quality evaluation")
    return path


def read_decision_quality_evaluation(path: Path) -> DecisionQualityEvaluation:
    """Read, decode, and validate one persisted decision-quality evaluation.

    Confirms the file's path agrees with its own embedded identity
    before returning.
    """
    raw = _json_object(path)
    evaluation = _decode_evaluation(raw)
    validate_decision_quality_evaluation(evaluation)
    repo = _artifact_repo(path)
    expected = decision_quality_evaluation_path(
        evaluation.schema_version, evaluation.evaluation_id, repo=repo
    )
    if path.resolve() != expected.resolve():
        raise ValueError("Decision-quality evaluation path and embedded identity disagree.")
    return evaluation


def _immutable_write(path: Path, payload: object, *, label: str) -> None:
    """Serialize, then publish via a colocated temp file and hard link.

    An existing identity with identical content is accepted silently
    (exact replay); an existing identity with different content raises.
    """
    encoded = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise ValueError(f"{label} identity cannot be reused with different content.")
        return
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(encoded, encoding="utf-8")
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_text(encoding="utf-8") != encoded:
                raise ValueError(
                    f"{label} identity cannot be reused with different content."
                ) from None
    finally:
        temporary.unlink(missing_ok=True)


def _encode(evaluation: DecisionQualityEvaluation) -> dict[str, object]:
    """Canonical JSON encoding for one DecisionQualityEvaluation."""
    return {
        "schema_version": evaluation.schema_version,
        "evaluation_id": evaluation.evaluation_id,
        "evaluated_at": evaluation.evaluated_at.isoformat(),
        "result_id": evaluation.result_id,
        "recommendation_evaluation_id": evaluation.recommendation_evaluation_id,
        "candidate_reference_id": evaluation.candidate_reference_id,
        "policy_id": evaluation.policy_id,
        "policy_schema_version": evaluation.policy_schema_version,
        "issuance_id": evaluation.issuance_id,
        "portfolio_snapshot_id": evaluation.portfolio_snapshot_id,
        "correlation_evidence_fingerprint": evaluation.correlation_evidence_fingerprint,
        "checks": [
            {
                "check_id": c.check_id,
                "mandatory": c.mandatory,
                "status": c.status.value,
                "reason": c.reason,
            }
            for c in evaluation.checks
        ],
        "decision_status": evaluation.decision_status.value,
        "realized_outcome": evaluation.realized_outcome.value,
    }


def _decode_evaluation(raw: object) -> DecisionQualityEvaluation:
    """Strictly decode one DecisionQualityEvaluation from exact JSON keys."""
    if not isinstance(raw, dict):
        raise ValueError("DecisionQualityEvaluation must be a JSON object.")
    expected_keys = {field.name for field in fields(DecisionQualityEvaluation)}
    if set(raw) != expected_keys:
        raise ValueError("DecisionQualityEvaluation keys do not match the current schema.")

    checks_raw = raw["checks"]
    if not isinstance(checks_raw, list):
        raise ValueError("checks must be a JSON list.")
    checks = tuple(_decode_check(item) for item in checks_raw)

    return DecisionQualityEvaluation(
        schema_version=_strict_int(raw["schema_version"], "schema_version"),
        evaluation_id=_strict_text(raw["evaluation_id"], "evaluation_id"),
        evaluated_at=_strict_datetime(raw["evaluated_at"], "evaluated_at"),
        result_id=_strict_text(raw["result_id"], "result_id"),
        recommendation_evaluation_id=_strict_text(
            raw["recommendation_evaluation_id"], "recommendation_evaluation_id"
        ),
        candidate_reference_id=_strict_text(
            raw["candidate_reference_id"], "candidate_reference_id"
        ),
        policy_id=_strict_text(raw["policy_id"], "policy_id"),
        policy_schema_version=_strict_int(raw["policy_schema_version"], "policy_schema_version"),
        issuance_id=_strict_text(raw["issuance_id"], "issuance_id"),
        portfolio_snapshot_id=_optional_text(raw["portfolio_snapshot_id"], "portfolio_snapshot_id"),
        correlation_evidence_fingerprint=_optional_text(
            raw["correlation_evidence_fingerprint"], "correlation_evidence_fingerprint"
        ),
        checks=checks,
        decision_status=DecisionQualityStatus(
            _strict_text(raw["decision_status"], "decision_status")
        ),
        realized_outcome=CandidateOutcome(
            _strict_text(raw["realized_outcome"], "realized_outcome")
        ),
    )


def _decode_check(raw: object) -> DecisionQualityCheck:
    if not isinstance(raw, dict):
        raise ValueError("DecisionQualityCheck must be a JSON object.")
    expected_keys = {field.name for field in fields(DecisionQualityCheck)}
    if set(raw) != expected_keys:
        raise ValueError("DecisionQualityCheck keys do not match the current schema.")
    mandatory = raw["mandatory"]
    if not isinstance(mandatory, bool):
        raise ValueError("mandatory must be boolean.")
    return DecisionQualityCheck(
        check_id=_strict_text(raw["check_id"], "check_id"),
        mandatory=mandatory,
        status=DecisionQualityStatus(_strict_text(raw["status"], "status")),
        reason=_strict_text(raw["reason"], "reason"),
    )


def _json_object(path: Path) -> dict[str, object]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Decision-quality artifact must contain a JSON object.")
    return raw


def _artifact_repo(path: Path) -> Path:
    resolved = path.resolve()
    marker = ("data", "output", "decision_quality_evaluations")
    parts = resolved.parts
    for index in range(len(parts) - len(marker) + 1):
        if tuple(parts[index : index + len(marker)]) == marker:
            return Path(*parts[:index])
    raise ValueError("Decision-quality artifact is outside the canonical store.")


def _schema(value: int) -> int:
    if isinstance(value, bool) or value < 1:
        raise ValueError("schema_version must be a positive integer.")
    return value


def _digest_format(value: str, label: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest.")
    return value


def _strict_text(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a string.")
    return value


def _optional_text(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _strict_text(value, label)


def _strict_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer.")
    return value


def _strict_datetime(value: object, label: str) -> datetime:
    text = _strict_text(value, label)
    result = datetime.fromisoformat(text)
    offset = result.utcoffset()
    if result.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{label} must be timezone-aware UTC.")
    return result
