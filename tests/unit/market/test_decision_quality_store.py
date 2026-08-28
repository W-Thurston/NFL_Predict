# tests/unit/market/test_decision_quality_store.py
"""Tests for immutable decision-quality evaluation persistence."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
import json
from pathlib import Path

import pytest

from gridiron_edge.market.candidate_outcome import CandidateOutcome
from gridiron_edge.market.decision_quality import (
    DecisionQualityCheck,
    DecisionQualityEvaluation,
    DecisionQualityStatus,
    decision_quality_evaluation_id,
    validate_decision_quality_evaluation,
)
from gridiron_edge.market.decision_quality_store import (
    decision_quality_evaluation_path,
    read_decision_quality_evaluation,
    write_decision_quality_evaluation,
)
from gridiron_edge.market.recommendation_policy import (
    RECOMMENDATION_POLICY_SCHEMA_VERSION,
)

_EVALUATED_AT = datetime(2026, 9, 1, 12, 10, tzinfo=UTC)


def _checks() -> tuple[DecisionQualityCheck, ...]:
    return (
        DecisionQualityCheck("result_integrity", True, DecisionQualityStatus.CONSISTENT, "ok"),
        DecisionQualityCheck(
            "recommendation_evaluation_reference", True, DecisionQualityStatus.CONSISTENT, "ok"
        ),
        DecisionQualityCheck("policy_reference", True, DecisionQualityStatus.CONSISTENT, "ok"),
        DecisionQualityCheck("candidate_reference", True, DecisionQualityStatus.CONSISTENT, "ok"),
        DecisionQualityCheck(
            "allocation_recomputation", False, DecisionQualityStatus.UNAVAILABLE, "absent"
        ),
    )


def _evaluation(**overrides: object) -> DecisionQualityEvaluation:
    checks = overrides.pop("checks", _checks())
    decision_status = overrides.pop("decision_status", DecisionQualityStatus.CONSISTENT)
    base: dict[str, object] = {
        "schema_version": 1,
        "result_id": "1" * 64,
        "recommendation_evaluation_id": "b" * 64,
        "candidate_reference_id": "2" * 64 + ":" + "3" * 64,
        "policy_id": "c" * 64,
        "policy_schema_version": RECOMMENDATION_POLICY_SCHEMA_VERSION,
        "issuance_id": "d" * 64,
        "portfolio_snapshot_id": None,
        "correlation_evidence_fingerprint": None,
        "checks": checks,
        "decision_status": decision_status,
        "realized_outcome": CandidateOutcome.UNAVAILABLE,
        "evaluated_at": _EVALUATED_AT,
    }
    base.update(overrides)

    provisional = DecisionQualityEvaluation(evaluation_id="0" * 64, **base)
    evaluation_id = decision_quality_evaluation_id(provisional)
    return replace(provisional, evaluation_id=evaluation_id)


class TestRoundTrip:
    def test_exact_round_trip(self, tmp_path) -> None:
        evaluation = _evaluation()
        path = write_decision_quality_evaluation(evaluation, repo=tmp_path)
        loaded = read_decision_quality_evaluation(path)
        assert loaded == evaluation

    def test_exact_replay_is_accepted(self, tmp_path) -> None:
        evaluation = _evaluation()
        first = write_decision_quality_evaluation(evaluation, repo=tmp_path)
        second = write_decision_quality_evaluation(evaluation, repo=tmp_path)
        assert first == second

    def test_same_identity_different_content_is_rejected(self, tmp_path) -> None:
        evaluation = _evaluation()
        write_decision_quality_evaluation(evaluation, repo=tmp_path)
        path = decision_quality_evaluation_path(
            evaluation.schema_version, evaluation.evaluation_id, repo=tmp_path
        )
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw["result_id"] = "tampered-result"
        path.write_text(json.dumps(raw, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        with pytest.raises(ValueError, match="cannot be reused with different content"):
            write_decision_quality_evaluation(evaluation, repo=tmp_path)

    def test_temporary_file_is_cleaned_up(self, tmp_path) -> None:
        evaluation = _evaluation()
        write_decision_quality_evaluation(evaluation, repo=tmp_path)
        path = decision_quality_evaluation_path(
            evaluation.schema_version, evaluation.evaluation_id, repo=tmp_path
        )
        assert list(path.parent.glob(f".{path.name}.*.tmp")) == []


class TestReadValidation:
    def test_wrong_path_is_rejected(self, tmp_path) -> None:
        evaluation = _evaluation()
        write_decision_quality_evaluation(evaluation, repo=tmp_path)
        real_path = decision_quality_evaluation_path(
            evaluation.schema_version, evaluation.evaluation_id, repo=tmp_path
        )
        wrong_path = decision_quality_evaluation_path(1, "f" * 64, repo=tmp_path)
        wrong_path.parent.mkdir(parents=True, exist_ok=True)
        wrong_path.write_text(real_path.read_text(encoding="utf-8"), encoding="utf-8")
        with pytest.raises(ValueError, match="path and embedded identity disagree"):
            read_decision_quality_evaluation(wrong_path)

    def test_missing_field_is_rejected(self, tmp_path) -> None:
        evaluation = _evaluation()
        path = write_decision_quality_evaluation(evaluation, repo=tmp_path)
        raw = json.loads(path.read_text(encoding="utf-8"))
        del raw["realized_outcome"]
        path.write_text(json.dumps(raw), encoding="utf-8")
        with pytest.raises(ValueError, match="keys do not match"):
            read_decision_quality_evaluation(path)

    def test_extra_field_is_rejected(self, tmp_path) -> None:
        evaluation = _evaluation()
        path = write_decision_quality_evaluation(evaluation, repo=tmp_path)
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw["unexpected_field"] = "value"
        path.write_text(json.dumps(raw), encoding="utf-8")
        with pytest.raises(ValueError, match="keys do not match"):
            read_decision_quality_evaluation(path)

    def test_unknown_enum_value_is_rejected(self, tmp_path) -> None:
        evaluation = _evaluation()
        path = write_decision_quality_evaluation(evaluation, repo=tmp_path)
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw["decision_status"] = "made_up_status"
        path.write_text(json.dumps(raw), encoding="utf-8")
        with pytest.raises(ValueError):
            read_decision_quality_evaluation(path)

    def test_naive_timestamp_is_rejected(self, tmp_path) -> None:
        evaluation = _evaluation()
        path = write_decision_quality_evaluation(evaluation, repo=tmp_path)
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw["evaluated_at"] = "2026-09-01T12:10:00"
        path.write_text(json.dumps(raw), encoding="utf-8")
        with pytest.raises(ValueError, match="timezone-aware UTC"):
            read_decision_quality_evaluation(path)

    def test_unsupported_schema_version_is_rejected(self, tmp_path) -> None:
        evaluation = _evaluation()
        path = write_decision_quality_evaluation(evaluation, repo=tmp_path)
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw["schema_version"] = 999
        path.write_text(json.dumps(raw), encoding="utf-8")
        with pytest.raises(ValueError, match="Unsupported"):
            read_decision_quality_evaluation(path)

    def test_tampered_evaluation_id_is_rejected(self, tmp_path) -> None:
        evaluation = _evaluation()
        path = write_decision_quality_evaluation(evaluation, repo=tmp_path)
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw["evaluation_id"] = "e" * 64
        path.write_text(json.dumps(raw), encoding="utf-8")
        with pytest.raises(ValueError, match="does not match canonical content"):
            read_decision_quality_evaluation(path)


class TestSemanticValidation:
    def test_duplicate_check_ids_are_rejected(self) -> None:
        duplicated = (
            DecisionQualityCheck("policy_reference", True, DecisionQualityStatus.CONSISTENT, "ok"),
            DecisionQualityCheck("policy_reference", True, DecisionQualityStatus.CONSISTENT, "ok"),
        )
        evaluation = _evaluation(checks=duplicated)
        with pytest.raises(ValueError, match="duplicate check IDs"):
            validate_decision_quality_evaluation(evaluation)

    def test_overall_status_disagreeing_with_checks_is_rejected(self) -> None:
        checks = (
            DecisionQualityCheck("result_integrity", True, DecisionQualityStatus.CONSISTENT, "ok"),
            DecisionQualityCheck(
                "recommendation_evaluation_reference", True, DecisionQualityStatus.CONSISTENT, "ok"
            ),
            DecisionQualityCheck(
                "policy_reference", True, DecisionQualityStatus.INCONSISTENT, "bad"
            ),
            DecisionQualityCheck(
                "candidate_reference", True, DecisionQualityStatus.CONSISTENT, "ok"
            ),
            DecisionQualityCheck(
                "allocation_recomputation", False, DecisionQualityStatus.UNAVAILABLE, "absent"
            ),
        )
        base_evaluation = _evaluation(
            checks=checks, decision_status=DecisionQualityStatus.INCONSISTENT
        )
        tampered = replace(base_evaluation, decision_status=DecisionQualityStatus.CONSISTENT)
        with pytest.raises(ValueError, match="decision_status does not agree"):
            validate_decision_quality_evaluation(tampered)

    def test_unsupported_policy_schema_version_is_rejected(self) -> None:
        evaluation = _evaluation(
            policy_schema_version=(RECOMMENDATION_POLICY_SCHEMA_VERSION + 1),
        )

        with pytest.raises(
            ValueError,
            match="Unsupported recommendation-policy schema version",
        ):
            validate_decision_quality_evaluation(evaluation)


class TestWriteAtomicity:
    def test_first_write_failure_leaves_no_destination(self, tmp_path, monkeypatch) -> None:
        evaluation = _evaluation()
        path = decision_quality_evaluation_path(
            evaluation.schema_version, evaluation.evaluation_id, repo=tmp_path
        )
        assert not path.exists()

        def failing_link(*_args, **_kwargs):
            raise OSError("simulated link failure")

        monkeypatch.setattr("gridiron_edge.market.decision_quality_store.os.link", failing_link)
        with pytest.raises(OSError, match="simulated link failure"):
            write_decision_quality_evaluation(evaluation, repo=tmp_path)
        assert not path.exists()
        assert list(path.parent.glob(".*tmp")) == []

    def test_temporary_write_failure_leaves_no_destination_and_cleans_temp(
        self, tmp_path, monkeypatch
    ) -> None:
        evaluation = _evaluation()
        path = decision_quality_evaluation_path(
            evaluation.schema_version, evaluation.evaluation_id, repo=tmp_path
        )
        assert not path.exists()

        real_write_text = Path.write_text

        def failing_write_text(self, *args, **kwargs):
            if str(self).endswith(".tmp"):
                raise RuntimeError("simulated temporary write failure")
            return real_write_text(self, *args, **kwargs)

        monkeypatch.setattr(Path, "write_text", failing_write_text)
        with pytest.raises(RuntimeError, match="simulated temporary write failure"):
            write_decision_quality_evaluation(evaluation, repo=tmp_path)
        assert not path.exists()
        assert list(path.parent.glob(".*tmp")) == []
