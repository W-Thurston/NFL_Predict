# tests/e2e/test_cli_workflows.py
"""E2E: CLI subcommand smoke tests covering all top-level commands."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from gridiron_edge.cli import app
from gridiron_edge.core.settings import Settings

runner = CliRunner()


def _settings_for_repo(repo: Path) -> Settings:
    """Return complete settings rooted at one isolated test repository."""
    return Settings(
        repo_root=repo,
        owm_api_key=None,
        odds_api_key=None,
        data_raw=repo / "data/raw",
        data_cleaned=repo / "data/cleaned",
        data_modeling=repo / "data/modeling",
        data_output=repo / "data/output",
    )


class TestCliHelp:
    """Every top-level command and subcommand should display help without error."""

    def test_root_help(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0

    def test_ingest_help(self) -> None:
        result = runner.invoke(app, ["ingest", "--help"])
        assert result.exit_code == 0

    def test_ratings_help(self) -> None:
        result = runner.invoke(app, ["ratings", "--help"])
        assert result.exit_code == 0

    def test_ratings_elo_help(self) -> None:
        result = runner.invoke(app, ["ratings", "elo", "--help"])
        assert result.exit_code == 0

    def test_evaluate_help(self) -> None:
        result = runner.invoke(app, ["evaluate", "--help"])
        assert result.exit_code == 0

    def test_features_help(self) -> None:
        result = runner.invoke(app, ["features", "--help"])
        assert result.exit_code == 0


class TestCliSubcommandDiscovery:
    """Verify expected subcommands are registered."""

    def test_ingest_has_nflverse_games(self) -> None:
        result = runner.invoke(app, ["ingest", "--help"])
        assert "nflverse-games" in result.stdout

    def test_ratings_elo_has_predict(self) -> None:
        result = runner.invoke(app, ["ratings", "elo", "--help"])
        assert "predict" in result.stdout

    def test_root_has_expected_commands(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        for cmd in ("ingest", "ratings", "evaluate"):
            assert cmd in result.stdout, f"Missing command: {cmd}"


# ---------------------------------------------------------------------------
# Workflow smoke tests - exercise the CLI surfaces end-to-end
# ---------------------------------------------------------------------------


class TestModelsListSmoke:
    """``gridiron models list`` runs to completion on an empty repo.

    The command walks ModelRegistry and reports trained/untrained
    status per model. With no artifacts on disk every entry should
    show "(not trained)" and the command should exit 0.
    """

    def test_models_list_exits_zero(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from tests.fixtures.repos import MiniRepoBuilder

        repo = MiniRepoBuilder(tmp_path).build()
        settings = _settings_for_repo(repo)
        monkeypatch.setattr(
            "gridiron_edge.core.settings.get_settings",
            lambda: settings,
        )

        result = runner.invoke(app, ["models", "list"])

        assert result.exit_code == 0
        assert result.exception is None
        assert "win_prob" in result.stdout
        assert "model_name" in result.stdout
        assert "model_type" in result.stdout


class TestModelsHelpSmoke:
    """``gridiron models --help`` displays the subcommands."""

    def test_models_help_shows_train(self) -> None:
        result = runner.invoke(app, ["models", "--help"])
        assert result.exit_code == 0
        assert "train" in result.stdout

    def test_models_help_shows_list(self) -> None:
        result = runner.invoke(app, ["models", "--help"])
        assert result.exit_code == 0
        assert "list" in result.stdout

    def test_models_help_shows_info(self) -> None:
        result = runner.invoke(app, ["models", "--help"])
        assert result.exit_code == 0
        assert "info" in result.stdout


class TestEvaluateSelectModelSmoke:
    """``gridiron evaluate select-model`` handles an empty archive gracefully."""

    def test_empty_archive_exits_with_message(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from tests.fixtures.repos import MiniRepoBuilder

        repo = MiniRepoBuilder(tmp_path).build()
        settings = _settings_for_repo(repo)
        monkeypatch.setattr(
            "gridiron_edge.core.settings.get_settings",
            lambda: settings,
        )

        result = runner.invoke(
            app,
            ["evaluate", "select-model"],
        )

        combined = result.stdout + result.stderr

        assert result.exit_code == 1
        assert isinstance(result.exception, SystemExit)
        assert result.exception.code == 1
        assert (
            "No models with archived predictions found. Run evaluate backfill first."
        ) in combined
        assert "Recommendation:" not in combined


class TestEvaluateBackfillHelpSmoke:
    """``gridiron evaluate backfill --help`` shows the option surface."""

    def test_backfill_help_shows_model_name_option(self) -> None:
        result = runner.invoke(app, ["evaluate", "backfill", "--help"])
        assert result.exit_code == 0
        assert "--model-name" in result.stdout
        assert "--model-type" in result.stdout
        assert "--mode" in result.stdout
        assert "--start-season" in result.stdout
        assert "--end-season" in result.stdout


class TestModelsTrainHelpSmoke:
    """``gridiron models train --help`` shows the positional arguments."""

    def test_train_help_shows_positional_args(self) -> None:
        result = runner.invoke(app, ["models", "train", "--help"])
        assert result.exit_code == 0
        # Two positional args: model_name then model_type
        assert "model_name" in result.stdout.lower()
        assert "model_type" in result.stdout.lower()
