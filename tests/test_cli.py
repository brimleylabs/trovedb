"""Smoke tests for the trovedb CLI."""

from __future__ import annotations

from typer.testing import CliRunner

from trovedb import __version__
from trovedb.cli import app

runner = CliRunner()


def test_version_flag_prints_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert f"trovedb {__version__}" in result.output


def test_help_flag_exits_zero() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "connect" in result.output


def test_import_has_version() -> None:
    assert __version__ == "0.0.1"
