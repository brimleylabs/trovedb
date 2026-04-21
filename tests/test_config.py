"""Tests for trovedb.config — connection profile loader."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
from pydantic import ValidationError  # noqa: F401

from trovedb.config import (
    ConfigError,
    ConnectionProfile,
    Driver,
    load_connections,
    resolve_password,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_toml(tmp_path: Path, content: str) -> Path:
    """Write *content* to a temporary connections.toml and return its path."""
    p = tmp_path / "connections.toml"
    p.write_text(content, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# load_connections — URL form
# ---------------------------------------------------------------------------


def test_load_url_form(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        '[prod]\ndriver = "postgres"\nurl = "postgresql://user:x@host/db"\n',
    )
    profiles = load_connections(path)

    assert "prod" in profiles
    p = profiles["prod"]
    assert p.driver == Driver.postgres
    assert p.url == "postgresql://user:x@host/db"
    assert p.name == "prod"
    # Discrete fields not set
    assert p.host is None
    assert p.port is None


# ---------------------------------------------------------------------------
# load_connections — discrete-fields form
# ---------------------------------------------------------------------------


def test_load_discrete_fields_sqlite(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        '[local]\ndriver = "sqlite"\ndatabase = "/tmp/test.db"\n',
    )
    profiles = load_connections(path)

    assert "local" in profiles
    p = profiles["local"]
    assert p.driver == Driver.sqlite
    assert p.database == "/tmp/test.db"
    assert p.url is None


def test_load_discrete_fields_mysql(tmp_path: Path) -> None:
    content = (
        '[mydb]\ndriver = "mysql"\nhost = "localhost"\nport = 3306\n'
        'user = "admin"\ndatabase = "mydb"\npassword_env = "MYSQL_PASSWORD"\n'
    )
    path = _write_toml(tmp_path, content)
    profiles = load_connections(path)

    p = profiles["mydb"]
    assert p.driver == Driver.mysql
    assert p.host == "localhost"
    assert p.port == 3306
    assert p.user == "admin"
    assert p.password_env == "MYSQL_PASSWORD"


def test_load_multiple_profiles(tmp_path: Path) -> None:
    content = (
        '[pg]\ndriver = "postgres"\nhost = "pg-host"\n\n'
        '[lite]\ndriver = "sqlite"\ndatabase = ":memory:"\n'
    )
    path = _write_toml(tmp_path, content)
    profiles = load_connections(path)

    assert set(profiles.keys()) == {"pg", "lite"}


# ---------------------------------------------------------------------------
# load_connections — file-missing
# ---------------------------------------------------------------------------


def test_file_missing_returns_empty_dict(tmp_path: Path) -> None:
    missing = tmp_path / "nonexistent.toml"
    result = load_connections(missing)
    assert result == {}


# ---------------------------------------------------------------------------
# load_connections — invalid driver
# ---------------------------------------------------------------------------


def test_invalid_driver_raises_validation_error(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        '[bad]\ndriver = "oracle"\nhost = "host"\n',
    )
    with pytest.raises(ValidationError):
        load_connections(path)


# ---------------------------------------------------------------------------
# load_connections — plaintext password warning
# ---------------------------------------------------------------------------


def test_plaintext_password_logs_warning_and_accepts(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = _write_toml(
        tmp_path,
        '[warn_me]\ndriver = "postgres"\nhost = "host"\npassword = "secret"\n',
    )
    with caplog.at_level(logging.WARNING, logger="trovedb.config"):
        profiles = load_connections(path)

    # Profile still loaded (don't break existing setups)
    assert "warn_me" in profiles

    # Warning was emitted
    warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("plaintext" in msg for msg in warning_messages), (
        f"Expected a plaintext-password warning; got: {warning_messages}"
    )


# ---------------------------------------------------------------------------
# resolve_password
# ---------------------------------------------------------------------------


def test_resolve_password_reads_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_DB_PASS", "supersecret")
    profile = ConnectionProfile(
        name="test", driver=Driver.postgres, password_env="MY_DB_PASS"
    )
    assert resolve_password(profile) == "supersecret"


def test_resolve_password_missing_env_var_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NONEXISTENT_VAR", raising=False)
    profile = ConnectionProfile(
        name="test", driver=Driver.postgres, password_env="NONEXISTENT_VAR"
    )
    with pytest.raises(ConfigError, match="NONEXISTENT_VAR"):
        resolve_password(profile)


def test_resolve_password_no_password_env_raises() -> None:
    profile = ConnectionProfile(name="test", driver=Driver.sqlite)
    with pytest.raises(ConfigError, match="no 'password_env'"):
        resolve_password(profile)
