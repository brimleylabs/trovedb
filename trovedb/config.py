"""Connection profile configuration for trovedb."""

from __future__ import annotations

import logging
import os
import tomllib
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Raised when configuration cannot be resolved at runtime."""


class Driver(StrEnum):
    """Supported database drivers."""

    postgres = "postgres"
    mysql = "mysql"
    sqlite = "sqlite"


class ConnectionProfile(BaseModel):
    """A single named connection profile."""

    name: str
    driver: Driver

    # Discrete-fields form
    host: str | None = None
    port: int | None = None
    user: str | None = None
    database: str | None = None

    # URL form — bypasses discrete fields when present
    url: str | None = None

    # Password indirection — never store plaintext here
    password_env: str | None = None

    ssl_mode: str | None = None


def default_config_path() -> Path:
    """Return the platform-appropriate path to connections.toml."""
    from platformdirs import user_config_dir  # lazy import to keep module light

    return Path(user_config_dir("trovedb")) / "connections.toml"


def load_connections(path: Path) -> dict[str, ConnectionProfile]:
    """Parse *path* as TOML and return a mapping of profile-name → ConnectionProfile.

    Returns an empty dict when the file does not exist.
    Logs a WARNING for any profile that contains a literal ``password`` field.
    Raises :class:`pydantic.ValidationError` on invalid field values (e.g. unknown driver).
    """
    if not path.exists():
        logger.debug("Config file not found at %s — returning empty dict", path)
        return {}

    with path.open("rb") as fh:
        data = tomllib.load(fh)

    profiles: dict[str, ConnectionProfile] = {}
    for key, raw in data.items():
        if not isinstance(raw, dict):
            logger.debug("Skipping non-table TOML key %r", key)
            continue

        if "password" in raw:
            logger.warning(
                "Profile %r contains a plaintext 'password' field — "
                "use 'password_env' instead to reference an environment variable. "
                "The plaintext value has been ignored for security.",
                key,
            )

        # Ensure `name` is always populated (defaults to the TOML table key)
        raw = {**raw, "name": raw.get("name", key)}

        profiles[key] = ConnectionProfile.model_validate(raw)

    return profiles


def resolve_password(profile: ConnectionProfile) -> str:
    """Return the password for *profile* by reading the named environment variable.

    Raises :class:`ConfigError` when ``password_env`` is not set on the profile
    or when the referenced environment variable is absent from the process environment.
    """
    if not profile.password_env:
        raise ConfigError(
            f"Profile {profile.name!r} has no 'password_env' set; "
            "cannot resolve a password."
        )

    value = os.environ.get(profile.password_env)
    if value is None:
        raise ConfigError(
            f"Environment variable {profile.password_env!r} (referenced by profile "
            f"{profile.name!r}) is not set in the current environment."
        )

    return value
