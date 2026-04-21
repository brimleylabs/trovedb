---
card_id: trovedb-02-config-loader
difficulty: moderate
stack: python
registered_at: 2026-04-21
---

# Config loader: connection profiles in TOML

## Goal

Add `trovedb/config.py` with Pydantic models + a TOML loader that
parses `~/.config/trovedb/connections.toml` into typed connection
profiles. Support both DSN-URL form and discrete-fields form. Resolve
passwords from `password_env` indirection (env-var name lookup) — never
plaintext password fields. Warn on first run if a `password` field is
ever found inline in TOML (security check).

## Acceptance criteria

1. `ConnectionProfile` model has fields: `name`, `driver` (enum:
   postgres/mysql/sqlite), `host`, `port`, `user`, `database`, `url`,
   `password_env`, `ssl_mode`. URL form bypasses discrete fields.
2. `load_connections(path)` returns a `dict[str, ConnectionProfile]`.
3. `resolve_password(profile)` reads from `os.environ[profile.password_env]`
   and raises a clean `ConfigError` if the env var is missing.
4. If a TOML profile has a literal `password = "..."` field, the loader
   logs a warning and accepts it (don't break existing setups, but flag).
5. Invalid driver value → `ValidationError` with a helpful message.
6. Tests cover: URL form, discrete-fields form, missing env var, invalid
   driver, plaintext-password warning, file-missing → empty dict.

## Notes

- Use `tomllib` (3.11 stdlib).
- Config dir is platform-aware: `~/.config/trovedb/` on POSIX,
  `%APPDATA%\trovedb\` on Windows. Use `platformdirs`.
- `ConfigError` should be a subclass of `Exception` defined in this module.

Registered before execution. Not edited after running.
