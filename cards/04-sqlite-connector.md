---
card_id: trovedb-04-sqlite-connector
difficulty: moderate
stack: python
registered_at: 2026-04-21
---

# SQLite connector — first working implementation

## Goal

Replace the SQLite stub from card #03 with a real implementation backed
by `aiosqlite`. SQLite is the easiest connector — no server, no auth,
no port — which makes it the right candidate for proving the full
Protocol works end-to-end before tackling Postgres + MySQL.

## Acceptance criteria

1. `SqliteConnector.connect(profile)` opens an `aiosqlite.Connection`
   on `profile.url` (or `profile.database` interpreted as a file path).
2. `list_databases()` returns the single attached schema (always one for
   SQLite — return `[Database(name="main")]`).
3. `list_tables(db)` returns rows from `sqlite_master WHERE type='table'`
   plus row counts (cheap `COUNT(*)`) and approximate sizes via
   `dbstat` virtual table if available, else `None`.
4. `describe_table(db, table)` populates `TableSchema` via
   `PRAGMA table_info(...)`, `PRAGMA index_list(...)`, and
   `PRAGMA foreign_key_list(...)`.
5. `execute(sql, params)` runs the SQL and returns a `ResultSet` with
   columns + rows. Read-only mode by default — wrap writes behind a
   `dangerous=True` parameter to be added in a later card.
6. `list_processes()` returns `[]` (SQLite has no equivalent). Document.
7. `kill_process(pid)` raises `NotImplementedError("SQLite has no process model")`.
8. `get_ddl(kind, db, name)` returns the original CREATE statement from
   `sqlite_master.sql`.
9. Tests use a real on-disk SQLite file in a `tmp_path` fixture. Cover:
   open + list + describe + execute (SELECT) + DDL retrieval.
10. All 7 implemented methods are tested. Coverage of this module ≥ 90%.

## Notes

- Use `aiosqlite` for async access.
- Be defensive about empty databases (no tables → empty list, not error).
- This card unblocks the schema-tree widget (card #07) which we'll wire
  to a SQLite connection first because it's setup-free.

Registered before execution. Not edited after running.
