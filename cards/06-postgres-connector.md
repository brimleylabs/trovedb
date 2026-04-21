---
card_id: trovedb-06-postgres-connector
difficulty: hard
stack: python
registered_at: 2026-04-21
status: superseded
superseded_by: 06-postgres-connector-v2.md
superseded_reason: |
  Original card specified `testcontainers-python` for the test suite,
  which requires Docker on the host. Docker was not available on the
  developer's machine at execution time; rather than alter the test approach
  in-place after registration, the card was superseded by v2 which
  uses a local Postgres instance (default `localhost`, `postgres`
  user) with credentials read from env vars. Test contract is the same;
  only the test fixture differs. v1 remains in the repo as the locked
  pre-registered text.
---

# Postgres connector — operator-first  (SUPERSEDED — see v2)

## Goal

Implement `PostgresConnector` in `trovedb/connectors/postgres.py` using
`psycopg` (>=3, async). Unlike the SQLite connector (which had no
notion of sessions), this connector must implement the full `Connector`
Protocol **with operator features as first-class behaviour, not afterthoughts**:

- `list_processes()` returns rich live session data from `pg_stat_activity`
  joined with `pg_locks` so we can see what's waiting.
- `kill_process(pid)` calls `pg_terminate_backend(pid)` (graceful) with
  a `force=True` flag for `pg_cancel_backend(pid)` (cancel only).
- `get_ddl(kind, db, name)` reconstructs CREATE statements from
  `pg_catalog` (no `pg_dump` shell-out — keeps the binary self-contained).

## Acceptance criteria

1. `connect(profile)` opens a `psycopg.AsyncConnection`. Reads
   `profile.url` if set, else builds DSN from discrete fields. Resolves
   password via `password_env` indirection (from card #02).
2. `list_databases()` returns rows from `pg_database WHERE NOT datistemplate`.
3. `list_tables(db)` queries `information_schema.tables` filtered to
   `BASE TABLE` and `VIEW` types, with row-count estimates from
   `pg_class.reltuples` (cheap — don't `COUNT(*)`).
4. `describe_table(db, table)` populates `TableSchema` from
   `information_schema.columns`, `pg_index`, `pg_constraint` (FKs).
5. `execute(sql, params)` runs the query and returns a `ResultSet`.
   Honor a `read_only=True` default — `SET TRANSACTION READ ONLY` before
   execution unless the caller passes `dangerous=True`.
6. **`list_processes()`** returns a list of `Process` records sourced from:
   ```sql
   SELECT pid, usename, datname, state, query,
          xact_start, query_start, wait_event_type, wait_event,
          backend_type
   FROM pg_stat_activity
   WHERE pid != pg_backend_pid()
   ```
   Plus a join to `pg_locks` indicating which `pid` is blocked by which.
   Returned `Process` objects must include: `pid`, `user`, `database`,
   `state`, `wait_event`, `query`, `runtime_seconds`, `blocked_by` (pid or None).
7. **`kill_process(pid, force=False)`**:
   - `force=False` (default) → `SELECT pg_cancel_backend(pid)` (cancels the
     running query but keeps the session open).
   - `force=True` → `SELECT pg_terminate_backend(pid)` (drops the session).
   - Both return `None` on success; raise `OperationalError` if the PID
     doesn't exist or the caller lacks permission.
8. `get_ddl("table", db, name)` reconstructs `CREATE TABLE ...` from
   `pg_catalog`. Include columns, types, NOT NULLs, defaults, primary
   key, indexes (separate CREATE INDEX statements), and foreign keys
   (separate ALTER TABLE statements). Output must be syntactically valid.
9. Tests use `testcontainers-python` to spin up a real Postgres 17
   container per test session. Cover:
   - connect + list_databases + list_tables + describe_table
   - execute (SELECT) returning typed rows
   - read-only enforcement: a write SQL with `dangerous=False` raises
   - **list_processes returns at least one row (the test session itself
     is filtered out, so spawn a second connection that holds a query)**
   - **list_processes correctly identifies a blocking pid when test holds
     a lock**
   - kill_process with `force=False` cancels a query in flight
   - kill_process with `force=True` terminates the session
   - get_ddl produces re-runnable SQL (parse it, run it on a fresh
     schema, no errors)
10. Coverage of `postgres.py` ≥ 85%.

## Notes

- Use `psycopg.AsyncConnection.connect(...)` and `cursor()`-based
  execution. Avoid `psycopg2`.
- For the DDL reconstruction: there are well-known catalog queries
  (look at how `pg_dump` does it). Don't reinvent the relational
  introspection — Postgres's `pg_catalog` views are stable.
- This is a hard card. Plan-then-execute should fire (high complexity).
- This card is the heart of the operator-console value prop. Take the
  time to get `list_processes()` and `kill_process()` right — they're
  the headline features.

Registered before execution. Not edited after running.
