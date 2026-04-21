---
card_id: trovedb-06-postgres-connector-v2
difficulty: hard
stack: python
registered_at: 2026-04-21
supersedes: 06-postgres-connector.md
supersedes_reason: |
  v1 specified testcontainers-python. v2 swaps the test fixture for
  a developer-provided local Postgres instance configured entirely
  via environment variables. Production code and Protocol contract
  are unchanged from v1.
---

# Postgres connector — operator-first

## Goal

Implement `PostgresConnector` in `trovedb/connectors/postgres.py` using
`psycopg` (>=3, async). This is the first connector against a real
server, so it must implement the full `Connector` Protocol with
**operator features as first-class behaviour, not afterthoughts**:

- `list_processes()` returns rich live session data from `pg_stat_activity`
  joined with `pg_locks` so we can see what's waiting.
- `kill_process(pid, force=False)` calls `pg_cancel_backend(pid)` by
  default (cancel only); `force=True` calls `pg_terminate_backend(pid)`.
- `get_ddl(kind, db, name)` reconstructs CREATE statements from
  `pg_catalog` (no `pg_dump` shell-out).

Follow the `implement-trovedb-connector` skill (in the auto-loaded
library) for the Protocol contract. Use the same domain types from
`trovedb/connectors/types.py` and register with `@register_connector("postgres")`.

## Acceptance criteria

1. `connect(profile)` opens a `psycopg.AsyncConnection`. Reads
   `profile.url` if set, else builds DSN from discrete fields. Resolves
   password via `password_env` indirection (from card #02).
2. `list_databases()` returns rows from `pg_database WHERE NOT datistemplate`.
3. `list_tables(db)` queries `information_schema.tables` filtered to
   `BASE TABLE` and `VIEW`, with row-count estimates from
   `pg_class.reltuples` (cheap — don't `COUNT(*)`).
4. `describe_table(db, table)` populates `TableSchema` from
   `information_schema.columns`, `pg_index`, `pg_constraint` (FKs).
5. `execute(sql, params)` runs the query and returns a `ResultSet`.
   Honor `read_only=True` default — `SET TRANSACTION READ ONLY` before
   execution unless caller passes `dangerous=True`.
6. **`list_processes()`** sourced from:
   ```sql
   SELECT pid, usename, datname, state, query,
          xact_start, query_start, wait_event_type, wait_event,
          backend_type
   FROM pg_stat_activity
   WHERE pid != pg_backend_pid()
   ```
   Plus a join to `pg_locks` indicating which `pid` is blocked by which.
   Returned `Process` objects must include: `pid`, `user`, `database`,
   `state`, `wait_event`, `query`, `runtime_seconds`, `blocked_by`
   (pid or None).
7. **`kill_process(pid, force=False)`**:
   - `force=False` → `SELECT pg_cancel_backend(pid)` (keeps session open).
   - `force=True` → `SELECT pg_terminate_backend(pid)` (drops session).
   - Raise `OperationalError` on permission denied / pid not found.
8. `get_ddl("table", db, name)` reconstructs `CREATE TABLE ...` from
   `pg_catalog`. Include columns, types, NOT NULLs, defaults, primary
   key, indexes (separate `CREATE INDEX`), and foreign keys (separate
   `ALTER TABLE`). Output must be syntactically valid.
9. **Tests connect to a developer-provided Postgres instance** read
   from env vars:
   - `TROVEDB_TEST_PG_DSN` (preferred, full DSN URL), OR fall back to
   - `TROVEDB_TEST_PG_HOST` / `TROVEDB_TEST_PG_PORT` /
     `TROVEDB_TEST_PG_USER` / `TROVEDB_TEST_PG_PASSWORD` /
     `TROVEDB_TEST_PG_DB`
   - If none of those are set, the test module should
     `pytest.skip("Postgres test vars not set; see CONTRIBUTING.md")`
     rather than attempting any default or failing red. The project
     CONTRIBUTING.md (not this card) documents how contributors
     point the tests at whatever Postgres they have locally.
   - If the connection attempt itself fails, `pytest.skip(f"Postgres
     not reachable: {error}")` so CI in environments without a reachable
     instance skips cleanly rather than red-fails.
10. Test cases (each as one `pytest` test):
    - `test_connect_and_list_databases` — connects, lists databases,
      asserts `the trovedb Postgres test DB` is in the list.
    - `test_list_tables_and_describe` — creates a temporary table in a
      fixture, lists tables, describes it, asserts column metadata.
    - `test_execute_select_returns_typed_rows` — SELECT 1::int, 'a'::text;
      asserts row types.
    - `test_execute_write_blocked_by_default` — INSERT without
      `dangerous=True` raises.
    - `test_execute_write_allowed_with_dangerous_true` — INSERT with
      `dangerous=True` succeeds.
    - `test_list_processes_returns_self_excluded_sessions` — opens a
      second connection, asserts list_processes sees it but not the
      test's own backend.
    - `test_list_processes_identifies_blocker` — second connection
      holds a row lock, third connection waits; asserts `blocked_by`
      on the third matches the second's pid.
    - `test_kill_process_force_false_cancels_query` — long-running
      `SELECT pg_sleep(30)` in another conn; cancel with `force=False`;
      assert query terminates but conn stays alive.
    - `test_kill_process_force_true_terminates_session` — same setup;
      `force=True`; assert next query on that conn raises.
    - `test_get_ddl_returns_runnable_create_table` — create table with
      indexes + FK; get DDL; drop table; run the DDL on a fresh schema;
      assert no error.
11. Coverage of `postgres.py` ≥ 85%.

## Notes

- Use `psycopg.AsyncConnection.connect(...)` and cursor-based execution.
  Avoid `psycopg2`.
- For the DDL reconstruction: there are well-known catalog queries
  (look at how `pg_dump` does it). Don't reinvent the relational
  introspection — Postgres's `pg_catalog` views are stable.
- Hard card. Plan-then-execute should fire.
- **This is the heart of trovedb's value prop** — `list_processes()`
  and `kill_process()` are the headline features. Take the time to get
  them right.
- Test isolation: tests are responsible for creating and dropping any
  schema they touch (use `CREATE SCHEMA test_<random>` so a shared DB
  stays clean).

Registered before execution (v2). Not edited after running.
