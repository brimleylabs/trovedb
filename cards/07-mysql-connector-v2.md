---
card_id: trovedb-07-mysql-connector-v2
difficulty: hard
stack: python
registered_at: 2026-04-21
supersedes: 07-mysql-connector.md
supersedes_reason: |
  v1 specified testcontainers-python. v2 swaps the test fixture for
  a developer-provided local MySQL instance configured entirely via
  environment variables. Production code and Protocol contract
  unchanged from v1.
---

# MySQL connector — operator-first

## Goal

Implement `MysqlConnector` in `trovedb/connectors/mysql.py` using
`aiomysql`. Symmetric in shape to the Postgres connector (card #06-v2)
but using MySQL's catalog views and process model. **Operator features
are first-class**: `SHOW PROCESSLIST` / `INFORMATION_SCHEMA.PROCESSLIST`
+ `INNODB_TRX` for in-flight transactions, `KILL QUERY` / `KILL CONNECTION`.

Follow the auto-loaded skills in the library (`implement-trovedb-connector`,
`connector-schema-introspection`, `database-connector-test-infrastructure`)
— they encode the pattern card 3 and card 6 established.

## Acceptance criteria

1. `connect(profile)` opens an `aiomysql.Connection` from `profile.url`
   or discrete fields. Resolves password via `password_env` indirection.
2. `list_databases()` returns rows from `INFORMATION_SCHEMA.SCHEMATA`
   excluding system schemas (`mysql`, `information_schema`,
   `performance_schema`, `sys`).
3. `list_tables(db)` returns rows from `INFORMATION_SCHEMA.TABLES` for
   the given schema, with row-count estimates from `TABLE_ROWS` (cheap),
   data length, index length.
4. `describe_table(db, table)` populates `TableSchema` from
   `INFORMATION_SCHEMA.COLUMNS`, `STATISTICS` (indexes),
   `KEY_COLUMN_USAGE` (foreign keys), and engine info from `TABLES`.
5. `execute(sql, params)` runs the query and returns a `ResultSet`.
   Honor `read_only=True` default — `SET SESSION TRANSACTION READ ONLY`
   before execution unless caller passes `dangerous=True`.
6. **`list_processes()`** sourced from:
   ```sql
   SELECT id, user, host, db, command, time, state, info
   FROM INFORMATION_SCHEMA.PROCESSLIST
   WHERE id != CONNECTION_ID()
   ```
   Joined with `INFORMATION_SCHEMA.INNODB_TRX` + `INNODB_LOCK_WAITS`
   so `blocked_by` on the returned `Process` resolves to the blocking
   pid (via `INNODB_TRX.trx_mysql_thread_id`, not just `trx_id`).
7. **`kill_process(pid, force=False)`**:
   - `force=False` (default) → `KILL QUERY <pid>` (cancels current
     statement, keeps connection).
   - `force=True` → `KILL <pid>` (drops connection entirely).
   - Raise `OperationalError` on permission denied / pid not found.
8. `get_ddl("table", db, name)` runs `SHOW CREATE TABLE <db>.<name>`
   and returns the second column. MySQL exposes this directly —
   no reconstruction needed.
9. **Tests connect to a developer-provided MySQL instance** read from
   env vars (symmetric to Postgres):
   - `TROVEDB_TEST_MYSQL_DSN` (preferred, full DSN), OR fall back to
   - `TROVEDB_TEST_MYSQL_HOST` / `TROVEDB_TEST_MYSQL_PORT` /
     `TROVEDB_TEST_MYSQL_USER` / `TROVEDB_TEST_MYSQL_PASSWORD` /
     `TROVEDB_TEST_MYSQL_DB`
   - If none of those are set, `pytest.skip("MySQL test vars not set;
     see CONTRIBUTING.md")`. No defaults assumed in the card.
   - If the connection attempt itself fails, `pytest.skip(f"MySQL not
     reachable: {error}")` so CI skips cleanly.
10. Test cases (one pytest test each):
    - `test_connect_and_list_databases` — asserts `the trovedb MySQL test DB`
      is in the list; system schemas are excluded.
    - `test_list_tables_and_describe` — creates a temporary table in
      a fixture, lists, describes, asserts column metadata.
    - `test_execute_select_returns_typed_rows`.
    - `test_execute_write_blocked_by_default` — INSERT without
      `dangerous=True` raises.
    - `test_execute_write_allowed_with_dangerous_true`.
    - `test_list_processes_returns_self_excluded_sessions` — open a
      second connection, assert list_processes sees it but not the
      test's own.
    - `test_list_processes_identifies_blocker` — second conn holds
      a row lock in InnoDB, third conn waits; asserts `blocked_by`
      on the third matches the second's thread id.
    - `test_kill_process_force_false_cancels_query` — long-running
      `SELECT SLEEP(30)` in another conn; cancel with `force=False`;
      assert query returns (cancelled) but conn still works.
    - `test_kill_process_force_true_terminates_session` — same setup;
      `force=True`; assert next query on that conn raises.
    - `test_get_ddl_returns_valid_create_table` — create a table
      with indexes + FK; get_ddl; drop; re-run the DDL; assert no error.
11. Coverage of `mysql.py` ≥ 85%.

## Notes

- Default port 3306. `default_port = 3306` on the class.
- Blocker resolution in `INNODB_LOCK_WAITS` is by `trx_id`. Resolve
  `blocking_trx_id` → `INNODB_TRX.trx_mysql_thread_id` for the pid.
  Without this join, `blocked_by` would always be None.
- `KILL QUERY` vs `KILL`: former cancels just the running statement,
  latter drops the session. Our `force` flag distinguishes.
- Hard card. Plan-then-execute should fire.

Registered before execution (v2). Not edited after running.
