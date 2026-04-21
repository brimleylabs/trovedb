---
card_id: trovedb-07-mysql-connector
difficulty: hard
stack: python
registered_at: 2026-04-21
status: superseded
superseded_by: 07-mysql-connector-v2.md
superseded_reason: |
  Same situation as card 6: original specified `testcontainers-python`
  which needs Docker, and Docker isn't available on the developer's machine.
  v2 swaps to a local MySQL instance (developer-provided) read from env
  vars. Production code and Protocol contract unchanged.
---

# MySQL connector — operator-first  (SUPERSEDED — see v2)

## Goal

Implement `MysqlConnector` in `trovedb/connectors/mysql.py` using
`aiomysql`. Symmetric in shape to the Postgres connector (card #06)
but using MySQL's catalog views and process model. **Operator features
are first-class**: `SHOW PROCESSLIST` / `INFORMATION_SCHEMA.PROCESSLIST`
+ `INNODB_TRX` for in-flight transactions, KILL QUERY / KILL CONNECTION.

## Acceptance criteria

1. `connect(profile)` opens an `aiomysql.Connection` from `profile.url`
   or discrete fields. Resolves password via `password_env` indirection.
2. `list_databases()` returns rows from `INFORMATION_SCHEMA.SCHEMATA`
   excluding system schemas (`mysql`, `information_schema`, `performance_schema`,
   `sys`).
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
   Joined with `INFORMATION_SCHEMA.INNODB_TRX` (transaction state) and
   `INNODB_LOCK_WAITS` (blocking pid → blocked pid). Returned `Process`
   objects: `pid` (id), `user`, `database` (db), `state` (command + state),
   `wait_event` (state field), `query` (info), `runtime_seconds` (time),
   `blocked_by` (blocking_trx_id resolved to pid via INNODB_TRX).
7. **`kill_process(pid, force=False)`**:
   - `force=False` (default) → `KILL QUERY <pid>` (cancels current statement,
     keeps connection).
   - `force=True` → `KILL <pid>` (drops the connection entirely).
   - Both raise `OperationalError` on permission denied / pid not found.
8. `get_ddl("table", db, name)` runs `SHOW CREATE TABLE <db>.<name>` and
   returns the second column. Trivially correct (MySQL exposes this
   directly — no reconstruction needed).
9. Tests use `testcontainers-python` MySQL 8.4 container. Mirror the
   Postgres test set:
   - connect + list + describe
   - execute SELECT + read-only enforcement (write with dangerous=False raises)
   - **list_processes returns sessions and identifies blockers** (use a
     second connection to hold a row lock during the test)
   - kill_process(force=False) cancels a query in flight
   - kill_process(force=True) drops the session (next query on that conn fails)
   - get_ddl returns valid CREATE TABLE
10. Coverage of `mysql.py` ≥ 85%.

## Notes

- Default port 3306. `default_port = 3306` on the class.
- MySQL doesn't have a native `EXPLAIN ANALYZE` until 8.0+ — use
  `EXPLAIN FORMAT=JSON` for the explain feature in a later card.
- The blocker resolution in `INNODB_LOCK_WAITS` is by `trx_id`, not pid.
  Resolve `blocking_trx_id` → `INNODB_TRX.trx_mysql_thread_id` to get the
  pid that's blocking. Without this join, `blocked_by` would always be None.
- Plan-then-execute should fire. Hard card.

Registered before execution. Not edited after running.
