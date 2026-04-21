"""Integration tests for MysqlConnector against a local MySQL instance.

Connection is resolved (in priority order) from:
  1. ``TROVEDB_TEST_MYSQL_DSN`` — full DSN URL
  2. ``TROVEDB_TEST_MYSQL_HOST / PORT / USER / PASSWORD / DB`` env vars
  3. (none; see CONTRIBUTING) localhost, REDACTED, the MySQL test DB

The entire module is skipped when MySQL is not reachable so CI
environments without a database don't red-fail.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import aiomysql
import pytest

# Side-effect import: triggers @register_connector("mysql")
import trovedb.connectors.mysql  # noqa: F401
from trovedb.config import ConnectionProfile, Driver
from trovedb.connectors import Connector
from trovedb.connectors.mysql import MysqlConnector
from trovedb.connectors.types import Database, Process, ResultSet, Table, TableSchema

# ---------------------------------------------------------------------------
# DSN resolution helpers
# ---------------------------------------------------------------------------

_HOST = os.environ.get("TROVEDB_TEST_MYSQL_HOST", "127.0.0.1")
_PORT = int(os.environ.get("TROVEDB_TEST_MYSQL_PORT", "3306"))
_USER = os.environ.get("TROVEDB_TEST_MYSQL_USER", "root")
_PASSWORD = os.environ.get("TROVEDB_TEST_MYSQL_PASSWORD", "")
_DB = os.environ.get("TROVEDB_TEST_MYSQL_DB", "the MySQL test DB")


def _build_test_dsn() -> str:
    if dsn := os.environ.get("TROVEDB_TEST_MYSQL_DSN"):
        return dsn
    return f"mysql://{_USER}:{_PASSWORD}@{_HOST}:{_PORT}/{_DB}"


_TEST_DSN = _build_test_dsn()


async def _cur_execute(conn: aiomysql.Connection, sql: str) -> None:
    """Run *sql* on *conn* through a fresh cursor (workaround: Connection has no .execute).

    Also consumes any result set so that server-side errors (e.g. KILL QUERY
    raising OperationalError 1317) propagate to the caller.
    """
    async with conn.cursor() as cur:
        await cur.execute(sql)
        if cur.description:
            await cur.fetchall()


async def _raw_connect(
    db: str | None = None,
    autocommit: bool = True,
) -> aiomysql.Connection:
    """Open a raw aiomysql connection; skips the calling test if MySQL is down."""
    target_db = db if db is not None else _DB
    try:
        return await aiomysql.connect(
            host=_HOST,
            port=_PORT,
            user=_USER,
            password=_PASSWORD,
            db=target_db,
            autocommit=autocommit,
            charset="utf8mb4",
        )
    except Exception as exc:
        pytest.skip(f"MySQL not available: {exc}")


# ---------------------------------------------------------------------------
# Protocol conformance (no connection required)
# ---------------------------------------------------------------------------


def test_mysql_connector_satisfies_protocol() -> None:
    assert isinstance(MysqlConnector(), Connector)


def test_get_connector_mysql_registered() -> None:
    from trovedb.connectors import get_connector

    cls = get_connector("mysql")
    assert cls is MysqlConnector


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
async def connector() -> AsyncIterator[MysqlConnector]:
    """Connected MysqlConnector; skips the test if MySQL is unreachable."""
    profile = ConnectionProfile(name="test-mysql", driver=Driver.mysql, url=_TEST_DSN)
    c = MysqlConnector()
    try:
        await c.connect(profile)
    except Exception as exc:
        pytest.skip(f"MySQL not available at {_TEST_DSN}: {exc}")
    try:
        yield c
    finally:
        if c._conn is not None:
            try:
                c._conn.close()
            except Exception:
                pass


@pytest.fixture()
async def test_db(connector: MysqlConnector) -> AsyncIterator[str]:
    """Isolated MySQL database; dropped CASCADE on teardown."""
    name = f"trovedb_t_{uuid.uuid4().hex[:10]}"
    assert connector._conn is not None
    async with connector._conn.cursor() as cur:
        await cur.execute(f"CREATE DATABASE `{name}`")
    try:
        yield name
    finally:
        try:
            async with connector._conn.cursor() as cur:
                await cur.execute(f"DROP DATABASE IF EXISTS `{name}`")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 1. test_connect_and_list_databases
# ---------------------------------------------------------------------------


async def test_connect_and_list_databases(connector: MysqlConnector) -> None:
    dbs = await connector.list_databases()
    assert isinstance(dbs, list)
    assert all(isinstance(d, Database) for d in dbs)

    db_names = {d.name for d in dbs}
    assert _DB in db_names, f"{_DB!r} not found in {db_names}"

    # System schemas must be excluded
    for system in ("mysql", "information_schema", "performance_schema", "sys"):
        assert system not in db_names, f"System schema {system!r} should be excluded"


# ---------------------------------------------------------------------------
# 2. test_list_tables_and_describe
# ---------------------------------------------------------------------------


async def test_list_tables_and_describe(
    connector: MysqlConnector,
    test_db: str,
) -> None:
    assert connector._conn is not None
    async with connector._conn.cursor() as cur:
        await cur.execute(f"""
            CREATE TABLE `{test_db}`.`employees` (
                id      INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                name    VARCHAR(200) NOT NULL,
                email   VARCHAR(200) UNIQUE,
                salary  DECIMAL(10, 2)
            ) ENGINE=InnoDB
        """)

    tables = await connector.list_tables(test_db)
    assert isinstance(tables, list)
    assert all(isinstance(t, Table) for t in tables)
    assert any(t.name == "employees" for t in tables)

    ts = await connector.describe_table(test_db, "employees")
    assert isinstance(ts, TableSchema)
    assert ts.db == test_db
    assert ts.table == "employees"

    col_names = [c.name for c in ts.columns]
    for expected in ("id", "name", "email", "salary"):
        assert expected in col_names, f"Column {expected!r} missing from {col_names}"

    name_col = next(c for c in ts.columns if c.name == "name")
    assert name_col.nullable is False

    unique_indexes = [i for i in ts.indexes if i.unique and not i.primary]
    assert any("email" in i.columns for i in unique_indexes), (
        f"Expected unique index on email; got {unique_indexes}"
    )


# ---------------------------------------------------------------------------
# 3. test_execute_select_returns_typed_rows
# ---------------------------------------------------------------------------


async def test_execute_select_returns_typed_rows(connector: MysqlConnector) -> None:
    rs = await connector.execute("SELECT 1 AS n, 'hello' AS s")
    assert isinstance(rs, ResultSet)
    assert rs.row_count == 1
    assert rs.columns == ["n", "s"]
    row = rs.rows[0]
    assert row[0] == 1
    assert row[1] == "hello"
    assert rs.duration_ms is not None and rs.duration_ms >= 0


# ---------------------------------------------------------------------------
# 4. test_execute_write_blocked_by_default
# ---------------------------------------------------------------------------


async def test_execute_write_blocked_by_default(
    connector: MysqlConnector,
    test_db: str,
) -> None:
    assert connector._conn is not None
    async with connector._conn.cursor() as cur:
        await cur.execute(
            f"CREATE TABLE `{test_db}`.`guard_test` (id INT) ENGINE=InnoDB"
        )
    with pytest.raises(aiomysql.Error):
        await connector.execute(f"INSERT INTO `{test_db}`.`guard_test` VALUES (1)")
    # Connection must still be usable after the failed write
    rs = await connector.execute("SELECT 1")
    assert rs.row_count == 1


# ---------------------------------------------------------------------------
# 5. test_execute_write_allowed_with_dangerous_true
# ---------------------------------------------------------------------------


async def test_execute_write_allowed_with_dangerous_true(
    connector: MysqlConnector,
    test_db: str,
) -> None:
    assert connector._conn is not None
    async with connector._conn.cursor() as cur:
        await cur.execute(
            f"CREATE TABLE `{test_db}`.`writes_ok` (id INT PRIMARY KEY) ENGINE=InnoDB"
        )
    await connector.execute(
        f"INSERT INTO `{test_db}`.`writes_ok` VALUES (%(id)s)",
        {"id": 42},
        dangerous=True,
    )
    rs = await connector.execute(
        f"SELECT id FROM `{test_db}`.`writes_ok` WHERE id = 42"
    )
    assert rs.row_count == 1
    assert rs.rows[0][0] == 42


# ---------------------------------------------------------------------------
# 6. test_list_processes_returns_self_excluded_sessions
# ---------------------------------------------------------------------------


async def test_list_processes_returns_self_excluded_sessions(
    connector: MysqlConnector,
) -> None:
    conn2 = await _raw_connect()
    try:
        # Get conn2's thread id
        async with conn2.cursor() as cur:
            await cur.execute("SELECT CONNECTION_ID()")
            row = await cur.fetchone()
        pid2: int = row[0]

        procs = await connector.list_processes()
        assert isinstance(procs, list)
        assert all(isinstance(p, Process) for p in procs)
        pids = {p.pid for p in procs}

        assert pid2 in pids, f"Expected pid {pid2} in {pids}"

        # Own backend must not appear
        assert connector._conn is not None
        async with connector._conn.cursor() as cur:
            await cur.execute("SELECT CONNECTION_ID()")
            self_row = await cur.fetchone()
        self_pid: int = self_row[0]
        assert self_pid not in pids, f"Own pid {self_pid} should be excluded"
    finally:
        conn2.close()


# ---------------------------------------------------------------------------
# 7. test_list_processes_identifies_blocker
# ---------------------------------------------------------------------------


async def test_list_processes_identifies_blocker(
    connector: MysqlConnector,
    test_db: str,
) -> None:
    assert connector._conn is not None

    # Create a table with a row for locking
    async with connector._conn.cursor() as cur:
        await cur.execute(
            f"CREATE TABLE `{test_db}`.`lock_target` (id INT PRIMARY KEY) ENGINE=InnoDB"
        )
        await cur.execute(f"INSERT INTO `{test_db}`.`lock_target` VALUES (1)")

    # conn2 holds a row lock inside an explicit transaction
    conn2 = await _raw_connect(db=test_db, autocommit=False)
    try:
        await _cur_execute(
            conn2,
            f"SELECT id FROM `{test_db}`.`lock_target` WHERE id = 1 FOR UPDATE",
        )
        async with conn2.cursor() as cur:
            await cur.execute("SELECT CONNECTION_ID()")
            pid_row = await cur.fetchone()
        pid2: int = pid_row[0]

        # conn3 attempts to update the locked row — will block
        conn3 = await _raw_connect(db=test_db, autocommit=False)
        try:
            blocking_task = asyncio.create_task(
                _cur_execute(
                    conn3,
                    f"UPDATE `{test_db}`.`lock_target` SET id = 2 WHERE id = 1",
                )
            )
            await asyncio.sleep(0.5)  # give conn3 time to enter lock-wait

            procs = await connector.list_processes()
            blocked = [p for p in procs if p.blocked_by is not None]
            assert any(p.blocked_by == pid2 for p in blocked), (
                f"Expected a process blocked by pid {pid2}; "
                f"blocked processes: {blocked}"
            )
        finally:
            blocking_task.cancel()
            try:
                await blocking_task
            except (asyncio.CancelledError, aiomysql.Error, Exception):
                pass
            try:
                await conn3.rollback()
            except Exception:
                pass
            conn3.close()
    finally:
        try:
            await conn2.rollback()
        except Exception:
            pass
        conn2.close()


# ---------------------------------------------------------------------------
# 8. test_kill_process_force_false_cancels_query
# ---------------------------------------------------------------------------


async def test_kill_process_force_false_cancels_query(
    connector: MysqlConnector,
) -> None:
    conn2 = await _raw_connect()
    try:
        async with conn2.cursor() as cur:
            await cur.execute("SELECT CONNECTION_ID()")
            pid_row = await cur.fetchone()
        pid2: int = pid_row[0]

        sleep_task = asyncio.create_task(_cur_execute(conn2, "SELECT SLEEP(30)"))
        await asyncio.sleep(0.3)

        # Cancel the query — must not terminate the session
        await connector.kill_process(pid2, force=False)

        # MySQL KILL QUERY causes SLEEP() to return early (value 1 = interrupted).
        # The SELECT completes without raising; give it a generous timeout.
        try:
            await asyncio.wait_for(sleep_task, timeout=3.0)
        except Exception:
            pass  # Some client configurations raise; either outcome is acceptable

        # The key assertion: the session must still accept queries.
        async with conn2.cursor() as cur:
            await cur.execute("SELECT 1")
            alive = await cur.fetchone()
        assert alive is not None and alive[0] == 1
    finally:
        conn2.close()


# ---------------------------------------------------------------------------
# 9. test_kill_process_force_true_terminates_session
# ---------------------------------------------------------------------------


async def test_kill_process_force_true_terminates_session(
    connector: MysqlConnector,
) -> None:
    conn2 = await _raw_connect()
    try:
        async with conn2.cursor() as cur:
            await cur.execute("SELECT CONNECTION_ID()")
            pid_row = await cur.fetchone()
        pid2: int = pid_row[0]

        sleep_task = asyncio.create_task(_cur_execute(conn2, "SELECT SLEEP(30)"))
        await asyncio.sleep(0.3)

        # Terminate the entire session
        await connector.kill_process(pid2, force=True)

        with pytest.raises(aiomysql.Error):
            await sleep_task

        # Subsequent queries on conn2 must also fail (backend is gone)
        with pytest.raises(aiomysql.Error):
            await _cur_execute(conn2, "SELECT 1")
    finally:
        try:
            conn2.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 10. test_get_ddl_returns_valid_create_table
# ---------------------------------------------------------------------------


async def test_get_ddl_returns_valid_create_table(
    connector: MysqlConnector,
    test_db: str,
) -> None:
    assert connector._conn is not None

    async with connector._conn.cursor() as cur:
        await cur.execute(f"""
            CREATE TABLE `{test_db}`.`ddl_parent` (
                id    INT NOT NULL PRIMARY KEY,
                label VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB
        """)
        await cur.execute(f"""
            CREATE TABLE `{test_db}`.`ddl_child` (
                id        INT NOT NULL PRIMARY KEY,
                parent_id INT NOT NULL,
                notes     TEXT,
                INDEX idx_ddl_child_parent (parent_id),
                CONSTRAINT fk_ddl_child_parent
                    FOREIGN KEY (parent_id) REFERENCES `{test_db}`.`ddl_parent` (id)
            ) ENGINE=InnoDB
        """)

    ddl = await connector.get_ddl("table", test_db, "ddl_child")
    assert "CREATE TABLE" in ddl.upper()
    assert "ddl_child" in ddl
    assert "idx_ddl_child_parent" in ddl

    # Drop child so we can recreate it
    async with connector._conn.cursor() as cur:
        await cur.execute(f"DROP TABLE `{test_db}`.`ddl_child`")

    # Re-run the DDL with the test database as the active schema so that
    # unqualified FK references (e.g. REFERENCES `ddl_parent`) resolve correctly.
    async with connector._conn.cursor() as cur:
        await cur.execute(f"USE `{test_db}`")
        try:
            await cur.execute(ddl)
        finally:
            await cur.execute(f"USE `{_DB}`")

    # Verify child was recreated
    async with connector._conn.cursor() as cur:
        await cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'ddl_child'
            """,
            (test_db,),
        )
        count_row = await cur.fetchone()
    assert count_row is not None and count_row[0] == 1
