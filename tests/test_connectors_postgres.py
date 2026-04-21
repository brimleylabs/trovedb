"""Integration tests for PostgresConnector against a local Postgres instance.

Connection is resolved (in priority order) from:
  1. ``TROVEDB_TEST_PG_DSN`` — full DSN URL
  2. ``TROVEDB_TEST_PG_HOST / PORT / USER / PASSWORD / DB`` env vars
  3. (none; see CONTRIBUTING) localhost, REDACTED, the trovedb Postgres test DB

The entire module is skipped when Postgres is not reachable so CI
environments without a database don't red-fail.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

import psycopg
import pytest

# Side-effect import: triggers @register_connector("postgres")
import trovedb.connectors.postgres  # noqa: F401
from trovedb.config import ConnectionProfile, Driver
from trovedb.connectors import Connector
from trovedb.connectors.postgres import PostgresConnector
from trovedb.connectors.types import Database, Process, ResultSet, Table, TableSchema

# ---------------------------------------------------------------------------
# DSN resolution helpers
# ---------------------------------------------------------------------------


def _build_test_dsn() -> str:
    if dsn := os.environ.get("TROVEDB_TEST_PG_DSN"):
        return dsn
    host = os.environ.get("TROVEDB_TEST_PG_HOST", "localhost")
    port = os.environ.get("TROVEDB_TEST_PG_PORT", "5432")
    user = os.environ.get("TROVEDB_TEST_PG_USER", "postgres")
    password = os.environ.get("TROVEDB_TEST_PG_PASSWORD", "postgres")
    db = os.environ.get("TROVEDB_TEST_PG_DB", "the trovedb Postgres test DB")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


_TEST_DSN = _build_test_dsn()


async def _raw_connect(autocommit: bool = True) -> psycopg.AsyncConnection[Any]:
    """Open a raw psycopg connection; skips the calling test if Postgres is down."""
    try:
        return await psycopg.AsyncConnection.connect(_TEST_DSN, autocommit=autocommit)
    except Exception as exc:
        pytest.skip(f"Postgres not available: {exc}")


# ---------------------------------------------------------------------------
# Protocol conformance (no connection required)
# ---------------------------------------------------------------------------


def test_postgres_connector_satisfies_protocol() -> None:
    assert isinstance(PostgresConnector(), Connector)


def test_get_connector_postgres_registered() -> None:
    from trovedb.connectors import get_connector

    cls = get_connector("postgres")
    assert cls is PostgresConnector


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
async def connector() -> AsyncIterator[PostgresConnector]:
    """Connected PostgresConnector; skips the test if Postgres is unreachable."""
    profile = ConnectionProfile(name="test-pg", driver=Driver.postgres, url=_TEST_DSN)
    c = PostgresConnector()
    try:
        await c.connect(profile)
    except Exception as exc:
        pytest.skip(f"Postgres not available at {_TEST_DSN}: {exc}")
    try:
        yield c
    finally:
        if c._conn is not None:
            try:
                await c._conn.close()
            except Exception:
                pass


@pytest.fixture()
async def schema(connector: PostgresConnector) -> AsyncIterator[str]:
    """Isolated Postgres schema; dropped CASCADE on teardown."""
    name = f"test_{uuid.uuid4().hex[:10]}"
    assert connector._conn is not None
    await connector._conn.execute(f"CREATE SCHEMA {name}")
    try:
        yield name
    finally:
        try:
            await connector._conn.execute(f"DROP SCHEMA {name} CASCADE")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 1. test_connect_and_list_databases
# ---------------------------------------------------------------------------


async def test_connect_and_list_databases(connector: PostgresConnector) -> None:
    dbs = await connector.list_databases()
    assert isinstance(dbs, list)
    assert all(isinstance(d, Database) for d in dbs)
    db_names = {d.name for d in dbs}
    expected = os.environ.get("TROVEDB_TEST_PG_DB", "the trovedb Postgres test DB")
    assert expected in db_names, f"{expected!r} not found in {db_names}"


# ---------------------------------------------------------------------------
# 2. test_list_tables_and_describe
# ---------------------------------------------------------------------------


async def test_list_tables_and_describe(
    connector: PostgresConnector, schema: str
) -> None:
    assert connector._conn is not None
    await connector._conn.execute(f"""
        CREATE TABLE {schema}.employees (
            id      SERIAL PRIMARY KEY,
            name    TEXT NOT NULL,
            email   TEXT UNIQUE,
            salary  NUMERIC(10, 2)
        )
    """)

    tables = await connector.list_tables(schema)
    assert isinstance(tables, list)
    assert all(isinstance(t, Table) for t in tables)
    assert any(t.name == "employees" for t in tables)

    ts = await connector.describe_table(schema, "employees")
    assert isinstance(ts, TableSchema)
    assert ts.db == schema
    assert ts.table == "employees"

    col_names = [c.name for c in ts.columns]
    for expected in ("id", "name", "email", "salary"):
        assert expected in col_names

    name_col = next(c for c in ts.columns if c.name == "name")
    assert name_col.nullable is False

    unique_indexes = [i for i in ts.indexes if i.unique and not i.primary]
    assert any("email" in i.columns for i in unique_indexes)


# ---------------------------------------------------------------------------
# 3. test_execute_select_returns_typed_rows
# ---------------------------------------------------------------------------


async def test_execute_select_returns_typed_rows(connector: PostgresConnector) -> None:
    rs = await connector.execute("SELECT 1::int AS n, 'hello'::text AS s")
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
    connector: PostgresConnector, schema: str
) -> None:
    assert connector._conn is not None
    await connector._conn.execute(f"CREATE TABLE {schema}.guard_test (id int)")
    with pytest.raises(psycopg.Error):
        await connector.execute(f"INSERT INTO {schema}.guard_test VALUES (1)")
    # Connection must still be usable
    rs = await connector.execute("SELECT 1")
    assert rs.row_count == 1


# ---------------------------------------------------------------------------
# 5. test_execute_write_allowed_with_dangerous_true
# ---------------------------------------------------------------------------


async def test_execute_write_allowed_with_dangerous_true(
    connector: PostgresConnector, schema: str
) -> None:
    assert connector._conn is not None
    await connector._conn.execute(
        f"CREATE TABLE {schema}.writes_ok (id int PRIMARY KEY)"
    )
    await connector.execute(
        f"INSERT INTO {schema}.writes_ok VALUES (%(id)s)",
        {"id": 42},
        dangerous=True,
    )
    rs = await connector.execute(f"SELECT id FROM {schema}.writes_ok WHERE id = 42")
    assert rs.row_count == 1
    assert rs.rows[0][0] == 42


# ---------------------------------------------------------------------------
# 6. test_list_processes_returns_self_excluded_sessions
# ---------------------------------------------------------------------------


async def test_list_processes_returns_self_excluded_sessions(
    connector: PostgresConnector,
) -> None:
    conn2 = await _raw_connect()
    try:
        async with conn2.cursor() as cur:
            await cur.execute("SELECT pg_backend_pid()")
            row = await cur.fetchone()
        pid2: int = row[0]  # type: ignore[index]

        procs = await connector.list_processes()
        assert isinstance(procs, list)
        assert all(isinstance(p, Process) for p in procs)
        pids = {p.pid for p in procs}

        assert pid2 in pids, f"Expected pid {pid2} in {pids}"

        # Own backend must not appear
        assert connector._conn is not None
        async with connector._conn.cursor() as cur:
            await cur.execute("SELECT pg_backend_pid()")
            self_row = await cur.fetchone()
        self_pid: int = self_row[0]  # type: ignore[index]
        assert self_pid not in pids
    finally:
        await conn2.close()


# ---------------------------------------------------------------------------
# 7. test_list_processes_identifies_blocker
# ---------------------------------------------------------------------------


async def test_list_processes_identifies_blocker(
    connector: PostgresConnector, schema: str
) -> None:
    assert connector._conn is not None

    await connector._conn.execute(
        f"CREATE TABLE {schema}.lock_target (id int PRIMARY KEY)"
    )
    await connector._conn.execute(
        f"INSERT INTO {schema}.lock_target VALUES (1)"
    )

    # conn2 holds a row lock inside an explicit transaction
    conn2 = await _raw_connect(autocommit=False)
    try:
        await conn2.execute(
            f"SELECT id FROM {schema}.lock_target FOR UPDATE"
        )
        async with conn2.cursor() as cur:
            await cur.execute("SELECT pg_backend_pid()")
            pid_row = await cur.fetchone()
        pid2: int = pid_row[0]  # type: ignore[index]

        # conn3 attempts to update the locked row — will block
        conn3 = await _raw_connect(autocommit=False)
        try:
            blocking_task = asyncio.create_task(
                conn3.execute(
                    f"UPDATE {schema}.lock_target SET id = 2 WHERE id = 1"
                )
            )
            await asyncio.sleep(0.4)  # give conn3 time to enter lock-wait

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
            except (asyncio.CancelledError, psycopg.Error):
                pass
            try:
                await conn3.rollback()
            except Exception:
                pass
            await conn3.close()
    finally:
        try:
            await conn2.rollback()
        except Exception:
            pass
        await conn2.close()


# ---------------------------------------------------------------------------
# 8. test_kill_process_force_false_cancels_query
# ---------------------------------------------------------------------------


async def test_kill_process_force_false_cancels_query(
    connector: PostgresConnector,
) -> None:
    conn2 = await _raw_connect()
    try:
        async with conn2.cursor() as cur:
            await cur.execute("SELECT pg_backend_pid()")
            pid_row = await cur.fetchone()
        pid2: int = pid_row[0]  # type: ignore[index]

        sleep_task = asyncio.create_task(conn2.execute("SELECT pg_sleep(30)"))
        await asyncio.sleep(0.3)

        # Cancel the query — must not terminate the session
        await connector.kill_process(pid2, force=False)

        with pytest.raises(psycopg.Error):
            await sleep_task

        # Session must still accept queries
        async with conn2.cursor() as cur:
            await cur.execute("SELECT 1")
            alive = await cur.fetchone()
        assert alive is not None and alive[0] == 1
    finally:
        await conn2.close()


# ---------------------------------------------------------------------------
# 9. test_kill_process_force_true_terminates_session
# ---------------------------------------------------------------------------


async def test_kill_process_force_true_terminates_session(
    connector: PostgresConnector,
) -> None:
    conn2 = await _raw_connect()
    try:
        async with conn2.cursor() as cur:
            await cur.execute("SELECT pg_backend_pid()")
            pid_row = await cur.fetchone()
        pid2: int = pid_row[0]  # type: ignore[index]

        sleep_task = asyncio.create_task(conn2.execute("SELECT pg_sleep(30)"))
        await asyncio.sleep(0.3)

        # Terminate the entire session
        await connector.kill_process(pid2, force=True)

        with pytest.raises(psycopg.Error):
            await sleep_task

        # Subsequent queries on conn2 must also fail (backend is gone)
        with pytest.raises(psycopg.Error):
            await conn2.execute("SELECT 1")
    finally:
        try:
            await conn2.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 10. test_get_ddl_returns_runnable_create_table
# ---------------------------------------------------------------------------


async def test_get_ddl_returns_runnable_create_table(
    connector: PostgresConnector, schema: str
) -> None:
    assert connector._conn is not None

    await connector._conn.execute(f"""
        CREATE TABLE {schema}.ddl_parent (
            id    INTEGER PRIMARY KEY,
            label TEXT NOT NULL
        )
    """)
    await connector._conn.execute(f"""
        CREATE TABLE {schema}.ddl_child (
            id        INTEGER PRIMARY KEY,
            parent_id INTEGER NOT NULL REFERENCES {schema}.ddl_parent(id),
            notes     TEXT
        )
    """)
    await connector._conn.execute(
        f"CREATE INDEX idx_ddl_child_notes ON {schema}.ddl_child(notes)"
    )

    ddl = await connector.get_ddl("table", schema, "ddl_child")
    assert "CREATE TABLE" in ddl.upper()
    assert "ddl_child" in ddl
    assert "idx_ddl_child_notes" in ddl

    # Drop child so we can recreate it
    await connector._conn.execute(f"DROP TABLE {schema}.ddl_child CASCADE")

    # Run the DDL with search_path set so unqualified names resolve correctly
    await connector._conn.execute(f"SET search_path TO {schema}, public")
    try:
        for stmt in ddl.split(";"):
            stmt = stmt.strip()
            if stmt:
                await connector._conn.execute(stmt)
    finally:
        await connector._conn.execute("SET search_path TO public")

    # Verify child was recreated
    async with connector._conn.cursor() as cur:
        await cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = %s AND table_name = 'ddl_child'
            """,
            (schema,),
        )
        count_row = await cur.fetchone()
    assert count_row is not None and count_row[0] == 1
