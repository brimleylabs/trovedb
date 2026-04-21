"""Tests for trovedb.connectors — Protocol, registry, and SQLite connector."""

from __future__ import annotations

from pathlib import Path

import aiosqlite
import pytest

# Importing sqlite triggers @register_connector("sqlite")
import trovedb.connectors.sqlite  # noqa: F401
from trovedb.config import ConnectionProfile, Driver
from trovedb.connectors import Connector, get_connector, register_connector
from trovedb.connectors.sqlite import LocalSqliteConnector
from trovedb.connectors.types import (
    Column,
    Database,
    ForeignKey,
    Index,
    ResultSet,
    Table,
    TableSchema,
)

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_get_connector_sqlite_returns_registered_class() -> None:
    cls = get_connector("sqlite")
    assert cls is LocalSqliteConnector


def test_get_connector_unknown_raises_key_error() -> None:
    with pytest.raises(KeyError, match="nope"):
        get_connector("nope")


def test_register_connector_decorator_registers_class() -> None:
    """A custom class registered with the decorator is discoverable."""

    @register_connector("_test_driver")
    class _FakeConnector:
        pass

    assert get_connector("_test_driver") is _FakeConnector


# ---------------------------------------------------------------------------
# Protocol conformance (runtime_checkable)
# ---------------------------------------------------------------------------


def test_sqlite_connector_is_instance_of_connector_protocol() -> None:
    instance = LocalSqliteConnector()
    assert isinstance(instance, Connector), (
        "LocalSqliteConnector must satisfy the Connector Protocol"
    )


# ---------------------------------------------------------------------------
# Fixtures — real on-disk SQLite database
# ---------------------------------------------------------------------------


@pytest.fixture()
async def db_path(tmp_path: Path) -> Path:
    """Create a real SQLite database file with sample tables and data."""
    path = tmp_path / "test.db"
    async with aiosqlite.connect(str(path)) as conn:
        await conn.execute("""
            CREATE TABLE users (
                id    INTEGER PRIMARY KEY,
                name  TEXT NOT NULL,
                email TEXT UNIQUE
            )
        """)
        await conn.execute("""
            CREATE TABLE posts (
                id      INTEGER PRIMARY KEY,
                user_id INTEGER,
                title   TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        await conn.execute(
            "CREATE UNIQUE INDEX idx_users_email ON users(email)"
        )
        await conn.executemany(
            "INSERT INTO users VALUES (?, ?, ?)",
            [
                (1, "Alice", "alice@example.com"),
                (2, "Bob", "bob@example.com"),
            ],
        )
        await conn.executemany(
            "INSERT INTO posts VALUES (?, ?, ?)",
            [
                (1, 1, "Hello world"),
                (2, 1, "Second post"),
                (3, 2, "Bob's first"),
            ],
        )
        await conn.commit()
    return path


@pytest.fixture()
async def connector(db_path: Path) -> LocalSqliteConnector:
    """Return a connected LocalSqliteConnector pointing at *db_path*."""
    profile = ConnectionProfile(
        name="test-sqlite",
        driver=Driver.sqlite,
        database=str(db_path),
    )
    c = LocalSqliteConnector()
    await c.connect(profile)
    return c


# ---------------------------------------------------------------------------
# connect()
# ---------------------------------------------------------------------------


async def test_connect_returns_connected_true(db_path: Path) -> None:
    profile = ConnectionProfile(
        name="test-sqlite",
        driver=Driver.sqlite,
        database=str(db_path),
    )
    c = LocalSqliteConnector()
    conn = await c.connect(profile)

    assert conn.driver == "sqlite"
    assert conn.dsn == str(db_path)
    assert conn.connected is True


async def test_connect_prefers_url_over_database(db_path: Path) -> None:
    """When both url and database are set, url wins for DSN."""
    profile = ConnectionProfile(
        name="test-sqlite-url",
        driver=Driver.sqlite,
        url=str(db_path),
        database="/ignored/path.db",
    )
    c = LocalSqliteConnector()
    conn = await c.connect(profile)

    assert conn.dsn == str(db_path)
    assert conn.connected is True


# ---------------------------------------------------------------------------
# list_blocking_chains (card 11)
# ---------------------------------------------------------------------------


async def test_sqlite_list_blocking_chains_returns_empty(
    connector: LocalSqliteConnector,
) -> None:
    """SQLite has no blocking model — list_blocking_chains() must return []."""
    from trovedb.connectors.types import BlockingChain

    chains = await connector.list_blocking_chains()
    assert chains == []
    assert all(isinstance(c, BlockingChain) for c in chains)  # trivially true


# ---------------------------------------------------------------------------
# list_databases()
# ---------------------------------------------------------------------------


async def test_list_databases_returns_single_main_entry(
    connector: LocalSqliteConnector,
) -> None:
    dbs = await connector.list_databases()

    assert len(dbs) == 1
    assert isinstance(dbs[0], Database)
    assert dbs[0].name == "main"


# ---------------------------------------------------------------------------
# list_tables()
# ---------------------------------------------------------------------------


async def test_list_tables_returns_expected_tables(
    connector: LocalSqliteConnector,
) -> None:
    tables = await connector.list_tables("main")

    assert isinstance(tables, list)
    assert all(isinstance(t, Table) for t in tables)
    names = {t.name for t in tables}
    assert {"users", "posts"}.issubset(names)


async def test_list_tables_sets_db_field(
    connector: LocalSqliteConnector,
) -> None:
    tables = await connector.list_tables("main")
    assert all(t.db == "main" for t in tables)


# ---------------------------------------------------------------------------
# describe_table()
# ---------------------------------------------------------------------------


async def test_describe_table_returns_table_schema(
    connector: LocalSqliteConnector,
) -> None:
    schema = await connector.describe_table("main", "users")
    assert isinstance(schema, TableSchema)
    assert schema.db == "main"
    assert schema.table == "users"


async def test_describe_table_columns(
    connector: LocalSqliteConnector,
) -> None:
    schema = await connector.describe_table("main", "users")
    col_names = [c.name for c in schema.columns]
    assert col_names == ["id", "name", "email"]
    assert all(isinstance(c, Column) for c in schema.columns)


async def test_describe_table_nullable_flag(
    connector: LocalSqliteConnector,
) -> None:
    schema = await connector.describe_table("main", "users")
    col_by_name = {c.name: c for c in schema.columns}
    # id is INTEGER PRIMARY KEY — effectively NOT NULL
    assert col_by_name["name"].nullable is False
    assert col_by_name["email"].nullable is True


async def test_describe_table_indexes(
    connector: LocalSqliteConnector,
) -> None:
    schema = await connector.describe_table("main", "users")
    assert isinstance(schema.indexes, list)
    assert all(isinstance(i, Index) for i in schema.indexes)
    unique_indexes = [i for i in schema.indexes if i.unique]
    assert any("email" in i.columns for i in unique_indexes)


async def test_describe_table_foreign_keys(
    connector: LocalSqliteConnector,
) -> None:
    schema = await connector.describe_table("main", "posts")
    assert isinstance(schema.foreign_keys, list)
    assert len(schema.foreign_keys) == 1
    fk = schema.foreign_keys[0]
    assert isinstance(fk, ForeignKey)
    assert fk.ref_table == "users"
    assert "user_id" in fk.columns


async def test_describe_table_ddl_is_populated(
    connector: LocalSqliteConnector,
) -> None:
    schema = await connector.describe_table("main", "users")
    assert schema.ddl is not None
    assert "CREATE TABLE" in schema.ddl.upper()


# ---------------------------------------------------------------------------
# execute()
# ---------------------------------------------------------------------------


async def test_execute_select_returns_result_set(
    connector: LocalSqliteConnector,
) -> None:
    rs = await connector.execute("SELECT id, name FROM users ORDER BY id")
    assert isinstance(rs, ResultSet)
    assert rs.columns == ["id", "name"]
    assert rs.row_count == 2
    assert len(rs.rows) == 2
    assert rs.duration_ms is not None and rs.duration_ms >= 0


async def test_execute_with_named_params(
    connector: LocalSqliteConnector,
) -> None:
    rs = await connector.execute(
        "SELECT name FROM users WHERE id = :uid", {"uid": 1}
    )
    assert rs.row_count == 1
    assert rs.rows[0][0] == "Alice"


async def test_execute_non_select_returns_empty_rows(
    connector: LocalSqliteConnector,
) -> None:
    rs = await connector.execute(
        "INSERT INTO users VALUES (:id, :name, :email)",
        {"id": 99, "name": "Charlie", "email": "charlie@example.com"},
    )
    # INSERT returns no rows
    assert rs.row_count == 0
    assert rs.rows == []


# ---------------------------------------------------------------------------
# list_processes()
# ---------------------------------------------------------------------------


async def test_list_processes_returns_empty_list(
    connector: LocalSqliteConnector,
) -> None:
    """SQLite has no server processes; always returns an empty list."""
    procs = await connector.list_processes()
    assert procs == []


# ---------------------------------------------------------------------------
# kill_process()
# ---------------------------------------------------------------------------


async def test_kill_process_raises_not_implemented(
    connector: LocalSqliteConnector,
) -> None:
    """SQLite has no process model — kill_process must raise NotImplementedError."""
    with pytest.raises(NotImplementedError, match="SQLite has no process model"):
        await connector.kill_process(1)


# ---------------------------------------------------------------------------
# get_ddl()
# ---------------------------------------------------------------------------


async def test_get_ddl_returns_create_statement(
    connector: LocalSqliteConnector,
) -> None:
    ddl = await connector.get_ddl("table", "main", "users")
    assert "CREATE TABLE" in ddl.upper()
    assert "users" in ddl.lower()


async def test_get_ddl_unknown_name_raises_key_error(
    connector: LocalSqliteConnector,
) -> None:
    with pytest.raises(KeyError, match="no_such_table"):
        await connector.get_ddl("table", "main", "no_such_table")


# ---------------------------------------------------------------------------
# Error guard — requires connection
# ---------------------------------------------------------------------------


async def test_methods_raise_when_not_connected() -> None:
    c = LocalSqliteConnector()
    with pytest.raises(RuntimeError, match="connect"):
        await c.list_tables("main")
