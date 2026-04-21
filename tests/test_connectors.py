"""Tests for trovedb.connectors — Protocol, registry, and SQLite stub."""

from __future__ import annotations

import pytest

# Importing sqlite triggers @register_connector("sqlite")
import trovedb.connectors.sqlite  # noqa: F401
from trovedb.config import ConnectionProfile, Driver
from trovedb.connectors import Connector, get_connector, register_connector
from trovedb.connectors.sqlite import LocalSqliteConnector

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


def test_sqlite_stub_is_instance_of_connector_protocol() -> None:
    instance = LocalSqliteConnector()
    assert isinstance(instance, Connector), (
        "LocalSqliteConnector must satisfy the Connector Protocol"
    )


# ---------------------------------------------------------------------------
# LocalSqliteConnector.connect — returns a placeholder Connection
# ---------------------------------------------------------------------------


async def test_sqlite_connect_returns_connection_with_correct_driver() -> None:
    profile = ConnectionProfile(
        name="test-sqlite",
        driver=Driver.sqlite,
        database=":memory:",
    )
    connector = LocalSqliteConnector()
    conn = await connector.connect(profile)

    assert conn.driver == "sqlite"
    assert conn.dsn == ":memory:"
    assert conn.connected is False


async def test_sqlite_connect_prefers_url_over_database() -> None:
    profile = ConnectionProfile(
        name="test-sqlite-url",
        driver=Driver.sqlite,
        url="file:test.db?mode=ro",
        database="/ignored/path.db",
    )
    connector = LocalSqliteConnector()
    conn = await connector.connect(profile)

    assert conn.dsn == "file:test.db?mode=ro"


# ---------------------------------------------------------------------------
# LocalSqliteConnector — stub methods raise NotImplementedError
# ---------------------------------------------------------------------------


async def test_sqlite_list_databases_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        await LocalSqliteConnector().list_databases()


async def test_sqlite_list_tables_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        await LocalSqliteConnector().list_tables("main")


async def test_sqlite_describe_table_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        await LocalSqliteConnector().describe_table("main", "users")


async def test_sqlite_execute_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        await LocalSqliteConnector().execute("SELECT 1")


async def test_sqlite_list_processes_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        await LocalSqliteConnector().list_processes()


async def test_sqlite_kill_process_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        await LocalSqliteConnector().kill_process(1)


async def test_sqlite_get_ddl_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        await LocalSqliteConnector().get_ddl("table", "main", "users")
