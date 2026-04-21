"""Connector Protocol and driver registry for trovedb."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, Protocol, TypeVar, runtime_checkable

from trovedb.config import ConnectionProfile
from trovedb.connectors.types import (
    BlockingChain,
    Connection,
    Database,
    Process,
    ResultSet,
    Table,
    TableSchema,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_registry: dict[str, type[Any]] = {}


def register_connector(name: str) -> Callable[[type[T]], type[T]]:
    """Class decorator that registers *cls* as the connector for *name*.

    Example::

        @register_connector("sqlite")
        class LocalSqliteConnector:
            ...
    """

    def decorator(cls: type[T]) -> type[T]:
        _registry[name] = cls
        logger.debug("Registered connector %r → %s", name, cls.__qualname__)
        return cls

    return decorator


def get_connector(name: str) -> type[Any]:
    """Return the connector class registered under *name*.

    Raises :class:`KeyError` if no connector has been registered for that name.
    """
    if name not in _registry:
        raise KeyError(f"No connector registered for driver {name!r}")
    return _registry[name]


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class Connector(Protocol):
    """Abstract contract that every database connector must satisfy.

    All methods are ``async`` — connectors must not block the event loop.
    The protocol is ``@runtime_checkable`` so tests can use ``isinstance``.
    """

    async def connect(self, profile: ConnectionProfile) -> Connection:
        """Open a connection to the server described by *profile*."""
        ...

    async def list_databases(self) -> list[Database]:
        """Return the databases visible on the current connection."""
        ...

    async def list_tables(self, db: str) -> list[Table]:
        """Return the tables (and views) available inside *db*."""
        ...

    async def describe_table(self, db: str, table: str) -> TableSchema:
        """Return the full structural description of *table* in *db*."""
        ...

    async def execute(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> ResultSet:
        """Execute *sql* (optionally with *params*) and return the result set."""
        ...

    async def list_processes(self) -> list[Process]:
        """Return the active server processes / sessions."""
        ...

    async def kill_process(self, pid: int, force: bool = False) -> None:
        """Cancel or terminate the server process identified by *pid*.

        When *force* is ``False`` (default) the query is cancelled but
        the session remains alive.  When *force* is ``True`` the session
        connection is dropped entirely.
        """
        ...

    async def get_ddl(self, kind: str, db: str, name: str) -> str:
        """Return the DDL statement that recreates object *name* of *kind* in *db*."""
        ...

    async def list_blocking_chains(self) -> list[BlockingChain]:
        """Return the current lock-blocking chains.

        Each :class:`~trovedb.connectors.types.BlockingChain` describes one
        (holder → waiter) relationship.  ``depth=1`` means a direct block;
        ``depth≥2`` means the holder is itself a waiter (transitive chain).

        Connectors that have no blocking concept (SQLite) return ``[]``.
        """
        ...


# ---------------------------------------------------------------------------
# Eager import of bundled connectors so their @register_connector decorators
# run at package import time. Without this the registry stays empty and
# get_connector() raises KeyError for every driver.
# ---------------------------------------------------------------------------
from trovedb.connectors import mysql as _mysql  # noqa: E402, F401
from trovedb.connectors import postgres as _postgres  # noqa: E402, F401
from trovedb.connectors import sqlite as _sqlite  # noqa: E402, F401
