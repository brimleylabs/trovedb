"""Stub SQLite connector — satisfies the Connector Protocol; not yet functional."""

from __future__ import annotations

import logging
from typing import Any

from trovedb.config import ConnectionProfile
from trovedb.connectors import register_connector
from trovedb.connectors.types import (
    Connection,
    Database,
    Process,
    ResultSet,
    Table,
    TableSchema,
)

logger = logging.getLogger(__name__)


@register_connector("sqlite")
class LocalSqliteConnector:
    """Placeholder connector for SQLite databases.

    All methods raise :exc:`NotImplementedError` except :meth:`connect`, which
    returns a disconnected :class:`~trovedb.connectors.types.Connection`
    placeholder.  Full implementation is delivered in a later card.
    """

    async def connect(self, profile: ConnectionProfile) -> Connection:
        """Return a placeholder Connection for the given profile."""
        dsn = profile.url or profile.database
        logger.debug("LocalSqliteConnector.connect: dsn=%r (stub)", dsn)
        return Connection(driver="sqlite", dsn=dsn, connected=False)

    async def list_databases(self) -> list[Database]:
        raise NotImplementedError("LocalSqliteConnector.list_databases not yet implemented")

    async def list_tables(self, db: str) -> list[Table]:
        raise NotImplementedError("LocalSqliteConnector.list_tables not yet implemented")

    async def describe_table(self, db: str, table: str) -> TableSchema:
        raise NotImplementedError("LocalSqliteConnector.describe_table not yet implemented")

    async def execute(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> ResultSet:
        raise NotImplementedError("LocalSqliteConnector.execute not yet implemented")

    async def list_processes(self) -> list[Process]:
        raise NotImplementedError("LocalSqliteConnector.list_processes not yet implemented")

    async def kill_process(self, pid: int) -> None:
        raise NotImplementedError("LocalSqliteConnector.kill_process not yet implemented")

    async def get_ddl(self, kind: str, db: str, name: str) -> str:
        raise NotImplementedError("LocalSqliteConnector.get_ddl not yet implemented")
