"""SQLite connector backed by aiosqlite."""

from __future__ import annotations

import logging
import time
from typing import Any

import aiosqlite

from trovedb.config import ConnectionProfile
from trovedb.connectors import register_connector
from trovedb.connectors.types import (
    Column,
    Connection,
    Database,
    ForeignKey,
    Index,
    Process,
    ResultSet,
    Table,
    TableSchema,
)

logger = logging.getLogger(__name__)


@register_connector("sqlite")
class LocalSqliteConnector:
    """SQLite connector backed by aiosqlite.

    SQLite has no concept of multiple databases per connection — only the
    attached ``main`` schema — so :meth:`list_databases` always returns a
    single entry.  It also has no server processes, so :meth:`list_processes`
    returns an empty list and :meth:`kill_process` raises
    :exc:`NotImplementedError` with an explicit message.
    """

    def __init__(self) -> None:
        self._conn: aiosqlite.Connection | None = None
        self._dsn: str | None = None

    # ------------------------------------------------------------------
    # connect
    # ------------------------------------------------------------------

    async def connect(self, profile: ConnectionProfile) -> Connection:
        """Open an aiosqlite connection for the given *profile*."""
        dsn = profile.url or profile.database
        self._dsn = dsn
        logger.debug("LocalSqliteConnector.connect: dsn=%r", dsn)
        self._conn = await aiosqlite.connect(dsn or ":memory:")
        self._conn.row_factory = aiosqlite.Row
        return Connection(driver="sqlite", dsn=dsn, connected=True)

    # ------------------------------------------------------------------
    # list_databases
    # ------------------------------------------------------------------

    async def list_databases(self) -> list[Database]:
        """Return the single attached schema — always ``main`` for SQLite."""
        return [Database(name="main")]

    # ------------------------------------------------------------------
    # list_tables
    # ------------------------------------------------------------------

    async def list_tables(self, db: str) -> list[Table]:
        """Return rows from ``sqlite_master WHERE type='table'``."""
        self._require_connection()
        assert self._conn is not None  # narrowing
        async with self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ) as cursor:
            rows = await cursor.fetchall()
        return [Table(name=row["name"], db=db) for row in rows]

    # ------------------------------------------------------------------
    # describe_table
    # ------------------------------------------------------------------

    async def describe_table(self, db: str, table: str) -> TableSchema:
        """Populate :class:`~trovedb.connectors.types.TableSchema` via PRAGMA calls.

        .. note::
            SQLite's PRAGMA statements do not support the ``?`` placeholder
            syntax for object names (only for *values*).  The table and index
            names are double-quoted to guard against whitespace, but callers
            must treat ``table`` as trusted input.
        """
        self._require_connection()
        assert self._conn is not None  # narrowing

        quoted = f'"{table}"'

        # ---- columns ---------------------------------------------------
        columns: list[Column] = []
        async with self._conn.execute(f"PRAGMA table_info({quoted})") as cursor:
            col_rows = await cursor.fetchall()
        for row in col_rows:
            columns.append(
                Column(
                    name=row["name"],
                    data_type=row["type"] or "TEXT",
                    nullable=not bool(row["notnull"]),
                    default=row["dflt_value"],
                )
            )

        # ---- indexes ---------------------------------------------------
        indexes: list[Index] = []
        async with self._conn.execute(f"PRAGMA index_list({quoted})") as cursor:
            idx_rows = await cursor.fetchall()
        for idx_row in idx_rows:
            idx_name: str = idx_row["name"]
            is_unique = bool(idx_row["unique"])
            quoted_idx = f'"{idx_name}"'
            async with self._conn.execute(f"PRAGMA index_info({quoted_idx})") as cursor:
                info_rows = await cursor.fetchall()
            idx_cols = [r["name"] for r in info_rows]
            indexes.append(Index(name=idx_name, columns=idx_cols, unique=is_unique))

        # ---- foreign keys ----------------------------------------------
        foreign_keys: list[ForeignKey] = []
        async with self._conn.execute(f"PRAGMA foreign_key_list({quoted})") as cursor:
            fk_rows = await cursor.fetchall()
        # Each multi-column FK has the same ``id``; collect columns in order.
        fk_map: dict[int, dict[str, Any]] = {}
        for fk_row in fk_rows:
            fk_id: int = fk_row["id"]
            if fk_id not in fk_map:
                fk_map[fk_id] = {
                    "ref_table": fk_row["table"],
                    "from_cols": [],
                    "to_cols": [],
                }
            fk_map[fk_id]["from_cols"].append(fk_row["from"])
            fk_map[fk_id]["to_cols"].append(fk_row["to"])
        for fk_id, fk_data in fk_map.items():
            foreign_keys.append(
                ForeignKey(
                    name=f"fk_{table}_{fk_id}",
                    columns=fk_data["from_cols"],
                    ref_table=fk_data["ref_table"],
                    ref_columns=fk_data["to_cols"],
                )
            )

        # ---- DDL -------------------------------------------------------
        async with self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ) as cursor:
            ddl_row = await cursor.fetchone()
        ddl = ddl_row["sql"] if ddl_row is not None else None

        return TableSchema(
            db=db,
            table=table,
            columns=columns,
            indexes=indexes,
            foreign_keys=foreign_keys,
            ddl=ddl,
        )

    # ------------------------------------------------------------------
    # execute
    # ------------------------------------------------------------------

    async def execute(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> ResultSet:
        """Execute *sql* (optionally with named *params*) and return a
        :class:`~trovedb.connectors.types.ResultSet`.

        Named parameters use the ``:name`` style accepted by SQLite, e.g.::

            await connector.execute("SELECT * FROM t WHERE id = :id", {"id": 1})
        """
        self._require_connection()
        assert self._conn is not None  # narrowing
        start = time.monotonic()
        async with self._conn.execute(sql, params or ()) as cursor:
            rows = await cursor.fetchall()
            columns = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )
        duration_ms = (time.monotonic() - start) * 1000
        return ResultSet(
            columns=columns,
            rows=[tuple(r) for r in rows],
            row_count=len(rows),
            duration_ms=duration_ms,
        )

    # ------------------------------------------------------------------
    # list_processes / kill_process
    # ------------------------------------------------------------------

    async def list_processes(self) -> list[Process]:
        """Return an empty list — SQLite has no server processes."""
        return []

    async def kill_process(self, pid: int) -> None:
        """Raise :exc:`NotImplementedError` — SQLite has no process model."""
        raise NotImplementedError("SQLite has no process model")

    # ------------------------------------------------------------------
    # get_ddl
    # ------------------------------------------------------------------

    async def get_ddl(self, kind: str, db: str, name: str) -> str:
        """Return the original CREATE statement from ``sqlite_master``.

        Args:
            kind: Object type hint (``'table'``, ``'index'``, etc.).  Not
                  used directly — ``sqlite_master`` is queried by *name* only
                  so views and triggers are also covered.
            db:   Ignored for SQLite (only one schema is ever attached).
            name: The object name as stored in ``sqlite_master``.

        Raises:
            KeyError: If no object with *name* exists in ``sqlite_master``.
        """
        self._require_connection()
        assert self._conn is not None  # narrowing
        async with self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE name = ?", (name,)
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            raise KeyError(f"No object named {name!r} found in sqlite_master")
        return row["sql"]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_connection(self) -> None:
        if self._conn is None:
            raise RuntimeError("Not connected — call connect() first")
