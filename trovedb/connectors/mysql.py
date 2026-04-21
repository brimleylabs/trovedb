"""MySQL connector backed by aiomysql (async)."""

from __future__ import annotations

import logging
import time
from typing import Any
from urllib.parse import urlparse

import aiomysql

from trovedb.config import ConnectionProfile, resolve_password
from trovedb.connectors import register_connector
from trovedb.connectors.types import (
    BlockingChain,
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

_SYSTEM_SCHEMAS: frozenset[str] = frozenset(
    {"mysql", "information_schema", "performance_schema", "sys"}
)


@register_connector("mysql")
class MysqlConnector:
    """MySQL connector backed by aiomysql async.

    The connection is opened with ``autocommit=True``.  The :meth:`execute`
    method wraps read-only queries in ``START TRANSACTION READ ONLY … ROLLBACK``
    to guard against accidental writes.  Pass ``dangerous=True`` to allow DML.

    The *db* parameter accepted by :meth:`describe_table`, :meth:`list_tables`,
    and :meth:`get_ddl` is the MySQL **schema/database name**.
    """

    default_port: int = 3306

    def __init__(self) -> None:
        self._conn: aiomysql.Connection | None = None
        self._dsn: str | None = None
        self._mysql_major_version: int = 8  # detected at connect time

    # ------------------------------------------------------------------
    # connect
    # ------------------------------------------------------------------

    async def connect(self, profile: ConnectionProfile) -> Connection:
        """Open an aiomysql Connection for *profile*.

        Reads ``profile.url`` if set (``mysql://user:pass@host:port/db``);
        otherwise assembles connection params from discrete fields.
        Password is resolved via ``password_env`` indirection when present.
        """
        if profile.url:
            parsed = urlparse(profile.url)
            host = parsed.hostname or "127.0.0.1"
            port = int(parsed.port or self.default_port)
            user = parsed.username or "root"
            password = parsed.password if parsed.password is not None else ""
            db: str | None = (parsed.path or "").lstrip("/") or None
            dsn = profile.url
        else:
            host = profile.host or "127.0.0.1"
            port = int(profile.port or self.default_port)
            user = profile.user or "root"
            password = ""
            if profile.password_env:
                try:
                    password = resolve_password(profile)
                except Exception as exc:
                    logger.warning(
                        "Could not resolve password for profile %r: %s",
                        profile.name,
                        exc,
                    )
            db = profile.database or None
            dsn = f"mysql://{user}@{host}:{port}/{db or ''}"

        self._dsn = dsn
        logger.debug("MysqlConnector.connect dsn=%r", dsn)
        self._conn = await aiomysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db,
            autocommit=True,
            charset="utf8mb4",
        )

        # Detect MySQL major version once at connect time so list_blocking_chains
        # can branch between the 8.0+ and 5.7 lock-wait schemas.
        try:
            async with self._conn.cursor() as _ver_cur:
                await _ver_cur.execute("SELECT VERSION()")
                ver_row = await _ver_cur.fetchone()
            version_str: str = ver_row[0] if ver_row else "8.0.0"
            self._mysql_major_version = int(version_str.split(".")[0])
        except Exception as exc:
            logger.warning(
                "Could not detect MySQL version, defaulting to 8: %s", exc
            )
            self._mysql_major_version = 8

        return Connection(driver="mysql", dsn=dsn, connected=True)

    # ------------------------------------------------------------------
    # list_databases
    # ------------------------------------------------------------------

    async def list_databases(self) -> list[Database]:
        """Return user databases from ``INFORMATION_SCHEMA.SCHEMATA``.

        System schemas (``mysql``, ``information_schema``,
        ``performance_schema``, ``sys``) are excluded.
        """
        self._require_connection()
        assert self._conn is not None
        placeholders = ", ".join(["%s"] * len(_SYSTEM_SCHEMAS))
        sql = f"""
            SELECT SCHEMA_NAME
            FROM information_schema.SCHEMATA
            WHERE SCHEMA_NAME NOT IN ({placeholders})
            ORDER BY SCHEMA_NAME
        """
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, tuple(_SYSTEM_SCHEMAS))
            rows = await cur.fetchall()
        return [Database(name=r["SCHEMA_NAME"]) for r in rows]

    # ------------------------------------------------------------------
    # list_tables
    # ------------------------------------------------------------------

    async def list_tables(self, db: str) -> list[Table]:
        """Return tables and views in database *db*.

        Uses ``TABLE_ROWS`` for cheap row-count estimates and
        ``DATA_LENGTH + INDEX_LENGTH`` for size.
        """
        self._require_connection()
        assert self._conn is not None
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT
                    TABLE_NAME,
                    TABLE_TYPE,
                    TABLE_ROWS,
                    DATA_LENGTH + IFNULL(INDEX_LENGTH, 0) AS size_bytes
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME
                """,
                (db,),
            )
            rows = await cur.fetchall()
        return [
            Table(
                name=r["TABLE_NAME"],
                db=db,
                row_count=r["TABLE_ROWS"],
                size_bytes=r["size_bytes"],
                table_type=r["TABLE_TYPE"],
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # describe_table
    # ------------------------------------------------------------------

    async def describe_table(self, db: str, table: str) -> TableSchema:
        """Populate :class:`~trovedb.connectors.types.TableSchema` from
        MySQL ``INFORMATION_SCHEMA``.

        Args:
            db:    Database (schema) name.
            table: Table name.
        """
        self._require_connection()
        assert self._conn is not None

        # ---- columns ---------------------------------------------------
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT
                    COLUMN_NAME,
                    COLUMN_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    COLUMN_COMMENT
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (db, table),
            )
            col_rows = await cur.fetchall()

        columns = [
            Column(
                name=r["COLUMN_NAME"],
                data_type=r["COLUMN_TYPE"],
                nullable=r["IS_NULLABLE"] == "YES",
                default=r["COLUMN_DEFAULT"],
                comment=r["COLUMN_COMMENT"] or None,
            )
            for r in col_rows
        ]

        # ---- indexes ---------------------------------------------------
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT
                    INDEX_NAME,
                    NON_UNIQUE,
                    COLUMN_NAME,
                    SEQ_IN_INDEX
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
                """,
                (db, table),
            )
            idx_rows = await cur.fetchall()

        # Group columns by index name
        idx_map: dict[str, dict[str, Any]] = {}
        for r in idx_rows:
            idx_name = r["INDEX_NAME"]
            if idx_name not in idx_map:
                idx_map[idx_name] = {
                    "columns": [],
                    "unique": r["NON_UNIQUE"] == 0,
                    "primary": idx_name == "PRIMARY",
                }
            idx_map[idx_name]["columns"].append(r["COLUMN_NAME"])

        indexes = [
            Index(
                name=idx_name,
                columns=info["columns"],
                unique=info["unique"],
                primary=info["primary"],
            )
            for idx_name, info in idx_map.items()
        ]

        # ---- foreign keys ----------------------------------------------
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT
                    kcu.CONSTRAINT_NAME,
                    kcu.COLUMN_NAME,
                    kcu.REFERENCED_TABLE_NAME,
                    kcu.REFERENCED_COLUMN_NAME,
                    kcu.ORDINAL_POSITION
                FROM information_schema.KEY_COLUMN_USAGE kcu
                JOIN information_schema.TABLE_CONSTRAINTS tc
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                   AND tc.TABLE_SCHEMA    = kcu.TABLE_SCHEMA
                   AND tc.TABLE_NAME      = kcu.TABLE_NAME
                WHERE kcu.TABLE_SCHEMA = %s
                  AND kcu.TABLE_NAME   = %s
                  AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
                ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
                """,
                (db, table),
            )
            fk_rows = await cur.fetchall()

        fk_map: dict[str, dict[str, Any]] = {}
        for r in fk_rows:
            fk_name = r["CONSTRAINT_NAME"]
            if fk_name not in fk_map:
                fk_map[fk_name] = {
                    "columns": [],
                    "ref_table": r["REFERENCED_TABLE_NAME"],
                    "ref_columns": [],
                }
            fk_map[fk_name]["columns"].append(r["COLUMN_NAME"])
            fk_map[fk_name]["ref_columns"].append(r["REFERENCED_COLUMN_NAME"])

        foreign_keys = [
            ForeignKey(
                name=fk_name,
                columns=info["columns"],
                ref_table=info["ref_table"],
                ref_columns=info["ref_columns"],
            )
            for fk_name, info in fk_map.items()
        ]

        # ---- DDL -------------------------------------------------------
        ddl = await self.get_ddl("table", db, table)

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
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        dangerous: bool = False,
    ) -> ResultSet:
        """Execute *sql* and return a :class:`~trovedb.connectors.types.ResultSet`.

        By default the query runs inside ``START TRANSACTION READ ONLY … ROLLBACK``
        to prevent accidental writes.  Pass ``dangerous=True`` to allow
        INSERT / UPDATE / DELETE / DDL (auto-committed via ``autocommit=True``).

        Named parameters follow MySQL's ``%(name)s`` style::

            await conn.execute(
                "SELECT * FROM t WHERE id = %(id)s", {"id": 1}
            )
        """
        self._require_connection()
        assert self._conn is not None
        start = time.monotonic()
        in_txn = False
        columns: list[str] = []
        rows: list[tuple[Any, ...]] = []

        async with self._conn.cursor() as cur:
            if not dangerous:
                await cur.execute("START TRANSACTION READ ONLY")
                in_txn = True
            try:
                await cur.execute(sql, params)
                if cur.description:
                    raw_rows = await cur.fetchall()
                    columns = [desc[0] for desc in cur.description]
                    rows = [tuple(r) for r in raw_rows]
            except Exception:
                if in_txn:
                    try:
                        await cur.execute("ROLLBACK")
                    except Exception:
                        pass
                raise
            else:
                if in_txn:
                    try:
                        await cur.execute("ROLLBACK")
                    except Exception as exc:
                        logger.warning("ROLLBACK after read-only execute failed: %s", exc)

        duration_ms = (time.monotonic() - start) * 1000
        return ResultSet(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            duration_ms=duration_ms,
        )

    # ------------------------------------------------------------------
    # list_processes
    # ------------------------------------------------------------------

    async def list_processes(self) -> list[Process]:
        """Return active sessions from ``INFORMATION_SCHEMA.PROCESSLIST``.

        Joins with ``INFORMATION_SCHEMA.INNODB_TRX`` and
        ``performance_schema.data_lock_waits`` (MySQL 8.0+) to populate
        ``blocked_by`` with the blocking thread's process ID.  The
        connector's own session is excluded via ``CONNECTION_ID()``.
        """
        self._require_connection()
        assert self._conn is not None
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT
                    p.ID                            AS pid,
                    p.USER                          AS user,
                    p.HOST                          AS host,
                    p.DB                            AS db,
                    p.TIME                          AS time_seconds,
                    p.STATE                         AS state,
                    p.INFO                          AS info,
                    bt.trx_mysql_thread_id          AS blocked_by
                FROM information_schema.PROCESSLIST p
                LEFT JOIN information_schema.INNODB_TRX wt
                       ON wt.trx_mysql_thread_id = p.ID
                LEFT JOIN performance_schema.data_lock_waits dlw
                       ON dlw.REQUESTING_ENGINE_TRANSACTION_ID = wt.trx_id
                LEFT JOIN information_schema.INNODB_TRX bt
                       ON bt.trx_id = dlw.BLOCKING_ENGINE_TRANSACTION_ID
                WHERE p.ID != CONNECTION_ID()
                ORDER BY p.ID
                """
            )
            rows = await cur.fetchall()
        return [
            Process(
                pid=int(r["pid"]),
                user=r["user"],
                db=r["db"],
                state=r["state"],
                info=r["info"],
                time_seconds=float(r["time_seconds"]) if r["time_seconds"] is not None else None,
                host=r["host"],
                blocked_by=int(r["blocked_by"]) if r["blocked_by"] is not None else None,
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # kill_process
    # ------------------------------------------------------------------

    async def kill_process(self, pid: int, force: bool = False) -> None:
        """Cancel or terminate a MySQL thread.

        Args:
            pid:   The target thread ID (from ``INFORMATION_SCHEMA.PROCESSLIST.ID``).
            force: ``False`` (default) runs ``KILL QUERY <pid>`` which cancels
                   the current statement but keeps the connection alive.
                   ``True`` runs ``KILL <pid>`` which drops the connection.

        Raises:
            :class:`aiomysql.OperationalError`: When the pid is not found or
                the caller lacks the required privilege.
        """
        self._require_connection()
        assert self._conn is not None
        kill_sql = f"KILL {'QUERY ' if not force else ''}{pid}"
        async with self._conn.cursor() as cur:
            await cur.execute(kill_sql)

    # ------------------------------------------------------------------
    # list_blocking_chains
    # ------------------------------------------------------------------

    async def list_blocking_chains(self) -> list[BlockingChain]:
        """Return current lock-blocking chains.

        Branches automatically on the MySQL major version detected at
        :meth:`connect` time:

        * **8.0+**: joins ``performance_schema.data_lock_waits`` and
          ``performance_schema.data_locks``.
        * **5.7**: falls back to deprecated
          ``INFORMATION_SCHEMA.INNODB_LOCK_WAITS`` and
          ``INNODB_LOCKS``.

        ``depth`` is computed in Python by walking the chain upward;
        see :meth:`trovedb.connectors.postgres.PostgresConnector.list_blocking_chains`
        for the algorithm.
        """
        self._require_connection()
        assert self._conn is not None

        if self._mysql_major_version >= 8:
            rows = await self._list_blocking_chains_8()
        else:
            rows = await self._list_blocking_chains_57()

        if not rows:
            return []

        # Compute depth (same algorithm as the Postgres connector).
        waiter_set: set[int] = {r["waiter_pid"] for r in rows}
        waiter_to_holder: dict[int, int] = {}
        for r in rows:
            waiter_to_holder.setdefault(r["waiter_pid"], r["holder_pid"])

        def _depth(holder_pid: int, seen: set[int]) -> int:
            if holder_pid not in waiter_set or holder_pid in seen:
                return 1
            seen.add(holder_pid)
            parent = waiter_to_holder.get(holder_pid)
            if parent is None:
                return 1
            return 1 + _depth(parent, seen)

        return [
            BlockingChain(
                waiter_pid=r["waiter_pid"],
                waiter_user=r["waiter_user"],
                waiter_query=r["waiter_query"],
                holder_pid=r["holder_pid"],
                holder_user=r["holder_user"],
                holder_query=r["holder_query"],
                lock_type=r["lock_type"],
                object_name=r["object_name"],
                waited_seconds=float(r["waited_seconds"]) if r["waited_seconds"] else 0.0,
                depth=_depth(r["holder_pid"], {r["waiter_pid"]}),
            )
            for r in rows
        ]

    async def _list_blocking_chains_8(self) -> list[dict]:
        """MySQL 8.0+ path — uses ``performance_schema.data_lock_waits``."""
        assert self._conn is not None
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT
                    wt.trx_mysql_thread_id          AS waiter_pid,
                    p_wait.USER                     AS waiter_user,
                    COALESCE(p_wait.INFO, '')       AS waiter_query,
                    bt.trx_mysql_thread_id          AS holder_pid,
                    p_block.USER                    AS holder_user,
                    COALESCE(p_block.INFO, '')      AS holder_query,
                    dl.LOCK_TYPE                    AS lock_type,
                    CONCAT(dl.OBJECT_SCHEMA, '.', dl.OBJECT_NAME) AS object_name,
                    COALESCE(p_wait.TIME, 0)        AS waited_seconds
                FROM performance_schema.data_lock_waits dlw
                JOIN information_schema.INNODB_TRX wt
                    ON wt.trx_id = dlw.REQUESTING_ENGINE_TRANSACTION_ID
                JOIN information_schema.INNODB_TRX bt
                    ON bt.trx_id = dlw.BLOCKING_ENGINE_TRANSACTION_ID
                JOIN information_schema.PROCESSLIST p_wait
                    ON p_wait.ID = wt.trx_mysql_thread_id
                JOIN information_schema.PROCESSLIST p_block
                    ON p_block.ID = bt.trx_mysql_thread_id
                JOIN performance_schema.data_locks dl
                    ON dl.ENGINE_TRANSACTION_ID = dlw.REQUESTING_ENGINE_TRANSACTION_ID
                   AND dl.LOCK_STATUS = 'WAITING'
                WHERE p_wait.ID != CONNECTION_ID()
                  AND p_block.ID != CONNECTION_ID()
                """
            )
            return await cur.fetchall()  # type: ignore[return-value]

    async def _list_blocking_chains_57(self) -> list[dict]:
        """MySQL 5.7 path — uses deprecated ``INNODB_LOCK_WAITS``."""
        assert self._conn is not None
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT
                    wt.trx_mysql_thread_id          AS waiter_pid,
                    p_wait.USER                     AS waiter_user,
                    COALESCE(p_wait.INFO, '')       AS waiter_query,
                    bt.trx_mysql_thread_id          AS holder_pid,
                    p_block.USER                    AS holder_user,
                    COALESCE(p_block.INFO, '')      AS holder_query,
                    lw.requested_lock_id            AS lock_type,
                    lw.blocking_lock_id             AS object_name,
                    COALESCE(p_wait.TIME, 0)        AS waited_seconds
                FROM information_schema.INNODB_LOCK_WAITS lw
                JOIN information_schema.INNODB_TRX wt
                    ON wt.trx_id = lw.requesting_trx_id
                JOIN information_schema.INNODB_TRX bt
                    ON bt.trx_id = lw.blocking_trx_id
                JOIN information_schema.PROCESSLIST p_wait
                    ON p_wait.ID = wt.trx_mysql_thread_id
                JOIN information_schema.PROCESSLIST p_block
                    ON p_block.ID = bt.trx_mysql_thread_id
                WHERE p_wait.ID != CONNECTION_ID()
                  AND p_block.ID != CONNECTION_ID()
                """
            )
            return await cur.fetchall()  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # get_ddl
    # ------------------------------------------------------------------

    async def get_ddl(self, kind: str, db: str, name: str) -> str:  # noqa: ARG002
        """Return the ``CREATE TABLE`` DDL for *name* in database *db*.

        Runs ``SHOW CREATE TABLE `db`.`name``` and returns the second
        column — MySQL provides exact DDL, no reconstruction needed.

        Args:
            kind: Object type hint (currently only ``'table'`` is supported).
            db:   Database (schema) name.
            name: Table name.

        Raises:
            :class:`KeyError`: If *name* is not found in database *db*.
        """
        self._require_connection()
        assert self._conn is not None
        async with self._conn.cursor() as cur:
            try:
                await cur.execute(f"SHOW CREATE TABLE `{db}`.`{name}`")
                row = await cur.fetchone()
            except aiomysql.OperationalError as exc:
                raise KeyError(f"Table {name!r} not found in database {db!r}") from exc
        if row is None:
            raise KeyError(f"Table {name!r} not found in database {db!r}")
        # SHOW CREATE TABLE returns (table_name, create_statement)
        return row[1]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_connection(self) -> None:
        if self._conn is None:
            raise RuntimeError("Not connected — call connect() first")
