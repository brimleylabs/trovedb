"""PostgreSQL connector backed by psycopg (>=3, async)."""

from __future__ import annotations

import logging
import time
from typing import Any

import psycopg
from psycopg.rows import dict_row

from trovedb.config import ConnectionProfile, resolve_password
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


@register_connector("postgres")
class PostgresConnector:
    """PostgreSQL connector backed by psycopg async.

    The connection is opened with ``autocommit=True`` so that operator
    commands (``pg_cancel_backend``, ``pg_terminate_backend``) work
    without needing to manage transaction state, and so that
    ``BEGIN READ ONLY / ROLLBACK`` can be used to guard the
    :meth:`execute` path against accidental writes.

    The *db* parameter accepted by :meth:`describe_table` and
    :meth:`get_ddl` is treated as the **schema name** (not the database
    name) because a psycopg connection is already scoped to a single
    database, and schemas are the namespacing mechanism within it.
    """

    def __init__(self) -> None:
        self._conn: psycopg.AsyncConnection[Any] | None = None
        self._dsn: str | None = None

    # ------------------------------------------------------------------
    # connect
    # ------------------------------------------------------------------

    async def connect(self, profile: ConnectionProfile) -> Connection:
        """Open a psycopg :class:`~psycopg.AsyncConnection` for *profile*.

        Reads ``profile.url`` if set; otherwise assembles a libpq
        connection string from the discrete fields.  Password is
        resolved via ``password_env`` indirection when present.
        """
        if profile.url:
            dsn = profile.url
        else:
            parts: list[str] = []
            if profile.host:
                parts.append(f"host={profile.host}")
            if profile.port:
                parts.append(f"port={profile.port}")
            if profile.user:
                parts.append(f"user={profile.user}")
            if profile.database:
                parts.append(f"dbname={profile.database}")
            if profile.password_env:
                try:
                    password = resolve_password(profile)
                    parts.append(f"password={password}")
                except Exception as exc:
                    logger.warning(
                        "Could not resolve password for profile %r: %s", profile.name, exc
                    )
            if profile.ssl_mode:
                parts.append(f"sslmode={profile.ssl_mode}")
            dsn = " ".join(parts) or "host=localhost"

        self._dsn = dsn
        logger.debug("PostgresConnector.connect dsn=%r", dsn)
        self._conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
        return Connection(driver="postgres", dsn=dsn, connected=True)

    # ------------------------------------------------------------------
    # list_databases
    # ------------------------------------------------------------------

    async def list_databases(self) -> list[Database]:
        """Return databases from ``pg_database WHERE NOT datistemplate``."""
        self._require_connection()
        assert self._conn is not None
        async with self._conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT
                    datname,
                    pg_catalog.pg_database_size(oid) AS size_bytes
                FROM pg_database
                WHERE NOT datistemplate
                ORDER BY datname
                """
            )
            rows = await cur.fetchall()
        return [Database(name=r["datname"], size_bytes=r["size_bytes"]) for r in rows]

    # ------------------------------------------------------------------
    # list_tables
    # ------------------------------------------------------------------

    async def list_tables(self, db: str) -> list[Table]:
        """Return tables and views in schema *db* with ``reltuples`` row estimates.

        ``pg_class.reltuples`` is cheap (no full-scan) and sufficient for
        the operator console.  Pass ``db`` as the schema name.
        """
        self._require_connection()
        assert self._conn is not None
        async with self._conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT
                    t.table_name,
                    t.table_type,
                    COALESCE(c.reltuples::bigint, -1) AS row_estimate,
                    pg_catalog.pg_total_relation_size(c.oid)  AS size_bytes
                FROM information_schema.tables t
                LEFT JOIN pg_catalog.pg_class c
                       ON c.relname = t.table_name
                LEFT JOIN pg_catalog.pg_namespace n
                       ON n.oid = c.relnamespace AND n.nspname = t.table_schema
                WHERE t.table_schema = %s
                  AND t.table_type IN ('BASE TABLE', 'VIEW')
                ORDER BY t.table_name
                """,
                (db,),
            )
            rows = await cur.fetchall()
        return [
            Table(
                name=r["table_name"],
                db=db,
                row_count=r["row_estimate"] if r["row_estimate"] >= 0 else None,
                size_bytes=r["size_bytes"],
                table_type=r["table_type"],
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # describe_table
    # ------------------------------------------------------------------

    async def describe_table(self, db: str, table: str) -> TableSchema:
        """Populate :class:`~trovedb.connectors.types.TableSchema` from
        ``information_schema`` and ``pg_catalog``.

        Args:
            db:    Schema name (e.g. ``'public'``).
            table: Table name.
        """
        self._require_connection()
        assert self._conn is not None
        schema = db

        # ---- columns ---------------------------------------------------
        async with self._conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, table),
            )
            col_rows = await cur.fetchall()

        columns = [
            Column(
                name=r["column_name"],
                data_type=r["data_type"],
                nullable=r["is_nullable"] == "YES",
                default=r["column_default"],
            )
            for r in col_rows
        ]

        # ---- indexes (via pg_catalog for accuracy) ---------------------
        async with self._conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT
                    i.relname                               AS index_name,
                    ix.indisunique                          AS is_unique,
                    ix.indisprimary                         AS is_primary,
                    array_agg(a.attname ORDER BY k.ord)     AS column_names
                FROM pg_catalog.pg_index ix
                JOIN pg_catalog.pg_class c  ON c.oid  = ix.indrelid
                JOIN pg_catalog.pg_class i  ON i.oid  = ix.indexrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord)
                JOIN pg_catalog.pg_attribute a
                     ON a.attrelid = c.oid AND a.attnum = k.attnum
                WHERE c.relname = %s AND n.nspname = %s
                GROUP BY i.relname, ix.indisunique, ix.indisprimary
                ORDER BY i.relname
                """,
                (table, schema),
            )
            idx_rows = await cur.fetchall()

        indexes = [
            Index(
                name=r["index_name"],
                columns=list(r["column_names"]),
                unique=r["is_unique"],
                primary=r["is_primary"],
            )
            for r in idx_rows
        ]

        # ---- foreign keys ----------------------------------------------
        async with self._conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT
                    c.conname                                           AS constraint_name,
                    array_agg(a.attname  ORDER BY k.ord)                AS from_columns,
                    cf.relname                                          AS ref_table,
                    array_agg(af.attname ORDER BY k.ord)                AS ref_columns
                FROM pg_catalog.pg_constraint c
                JOIN pg_catalog.pg_class cl   ON cl.oid  = c.conrelid
                JOIN pg_catalog.pg_namespace n ON n.oid  = cl.relnamespace
                JOIN pg_catalog.pg_class cf   ON cf.oid  = c.confrelid
                CROSS JOIN LATERAL
                    unnest(c.conkey, c.confkey) WITH ORDINALITY
                        AS k(attnum, ref_attnum, ord)
                JOIN pg_catalog.pg_attribute a
                     ON a.attrelid = c.conrelid  AND a.attnum = k.attnum
                JOIN pg_catalog.pg_attribute af
                     ON af.attrelid = c.confrelid AND af.attnum = k.ref_attnum
                WHERE cl.relname = %s AND n.nspname = %s AND c.contype = 'f'
                GROUP BY c.conname, cf.relname
                ORDER BY c.conname
                """,
                (table, schema),
            )
            fk_rows = await cur.fetchall()

        foreign_keys = [
            ForeignKey(
                name=r["constraint_name"],
                columns=list(r["from_columns"]),
                ref_table=r["ref_table"],
                ref_columns=list(r["ref_columns"]),
            )
            for r in fk_rows
        ]

        # ---- DDL -------------------------------------------------------
        ddl = await self.get_ddl("table", schema, table)

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

        By default the query runs inside ``BEGIN READ ONLY … ROLLBACK`` to
        prevent accidental writes.  Pass ``dangerous=True`` to allow
        INSERT/UPDATE/DELETE/DDL (each auto-commits via the connection's
        ``autocommit=True`` setting).

        Named parameters follow psycopg's ``%(name)s`` style::

            await conn.execute(
                "SELECT * FROM t WHERE id = %(id)s", {"id": 1}
            )
        """
        self._require_connection()
        assert self._conn is not None
        start = time.monotonic()
        in_readonly_txn = False
        try:
            if not dangerous:
                await self._conn.execute("BEGIN READ ONLY")
                in_readonly_txn = True
            async with self._conn.cursor() as cur:
                await cur.execute(sql, params)
                if cur.description:
                    rows = await cur.fetchall()
                    columns = [desc.name for desc in cur.description]
                else:
                    rows = []
                    columns = []
        except Exception:
            if in_readonly_txn:
                try:
                    await self._conn.execute("ROLLBACK")
                except Exception:
                    pass
            raise
        if in_readonly_txn:
            try:
                await self._conn.execute("ROLLBACK")
            except Exception as exc:
                logger.warning("ROLLBACK after read-only execute failed: %s", exc)
        duration_ms = (time.monotonic() - start) * 1000
        return ResultSet(
            columns=columns,
            rows=[tuple(r) for r in rows],
            row_count=len(rows),
            duration_ms=duration_ms,
        )

    # ------------------------------------------------------------------
    # list_processes
    # ------------------------------------------------------------------

    async def list_processes(self) -> list[Process]:
        """Return active sessions from ``pg_stat_activity``, excluding self.

        Uses ``pg_blocking_pids()`` to populate ``blocked_by`` with the
        first PID that is blocking each session (or ``None`` if not blocked).
        """
        self._require_connection()
        assert self._conn is not None
        async with self._conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT
                    a.pid,
                    a.usename,
                    a.datname,
                    a.state,
                    a.query,
                    EXTRACT(EPOCH FROM (now() - a.query_start))::float
                        AS runtime_seconds,
                    CASE
                        WHEN a.wait_event IS NOT NULL
                        THEN a.wait_event_type || ': ' || a.wait_event
                        ELSE NULL
                    END                                                 AS wait_event,
                    a.client_addr::text                                 AS host,
                    (pg_blocking_pids(a.pid))[1]                       AS blocked_by
                FROM pg_stat_activity a
                WHERE a.pid != pg_backend_pid()
                ORDER BY a.pid
                """
            )
            rows = await cur.fetchall()
        return [
            Process(
                pid=r["pid"],
                user=r["usename"],
                db=r["datname"],
                state=r["state"],
                info=r["query"],
                time_seconds=r["runtime_seconds"],
                host=r["host"],
                wait_event=r["wait_event"],
                blocked_by=r["blocked_by"],
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # kill_process
    # ------------------------------------------------------------------

    async def kill_process(self, pid: int, force: bool = False) -> None:  # type: ignore[override]
        """Cancel or terminate a backend process.

        Args:
            pid:   The target backend PID.
            force: ``False`` (default) calls ``pg_cancel_backend(pid)``
                   which cancels the current query but keeps the session
                   alive.  ``True`` calls ``pg_terminate_backend(pid)``
                   which drops the session entirely.

        Raises:
            :class:`psycopg.OperationalError`: When the backend function
                returns ``False`` (pid not found or permission denied).
        """
        self._require_connection()
        assert self._conn is not None
        fn = "pg_terminate_backend" if force else "pg_cancel_backend"
        async with self._conn.cursor() as cur:
            await cur.execute(f"SELECT {fn}(%s)", (pid,))
            row = await cur.fetchone()
        if row is None or not row[0]:
            raise psycopg.OperationalError(
                f"{fn}({pid}) returned False — pid not found or permission denied"
            )

    # ------------------------------------------------------------------
    # get_ddl
    # ------------------------------------------------------------------

    async def get_ddl(self, kind: str, db: str, name: str) -> str:  # noqa: ARG002
        """Reconstruct a syntactically valid ``CREATE TABLE`` statement from
        ``pg_catalog``.

        The output includes:

        * Column definitions with ``format_type``-accurate type names,
          ``DEFAULT``, and ``NOT NULL``.
        * An inline ``PRIMARY KEY`` clause (if one exists).
        * Separate ``CREATE INDEX`` statements for non-PK indexes.
        * ``ALTER TABLE … ADD CONSTRAINT`` clauses for foreign keys.

        Args:
            kind: Object type hint (currently only ``'table'`` is supported).
            db:   Schema name (e.g. ``'public'``).
            name: Table name.

        Raises:
            :class:`KeyError`: If *name* is not found in schema *db*.
        """
        self._require_connection()
        assert self._conn is not None
        schema = db

        # ---- table OID -------------------------------------------------
        async with self._conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT c.oid
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s AND n.nspname = %s AND c.relkind = 'r'
                """,
                (name, schema),
            )
            oid_row = await cur.fetchone()
        if oid_row is None:
            raise KeyError(f"Table {name!r} not found in schema {schema!r}")
        table_oid: int = oid_row["oid"]

        # ---- columns ---------------------------------------------------
        async with self._conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT
                    a.attname                                           AS column_name,
                    pg_catalog.format_type(a.atttypid, a.atttypmod)    AS data_type,
                    a.attnotnull                                        AS not_null,
                    pg_catalog.pg_get_expr(d.adbin, d.adrelid)         AS column_default
                FROM pg_catalog.pg_attribute a
                LEFT JOIN pg_catalog.pg_attrdef d
                       ON d.adrelid = a.attrelid AND d.adnum = a.attnum
                WHERE a.attrelid = %s
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                ORDER BY a.attnum
                """,
                (table_oid,),
            )
            col_rows = await cur.fetchall()

        # ---- primary key columns ---------------------------------------
        async with self._conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT array_agg(a.attname ORDER BY k.ord) AS pk_columns
                FROM pg_catalog.pg_constraint c
                CROSS JOIN LATERAL
                    unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
                JOIN pg_catalog.pg_attribute a
                     ON a.attrelid = c.conrelid AND a.attnum = k.attnum
                WHERE c.conrelid = %s AND c.contype = 'p'
                GROUP BY c.conname
                """,
                (table_oid,),
            )
            pk_row = await cur.fetchone()
        pk_columns: list[str] = (
            list(pk_row["pk_columns"]) if (pk_row and pk_row["pk_columns"]) else []
        )

        # ---- build column list -----------------------------------------
        col_defs: list[str] = []
        for row in col_rows:
            col_def = f"    {row['column_name']} {row['data_type']}"
            if row["column_default"] is not None:
                col_def += f" DEFAULT {row['column_default']}"
            if row["not_null"]:
                col_def += " NOT NULL"
            col_defs.append(col_def)
        if pk_columns:
            col_defs.append(f"    PRIMARY KEY ({', '.join(pk_columns)})")

        ddl_parts: list[str] = [
            f"CREATE TABLE {name} (\n" + ",\n".join(col_defs) + "\n);"
        ]

        # ---- non-PK indexes --------------------------------------------
        async with self._conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT pi.indexdef
                FROM pg_catalog.pg_indexes pi
                WHERE pi.tablename = %s
                  AND pi.schemaname = %s
                  AND pi.indexname NOT IN (
                      SELECT conname
                      FROM pg_catalog.pg_constraint
                      WHERE conrelid = %s AND contype = 'p'
                  )
                ORDER BY pi.indexname
                """,
                (name, schema, table_oid),
            )
            idx_rows = await cur.fetchall()
        for idx_row in idx_rows:
            ddl_parts.append(idx_row["indexdef"] + ";")

        # ---- foreign keys ----------------------------------------------
        async with self._conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT
                    c.conname,
                    pg_catalog.pg_get_constraintdef(c.oid) AS constraintdef
                FROM pg_catalog.pg_constraint c
                WHERE c.conrelid = %s AND c.contype = 'f'
                ORDER BY c.conname
                """,
                (table_oid,),
            )
            fk_rows = await cur.fetchall()
        for fk_row in fk_rows:
            ddl_parts.append(
                f"ALTER TABLE {name} ADD CONSTRAINT"
                f" {fk_row['conname']} {fk_row['constraintdef']};"
            )

        return "\n".join(ddl_parts)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_connection(self) -> None:
        if self._conn is None:
            raise RuntimeError("Not connected — call connect() first")
