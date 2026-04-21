"""Shared fake connectors for trovedb screen tests.

Each fake tracks which connector methods were called so tests can assert
lazy-loading and execution-path contracts without a real database.
"""

from __future__ import annotations

from trovedb.connectors.types import (
    Database,
    ResultSet,
    Table,
    TableSchema,
)


class FakeSchemaConnector:
    """In-memory connector returning canned schema data for SchemaScreen tests."""

    def __init__(
        self,
        databases: list[Database] | None = None,
        tables_by_db: dict[str, list[Table]] | None = None,
        schema_by_table: dict[str, TableSchema] | None = None,
        ddl_by_table: dict[str, str] | None = None,
        *,
        fail_list_databases: bool = False,
    ) -> None:
        self._databases = databases if databases is not None else [Database(name="testdb")]
        self._tables_by_db: dict[str, list[Table]] = tables_by_db or {}
        self._schema_by_table: dict[str, TableSchema] = schema_by_table or {}
        self._ddl_by_table: dict[str, str] = ddl_by_table or {}
        self._fail_list_databases = fail_list_databases

        # Call-tracking attributes (tests inspect these)
        self.list_databases_calls: int = 0
        self.list_tables_calls: list[str] = []
        self.describe_table_calls: list[tuple[str, str]] = []
        self.get_ddl_calls: list[tuple[str, str, str]] = []
        self.execute_calls: list[str] = []

    async def list_databases(self) -> list[Database]:
        self.list_databases_calls += 1
        if self._fail_list_databases:
            raise RuntimeError("connection lost")
        return list(self._databases)

    async def list_tables(self, db: str) -> list[Table]:
        self.list_tables_calls.append(db)
        return list(self._tables_by_db.get(db, []))

    async def describe_table(self, db: str, table: str) -> TableSchema:
        self.describe_table_calls.append((db, table))
        key = f"{db}.{table}"
        if key in self._schema_by_table:
            return self._schema_by_table[key]
        return TableSchema(db=db, table=table)

    async def get_ddl(self, kind: str, db: str, name: str) -> str:
        self.get_ddl_calls.append((kind, db, name))
        key = f"{db}.{name}"
        return self._ddl_by_table.get(key, f"CREATE TABLE {name} (id INT);")

    async def execute(self, sql: str, params: object = None) -> ResultSet:  # noqa: ARG002
        self.execute_calls.append(sql)
        return ResultSet(columns=[], rows=[], row_count=0)
