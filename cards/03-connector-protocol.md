---
card_id: trovedb-03-connector-protocol
difficulty: moderate
stack: python
registered_at: 2026-04-21
---

# Connector Protocol + registry

## Goal

Define the abstract `Connector` interface in `trovedb/connectors/__init__.py`.
This is the contract every database driver implementation will satisfy.
Add a registry that maps driver-name strings to implementations, so the
config layer's `driver: "postgres"` resolves to the right class.

## Acceptance criteria

1. `Connector` is a `typing.Protocol` (not a base class) with these
   methods (all `async`):
   - `connect(profile: ConnectionProfile) -> Connection`
   - `list_databases() -> list[Database]`
   - `list_tables(db: str) -> list[Table]`
   - `describe_table(db: str, table: str) -> TableSchema`
   - `execute(sql: str, params: dict | None = None) -> ResultSet`
   - `list_processes() -> list[Process]`
   - `kill_process(pid: int) -> None`
   - `get_ddl(kind: str, db: str, name: str) -> str`
2. Domain types defined in `trovedb/connectors/types.py`: `Database`,
   `Table`, `TableSchema`, `Column`, `Index`, `ForeignKey`, `Process`,
   `ResultSet`, `Connection`. All `dataclass` or `pydantic.BaseModel`.
3. `register_connector(name)` decorator + `get_connector(name)` lookup.
   Registry lives at module level.
4. Stub `LocalSqliteConnector` in `trovedb/connectors/sqlite.py` that
   implements the Protocol with all methods raising `NotImplementedError`
   except `connect()` (which returns a placeholder). This proves the
   Protocol is checkable.
5. Tests:
   - `get_connector("sqlite")` returns the registered stub class.
   - `get_connector("nope")` raises a clean `KeyError`.
   - `mypy trovedb/connectors` (or pyright) is satisfied that the stub
     conforms to the Protocol. (If the project doesn't yet have type-
     check CI, just verify with a runtime `isinstance` check on a
     `runtime_checkable` Protocol.)

## Notes

- Mark the Protocol `@runtime_checkable` so we can verify in tests.
- This card produces ZERO functional database access. It only nails
  down the contract every subsequent connector card must satisfy.

Registered before execution. Not edited after running.
