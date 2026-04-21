"""Domain types shared across all connector implementations."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Database:
    """A database (schema) available on a connection."""

    name: str
    size_bytes: int | None = None
    comment: str | None = None


@dataclass
class Column:
    """A single column in a table."""

    name: str
    data_type: str
    nullable: bool = True
    default: str | None = None
    comment: str | None = None


@dataclass
class Index:
    """An index on a table."""

    name: str
    columns: list[str] = field(default_factory=list)
    unique: bool = False
    primary: bool = False


@dataclass
class ForeignKey:
    """A foreign-key constraint on a table."""

    name: str
    columns: list[str]
    ref_table: str
    ref_columns: list[str]


@dataclass
class TableSchema:
    """Full structural description of a table."""

    db: str
    table: str
    columns: list[Column] = field(default_factory=list)
    indexes: list[Index] = field(default_factory=list)
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    ddl: str | None = None


@dataclass
class Table:
    """A table (or view) available inside a database."""

    name: str
    db: str
    row_count: int | None = None
    size_bytes: int | None = None
    table_type: str = "BASE TABLE"  # e.g. "VIEW"


@dataclass
class Process:
    """A server process / session visible via SHOW PROCESSLIST or pg_stat_activity."""

    pid: int
    user: str | None = None
    db: str | None = None
    state: str | None = None
    info: str | None = None
    time_seconds: float | None = None
    host: str | None = None
    wait_event: str | None = None
    blocked_by: int | None = None


@dataclass
class ResultSet:
    """The result of an executed SQL statement."""

    columns: list[str]
    rows: list[tuple]  # type: ignore[type-arg]
    row_count: int = 0
    duration_ms: float | None = None


@dataclass
class Connection:
    """An open (or placeholder) connection to a database server."""

    driver: str
    dsn: str | None = None
    connected: bool = False
    backend_pid: int | None = None


@dataclass(frozen=True, slots=True)
class BlockingChain:
    """A single (holder → waiter) pair in a lock-blocking chain.

    ``depth=1`` means the waiter is directly blocked: the holder is not
    itself waiting on any lock.  ``depth≥2`` means the relationship is
    transitive — the holder is itself a waiter further up the chain.
    """

    waiter_pid: int
    waiter_user: str
    waiter_query: str
    holder_pid: int
    holder_user: str
    holder_query: str
    lock_type: str  # e.g. "ROW", "TABLE", "ADVISORY", "relation", …
    object_name: str | None  # e.g. "public.trips" or None for advisory
    waited_seconds: float
    depth: int  # 1 for direct block, >1 for transitive
