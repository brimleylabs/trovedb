"""Data helpers: query history, saved queries, and path utilities."""

from __future__ import annotations

import logging
import re
from pathlib import Path

import aiosqlite
from platformdirs import user_data_dir

logger = logging.getLogger(__name__)

# SQL keywords that identify read-only statements.
_READ_ONLY_PREFIXES: frozenset[str] = frozenset(
    {
        "select",
        "with",
        "show",
        "explain",
        "describe",
        "desc",
        "pragma",
        "values",
        "table",
    }
)

# Strip leading block and line comments before checking the first keyword.
_COMMENT_RE = re.compile(r"(--[^\n]*|/\*.*?\*/)", re.DOTALL)


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def get_history_db_path() -> Path:
    """Return the path to the query history SQLite database.

    Resolves to ``~/.local/share/trovedb/history.db`` on Linux /
    ``%APPDATA%/trovedb/history.db`` on Windows.
    """
    return Path(user_data_dir("trovedb")) / "history.db"


def get_queries_dir() -> Path:
    """Return the directory where manually saved queries are stored."""
    return Path(user_data_dir("trovedb")) / "queries"


# ---------------------------------------------------------------------------
# Write-query heuristic
# ---------------------------------------------------------------------------


def is_write_query(sql: str) -> bool:
    """Return ``True`` if *sql* looks like a write (non-read) statement.

    Uses a naive first-keyword heuristic — the same approach as pgcli/mycli.
    Leading block comments and line comments are stripped before the check.
    """
    stripped = _COMMENT_RE.sub("", sql).strip()
    if not stripped:
        return False
    first_word = re.split(r"\s+", stripped, maxsplit=1)[0].lower()
    return first_word not in _READ_ONLY_PREFIXES


# ---------------------------------------------------------------------------
# QueryHistory
# ---------------------------------------------------------------------------


class QueryHistory:
    """Async manager for the query history database.

    Every successful or failed ``execute`` call is appended to a SQLite
    table at ``db_path`` (default: :func:`get_history_db_path`).
    """

    _CREATE_SQL = """
        CREATE TABLE IF NOT EXISTS history (
            id        INTEGER PRIMARY KEY,
            profile   TEXT NOT NULL,
            sql       TEXT NOT NULL,
            ran_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
            duration_ms INTEGER,
            error     TEXT
        )
    """

    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = db_path or get_history_db_path()

    async def _ensure_schema(self) -> None:
        """Create the history table if it does not exist."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(self._CREATE_SQL)
            await db.commit()

    async def record(
        self,
        profile: str,
        sql: str,
        duration_ms: int | None = None,
        error: str | None = None,
    ) -> None:
        """Append a history row for *profile*."""
        await self._ensure_schema()
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT INTO history (profile, sql, duration_ms, error)"
                " VALUES (?, ?, ?, ?)",
                (profile, sql, duration_ms, error),
            )
            await db.commit()
        logger.debug(
            "history: recorded profile=%r sql=%r error=%r", profile, sql[:60], error
        )

    async def fetch(
        self,
        profile: str,
        limit: int = 100,
    ) -> list[tuple[int, str]]:
        """Return ``(id, sql)`` pairs for *profile*, most recent first.

        Returns an empty list if the DB file does not exist yet.
        """
        if not self._db_path.exists():
            return []
        await self._ensure_schema()
        async with aiosqlite.connect(self._db_path) as db:
            async with db.execute(
                "SELECT id, sql FROM history"
                " WHERE profile = ?"
                " ORDER BY ran_at DESC, id DESC"
                " LIMIT ?",
                (profile, limit),
            ) as cursor:
                return list(await cursor.fetchall())
