"""QueryScreen — single-tab SQL editor for ad-hoc operator queries.

Layout
------
  Top half  — TextArea with SQL syntax highlighting, editable, multiline.
  Bot half  — DataTable result grid from the last successful execute().

Key bindings
------------
  F5 / Ctrl+Enter  Execute query
  Ctrl+R           Open history search (last 100 entries, /‐style filter)
  Ctrl+↑ / Ctrl+↓  Walk history for the current profile
  Ctrl+S           Save query to ~/.local/share/trovedb/queries/
  Ctrl+L           Clear the editor
  Ctrl+Shift+L     Clear both editor and result grid
  C                Copy highlighted cell (when result table focused)
  Shift+C          Copy highlighted row as TSV (when result table focused)
  Esc              Dismiss screen
  q                Quit app
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

import pyperclip
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import DataTable, Input, LoadingIndicator, Static, TextArea

from trovedb.config import ConnectionProfile
from trovedb.connectors.types import Connection, ResultSet
from trovedb.data import QueryHistory, get_queries_dir, is_write_query

logger = logging.getLogger(__name__)

_HINT = (
    "Ctrl+G / F5: run  Ctrl+R: history  Ctrl+S: save  Ctrl+L: clear"
    "  ?: help  Esc: back  q: quit"
)
_RESULT_LIMIT = 1000


# ---------------------------------------------------------------------------
# WriteConfirmModal
# ---------------------------------------------------------------------------


class WriteConfirmModal(ModalScreen[bool]):
    """Confirm modal for write queries.  Dismisses with ``True`` (run) or ``False`` (cancel)."""

    DEFAULT_CSS = """
    WriteConfirmModal {
        align: center middle;
    }
    WriteConfirmModal #wconfirm-dialog {
        width: 62;
        height: auto;
        border: thick $warning;
        background: $surface;
        padding: 1 2;
    }
    WriteConfirmModal #wconfirm-text {
        color: $text;
    }
    WriteConfirmModal #wconfirm-keys {
        color: $text-muted;
        margin-top: 1;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel", "Cancel", show=False),
        Binding("n", "cancel", "No", show=False),
    ]

    def compose(self) -> ComposeResult:
        with Vertical(id="wconfirm-dialog"):
            yield Static(
                "This looks like a write query. Run anyway?",
                id="wconfirm-text",
            )
            yield Static("[y] run  [N / Esc] cancel", id="wconfirm-keys")

    def on_key(self, event: Any) -> None:  # type: ignore[override]
        char = event.character or ""
        key = event.key
        if char.lower() == "y":
            event.stop()
            self.dismiss(True)
        elif char.lower() == "n" or key == "escape":
            event.stop()
            self.dismiss(False)

    def action_cancel(self) -> None:
        self.dismiss(False)


# ---------------------------------------------------------------------------
# HistorySearchModal
# ---------------------------------------------------------------------------


class HistorySearchModal(ModalScreen["str | None"]):
    """Inline history search.  Dismisses with selected SQL or ``None``."""

    DEFAULT_CSS = """
    HistorySearchModal {
        align: center middle;
    }
    HistorySearchModal #hsearch-dialog {
        width: 84;
        height: 20;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }
    HistorySearchModal #hsearch-title {
        color: $text-muted;
        height: 1;
    }
    HistorySearchModal #hsearch-filter {
        height: 3;
    }
    HistorySearchModal #hsearch-list {
        height: 1fr;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel", "Cancel", show=False),
    ]

    def __init__(self, entries: list[tuple[int, str]]) -> None:
        super().__init__()
        # entries are (id, sql), newest first (from history.fetch)
        self._all_entries = entries
        self._filtered: list[tuple[int, str]] = list(entries)

    def compose(self) -> ComposeResult:
        with Vertical(id="hsearch-dialog"):
            yield Static(
                "History — type to filter, Enter to select, Esc to cancel",
                id="hsearch-title",
            )
            yield Input(placeholder="Filter…", id="hsearch-filter")
            yield DataTable(id="hsearch-list", cursor_type="row")

    async def on_mount(self) -> None:
        table = self.query_one("#hsearch-list", DataTable)
        table.add_column("sql", key="sql", width=72)
        self._rebuild_list(self._all_entries)
        table.focus()

    def _rebuild_list(self, entries: list[tuple[int, str]]) -> None:
        table = self.query_one("#hsearch-list", DataTable)
        table.clear()
        self._filtered = list(entries)
        for eid, sql in entries:
            preview = sql.strip().split("\n")[0][:72]
            table.add_row(preview, key=str(eid))

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "hsearch-filter":
            q = event.value.lower()
            filtered = [(i, s) for (i, s) in self._all_entries if q in s.lower()]
            self._rebuild_list(filtered)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        row_key = str(event.row_key.value)
        for eid, sql in self._filtered:
            if str(eid) == row_key:
                self.dismiss(sql)
                return
        self.dismiss(None)

    def action_cancel(self) -> None:
        self.dismiss(None)


# ---------------------------------------------------------------------------
# QueryScreen
# ---------------------------------------------------------------------------


class QueryScreen(Screen[None]):
    """Single-tab SQL editor for ad-hoc operator queries."""

    DEFAULT_CSS = """
    QueryScreen #query-status {
        dock: top;
        height: 1;
        background: $primary-darken-2;
        color: $text;
        padding: 0 1;
    }
    QueryScreen #query-banner {
        dock: top;
        height: 1;
        background: $success;
        color: $text;
        padding: 0 1;
    }
    QueryScreen #query-editor {
        height: 40%;
        border-bottom: solid $primary;
    }
    QueryScreen #query-result-area {
        height: 1fr;
    }
    QueryScreen #query-error {
        background: $error;
        color: $text;
        padding: 0 1;
    }
    QueryScreen #query-result {
        height: 1fr;
    }
    QueryScreen #query-loading {
        height: 1fr;
    }
    QueryScreen #query-result-status {
        height: 1;
        background: $primary-darken-3;
        color: $text-muted;
        padding: 0 1;
    }
    QueryScreen #query-hint {
        dock: bottom;
        height: 1;
        background: $primary-darken-2;
        color: $text-muted;
        padding: 0 1;
    }
    """

    BINDINGS = [
        Binding("f5", "execute_query", "Run", show=False, priority=True),
        Binding("ctrl+enter", "execute_query", "Run", show=False, priority=True),
        # Mintty / Git Bash delivers Ctrl+Enter as Ctrl+J; some terminals
        # swallow F5 entirely. Provide reliable fallbacks.
        Binding("ctrl+j", "execute_query", "Run", show=False, priority=True),
        Binding("ctrl+g", "execute_query", "Run", show=False, priority=True),
        Binding("alt+enter", "execute_query", "Run", show=False, priority=True),
        Binding("ctrl+r", "open_history", "History", show=False, priority=True),
        Binding("ctrl+s", "save_query", "Save", show=False, priority=True),
        Binding("ctrl+l", "clear_editor", "Clear", show=False, priority=True),
        Binding("ctrl+shift+l", "clear_all", "Clear All", show=False, priority=True),
        Binding("ctrl+up", "history_prev", "Prev History", show=False, priority=True),
        Binding("ctrl+down", "history_next", "Next History", show=False, priority=True),
        # Copy bindings: low priority so TextArea handles 'c'/'C' when focused.
        Binding("c", "copy_cell", "Copy Cell", show=False),
        Binding("shift+c", "copy_row", "Copy Row", show=False),
        Binding("escape", "go_back", "Back", show=False),
        Binding("q", "quit", "Quit", show=False),
    ]

    def __init__(
        self,
        profile: ConnectionProfile,
        connector: Any,
        connection: Connection,
        *,
        history: QueryHistory | None = None,
    ) -> None:
        super().__init__()
        self._profile = profile
        self._connector = connector
        self._connection = connection
        self._history = history or QueryHistory()
        self._running = False
        self._last_result: ResultSet | None = None
        self._banner_timer: Any = None
        # History walk: oldest→newest list, current index (None = not walking)
        self._history_entries: list[tuple[int, str]] = []
        self._history_idx: int | None = None

    # ------------------------------------------------------------------
    # Compose / Mount
    # ------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        yield Static("", id="query-status")
        yield Static("", id="query-banner")
        yield TextArea(language="sql", id="query-editor")
        with Vertical(id="query-result-area"):
            yield Static("", id="query-error")
            yield DataTable(
                id="query-result", zebra_stripes=True, cursor_type="cell"
            )
            yield LoadingIndicator(id="query-loading")
            yield Static("", id="query-result-status")
        yield Static(_HINT, id="query-hint")

    async def on_mount(self) -> None:
        self.query_one("#query-banner", Static).display = False
        self.query_one("#query-error", Static).display = False
        self.query_one("#query-loading", LoadingIndicator).display = False
        self._update_status(f"Query — {self._profile.name}")
        self.query_one("#query-editor", TextArea).focus()
        await self._reload_history()

    # ------------------------------------------------------------------
    # History state
    # ------------------------------------------------------------------

    async def _reload_history(self) -> None:
        """Reload history entries for the current profile (oldest first)."""
        try:
            entries = await self._history.fetch(self._profile.name, limit=100)
            # fetch() returns newest first; reverse so index 0 = oldest
            self._history_entries = list(reversed(entries))
        except Exception as exc:
            logger.warning("Failed to load query history: %s", exc)
            self._history_entries = []
        self._history_idx = None

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------

    async def action_execute_query(self, _sql: str | None = None) -> None:
        """Execute the SQL in the editor (F5 / Ctrl+Enter).

        *_sql* is an optional override used in tests to bypass the TextArea
        read (useful when TextArea reactive state hasn't propagated yet in the
        test harness).  Production code always leaves it as ``None``.
        """
        if self._running:
            return
        sql = _sql if _sql is not None else self.query_one("#query-editor", TextArea).text.strip()
        if not sql:
            return
        if is_write_query(sql):
            def _on_confirm(confirmed: bool) -> None:
                if confirmed:
                    self.run_worker(
                        self._do_execute(sql, dangerous=True), exclusive=True
                    )

            self.app.push_screen(WriteConfirmModal(), _on_confirm)
            return
        await self._do_execute(sql, dangerous=False)

    async def _do_execute(self, sql: str, *, dangerous: bool = False) -> None:
        """Run *sql* against the connector and populate the result grid."""
        self._running = True
        self._show_loading(True)
        self._hide_error()

        start = time.monotonic()
        error_str: str | None = None
        result: ResultSet | None = None

        try:
            # Try the dangerous= kwarg path first; fall back if not supported
            # (e.g. SQLite connector which has no read-only guard).
            try:
                result = await self._connector.execute(sql, dangerous=dangerous)
            except TypeError:
                result = await self._connector.execute(sql)
        except Exception as exc:
            error_str = str(exc)
            logger.exception("query execute failed")

        duration_ms = int((time.monotonic() - start) * 1000)

        # Record to history (success or failure)
        try:
            await self._history.record(
                profile=self._profile.name,
                sql=sql,
                duration_ms=duration_ms if error_str is None else None,
                error=error_str,
            )
            await self._reload_history()
        except Exception as exc:
            logger.warning("Failed to record history: %s", exc)

        self._show_loading(False)
        self._running = False

        if error_str is not None:
            self._show_error(f"ERROR: {error_str}")
            self._update_result_status("")
        else:
            assert result is not None
            self._last_result = result
            self._render_results(result)

    # ------------------------------------------------------------------
    # Result rendering
    # ------------------------------------------------------------------

    def _render_results(self, result: ResultSet) -> None:
        """Populate the DataTable with *result*, capped at _RESULT_LIMIT rows."""
        table = self.query_one("#query-result", DataTable)
        table.clear(columns=True)

        if not result.columns:
            # DML / DDL success message
            affected = result.row_count
            duration = (
                f" ({result.duration_ms:.0f}ms)" if result.duration_ms else ""
            )
            self._update_result_status(
                f"Query OK — {affected} row{'s' if affected != 1 else ''} affected{duration}"
            )
            return

        for col in result.columns:
            table.add_column(col, key=col)

        rows = result.rows
        total = len(rows)
        truncated = total > _RESULT_LIMIT
        if truncated:
            rows = rows[:_RESULT_LIMIT]
            self._update_result_status(
                f"Showing first {_RESULT_LIMIT} of {total}"
                " — raise with :set result_limit N"
            )
        else:
            duration = (
                f" ({result.duration_ms:.0f}ms)" if result.duration_ms else ""
            )
            self._update_result_status(
                f"{total} row{'s' if total != 1 else ''}{duration}"
            )

        for row in rows:
            table.add_row(*[str(v) if v is not None else "NULL" for v in row])

    # ------------------------------------------------------------------
    # History walk
    # ------------------------------------------------------------------

    async def action_history_prev(self) -> None:
        """Walk toward older history entries (Ctrl+↑)."""
        if not self._history_entries:
            return
        if self._history_idx is None:
            # Start at the newest entry
            self._history_idx = len(self._history_entries) - 1
        elif self._history_idx > 0:
            self._history_idx -= 1
        sql = self._history_entries[self._history_idx][1]
        self.query_one("#query-editor", TextArea).load_text(sql)

    async def action_history_next(self) -> None:
        """Walk toward newer history entries (Ctrl+↓)."""
        if not self._history_entries or self._history_idx is None:
            return
        if self._history_idx < len(self._history_entries) - 1:
            self._history_idx += 1
            sql = self._history_entries[self._history_idx][1]
            self.query_one("#query-editor", TextArea).load_text(sql)
        else:
            # Past the newest entry — clear the editor
            self._history_idx = None
            self.query_one("#query-editor", TextArea).load_text("")

    async def action_open_history(self) -> None:
        """Open the inline history search modal (Ctrl+R)."""
        entries = await self._history.fetch(self._profile.name, limit=100)
        if not entries:
            self._show_banner("No history for this profile yet")
            return

        def _on_select(sql: str | None) -> None:
            if sql:
                self.query_one("#query-editor", TextArea).load_text(sql)
                self._history_idx = None

        self.app.push_screen(HistorySearchModal(entries), _on_select)

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------

    async def action_save_query(self) -> None:
        """Save the current query text to a timestamped .sql file (Ctrl+S)."""
        sql = self.query_one("#query-editor", TextArea).text.strip()
        if not sql:
            return
        queries_dir = get_queries_dir()
        queries_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"{self._profile.name}-{timestamp}.sql"
        path = queries_dir / filename
        try:
            path.write_text(sql, encoding="utf-8")
            self._show_banner(f"Saved to {filename}")
        except OSError as exc:
            self._show_banner(f"Save failed: {exc}", error=True)

    # ------------------------------------------------------------------
    # Clear
    # ------------------------------------------------------------------

    def action_clear_editor(self) -> None:
        """Clear the editor contents (Ctrl+L)."""
        self.query_one("#query-editor", TextArea).load_text("")
        self._history_idx = None

    def action_clear_all(self) -> None:
        """Clear both editor and result grid (Ctrl+Shift+L)."""
        self.action_clear_editor()
        table = self.query_one("#query-result", DataTable)
        table.clear(columns=True)
        self._last_result = None
        self._hide_error()
        self._update_result_status("")

    # ------------------------------------------------------------------
    # Copy
    # ------------------------------------------------------------------

    def action_copy_cell(self) -> None:
        """Copy the highlighted cell value to clipboard (C)."""
        table = self.query_one("#query-result", DataTable)
        if table.row_count == 0:
            return
        try:
            cell = table.get_cell_at(table.cursor_coordinate)
            pyperclip.copy(str(cell))
            self._show_banner("Cell copied to clipboard")
        except Exception as exc:
            logger.warning("copy_cell failed: %s", exc)

    def action_copy_row(self) -> None:
        """Copy the highlighted row as TSV to clipboard (Shift+C)."""
        table = self.query_one("#query-result", DataTable)
        if table.row_count == 0:
            return
        try:
            row_key = table.coordinate_to_cell_key(table.cursor_coordinate).row_key
            row_data = table.get_row(row_key)
            tsv = "\t".join(str(v) for v in row_data)
            pyperclip.copy(tsv)
            self._show_banner("Row copied as TSV")
        except Exception as exc:
            logger.warning("copy_row failed: %s", exc)

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def action_go_back(self) -> None:
        """Dismiss this screen (Esc)."""
        self.dismiss()

    def action_quit(self) -> None:
        """Quit the application (q)."""
        self.app.exit()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _show_loading(self, visible: bool) -> None:
        self.query_one("#query-loading", LoadingIndicator).display = visible
        self.query_one("#query-result", DataTable).display = not visible

    def _update_status(self, msg: str) -> None:
        self.query_one("#query-status", Static).update(msg)

    def _update_result_status(self, msg: str) -> None:
        self.query_one("#query-result-status", Static).update(msg)

    def _show_error(self, msg: str) -> None:
        err = self.query_one("#query-error", Static)
        err.update(msg)
        err.display = True

    def _hide_error(self) -> None:
        self.query_one("#query-error", Static).display = False

    def _show_banner(self, msg: str, *, error: bool = False) -> None:
        if error:
            self._show_error(msg)
            return
        banner = self.query_one("#query-banner", Static)
        banner.update(msg)
        banner.display = True
        if self._banner_timer is not None:
            self._banner_timer.stop()
        self._banner_timer = self.set_timer(3.0, self._hide_banner)

    def _hide_banner(self) -> None:
        self.query_one("#query-banner", Static).display = False
