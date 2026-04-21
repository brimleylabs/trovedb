"""ProclistScreen — live session table (headline view of trovedb)."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import DataTable, Input, Static

from trovedb.config import ConnectionProfile
from trovedb.connectors.types import Connection, Process

logger = logging.getLogger(__name__)

_HINT = "R: refresh  /: filter  Esc: back  q: quit"

# Column fixed widths (characters)
_W_GUTTER = 2
_W_PID = 6
_W_USER = 10
_W_DB = 14
_W_STATE = 9
_W_RUNTIME = 8
_W_WAIT = 14
_QUERY_TRUNC = 55  # chars shown in the table cell


# ---------------------------------------------------------------------------
# Pure formatting helpers — reusable by future views (locks, etc.)
# ---------------------------------------------------------------------------


def format_runtime(seconds: float | None) -> str:
    """Return a human-readable elapsed-time string.

    Examples:
        ``None``  → ``"—"``
        ``2.4``   → ``"2.4s"``
        ``61``    → ``"1m 01s"``
        ``3720``  → ``"1h 02m"``
    """
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours, mins = divmod(minutes, 60)
    return f"{hours}h {mins:02d}m"


def truncate(text: str, max_width: int) -> str:
    """Truncate *text* to *max_width* chars, appending ``…`` if needed."""
    if len(text) <= max_width:
        return text
    return text[: max_width - 1] + "…"


def _host_label(profile: ConnectionProfile) -> str:
    """Return a concise host/url string for display in the status bar."""
    if profile.url:
        return profile.url
    host = profile.host or "?"
    return f"{host}:{profile.port}" if profile.port else host


# ---------------------------------------------------------------------------
# ProclistScreen
# ---------------------------------------------------------------------------


class ProclistScreen(Screen[None]):
    """Live session table — the headline view of trovedb.

    Displays active server processes sourced from
    ``connector.list_processes()``.  Refreshes manually via ``R``/``F5``.
    """

    DEFAULT_CSS = """
    ProclistScreen #proclist-status {
        dock: top;
        height: 1;
        background: $primary-darken-2;
        color: $text;
        padding: 0 1;
    }
    ProclistScreen #proclist-error {
        dock: top;
        height: 1;
        background: $error;
        color: $text;
        padding: 0 1;
    }
    ProclistScreen #proclist-hint {
        dock: bottom;
        height: 1;
        background: $primary-darken-2;
        color: $text-muted;
        padding: 0 1;
    }
    ProclistScreen #filter-input {
        dock: bottom;
        height: 3;
    }
    ProclistScreen #proclist-table {
        height: 1fr;
    }
    ProclistScreen #proclist-empty {
        height: 1fr;
        content-align: center middle;
        color: $text-muted;
    }
    ProclistScreen #proclist-footer {
        height: 3;
        background: $surface-darken-1;
        padding: 0 1;
        color: $text-muted;
    }
    """

    BINDINGS = [
        Binding("r", "refresh", "Refresh", show=False),
        Binding("f5", "refresh", "Refresh", show=False),
        Binding("slash", "open_filter", "Filter", show=False),
        Binding("escape", "go_back", "Back", show=False),
        Binding("q", "quit", "Quit", show=False),
    ]

    def __init__(
        self,
        profile: ConnectionProfile,
        connector: Any,
        connection: Connection,
    ) -> None:
        super().__init__()
        self._profile = profile
        self._connector = connector
        self._connection = connection
        self._processes: list[Process] = []
        self._displayed_processes: list[Process] = []
        self._filter_text = ""
        self._last_refresh: datetime | None = None

    # ------------------------------------------------------------------
    # Compose
    # ------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        yield Static("", id="proclist-status")
        yield Static("", id="proclist-error")
        yield DataTable(id="proclist-table", zebra_stripes=True, cursor_type="row")
        yield Static(
            "No active sessions. Press R to refresh.",
            id="proclist-empty",
        )
        yield Static("(idle)", id="proclist-footer")
        yield Input(
            placeholder="Filter: type to search, Esc/Enter to close",
            id="filter-input",
        )
        yield Static(_HINT, id="proclist-hint")

    # ------------------------------------------------------------------
    # Mount
    # ------------------------------------------------------------------

    def on_mount(self) -> None:
        """Set up DataTable columns and start initial data load."""
        table = self.query_one("#proclist-table", DataTable)
        table.add_column("", width=_W_GUTTER, key="gutter")
        table.add_column("pid", width=_W_PID, key="pid")
        table.add_column("user", width=_W_USER, key="user")
        table.add_column("database", width=_W_DB, key="database")
        table.add_column("state", width=_W_STATE, key="state")
        table.add_column("runtime", width=_W_RUNTIME, key="runtime")
        table.add_column("wait_event", width=_W_WAIT, key="wait_event")
        table.add_column("query", key="query")

        # Hide widgets that are conditionally visible
        self.query_one("#proclist-error", Static).display = False
        self.query_one("#proclist-empty", Static).display = False
        self.query_one("#filter-input", Input).display = False

        self._update_status()
        self.run_worker(self._do_refresh(), exclusive=True)

    # ------------------------------------------------------------------
    # Status bar
    # ------------------------------------------------------------------

    def _update_status(self, *, refreshing: bool = False) -> None:
        """Re-render the top status line."""
        driver = (
            self._profile.driver.value
            if hasattr(self._profile.driver, "value")
            else str(self._profile.driver)
        )
        host = _host_label(self._profile)
        n = len(self._processes)
        ts = (
            self._last_refresh.strftime("%H:%M:%S")
            if self._last_refresh
            else "--:--:--"
        )
        suffix = "... refreshing" if refreshing else f"last refresh: {ts}"
        text = (
            f"trovedb — {self._profile.name} · {host} · {driver}"
            f" · {n} sessions · {suffix}"
        )
        self.query_one("#proclist-status", Static).update(text)

    # ------------------------------------------------------------------
    # Refresh logic
    # ------------------------------------------------------------------

    async def _do_refresh(self) -> None:
        """Fetch processes from the connector and repopulate the table."""
        self._update_status(refreshing=True)
        try:
            processes = await self._connector.list_processes()
        except Exception as exc:
            logger.warning("Refresh failed: %s", exc)
            self._show_error(str(exc))
            self._update_status()
            return
        self._processes = processes
        self._last_refresh = datetime.now()
        self._hide_error()
        self._update_status()
        self._render_table()

    def _show_error(self, message: str) -> None:
        err = self.query_one("#proclist-error", Static)
        err.update(
            f"Connection lost: {message}. Press R to retry, Esc to go back."
        )
        err.display = True

    def _hide_error(self) -> None:
        self.query_one("#proclist-error", Static).display = False

    # ------------------------------------------------------------------
    # Table rendering
    # ------------------------------------------------------------------

    def _apply_filter(self, processes: list[Process]) -> list[Process]:
        """Return only processes matching ``self._filter_text``."""
        if not self._filter_text:
            return processes
        needle = self._filter_text.lower()
        return [
            p
            for p in processes
            if needle in (p.user or "").lower()
            or needle in (p.db or "").lower()
            or needle in (p.state or "").lower()
            or needle in (p.info or "").lower()
        ]

    def _render_table(self) -> None:
        """Populate the DataTable from ``self._processes`` + current filter."""
        table = self.query_one("#proclist-table", DataTable)
        empty_label = self.query_one("#proclist-empty", Static)
        table.clear()

        filtered = self._apply_filter(self._processes)
        self._displayed_processes = filtered

        if not filtered:
            table.display = False
            empty_label.display = True
            self.query_one("#proclist-footer", Static).update("(idle)")
            return

        table.display = True
        empty_label.display = False

        for proc in filtered:
            gutter = "▶" if proc.blocked_by is not None else " "
            query_full = proc.info or ""
            query_cell = truncate(query_full, _QUERY_TRUNC)
            table.add_row(
                gutter,
                str(proc.pid),
                proc.user or "",
                proc.db or "",
                proc.state or "",
                format_runtime(proc.time_seconds),
                proc.wait_event or "",
                query_cell,
                key=str(proc.pid),
            )

        # Update footer for the initially-selected row (typically row 0)
        if filtered:
            self._update_footer_for_pid(str(filtered[0].pid))

    def _update_footer_for_pid(self, pid_str: str) -> None:
        """Set the footer detail panel to the full query of *pid_str*."""
        footer = self.query_one("#proclist-footer", Static)
        proc = next(
            (p for p in self._displayed_processes if str(p.pid) == pid_str),
            None,
        )
        if proc:
            footer.update(proc.info if proc.info else "(idle)")
        else:
            footer.update("(idle)")

    # ------------------------------------------------------------------
    # DataTable events
    # ------------------------------------------------------------------

    def on_data_table_row_highlighted(
        self, event: DataTable.RowHighlighted
    ) -> None:
        """Update the footer whenever the cursor row changes."""
        if event.row_key is None:
            self.query_one("#proclist-footer", Static).update("(idle)")
            return
        self._update_footer_for_pid(str(event.row_key.value))

    # ------------------------------------------------------------------
    # Input events (filter)
    # ------------------------------------------------------------------

    def on_input_changed(self, event: Input.Changed) -> None:
        """Live-filter the table as the user types."""
        if event.input.id == "filter-input":
            self._filter_text = event.value
            self._render_table()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Close the filter prompt on Enter, keeping the active filter."""
        if event.input.id == "filter-input":
            self._close_filter()

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def action_refresh(self) -> None:
        """Trigger a manual refresh (R / F5)."""
        self.run_worker(self._do_refresh(), exclusive=True)

    def action_open_filter(self) -> None:
        """Show the inline filter input and give it focus."""
        fi = self.query_one("#filter-input", Input)
        fi.display = True
        fi.focus()

    def action_go_back(self) -> None:
        """Close the filter if active; otherwise pop back to the picker."""
        fi = self.query_one("#filter-input", Input)
        if fi.display:
            fi.value = ""
            self._filter_text = ""
            self._close_filter()
            self._render_table()
        else:
            self.dismiss()

    def action_quit(self) -> None:
        """Quit the application."""
        self.app.exit()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _close_filter(self) -> None:
        """Hide the filter input and return focus to the table."""
        fi = self.query_one("#filter-input", Input)
        fi.display = False
        self.query_one("#proclist-table", DataTable).focus()
