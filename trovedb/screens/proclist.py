"""ProclistScreen — live session table with watch mode, kill, and EXPLAIN."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import pyperclip
from textual import events
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import DataTable, Input, Static, TextArea

from trovedb.config import ConnectionProfile
from trovedb.connectors.types import Connection, Process
from trovedb.widgets._format import format_runtime as _fmt_runtime
from trovedb.widgets._format import truncate as _truncate

logger = logging.getLogger(__name__)

_HINT = (
    "W: watch  R: refresh  K: kill  E: explain  C: copy"
    "  L: locks  S: schema  Shift+Q: query  /: filter  ?: help  Esc: back  q: quit"
)

# Interval shortcut key → seconds.
# '1' selects 10 s, '3' selects 30 s (mnemonic first-digit).
_INTERVAL_KEYS: dict[str, int] = {"2": 2, "5": 5, "1": 10, "3": 30}

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

    Delegates to :func:`trovedb.widgets._format.format_runtime`.
    Kept here for backward compatibility with existing imports.
    """
    return _fmt_runtime(seconds)


def truncate(text: str, max_width: int) -> str:
    """Truncate *text* to *max_width* chars, appending ``…`` if needed.

    Delegates to :func:`trovedb.widgets._format.truncate`.
    Kept here for backward compatibility with existing imports.
    """
    return _truncate(text, max_width)


def _host_label(profile: ConnectionProfile) -> str:
    """Return a concise host/url string for display in the status bar."""
    if profile.url:
        return profile.url
    host = profile.host or "?"
    return f"{host}:{profile.port}" if profile.port else host


def _explain_prefix(driver: str) -> str:
    """Return the driver-specific EXPLAIN prefix for the given *driver*."""
    if driver == "postgres":
        return "EXPLAIN (ANALYZE, BUFFERS) "
    if driver == "mysql":
        return "EXPLAIN FORMAT=TREE "
    return "EXPLAIN QUERY PLAN "


# ---------------------------------------------------------------------------
# Kill confirmation modal
# ---------------------------------------------------------------------------


class KillConfirmModal(ModalScreen["tuple[int, bool] | None"]):
    """Small centered modal: asks the operator to confirm a kill/cancel.

    Returns ``(pid, force)`` on confirmation or ``None`` on cancel.
    Double-tap is required for uppercase ``Y`` (force-terminate) to
    prevent accidental session drops.
    """

    DEFAULT_CSS = """
    KillConfirmModal {
        align: center middle;
    }
    KillConfirmModal #kill-dialog {
        width: 64;
        height: auto;
        border: thick $error;
        background: $surface;
        padding: 1 2;
    }
    KillConfirmModal #kill-warning {
        color: $warning;
        height: 1;
    }
    """

    def __init__(self, pid: int) -> None:
        super().__init__()
        self._pid = pid
        self._y_pressed_at: datetime | None = None

    def compose(self) -> ComposeResult:
        with Vertical(id="kill-dialog"):
            yield Static(f"Kill PID {self._pid}?", id="kill-title")
            yield Static(
                "[C]ancel  [y]: cancel query  [Y]: terminate session (double-tap)",
                id="kill-keys",
            )
            yield Static("", id="kill-warning")

    def on_key(self, event: events.Key) -> None:  # noqa: PLR0912
        """Handle modal keypresses with case-sensitive kill semantics."""
        char = event.character or ""
        key = event.key

        if char == "c" or key == "escape":
            event.stop()
            self.dismiss(None)

        elif char == "y":  # lowercase — cancel query, no confirmation needed
            event.stop()
            self.dismiss((self._pid, False))

        elif char == "Y":  # uppercase — terminate; double-tap required
            event.stop()
            now = datetime.now()
            if (
                self._y_pressed_at is not None
                and (now - self._y_pressed_at).total_seconds() <= 2.0
            ):
                self.dismiss((self._pid, True))
            else:
                self._y_pressed_at = now
                self.query_one("#kill-warning", Static).update(
                    "⚠  Press Y again within 2s to confirm terminate"
                )

        else:
            # Any other key resets the double-tap timer
            self._y_pressed_at = None


# ---------------------------------------------------------------------------
# EXPLAIN output modal
# ---------------------------------------------------------------------------


class ExplainModal(ModalScreen[None]):
    """Scrollable read-only view of EXPLAIN output."""

    DEFAULT_CSS = """
    ExplainModal {
        align: center middle;
    }
    ExplainModal #explain-dialog {
        width: 80%;
        height: 80%;
        border: thick $primary;
        background: $surface;
        padding: 0;
    }
    ExplainModal #explain-header {
        height: 1;
        background: $primary-darken-2;
        color: $text-muted;
        padding: 0 1;
    }
    ExplainModal #explain-output {
        height: 1fr;
    }
    """

    BINDINGS = [Binding("escape", "dismiss", show=False)]

    def __init__(self, output: str) -> None:
        super().__init__()
        self._output = output

    def compose(self) -> ComposeResult:
        with Vertical(id="explain-dialog"):
            yield Static("EXPLAIN output — Esc: close", id="explain-header")
            yield TextArea(self._output, read_only=True, id="explain-output")


# ---------------------------------------------------------------------------
# ProclistScreen
# ---------------------------------------------------------------------------


class ProclistScreen(Screen[None]):
    """Live session table — the headline view of trovedb.

    Displays active server processes sourced from
    ``connector.list_processes()``.  Auto-refreshes every *watch_interval*
    seconds when watch mode is active (default: on).

    Keybindings
    -----------
    W       toggle auto-refresh (watch mode)
    2/5     set watch interval to 2 s / 5 s
    1/3     set watch interval to 10 s / 30 s
    R/F5    manual refresh
    K       open kill confirmation modal
    E       run EXPLAIN on highlighted query
    C       copy full query text to clipboard
    /       open inline filter
    Esc     close filter / go back
    q       quit application
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
    ProclistScreen #proclist-banner {
        dock: top;
        height: 1;
        background: $success;
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
        Binding("w", "toggle_watch", "Watch", show=False),
        Binding("k", "kill", "Kill", show=False),
        Binding("e", "explain", "Explain", show=False),
        Binding("c", "copy_sql", "Copy SQL", show=False),
        Binding("2", "set_interval('2')", "2s", show=False),
        Binding("5", "set_interval('5')", "5s", show=False),
        Binding("1", "set_interval('1')", "10s", show=False),
        Binding("3", "set_interval('3')", "30s", show=False),
        Binding("l", "open_locks", "Locks", show=False),
        Binding("s", "open_schema", "Schema", show=False),
        Binding("shift+q", "open_query", "Query", show=False),
        Binding("slash", "open_filter", "Filter", show=False),
        Binding("escape", "go_back", "Back", show=False),
        Binding("q", "quit", "Quit", show=False),
    ]

    def __init__(
        self,
        profile: ConnectionProfile,
        connector: Any,
        connection: Connection,
        *,
        watch_interval: int = 2,
    ) -> None:
        super().__init__()
        self._profile = profile
        self._connector = connector
        self._connection = connection
        self._processes: list[Process] = []
        self._displayed_processes: list[Process] = []
        self._filter_text = ""
        self._last_refresh: datetime | None = None
        self._watch_active: bool = True
        self._watch_interval: int = watch_interval
        self._watch_timer: Any = None  # Textual Timer handle

    # ------------------------------------------------------------------
    # Compose
    # ------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        yield Static("", id="proclist-status")
        yield Static("", id="proclist-error")
        yield Static("", id="proclist-banner")
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

    async def on_mount(self) -> None:
        """Set up DataTable columns, start the initial data load and watch timer."""
        table = self.query_one("#proclist-table", DataTable)
        table.add_column("", width=_W_GUTTER, key="gutter")
        table.add_column("pid", width=_W_PID, key="pid")
        table.add_column("user", width=_W_USER, key="user")
        table.add_column("database", width=_W_DB, key="database")
        table.add_column("state", width=_W_STATE, key="state")
        table.add_column("runtime", width=_W_RUNTIME, key="runtime")
        table.add_column("wait_event", width=_W_WAIT, key="wait_event")
        table.add_column("query", key="query")

        # Widgets that are conditionally visible start hidden
        self.query_one("#proclist-error", Static).display = False
        self.query_one("#proclist-banner", Static).display = False
        self.query_one("#proclist-empty", Static).display = False
        self.query_one("#filter-input", Input).display = False

        self._update_status()
        await self._do_refresh()
        self._start_watch_timer()

    # ------------------------------------------------------------------
    # Watch timer management
    # ------------------------------------------------------------------

    def _start_watch_timer(self) -> None:
        """Start (or restart) the periodic refresh timer at the current interval."""
        if self._watch_timer is not None:
            self._watch_timer.stop()
        self._watch_timer = self.set_interval(
            self._watch_interval, self._on_watch_tick
        )

    async def _on_watch_tick(self) -> None:
        """Timer callback — refresh only when watch mode is active."""
        if self._watch_active:
            await self._do_refresh()

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
        watch_indicator = (
            f"⏱ watch {self._watch_interval}s" if self._watch_active else "⏸ paused"
        )
        suffix = "... refreshing" if refreshing else f"last refresh: {ts}"
        text = (
            f"trovedb — {self._profile.name} · {host} · {driver}"
            f" · {watch_indicator} · {n} sessions · {suffix}"
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
    # Banner (success / operation error — auto-dismisses after 3 s)
    # ------------------------------------------------------------------

    def _show_banner(self, message: str, *, error: bool = False) -> None:  # noqa: ARG002
        """Show *message* in the notification banner for 3 seconds."""
        banner = self.query_one("#proclist-banner", Static)
        banner.update(message)
        banner.display = True
        self.set_timer(3.0, self._hide_banner)

    def _hide_banner(self) -> None:
        self.query_one("#proclist-banner", Static).display = False

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
        """Populate the DataTable from ``self._processes`` + current filter.

        Preserves the cursor on the same ``pid`` if it is still present;
        otherwise snaps to the nearest row by index.
        """
        table = self.query_one("#proclist-table", DataTable)
        empty_label = self.query_one("#proclist-empty", Static)

        # ── Capture current cursor state before clearing ────────────────
        saved_pid: str | None = None
        old_cursor_row = table.cursor_row
        if 0 <= old_cursor_row < len(self._displayed_processes):
            saved_pid = str(self._displayed_processes[old_cursor_row].pid)

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

        # ── Restore cursor ──────────────────────────────────────────────
        new_cursor_row = 0
        if saved_pid is not None:
            for i, p in enumerate(filtered):
                if str(p.pid) == saved_pid:
                    new_cursor_row = i
                    break
            else:
                # PID gone — snap to nearest row by index
                new_cursor_row = max(0, min(old_cursor_row, len(filtered) - 1))
        table.move_cursor(row=new_cursor_row)

        # Update the detail footer for the selected row
        if filtered:
            self._update_footer_for_pid(str(filtered[new_cursor_row].pid))

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

    def action_toggle_watch(self) -> None:
        """Toggle auto-refresh on/off (W)."""
        self._watch_active = not self._watch_active
        self._update_status()

    def action_set_interval(self, key: str) -> None:
        """Change the watch interval (keys: 2=2s, 5=5s, 1=10s, 3=30s)."""
        seconds = _INTERVAL_KEYS.get(key)
        if seconds is None:
            return
        self._watch_interval = seconds
        self._start_watch_timer()
        self._update_status()

    def action_kill(self) -> None:
        """Open the kill confirmation modal for the highlighted row (K)."""
        table = self.query_one("#proclist-table", DataTable)
        if not self._displayed_processes:
            return
        cursor_row = table.cursor_row
        if cursor_row < 0 or cursor_row >= len(self._displayed_processes):
            return
        proc = self._displayed_processes[cursor_row]

        # Kill-self guardrail: never allow killing the trovedb session
        if (
            self._connection.backend_pid is not None
            and proc.pid == self._connection.backend_pid
        ):
            self._show_banner("Cannot kill the trovedb session itself", error=True)
            return

        def _on_kill_result(result: tuple[int, bool] | None) -> None:
            if result is None:
                return
            pid, force = result
            self.run_worker(self._do_kill(pid, force), exclusive=False)

        self.app.push_screen(KillConfirmModal(proc.pid), _on_kill_result)

    async def _do_kill(self, pid: int, force: bool) -> None:
        """Call the connector's kill_process and display the result."""
        try:
            await self._connector.kill_process(pid, force=force)
        except Exception as exc:
            logger.warning("kill_process(%s, force=%s) failed: %s", pid, force, exc)
            self._show_banner(f"Error killing PID {pid}: {exc}", error=True)
            return
        verb = "Terminated session" if force else "Cancelled query on pid"
        self._show_banner(f"{verb} {pid}")
        # Immediate refresh so the change is visible in the table
        self.run_worker(self._do_refresh(), exclusive=True)

    def action_explain(self) -> None:
        """Run EXPLAIN on the highlighted row's query and show the modal (E)."""
        table = self.query_one("#proclist-table", DataTable)
        if not self._displayed_processes:
            return
        cursor_row = table.cursor_row
        if cursor_row < 0 or cursor_row >= len(self._displayed_processes):
            return
        proc = self._displayed_processes[cursor_row]
        sql = proc.info or ""
        if not sql.strip():
            self._show_banner("Cannot EXPLAIN an idle session", error=True)
            return
        self.run_worker(self._do_explain(proc.pid, sql), exclusive=False)

    async def _do_explain(self, pid: int, sql: str) -> None:
        """Execute EXPLAIN and push the result modal."""
        driver = self._connection.driver
        prefix = _explain_prefix(driver)
        explain_sql = prefix + sql
        try:
            result = await self._connector.execute(explain_sql)
        except Exception as exc:
            logger.warning("EXPLAIN failed for pid %s: %s", pid, exc)
            self._show_banner(f"EXPLAIN failed: {exc}", error=True)
            return
        lines = ["\t".join(str(v) for v in row) for row in result.rows]
        output = "\n".join(lines) if lines else "(no output)"
        self.app.push_screen(ExplainModal(output))

    def action_copy_sql(self) -> None:
        """Copy the highlighted row's full query text to the clipboard (C)."""
        table = self.query_one("#proclist-table", DataTable)
        if not self._displayed_processes:
            return
        cursor_row = table.cursor_row
        if cursor_row < 0 or cursor_row >= len(self._displayed_processes):
            return
        proc = self._displayed_processes[cursor_row]
        sql = proc.info or ""
        try:
            pyperclip.copy(sql)
            self._show_banner(f"Copied query for PID {proc.pid}")
        except Exception as exc:
            logger.warning("pyperclip.copy failed: %s", exc)
            self._show_banner(f"Copy failed: {exc}", error=True)

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

    def action_open_locks(self) -> None:
        """Push the blocking-chain LocksScreen (L)."""
        from trovedb.screens.locks import LocksScreen  # lazy import to avoid cycles

        self.app.push_screen(
            LocksScreen(
                self._profile,
                self._connector,
                self._connection,
            )
        )

    def action_open_schema(self) -> None:
        """Push the schema browser SchemaScreen (S)."""
        from trovedb.screens.schema import SchemaScreen  # lazy import to avoid cycles

        self.app.push_screen(
            SchemaScreen(
                self._profile,
                self._connector,
                self._connection,
            )
        )

    def action_open_query(self) -> None:
        """Push the SQL editor QueryScreen (Shift+Q)."""
        from trovedb.screens.query import QueryScreen  # lazy import to avoid cycles

        self.app.push_screen(
            QueryScreen(
                self._profile,
                self._connector,
                self._connection,
            )
        )

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
