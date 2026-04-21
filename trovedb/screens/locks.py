"""LocksScreen — blocking-chain view for trovedb.

Displays the current lock-blocking relationships as a tree-style table:

    ▶ holder pid 1247  app_rw   UPDATE trips SET ...        (4.2s)
      ├─ waiter pid 1251  app_rw  DELETE FROM trips ...    (0.9s)
      └─ waiter pid 1263  reporter SELECT * FROM trips     (0.3s)

SQLite has no blocking model — the screen shows an informational notice.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from typing import Any

import pyperclip
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import DataTable, Input, Static, TextArea

from trovedb.config import ConnectionProfile
from trovedb.connectors.types import BlockingChain, Connection
from trovedb.widgets._format import format_runtime, truncate

logger = logging.getLogger(__name__)

_HINT = (
    "W: watch  R: refresh  K: kill holder  E: explain  C: copy"
    "  S: schema  Shift+Q: query  /: filter  ?: help  Esc: back  q: quit"
)

_INTERVAL_KEYS: dict[str, int] = {"2": 2, "5": 5, "1": 10, "3": 30}

_W_ROLE = 12   # "▶ holder" / "  ├─ waiter"
_W_PID = 6
_W_USER = 12
_W_WAIT = 8
_QUERY_TRUNC = 55

_SQLITE_NOTICE = (
    "SQLite is single-writer; use .proclist to see the active writer."
)


def _explain_prefix(driver: str) -> str:
    if driver == "postgres":
        return "EXPLAIN (ANALYZE, BUFFERS) "
    if driver == "mysql":
        return "EXPLAIN FORMAT=TREE "
    return "EXPLAIN QUERY PLAN "


# ---------------------------------------------------------------------------
# Kill confirmation modal (shared pattern with ProclistScreen)
# ---------------------------------------------------------------------------


class LockKillModal(ModalScreen["tuple[int, bool] | None"]):
    """Confirm kill of a holder PID.  Double-tap ``Y`` for force."""

    DEFAULT_CSS = """
    LockKillModal {
        align: center middle;
    }
    LockKillModal #lock-kill-dialog {
        width: 60;
        height: auto;
        padding: 1 2;
        background: $surface;
        border: thick $error;
    }
    """

    BINDINGS = [
        Binding("y", "soft_kill", show=False),
        Binding("Y", "force_kill", show=False),
        Binding("c", "dismiss_none", show=False),
        Binding("escape", "dismiss_none", show=False),
    ]

    def __init__(self, pid: int) -> None:
        super().__init__()
        self._pid = pid
        self._y_count = 0

    def compose(self) -> ComposeResult:
        with Vertical(id="lock-kill-dialog"):
            yield Static(
                f"Kill holder PID {self._pid}?\n\n"
                "[y] cancel query (soft)  [Y] terminate session (force)\n"
                "[c / Esc] dismiss",
                id="lock-kill-body",
            )

    def action_soft_kill(self) -> None:
        self.dismiss((self._pid, False))

    def action_force_kill(self) -> None:
        self._y_count += 1
        if self._y_count >= 2:
            self.dismiss((self._pid, True))

    def action_dismiss_none(self) -> None:
        self.dismiss(None)


# ---------------------------------------------------------------------------
# EXPLAIN modal (same pattern as ProclistScreen)
# ---------------------------------------------------------------------------


class LockExplainModal(ModalScreen[None]):
    """Show EXPLAIN output in a read-only text area."""

    DEFAULT_CSS = """
    LockExplainModal {
        align: center middle;
    }
    LockExplainModal #lock-explain-dialog {
        width: 90%;
        height: 80%;
        padding: 1 2;
        background: $surface;
        border: thick $primary;
    }
    LockExplainModal #lock-explain-output {
        height: 1fr;
    }
    """

    BINDINGS = [Binding("escape", "dismiss", show=False)]

    def __init__(self, output: str) -> None:
        super().__init__()
        self._output = output

    def compose(self) -> ComposeResult:
        with Vertical(id="lock-explain-dialog"):
            yield Static("EXPLAIN output — Esc: close", id="lock-explain-header")
            yield TextArea(self._output, read_only=True, id="lock-explain-output")


# ---------------------------------------------------------------------------
# LocksScreen
# ---------------------------------------------------------------------------


class LocksScreen(Screen[None]):
    """Blocking-chain tree view.

    Displays lock-blocking relationships sourced from
    ``connector.list_blocking_chains()``.  Auto-refreshes every
    *watch_interval* seconds when watch mode is active.

    Keybindings
    -----------
    W       toggle auto-refresh
    2/5     set watch interval to 2 s / 5 s
    1/3     set watch interval to 10 s / 30 s
    R/F5    manual refresh
    K       kill the holder of the highlighted row
    E       EXPLAIN on highlighted query
    C       copy full query text to clipboard
    /       open inline filter
    Esc     close filter / go back
    q       quit application
    """

    DEFAULT_CSS = """
    LocksScreen #locks-status {
        dock: top;
        height: 1;
        background: $primary-darken-2;
        color: $text;
        padding: 0 1;
    }
    LocksScreen #locks-error {
        dock: top;
        height: 1;
        background: $error;
        color: $text;
        padding: 0 1;
    }
    LocksScreen #locks-banner {
        dock: top;
        height: 1;
        background: $success;
        color: $text;
        padding: 0 1;
    }
    LocksScreen #locks-hint {
        dock: bottom;
        height: 1;
        background: $primary-darken-2;
        color: $text-muted;
        padding: 0 1;
    }
    LocksScreen #filter-input {
        dock: bottom;
        height: 3;
    }
    LocksScreen #locks-table {
        height: 1fr;
    }
    LocksScreen #locks-empty {
        height: 1fr;
        content-align: center middle;
        color: $text-muted;
    }
    LocksScreen #locks-sqlite {
        height: 1fr;
        content-align: center middle;
        color: $text-muted;
    }
    LocksScreen #locks-footer {
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
        Binding("s", "open_schema", "Schema", show=False),
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
        self._chains: list[BlockingChain] = []
        # Flat ordered list of (chain, is_holder) used for table row indexing.
        self._display_rows: list[tuple[BlockingChain, bool]] = []
        self._filter_text = ""
        self._last_refresh: datetime | None = None
        self._watch_active: bool = True
        self._watch_interval: int = watch_interval
        self._watch_timer: Any = None
        self._is_sqlite = connection.driver == "sqlite"

    # ------------------------------------------------------------------
    # Compose
    # ------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        yield Static("", id="locks-status")
        yield Static("", id="locks-error")
        yield Static("", id="locks-banner")
        yield DataTable(id="locks-table", zebra_stripes=True, cursor_type="row")
        yield Static(
            "No blocking chains — all clear.",
            id="locks-empty",
        )
        yield Static(
            _SQLITE_NOTICE,
            id="locks-sqlite",
        )
        yield Static("(idle)", id="locks-footer")
        yield Input(
            placeholder="Filter: type to search, Esc/Enter to close",
            id="filter-input",
        )
        yield Static(_HINT, id="locks-hint")

    # ------------------------------------------------------------------
    # Mount
    # ------------------------------------------------------------------

    async def on_mount(self) -> None:
        """Set up DataTable columns, kick off initial load, start timer."""
        table = self.query_one("#locks-table", DataTable)
        table.add_column("role", width=_W_ROLE, key="role")
        table.add_column("pid", width=_W_PID, key="pid")
        table.add_column("user", width=_W_USER, key="user")
        table.add_column("waited", width=_W_WAIT, key="waited")
        table.add_column("query", key="query")

        self.query_one("#locks-error", Static).display = False
        self.query_one("#locks-banner", Static).display = False
        self.query_one("#locks-empty", Static).display = False
        self.query_one("#locks-sqlite", Static).display = False
        self.query_one("#filter-input", Input).display = False

        if self._is_sqlite:
            self.query_one("#locks-table", DataTable).display = False
            self.query_one("#locks-sqlite", Static).display = True
            self._update_status()
            return

        self._update_status()
        await self._do_refresh()
        self._start_watch_timer()

    # ------------------------------------------------------------------
    # Watch timer
    # ------------------------------------------------------------------

    def _start_watch_timer(self) -> None:
        if self._watch_timer is not None:
            self._watch_timer.stop()
        self._watch_timer = self.set_interval(
            self._watch_interval, self._on_watch_tick
        )

    async def _on_watch_tick(self) -> None:
        if self._watch_active:
            await self._do_refresh()

    # ------------------------------------------------------------------
    # Status bar
    # ------------------------------------------------------------------

    def _update_status(self, *, refreshing: bool = False) -> None:
        driver = (
            self._profile.driver.value
            if hasattr(self._profile.driver, "value")
            else str(self._profile.driver)
        )
        n = len(self._chains)
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
            f"trovedb — {self._profile.name} · {driver}"
            f" · locks · {watch_indicator} · {n} chains · {suffix}"
        )
        self.query_one("#locks-status", Static).update(text)

    # ------------------------------------------------------------------
    # Refresh
    # ------------------------------------------------------------------

    async def _do_refresh(self) -> None:
        self._update_status(refreshing=True)
        try:
            chains = await self._connector.list_blocking_chains()
        except Exception as exc:
            logger.warning("Locks refresh failed: %s", exc)
            self._show_error(str(exc))
            self._update_status()
            return
        self._chains = chains
        self._last_refresh = datetime.now()
        self._hide_error()
        self._update_status()
        self._render_table()

    def _show_error(self, message: str) -> None:
        err = self.query_one("#locks-error", Static)
        err.update(f"Connection lost: {message}. Press R to retry, Esc to go back.")
        err.display = True

    def _hide_error(self) -> None:
        self.query_one("#locks-error", Static).display = False

    def _show_banner(self, message: str, *, error: bool = False) -> None:  # noqa: ARG002
        banner = self.query_one("#locks-banner", Static)
        banner.update(message)
        banner.display = True
        self.set_timer(3.0, self._hide_banner)

    def _hide_banner(self) -> None:
        self.query_one("#locks-banner", Static).display = False

    # ------------------------------------------------------------------
    # Table rendering
    # ------------------------------------------------------------------

    def _apply_filter(self, chains: list[BlockingChain]) -> list[BlockingChain]:
        if not self._filter_text:
            return chains
        needle = self._filter_text.lower()
        return [
            c
            for c in chains
            if needle in c.holder_user.lower()
            or needle in c.waiter_user.lower()
            or needle in c.holder_query.lower()
            or needle in c.waiter_query.lower()
            or needle in (c.object_name or "").lower()
        ]

    def _render_table(self) -> None:
        """Populate the DataTable with tree-style rows.

        Each holder appears once; its waiters are indented beneath it.
        Colors: holder=yellow, direct waiter=red, transitive waiter=dim red.
        """
        table = self.query_one("#locks-table", DataTable)
        empty_label = self.query_one("#locks-empty", Static)

        table.clear()
        self._display_rows = []

        filtered = self._apply_filter(self._chains)

        if not filtered:
            table.display = False
            empty_label.display = True
            self.query_one("#locks-footer", Static).update("(idle)")
            return

        table.display = True
        empty_label.display = False

        # Group by holder_pid, preserving first-seen order.
        holder_order: list[int] = []
        by_holder: dict[int, list[BlockingChain]] = defaultdict(list)
        for chain in filtered:
            if chain.holder_pid not in by_holder:
                holder_order.append(chain.holder_pid)
            by_holder[chain.holder_pid].append(chain)

        row_key_counter = 0

        for holder_pid in holder_order:
            waiters = by_holder[holder_pid]
            # Use the first waiter row to get holder info.
            sample = waiters[0]

            # ── Holder row (yellow) ──────────────────────────────────────
            holder_query = truncate(sample.holder_query, _QUERY_TRUNC)
            holder_waited = format_runtime(
                max(w.waited_seconds for w in waiters)
            )
            table.add_row(
                Text("▶ holder", style="yellow bold"),
                Text(str(holder_pid), style="yellow"),
                Text(sample.holder_user, style="yellow"),
                Text(holder_waited, style="yellow"),
                Text(holder_query, style="yellow"),
                key=f"h_{row_key_counter}",
            )
            self._display_rows.append((sample, True))
            row_key_counter += 1

            # ── Waiter rows (red / dim red for transitive) ───────────────
            for i, chain in enumerate(waiters):
                is_last = i == len(waiters) - 1
                prefix = "  └─" if is_last else "  ├─"
                waiter_style = "red" if chain.depth == 1 else "dim red"
                waiter_query = truncate(chain.waiter_query, _QUERY_TRUNC)
                table.add_row(
                    Text(f"{prefix} waiter", style=waiter_style),
                    Text(str(chain.waiter_pid), style=waiter_style),
                    Text(chain.waiter_user, style=waiter_style),
                    Text(format_runtime(chain.waited_seconds), style=waiter_style),
                    Text(waiter_query, style=waiter_style),
                    key=f"w_{row_key_counter}",
                )
                self._display_rows.append((chain, False))
                row_key_counter += 1

        if self._display_rows:
            chain0, is_holder = self._display_rows[0]
            self._update_footer(chain0, is_holder)

    def _update_footer(self, chain: BlockingChain, is_holder: bool) -> None:
        query = chain.holder_query if is_holder else chain.waiter_query
        self.query_one("#locks-footer", Static).update(query or "(idle)")

    # ------------------------------------------------------------------
    # DataTable events
    # ------------------------------------------------------------------

    def on_data_table_row_highlighted(
        self, event: DataTable.RowHighlighted
    ) -> None:
        if event.row_key is None or not self._display_rows:
            self.query_one("#locks-footer", Static).update("(idle)")
            return
        # Row key format: "h_N" or "w_N" where N is the flat index.
        key_str = str(event.row_key.value)
        try:
            idx = int(key_str.split("_", 1)[1])
        except (ValueError, IndexError):
            return
        if 0 <= idx < len(self._display_rows):
            chain, is_holder = self._display_rows[idx]
            self._update_footer(chain, is_holder)

    # ------------------------------------------------------------------
    # Input events (filter)
    # ------------------------------------------------------------------

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "filter-input":
            self._filter_text = event.value
            self._render_table()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "filter-input":
            self._close_filter()

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def action_refresh(self) -> None:
        if not self._is_sqlite:
            self.run_worker(self._do_refresh(), exclusive=True)

    def action_toggle_watch(self) -> None:
        self._watch_active = not self._watch_active
        self._update_status()

    def action_set_interval(self, key: str) -> None:
        seconds = _INTERVAL_KEYS.get(key)
        if seconds is None:
            return
        self._watch_interval = seconds
        self._start_watch_timer()
        self._update_status()

    def action_kill(self) -> None:
        """Kill the holder PID of the highlighted row (K)."""
        if self._is_sqlite or not self._display_rows:
            return
        table = self.query_one("#locks-table", DataTable)
        cursor_row = table.cursor_row
        if cursor_row < 0 or cursor_row >= len(self._display_rows):
            return
        chain, is_holder = self._display_rows[cursor_row]
        target_pid = chain.holder_pid

        if (
            self._connection.backend_pid is not None
            and target_pid == self._connection.backend_pid
        ):
            self._show_banner("Cannot kill the trovedb session itself", error=True)
            return

        def _on_kill_result(result: tuple[int, bool] | None) -> None:
            if result is None:
                return
            pid, force = result
            self.run_worker(self._do_kill(pid, force), exclusive=False)

        self.app.push_screen(LockKillModal(target_pid), _on_kill_result)

    async def _do_kill(self, pid: int, force: bool) -> None:
        try:
            await self._connector.kill_process(pid, force=force)
        except Exception as exc:
            logger.warning("kill_process(%s, force=%s) failed: %s", pid, force, exc)
            self._show_banner(f"Error killing PID {pid}: {exc}", error=True)
            return
        verb = "Terminated session" if force else "Cancelled query on pid"
        self._show_banner(f"{verb} {pid}")
        self.run_worker(self._do_refresh(), exclusive=True)

    def action_explain(self) -> None:
        """EXPLAIN the highlighted row's query (E)."""
        if self._is_sqlite or not self._display_rows:
            return
        table = self.query_one("#locks-table", DataTable)
        cursor_row = table.cursor_row
        if cursor_row < 0 or cursor_row >= len(self._display_rows):
            return
        chain, is_holder = self._display_rows[cursor_row]
        sql = chain.holder_query if is_holder else chain.waiter_query
        if not sql.strip():
            self._show_banner("Cannot EXPLAIN an idle query", error=True)
            return
        self.run_worker(self._do_explain(sql), exclusive=False)

    async def _do_explain(self, sql: str) -> None:
        driver = self._connection.driver
        prefix = _explain_prefix(driver)
        explain_sql = prefix + sql
        try:
            result = await self._connector.execute(explain_sql)
        except Exception as exc:
            logger.warning("EXPLAIN failed: %s", exc)
            self._show_banner(f"EXPLAIN failed: {exc}", error=True)
            return
        lines = ["\t".join(str(v) for v in row) for row in result.rows]
        output = "\n".join(lines) if lines else "(no output)"
        self.app.push_screen(LockExplainModal(output))

    def action_copy_sql(self) -> None:
        """Copy the highlighted row's query to clipboard (C)."""
        if not self._display_rows:
            return
        table = self.query_one("#locks-table", DataTable)
        cursor_row = table.cursor_row
        if cursor_row < 0 or cursor_row >= len(self._display_rows):
            return
        chain, is_holder = self._display_rows[cursor_row]
        sql = chain.holder_query if is_holder else chain.waiter_query
        try:
            pyperclip.copy(sql)
            self._show_banner("Copied query to clipboard")
        except Exception as exc:
            logger.warning("pyperclip.copy failed: %s", exc)
            self._show_banner(f"Copy failed: {exc}", error=True)

    def action_open_filter(self) -> None:
        fi = self.query_one("#filter-input", Input)
        fi.display = True
        fi.focus()

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

    def action_go_back(self) -> None:
        fi = self.query_one("#filter-input", Input)
        if fi.display:
            fi.value = ""
            self._filter_text = ""
            self._close_filter()
            self._render_table()
        else:
            self.dismiss()

    def action_quit(self) -> None:
        self.app.exit()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _close_filter(self) -> None:
        fi = self.query_one("#filter-input", Input)
        fi.display = False
        self.query_one("#locks-table", DataTable).focus()
