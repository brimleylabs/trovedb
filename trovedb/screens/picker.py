"""Connection picker — the first screen the operator sees when launching trovedb."""

from __future__ import annotations

import logging

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import DataTable, Input, Static
from textual.worker import Worker, WorkerState

from trovedb.config import ConnectionProfile, Driver

logger = logging.getLogger(__name__)

_HINT_TEXT = (
    "\u2191\u2193 jk: navigate  Enter: connect  n: new URL  /: filter  q: quit"
)


class ConnectionPickerScreen(Screen[None]):
    """List saved connection profiles and let the operator choose one.

    Profiles are injected via the constructor so tests can bypass the
    filesystem entirely.  Pressing *n* opens an inline DSN-URL prompt;
    pressing */* opens a live filter bar.
    """

    BINDINGS = [
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("n", "new_connection", "New URL", show=True),
        Binding("slash", "start_filter", "Filter", show=True),
        Binding("escape", "cancel_input", "Cancel", show=False),
    ]

    DEFAULT_CSS = """
    ConnectionPickerScreen {
        background: $background;
    }

    #picker-status {
        dock: top;
        height: 1;
        background: $primary-darken-2;
        color: $text;
        padding: 0 1;
    }

    #picker-hint {
        dock: bottom;
        height: 1;
        background: $primary-darken-2;
        color: $text-muted;
        padding: 0 1;
    }

    #filter-input {
        dock: bottom;
        display: none;
    }

    #url-input {
        dock: bottom;
        display: none;
    }

    #profile-table {
        height: 1fr;
    }

    #empty-label {
        height: 1fr;
        content-align: center middle;
        color: $text-muted;
    }
    """

    def __init__(self, profiles: dict[str, ConnectionProfile]) -> None:
        super().__init__()
        self._profiles = profiles
        # Tracks which profile keys are currently visible after filtering.
        self._filtered_keys: list[str] = list(profiles.keys())

    # ------------------------------------------------------------------
    # Compose & mount
    # ------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        yield Static("trovedb \u2014 select connection", id="picker-status")
        yield DataTable(id="profile-table", cursor_type="row", show_cursor=True)
        yield Static(
            "No saved connections. Press [bold]n[/bold] to add one.",
            id="empty-label",
        )
        yield Input(placeholder="Filter connections\u2026", id="filter-input")
        yield Input(
            placeholder="DSN: postgres://user@host/db",
            id="url-input",
        )
        yield Static(_HINT_TEXT, id="picker-hint")

    def on_mount(self) -> None:
        has_profiles = bool(self._profiles)
        self.query_one("#profile-table").display = has_profiles
        self.query_one("#empty-label").display = not has_profiles

        if has_profiles:
            self._populate_table()
            self.query_one("#profile-table", DataTable).focus()

    # ------------------------------------------------------------------
    # Table helpers
    # ------------------------------------------------------------------

    def _populate_table(self) -> None:
        """Rebuild the DataTable rows from *self._filtered_keys*."""
        table = self.query_one("#profile-table", DataTable)
        table.clear(columns=True)
        table.add_columns("Name", "Driver", "Host / URL")
        for key in self._filtered_keys:
            p = self._profiles[key]
            host_display = p.url or p.host or "\u2014"
            table.add_row(key, str(p.driver), host_display)

    # ------------------------------------------------------------------
    # Navigation actions
    # ------------------------------------------------------------------

    def action_cursor_down(self) -> None:
        """Move the table cursor one row down."""
        if not self._filtered_keys:
            return
        table = self.query_one("#profile-table", DataTable)
        table.move_cursor(
            row=min(table.cursor_row + 1, len(self._filtered_keys) - 1)
        )

    def action_cursor_up(self) -> None:
        """Move the table cursor one row up."""
        if not self._filtered_keys:
            return
        table = self.query_one("#profile-table", DataTable)
        table.move_cursor(row=max(table.cursor_row - 1, 0))

    # ------------------------------------------------------------------
    # Filter actions
    # ------------------------------------------------------------------

    def action_start_filter(self) -> None:
        """Open the filter bar."""
        self.query_one("#url-input", Input).display = False
        filter_input = self.query_one("#filter-input", Input)
        filter_input.display = True
        filter_input.focus()

    def action_cancel_input(self) -> None:
        """Close the active input widget (filter or URL) without acting."""
        filter_input = self.query_one("#filter-input", Input)
        url_input = self.query_one("#url-input", Input)
        if filter_input.display:
            filter_input.display = False
            filter_input.value = ""
            self._filtered_keys = list(self._profiles.keys())
            self._populate_table()
            if self._profiles:
                self.query_one("#profile-table", DataTable).focus()
        elif url_input.display:
            url_input.display = False
            url_input.value = ""
            if self._profiles:
                self.query_one("#profile-table", DataTable).focus()

    def action_new_connection(self) -> None:
        """Open the inline DSN-URL prompt."""
        self.query_one("#filter-input", Input).display = False
        url_input = self.query_one("#url-input", Input)
        url_input.display = True
        url_input.focus()

    # ------------------------------------------------------------------
    # Connect actions
    # ------------------------------------------------------------------

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Connect when the user presses Enter (or clicks) on a DataTable row."""
        row = event.cursor_row
        if 0 <= row < len(self._filtered_keys):
            self._start_connect(self._profiles[self._filtered_keys[row]])

    def action_select_profile(self) -> None:
        """Connect to the currently highlighted profile (fallback for non-table focus)."""
        if not self._filtered_keys:
            return
        table = self.query_one("#profile-table", DataTable)
        row = table.cursor_row
        if row < 0 or row >= len(self._filtered_keys):
            return
        profile = self._profiles[self._filtered_keys[row]]
        self._start_connect(profile)

    def _connect_from_url(self, url: str) -> None:
        """Build a transient profile from *url* and attempt connection."""
        if url.startswith("postgres"):
            driver = Driver.postgres
        elif url.startswith("mysql"):
            driver = Driver.mysql
        else:
            driver = Driver.sqlite
        profile = ConnectionProfile(name=url, driver=driver, url=url)
        self._start_connect(profile)

    def _start_connect(self, profile: ConnectionProfile) -> None:
        """Update status and kick off the async connection worker."""
        self.query_one("#picker-status", Static).update(
            f"Connecting to {profile.name}\u2026"
        )
        self.run_worker(
            self._connect_worker(profile),
            exclusive=True,
            exit_on_error=False,
        )

    async def _connect_worker(self, profile: ConnectionProfile) -> None:
        """Async worker: open the connector and transition to PROCLIST."""
        from trovedb.connectors import get_connector
        from trovedb.screens.proclist import ProclistScreen

        connector_cls = get_connector(str(profile.driver))
        connector = connector_cls()
        await connector.connect(profile)
        self.app.push_screen(ProclistScreen(profile))

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def on_input_changed(self, event: Input.Changed) -> None:
        """Live-filter the profile list when the filter bar changes."""
        if event.input.id != "filter-input":
            return
        text = event.value.lower()
        if not text:
            self._filtered_keys = list(self._profiles.keys())
        else:
            self._filtered_keys = [
                k
                for k, p in self._profiles.items()
                if (
                    text in k.lower()
                    or text in str(p.driver).lower()
                    or text in (p.host or "").lower()
                    or text in (p.url or "").lower()
                )
            ]
        self._populate_table()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle Enter inside the filter or URL input."""
        if event.input.id == "url-input":
            url = event.value.strip()
            if url:
                self._connect_from_url(url)
        elif event.input.id == "filter-input":
            # Dismiss the filter bar and connect to the highlighted row.
            event.input.display = False
            if self._profiles:
                self.query_one("#profile-table", DataTable).focus()
            self.action_select_profile()

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        """Show a connection-failure message when the worker errors out."""
        if event.state == WorkerState.ERROR:
            error = event.worker.error
            logger.error("Connection attempt failed: %s", error)
            self.query_one("#picker-status", Static).update(
                f"Connection failed: {error}"
            )
