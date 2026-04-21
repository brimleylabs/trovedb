"""trovedb Textual application shell."""

from __future__ import annotations

import logging

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import Static

from trovedb import __version__

logger = logging.getLogger(__name__)

_KEYMAP_HELP = """\
 Keybindings
 ───────────────────────────
 ?      Open / close help
 q      Quit
 Esc    Close this overlay
"""


class HelpOverlay(ModalScreen[None]):
    """Modal help overlay showing the current keymap."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close", show=False),
        Binding("question_mark", "dismiss", "Close", show=False),
    ]

    DEFAULT_CSS = """
    HelpOverlay {
        align: center middle;
    }

    HelpOverlay > #help-content {
        background: $surface;
        border: solid $primary;
        padding: 1 2;
        width: 40;
        height: auto;
    }
    """

    def compose(self) -> ComposeResult:
        yield Static(_KEYMAP_HELP, id="help-content")


class TroveApp(App[None]):
    """trovedb TUI application — operator console for SQL databases."""

    CSS_PATH = "theme/default.tcss"

    BINDINGS = [
        Binding("question_mark", "toggle_help", "Help", show=False),
        Binding("q", "quit", "Quit", show=False),
    ]

    def __init__(
        self,
        conn_name: str | None = None,
        conn_url: str | None = None,
    ) -> None:
        """Initialise the app.

        Parameters
        ----------
        conn_name:
            Named profile from ``~/.config/trovedb/connections.toml`` to
            connect to directly, bypassing the picker.
        conn_url:
            Ad-hoc DSN URL to connect to directly, bypassing the picker.
        """
        super().__init__()
        self._conn_name = conn_name
        self._conn_url = conn_url

    def compose(self) -> ComposeResult:
        # The app-level chrome widgets stay in the DOM and are visible
        # when no picker / PROCLIST screen is pushed on top.  They are
        # also queried by the existing test suite, so keep their IDs stable.
        yield Static(
            f"trovedb {__version__} \u2014 (no connection)",
            id="status-bar",
        )
        yield Static("Welcome to trovedb", id="main-content")
        yield Static("?: help  q: quit", id="hint-bar")

    def on_mount(self) -> None:
        """Push the appropriate first screen after the shell mounts."""
        from trovedb.config import default_config_path, load_connections
        from trovedb.screens.picker import ConnectionPickerScreen

        profiles = load_connections(default_config_path())

        if self._conn_url:
            # Positional URL — treat as a single ad-hoc profile.
            picker = ConnectionPickerScreen(profiles)
            self.push_screen(picker)
            # Immediately trigger a URL-based connection after the screen mounts.
            self.call_after_refresh(
                picker._connect_from_url, self._conn_url  # noqa: SLF001
            )
        elif self._conn_name:
            if self._conn_name not in profiles:
                self.exit(message=f"Unknown connection: {self._conn_name!r}")
                return
            picker = ConnectionPickerScreen(profiles)
            self.push_screen(picker)
            self.call_after_refresh(
                picker._start_connect,  # noqa: SLF001
                profiles[self._conn_name],
            )
        else:
            self.push_screen(ConnectionPickerScreen(profiles))

    def action_toggle_help(self) -> None:
        """Open the help overlay."""
        self.push_screen(HelpOverlay())
