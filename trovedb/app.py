"""trovedb Textual application shell."""

from __future__ import annotations

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import Static

from trovedb import __version__

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

    def compose(self) -> ComposeResult:
        yield Static(
            f"trovedb {__version__} \u2014 (no connection)",
            id="status-bar",
        )
        yield Static("Welcome to trovedb", id="main-content")
        yield Static("?: help  q: quit", id="hint-bar")

    def on_mount(self) -> None:
        """Push the connection picker on startup."""
        from trovedb.screens.picker import ConnectionPickerScreen  # lazy to avoid cycles

        self.push_screen(ConnectionPickerScreen())

    def action_toggle_help(self) -> None:
        """Open the help overlay."""
        self.push_screen(HelpOverlay())
