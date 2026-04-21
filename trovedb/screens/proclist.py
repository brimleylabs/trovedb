"""Placeholder PROCLIST screen (full implementation is a future card)."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import Static

from trovedb.config import ConnectionProfile


class ProclistScreen(Screen[None]):
    """Placeholder for the live PROCLIST operator view.

    Confirms a successful connection and will be replaced by the full
    PROCLIST implementation in a later card.
    """

    BINDINGS = [
        Binding("escape", "go_back", "Back", show=True),
    ]

    DEFAULT_CSS = """
    ProclistScreen {
        background: $background;
    }

    #proclist-status {
        dock: top;
        height: 1;
        background: $primary-darken-2;
        color: $text;
        padding: 0 1;
    }

    #proclist-content {
        height: 1fr;
        content-align: center middle;
        color: $text-muted;
    }

    #proclist-hint {
        dock: bottom;
        height: 1;
        background: $primary-darken-2;
        color: $text-muted;
        padding: 0 1;
    }
    """

    def __init__(self, profile: ConnectionProfile) -> None:
        super().__init__()
        self._profile = profile

    def compose(self) -> ComposeResult:
        yield Static(
            f"trovedb \u2014 {self._profile.name}",
            id="proclist-status",
        )
        yield Static(
            f"Connected to {self._profile.name}",
            id="proclist-content",
        )
        yield Static("Esc: back  q: quit", id="proclist-hint")

    def action_go_back(self) -> None:
        """Return to the connection picker."""
        self.app.pop_screen()
