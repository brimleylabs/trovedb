"""ConnectionPickerScreen — profile selector and connection opener."""

from __future__ import annotations

import logging

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import DataTable, Input, Label, Static

from trovedb.config import ConnectionProfile, default_config_path, load_connections
from trovedb.connectors import get_connector
from trovedb.connectors.types import Connection

logger = logging.getLogger(__name__)


class ConnectionPickerScreen(Screen[None]):
    """Connection profile picker.

    Loads saved profiles from ``~/.config/trovedb/connections.toml``,
    presents them in a DataTable, and connects to the selected one.
    On success it pushes :class:`~trovedb.screens.proclist.ProclistScreen`.
    """

    DEFAULT_CSS = """
    ConnectionPickerScreen #picker-status {
        dock: top;
        height: 1;
        background: $primary-darken-2;
        color: $text;
        padding: 0 1;
    }
    ConnectionPickerScreen #picker-hint {
        dock: bottom;
        height: 1;
        background: $primary-darken-2;
        color: $text-muted;
        padding: 0 1;
    }
    ConnectionPickerScreen #profile-table {
        height: 1fr;
    }
    ConnectionPickerScreen #empty-label {
        height: 1fr;
        content-align: center middle;
        color: $text-muted;
    }
    ConnectionPickerScreen #url-input {
        dock: bottom;
        height: 3;
    }
    """

    BINDINGS = [
        Binding("escape", "dismiss", "Back", show=False),
        Binding("q", "quit_app", "Quit", show=False),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._profiles: dict[str, ConnectionProfile] = {}
        self._profile_names: list[str] = []

    def compose(self) -> ComposeResult:
        yield Static("trovedb — select a connection", id="picker-status")
        yield DataTable(id="profile-table", cursor_type="row")
        yield Label(
            "No connection profiles found.\n"
            "Add profiles to ~/.config/trovedb/connections.toml",
            id="empty-label",
        )
        yield Input(
            placeholder="Or enter a connection URL and press Enter…",
            id="url-input",
        )
        yield Static(
            "Enter: connect  Esc: back  q: quit", id="picker-hint"
        )

    def on_mount(self) -> None:
        table = self.query_one("#profile-table", DataTable)
        table.add_column("name", key="name")
        table.add_column("driver", key="driver")
        table.add_column("host / url", key="host")

        try:
            self._profiles = load_connections(default_config_path())
        except Exception as exc:
            logger.warning("Failed to load connection profiles: %s", exc)
            self._profiles = {}

        self._profile_names = list(self._profiles.keys())

        if self._profile_names:
            self.query_one("#empty-label", Label).display = False
            for name in self._profile_names:
                profile = self._profiles[name]
                driver = (
                    profile.driver.value
                    if hasattr(profile.driver, "value")
                    else str(profile.driver)
                )
                host = profile.url or profile.host or "?"
                table.add_row(name, driver, host, key=name)
        else:
            table.display = False

    def on_data_table_row_selected(
        self, event: DataTable.RowSelected
    ) -> None:
        """Connect to the selected profile."""
        name = str(event.row_key.value)
        profile = self._profiles.get(name)
        if profile is not None:
            self.run_worker(self._connect(profile), exclusive=True)

    async def _connect(self, profile: ConnectionProfile) -> None:
        """Open a connection for *profile* and push ProclistScreen."""
        status = self.query_one("#picker-status", Static)
        status.update(f"trovedb — connecting to {profile.name}…")
        try:
            driver_name = (
                profile.driver.value
                if hasattr(profile.driver, "value")
                else str(profile.driver)
            )
            connector_cls = get_connector(driver_name)
            connector = connector_cls()
            connection: Connection = await connector.connect(profile)
        except Exception as exc:
            logger.error("Connection to %r failed: %s", profile.name, exc)
            status.update(f"trovedb — connection failed: {exc}")
            return

        from trovedb.screens.proclist import ProclistScreen  # avoid circular import

        self.app.push_screen(ProclistScreen(profile, connector, connection))

    def action_quit_app(self) -> None:
        """Quit the application."""
        self.app.exit()
