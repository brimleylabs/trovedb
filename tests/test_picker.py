"""Tests for ConnectionPickerScreen.

Each test creates a minimal host App that pushes ConnectionPickerScreen with
an injected profile dict — no filesystem access required.

Widget queries use ``pilot.app.screen.query_one(selector)`` because the
picker is a pushed Screen with its own DOM, separate from the app compose.
"""

from __future__ import annotations

from textual.app import App, ComposeResult
from textual.widgets import DataTable, Input, Static

from trovedb.config import ConnectionProfile, Driver
from trovedb.screens.picker import ConnectionPickerScreen

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _profile(
    name: str,
    driver: Driver = Driver.postgres,
    host: str = "localhost",
) -> ConnectionProfile:
    return ConnectionProfile(name=name, driver=driver, host=host, database="db")


def _url_profile(name: str, url: str) -> ConnectionProfile:
    return ConnectionProfile(name=name, driver=Driver.postgres, url=url)


class _PickerApp(App[None]):
    """Minimal host app that pushes the picker immediately."""

    def __init__(self, profiles: dict[str, ConnectionProfile]) -> None:
        super().__init__()
        self._profiles = profiles

    def compose(self) -> ComposeResult:
        yield from ()  # nothing — picker covers the screen

    async def on_mount(self) -> None:
        await self.push_screen(ConnectionPickerScreen(self._profiles))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_picker_mounts_without_error_empty_profiles() -> None:
    """Picker should mount cleanly when the profile dict is empty."""
    app = _PickerApp({})
    async with app.run_test() as pilot:
        assert isinstance(pilot.app.screen, ConnectionPickerScreen)


async def test_picker_mounts_without_error_with_profiles() -> None:
    """Picker should mount cleanly when profiles are present."""
    profiles = {"prod": _profile("prod")}
    app = _PickerApp(profiles)
    async with app.run_test() as pilot:
        assert isinstance(pilot.app.screen, ConnectionPickerScreen)


async def test_empty_profiles_shows_no_saved_connections_message() -> None:
    """Empty profile dict must render the 'No saved connections' message."""
    app = _PickerApp({})
    async with app.run_test() as pilot:
        label = pilot.app.screen.query_one("#empty-label", Static)
        assert label.display is True
        content = str(label.content)
        assert "No saved connections" in content


async def test_empty_profiles_hides_table() -> None:
    """Profile table must be hidden when there are no profiles."""
    app = _PickerApp({})
    async with app.run_test() as pilot:
        table = pilot.app.screen.query_one("#profile-table", DataTable)
        assert table.display is False


async def test_profiles_appear_in_table() -> None:
    """Profile names must appear as rows in the DataTable."""
    profiles = {
        "dev": _profile("dev", Driver.postgres, "dev.internal"),
        "staging": _profile("staging", Driver.mysql, "staging.internal"),
    }
    app = _PickerApp(profiles)
    async with app.run_test() as pilot:
        table = pilot.app.screen.query_one("#profile-table", DataTable)
        assert table.display is True
        assert table.row_count == 2
        # First column of row 0 must be the first profile name.
        first_row = table.get_row_at(0)
        assert first_row[0] == "dev"
        second_row = table.get_row_at(1)
        assert second_row[0] == "staging"


async def test_filter_shows_matching_profiles() -> None:
    """Typing in the filter bar should narrow the table to matching rows."""
    profiles = {
        "prod-pg": _profile("prod-pg", Driver.postgres, "pg.prod.internal"),
        "dev-mysql": _profile("dev-mysql", Driver.mysql, "mysql.dev.internal"),
        "local-sqlite": _profile("local-sqlite", Driver.sqlite),
    }
    app = _PickerApp(profiles)
    async with app.run_test() as pilot:
        # Open filter bar
        await pilot.press("slash")
        await pilot.pause()

        filter_input = pilot.app.screen.query_one("#filter-input", Input)
        assert filter_input.display is True

        # Type a filter string that matches only one profile
        await pilot.press("p", "g")
        await pilot.pause()

        table = pilot.app.screen.query_one("#profile-table", DataTable)
        assert table.row_count == 1
        first_row = table.get_row_at(0)
        assert first_row[0] == "prod-pg"


async def test_filter_is_case_insensitive() -> None:
    """Filter matching must be case-insensitive (lowercase input matches mixed-case name)."""
    profiles = {
        # Both profiles use postgres to avoid driver-name accidents (e.g. "mysql" contains "my").
        "MyProd": _profile("MyProd", Driver.postgres, "prod.db"),
        "staging": _profile("staging", Driver.postgres, "stg.db"),
    }
    app = _PickerApp(profiles)
    async with app.run_test() as pilot:
        await pilot.press("slash")
        await pilot.pause()
        # Type "prod" (lowercase) — must match "MyProd" case-insensitively but not "staging".
        await pilot.press("p", "r", "o", "d")
        await pilot.pause()
        table = pilot.app.screen.query_one("#profile-table", DataTable)
        assert table.row_count == 1
        assert table.get_row_at(0)[0] == "MyProd"


async def test_j_moves_cursor_down() -> None:
    """Pressing 'j' should advance the DataTable cursor by one row."""
    profiles = {
        "alpha": _profile("alpha"),
        "beta": _profile("beta"),
    }
    app = _PickerApp(profiles)
    async with app.run_test() as pilot:
        table = pilot.app.screen.query_one("#profile-table", DataTable)
        initial_row = table.cursor_row
        await pilot.press("j")
        await pilot.pause()
        assert table.cursor_row == initial_row + 1


async def test_k_moves_cursor_up() -> None:
    """Pressing 'k' should move the DataTable cursor one row up."""
    profiles = {
        "alpha": _profile("alpha"),
        "beta": _profile("beta"),
        "gamma": _profile("gamma"),
    }
    app = _PickerApp(profiles)
    async with app.run_test() as pilot:
        # Move down twice first, then back up once.
        await pilot.press("j")
        await pilot.press("j")
        await pilot.pause()
        table = pilot.app.screen.query_one("#profile-table", DataTable)
        row_before_k = table.cursor_row
        await pilot.press("k")
        await pilot.pause()
        assert table.cursor_row == row_before_k - 1


async def test_n_opens_url_input() -> None:
    """Pressing 'n' must make the URL input visible."""
    profiles = {"prod": _profile("prod")}
    app = _PickerApp(profiles)
    async with app.run_test() as pilot:
        url_input = pilot.app.screen.query_one("#url-input", Input)
        assert url_input.display is False

        await pilot.press("n")
        await pilot.pause()

        assert url_input.display is True


async def test_escape_closes_url_input() -> None:
    """Pressing Escape after 'n' should hide the URL input again."""
    profiles = {"prod": _profile("prod")}
    app = _PickerApp(profiles)
    async with app.run_test() as pilot:
        await pilot.press("n")
        await pilot.pause()
        url_input = pilot.app.screen.query_one("#url-input", Input)
        assert url_input.display is True

        await pilot.press("escape")
        await pilot.pause()
        assert url_input.display is False


async def test_enter_on_profile_shows_connecting_status() -> None:
    """Pressing Enter on a highlighted profile must show 'Connecting to …'.

    ``run_worker`` is patched to a no-op so the async connection attempt does
    not overwrite the status before we can read it.  The assertion verifies
    that ``_start_connect`` updated the status bar synchronously.
    """
    from unittest.mock import patch

    profiles = {"prod": _profile("prod")}
    app = _PickerApp(profiles)
    async with app.run_test() as pilot:
        screen = pilot.app.screen
        assert isinstance(screen, ConnectionPickerScreen)

        with patch.object(screen, "run_worker", return_value=None):
            await pilot.press("enter")

        status = screen.query_one("#picker-status", Static)
        assert "Connecting to" in str(status.content)
        assert "prod" in str(status.content)


async def test_enter_on_empty_list_does_nothing() -> None:
    """Enter on an empty picker (no profiles) must not raise."""
    app = _PickerApp({})
    async with app.run_test() as pilot:
        await pilot.press("enter")
        await pilot.pause()
        # App should still be running — no exception
        assert isinstance(pilot.app.screen, ConnectionPickerScreen)


async def test_url_profile_host_column_shows_url() -> None:
    """URL-form profiles should display the URL in the Host / URL column."""
    url = "postgres://admin@prod.db:5432/app"
    profiles = {"cloud": _url_profile("cloud", url)}
    app = _PickerApp(profiles)
    async with app.run_test() as pilot:
        table = pilot.app.screen.query_one("#profile-table", DataTable)
        row = table.get_row_at(0)
        assert row[2] == url  # third column is Host / URL
