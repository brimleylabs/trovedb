"""Tests for ProclistScreen — the live session table headline view."""

from __future__ import annotations

from typing import Any

import pytest
from textual.app import App
from textual.widgets import DataTable, Static

from trovedb.config import ConnectionProfile, Driver
from trovedb.connectors.types import Connection, Process
from trovedb.screens.proclist import ProclistScreen, format_runtime

# ---------------------------------------------------------------------------
# Fake connector + test helpers
# ---------------------------------------------------------------------------


class FakeConnector:
    """In-memory connector that returns a preset list of processes."""

    def __init__(
        self,
        processes: list[Process],
        *,
        fail_after: int | None = None,
    ) -> None:
        self._processes = processes
        self._fail_after = fail_after
        self.call_count = 0

    async def list_processes(self) -> list[Process]:
        self.call_count += 1
        if self._fail_after is not None and self.call_count > self._fail_after:
            raise RuntimeError("connection lost")
        return list(self._processes)


def make_process(
    pid: int,
    user: str = "app_user",
    db: str = "mydb",
    state: str = "active",
    info: str | None = "SELECT 1",
    time_seconds: float | None = 1.0,
    blocked_by: int | None = None,
    wait_event: str | None = None,
) -> Process:
    """Build a Process for use in tests."""
    return Process(
        pid=pid,
        user=user,
        db=db,
        state=state,
        info=info,
        time_seconds=time_seconds,
        blocked_by=blocked_by,
        wait_event=wait_event,
    )


def _make_profile(name: str = "test-conn") -> ConnectionProfile:
    return ConnectionProfile(
        name=name,
        driver=Driver.sqlite,
        url="file:test.db",
    )


def _make_connection() -> Connection:
    return Connection(driver="sqlite", dsn="file:test.db", connected=True)


class _ProclistApp(App[None]):
    """Minimal host app that immediately pushes ProclistScreen for testing."""

    def __init__(self, connector: Any) -> None:
        super().__init__()
        self._connector = connector
        self._profile = _make_profile()
        self._connection = _make_connection()

    async def on_mount(self) -> None:
        await self.push_screen(
            ProclistScreen(self._profile, self._connector, self._connection)
        )


# ---------------------------------------------------------------------------
# Runtime formatting (pure unit tests — no TUI involved)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (None, "—"),
        (0.4, "0.4s"),
        (2.4, "2.4s"),
        (61, "1m 01s"),
        (4 * 60 + 12, "4m 12s"),
        (3720, "1h 02m"),
    ],
)
def test_proclist_runtime_formatting(seconds: float | None, expected: str) -> None:
    assert format_runtime(seconds) == expected


# ---------------------------------------------------------------------------
# TUI tests
# ---------------------------------------------------------------------------


async def test_proclist_renders_rows_from_connector() -> None:
    """Three fake processes → three rows in the DataTable."""
    processes = [make_process(i, user=f"user{i}") for i in range(1, 4)]
    fake = FakeConnector(processes)
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        table = screen.query_one("#proclist-table", DataTable)
        assert table.row_count == 3


async def test_proclist_empty_state() -> None:
    """When connector returns [], the empty-state label is visible."""
    fake = FakeConnector([])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        empty = screen.query_one("#proclist-empty", Static)
        assert empty.display is True

        table = screen.query_one("#proclist-table", DataTable)
        assert table.display is False

        assert "No active sessions" in str(empty.content)


async def test_proclist_detail_footer_shows_full_query_on_selection() -> None:
    """Long queries are truncated in the table cell but shown in full in the footer."""
    long_query = "SELECT " + "x" * 500
    proc = make_process(42, info=long_query)
    fake = FakeConnector([proc])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        table = screen.query_one("#proclist-table", DataTable)
        # Cell should be truncated (contains the ellipsis character)
        cell_value = str(table.get_row_at(0)[-1])
        assert "…" in cell_value
        assert len(cell_value) < len(long_query)

        # Footer should show the full (untruncated) query
        footer = screen.query_one("#proclist-footer", Static)
        assert str(footer.content) == long_query


async def test_proclist_refresh_key_calls_list_processes() -> None:
    """Pressing R triggers a second call to list_processes."""
    fake = FakeConnector([make_process(1)])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        calls_before = fake.call_count

        await pilot.press("r")
        # Allow the worker spawned by action_refresh to run to completion
        await pilot.pause(0.3)

        assert fake.call_count > calls_before


async def test_proclist_filter_narrows_rows() -> None:
    """Typing a filter string client-side hides non-matching rows."""
    processes = [
        make_process(1, user="app_rw", db="prod"),
        make_process(2, user="app_ro", db="prod"),
        make_process(3, user="app_rw", db="staging"),
        make_process(4, user="backup", db="prod"),
        make_process(5, user="app_rw", db="dev"),
    ]
    fake = FakeConnector(processes)
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        # Apply filter directly (equivalent to pressing / then typing "app_rw")
        screen._filter_text = "app_rw"
        screen._render_table()
        await pilot.pause()

        table = screen.query_one("#proclist-table", DataTable)
        # Only user=="app_rw" rows match (3 out of 5)
        assert table.row_count == 3

        # Clearing filter restores all rows
        screen._filter_text = ""
        screen._render_table()
        await pilot.pause()
        assert table.row_count == 5


async def test_proclist_filter_open_via_slash_key() -> None:
    """Pressing '/' makes the filter input visible and focused."""
    fake = FakeConnector([make_process(1)])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        from textual.widgets import Input

        fi = screen.query_one("#filter-input", Input)
        assert fi.display is False

        await pilot.press("slash")
        await pilot.pause()

        assert fi.display is True


async def test_proclist_handles_connection_error() -> None:
    """On refresh error: error banner is visible, table retains previous rows."""
    # First call succeeds, second raises
    fake = FakeConnector([make_process(1), make_process(2)], fail_after=1)
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()  # call 1 — succeeds

        table = screen.query_one("#proclist-table", DataTable)
        assert table.row_count == 2

        await screen._do_refresh()  # call 2 — fails

        # Error banner visible
        err = screen.query_one("#proclist-error", Static)
        assert err.display is True
        assert "Connection lost" in str(err.content)

        # Table still shows previous data
        assert table.row_count == 2


async def test_proclist_idle_rows_render_dash_for_runtime_and_paren_idle_for_query() -> None:
    """Idle processes: runtime column shows '—', footer shows '(idle)'."""
    proc = make_process(99, time_seconds=None, info=None, state="idle")
    fake = FakeConnector([proc])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        table = screen.query_one("#proclist-table", DataTable)
        row = table.get_row_at(0)
        # runtime column is index 5 (gutter=0, pid=1, user=2, db=3, state=4, runtime=5)
        runtime_val = str(row[5])
        assert runtime_val == "—"

        footer = screen.query_one("#proclist-footer", Static)
        assert str(footer.content) == "(idle)"


async def test_proclist_blocked_row_shows_marker() -> None:
    """Blocked rows (blocked_by != None) render with the ▶ gutter marker."""
    proc = make_process(7, blocked_by=5)
    fake = FakeConnector([proc])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        table = screen.query_one("#proclist-table", DataTable)
        row = table.get_row_at(0)
        # gutter is first column (index 0)
        assert str(row[0]) == "▶"


async def test_proclist_status_bar_content() -> None:
    """Status bar includes profile name, driver, session count, and refresh time."""
    fake = FakeConnector([make_process(1), make_process(2)])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        status = screen.query_one("#proclist-status", Static)
        content = str(status.content)
        assert "test-conn" in content
        assert "sqlite" in content
        assert "2 sessions" in content
        assert "last refresh:" in content


async def test_proclist_hint_bar_shows_expected_bindings() -> None:
    """Hint bar shows R, /, Esc, q as the actionable keys."""
    fake = FakeConnector([])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        hint = screen.query_one("#proclist-hint", Static)
        content = str(hint.content)
        assert "R: refresh" in content
        assert "/: filter" in content
        assert "Esc: back" in content
        assert "q: quit" in content
