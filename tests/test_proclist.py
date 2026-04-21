"""Tests for ProclistScreen — the live session table headline view."""

from __future__ import annotations

import unittest.mock
from typing import Any

import pytest
from textual.app import App
from textual.widgets import DataTable, Static

from trovedb.config import ConnectionProfile, Driver
from trovedb.connectors.types import Connection, Process, ResultSet
from trovedb.screens.proclist import (
    ExplainModal,
    KillConfirmModal,
    ProclistScreen,
    format_runtime,
)

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

    async def kill_process(self, pid: int, force: bool = False) -> None:
        raise NotImplementedError("FakeConnector does not support kill_process")

    async def execute(self, sql: str, params: Any = None) -> ResultSet:
        raise NotImplementedError("FakeConnector does not support execute")


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


def _make_connection(
    driver: str = "sqlite",
    backend_pid: int | None = None,
) -> Connection:
    return Connection(
        driver=driver,
        dsn="file:test.db",
        connected=True,
        backend_pid=backend_pid,
    )


class _ProclistApp(App[None]):
    """Minimal host app that immediately pushes ProclistScreen for testing."""

    def __init__(self, connector: Any, *, watch_interval: int = 2) -> None:
        super().__init__()
        self._connector = connector
        self._profile = _make_profile()
        self._connection = _make_connection()
        self._watch_interval = watch_interval

    async def on_mount(self) -> None:
        await self.push_screen(
            ProclistScreen(
                self._profile,
                self._connector,
                self._connection,
                watch_interval=self._watch_interval,
            )
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
# Existing TUI tests (card 9)
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
    """Hint bar shows W, R, K, E, C, /, Esc, q as the actionable keys."""
    fake = FakeConnector([])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        hint = screen.query_one("#proclist-hint", Static)
        content = str(hint.content)
        assert "W: watch" in content
        assert "R: refresh" in content
        assert "K: kill" in content
        assert "E: explain" in content
        assert "C: copy" in content
        assert "/: filter" in content
        assert "Esc: back" in content
        assert "q: quit" in content


# ---------------------------------------------------------------------------
# Card 10: Watch mode tests
# ---------------------------------------------------------------------------


async def test_watch_mode_default_on_calls_list_processes_repeatedly() -> None:
    """With watch active, _on_watch_tick triggers list_processes calls."""
    fake = FakeConnector([make_process(1)])
    app = _ProclistApp(fake, watch_interval=1)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await pilot.pause()

        # Simulate the timer callback firing twice — watch is ON by default
        count_before = fake.call_count
        await screen._on_watch_tick()
        await screen._on_watch_tick()
        await pilot.pause()

        assert fake.call_count >= count_before + 2


async def test_watch_mode_toggle_stops_refresh() -> None:
    """When watch is toggled off, _on_watch_tick does not call list_processes."""
    fake = FakeConnector([make_process(1)])
    app = _ProclistApp(fake, watch_interval=1)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await pilot.pause()

        # Toggle watch OFF via the W key
        await pilot.press("w")
        await pilot.pause()

        count_after_toggle = fake.call_count

        # Simulating timer ticks with watch OFF should NOT call list_processes
        await screen._on_watch_tick()
        await screen._on_watch_tick()
        await pilot.pause()

        assert fake.call_count == count_after_toggle


async def test_interval_key_changes_refresh_rate() -> None:
    """Pressing '5' sets the watch interval to 5s and updates the status bar."""
    fake = FakeConnector([make_process(1)])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        await pilot.press("5")
        await pilot.pause()

        status = screen.query_one("#proclist-status", Static)
        assert "watch 5s" in str(status.content)


async def test_watch_mode_shows_paused_indicator() -> None:
    """After toggling off, the status bar shows '⏸ paused'."""
    fake = FakeConnector([make_process(1)])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        await pilot.press("w")
        await pilot.pause()

        status = screen.query_one("#proclist-status", Static)
        assert "⏸ paused" in str(status.content)


# ---------------------------------------------------------------------------
# Card 10: Cursor persistence tests
# ---------------------------------------------------------------------------


async def test_cursor_persists_on_refresh_by_pid() -> None:
    """Cursor stays on the same pid after a refresh that returns the same rows."""
    processes = [make_process(1), make_process(2), make_process(3)]
    fake = FakeConnector(processes)
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        table = screen.query_one("#proclist-table", DataTable)
        table.move_cursor(row=1)  # select pid=2
        await pilot.pause()

        await screen._do_refresh()  # same pids, cursor should stay on pid=2
        await pilot.pause()

        assert table.cursor_row == 1


async def test_cursor_snaps_when_selected_pid_disappears() -> None:
    """When the highlighted pid vanishes on refresh, cursor snaps to nearest row."""
    fake = FakeConnector([make_process(1), make_process(2), make_process(3)])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        table = screen.query_one("#proclist-table", DataTable)
        table.move_cursor(row=2)  # pid=3
        await pilot.pause()

        # Remove pid=3 so cursor has to snap
        fake._processes = [make_process(1), make_process(2)]
        await screen._do_refresh()
        await pilot.pause()

        # Should snap to row 1 (max(0, min(2, 2-1)))
        assert table.row_count == 2
        assert table.cursor_row == 1


# ---------------------------------------------------------------------------
# Card 10: Kill tests
# ---------------------------------------------------------------------------


class _KillFakeConnector(FakeConnector):
    """FakeConnector that records kill_process calls."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.kill_calls: list[tuple[int, bool]] = []

    async def kill_process(self, pid: int, force: bool = False) -> None:
        self.kill_calls.append((pid, force))


async def test_kill_lowercase_y_cancels_query() -> None:
    """Highlight a row, press K then y → kill_process(pid, force=False) called."""
    proc = make_process(42)
    fake = _KillFakeConnector([proc])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        await pilot.press("k")
        await pilot.pause()

        # Modal should now be the active screen
        assert isinstance(pilot.app.screen, KillConfirmModal)

        await pilot.press("y")
        await pilot.pause(0.5)

        assert (42, False) in fake.kill_calls


async def test_kill_uppercase_y_requires_double_tap() -> None:
    """Single Y does not kill; second Y within 2s calls kill_process(force=True)."""
    proc = make_process(42)
    fake = _KillFakeConnector([proc])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        await pilot.press("k")
        await pilot.pause()

        assert isinstance(pilot.app.screen, KillConfirmModal)

        # First uppercase Y — should NOT call kill yet
        await pilot.press("Y")
        await pilot.pause()
        assert (42, True) not in fake.kill_calls

        # Second uppercase Y within 2s — should confirm terminate
        await pilot.press("Y")
        await pilot.pause(0.5)
        assert (42, True) in fake.kill_calls


async def test_kill_cancel_dismisses_modal_without_calling() -> None:
    """Pressing C in the kill modal dismisses it without calling kill_process."""
    proc = make_process(42)
    fake = _KillFakeConnector([proc])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        await pilot.press("k")
        await pilot.pause()

        assert isinstance(pilot.app.screen, KillConfirmModal)

        await pilot.press("c")
        await pilot.pause()

        # Modal dismissed, no kill call
        assert not isinstance(pilot.app.screen, KillConfirmModal)
        assert len(fake.kill_calls) == 0


async def test_kill_escape_dismisses_modal_without_calling() -> None:
    """Pressing Escape in the kill modal dismisses it without calling kill_process."""
    proc = make_process(42)
    fake = _KillFakeConnector([proc])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        await pilot.press("k")
        await pilot.pause()

        assert isinstance(pilot.app.screen, KillConfirmModal)

        await pilot.press("escape")
        await pilot.pause()

        assert not isinstance(pilot.app.screen, KillConfirmModal)
        assert len(fake.kill_calls) == 0


async def test_kill_permission_denied_renders_error_banner_no_crash() -> None:
    """When kill_process raises, the error banner appears and the app doesn't crash."""

    class _FailKillConnector(FakeConnector):
        async def kill_process(self, pid: int, force: bool = False) -> None:
            raise RuntimeError("permission denied for role app_user")

    proc = make_process(42)
    fake = _FailKillConnector([proc])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        await pilot.press("k")
        await pilot.pause()

        await pilot.press("y")
        await pilot.pause(0.5)

        banner = screen.query_one("#proclist-banner", Static)
        assert banner.display is True
        assert "Error" in str(banner.content)


async def test_kill_self_session_is_blocked() -> None:
    """Pressing K on the session's own backend_pid shows a banner, opens no modal."""
    proc = make_process(42)
    fake = _KillFakeConnector([proc])
    # Connection backend_pid matches the highlighted process
    connection = _make_connection(backend_pid=42)

    class _SelfKillApp(App[None]):
        async def on_mount(self) -> None:
            await self.push_screen(
                ProclistScreen(_make_profile(), fake, connection)
            )

    app = _SelfKillApp()
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        await pilot.press("k")
        await pilot.pause()

        # No modal opened, no kill called
        assert not isinstance(pilot.app.screen, KillConfirmModal)
        assert len(fake.kill_calls) == 0

        # Banner shows the guardrail message
        banner = screen.query_one("#proclist-banner", Static)
        assert banner.display is True
        assert "Cannot kill" in str(banner.content)


# ---------------------------------------------------------------------------
# Card 10: EXPLAIN tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("driver", "expected_prefix"),
    [
        ("postgres", "EXPLAIN (ANALYZE, BUFFERS) "),
        ("mysql", "EXPLAIN FORMAT=TREE "),
        ("sqlite", "EXPLAIN QUERY PLAN "),
    ],
)
async def test_explain_runs_connector_execute_with_driver_specific_prefix(
    driver: str, expected_prefix: str
) -> None:
    """EXPLAIN prepends the driver-specific prefix before the query."""
    executed_sql: list[str] = []

    class _ExplainConnector(FakeConnector):
        async def execute(self, sql: str, params: Any = None) -> ResultSet:
            executed_sql.append(sql)
            return ResultSet(columns=["QUERY PLAN"], rows=[("Seq Scan on t",)], row_count=1)

    proc = make_process(1, info="SELECT 1")
    fake = _ExplainConnector([proc])
    connection = _make_connection(driver=driver)

    class _ExplainApp(App[None]):
        async def on_mount(self) -> None:
            await self.push_screen(
                ProclistScreen(_make_profile(), fake, connection)
            )

    app = _ExplainApp()
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        await pilot.press("e")
        await pilot.pause(0.5)

        assert any(sql.startswith(expected_prefix) for sql in executed_sql), (
            f"Expected EXPLAIN prefix {expected_prefix!r}, got: {executed_sql}"
        )


async def test_explain_disabled_on_idle_rows() -> None:
    """Pressing E on an idle row shows a banner and does NOT call execute."""
    executed_sql: list[str] = []

    class _ExplainConnector(FakeConnector):
        async def execute(self, sql: str, params: Any = None) -> ResultSet:
            executed_sql.append(sql)
            return ResultSet(columns=[], rows=[], row_count=0)

    proc = make_process(1, info=None, state="idle")
    fake = _ExplainConnector([proc])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        await pilot.press("e")
        await pilot.pause(0.3)

        assert len(executed_sql) == 0
        # Banner shown instead
        banner = screen.query_one("#proclist-banner", Static)
        assert banner.display is True


async def test_explain_shows_modal_with_output() -> None:
    """A successful EXPLAIN pushes the ExplainModal onto the screen stack."""

    class _ExplainConnector(FakeConnector):
        async def execute(self, sql: str, params: Any = None) -> ResultSet:
            return ResultSet(columns=["QUERY PLAN"], rows=[("Seq Scan",)], row_count=1)

    proc = make_process(1, info="SELECT 1")
    fake = _ExplainConnector([proc])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        await pilot.press("e")
        await pilot.pause(0.5)

        assert isinstance(pilot.app.screen, ExplainModal)


# ---------------------------------------------------------------------------
# Card 10: Copy SQL test
# ---------------------------------------------------------------------------


async def test_copy_sql_calls_pyperclip_with_full_query_text() -> None:
    """Pressing C copies the full (untruncated) query to the clipboard."""
    long_query = "SELECT " + "col" * 200
    proc = make_process(1, info=long_query)
    fake = FakeConnector([proc])
    app = _ProclistApp(fake)
    async with app.run_test() as pilot:
        screen: ProclistScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        with unittest.mock.patch("pyperclip.copy") as mock_copy:
            await pilot.press("c")
            await pilot.pause()

            mock_copy.assert_called_once_with(long_query)
