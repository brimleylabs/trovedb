"""Tests for LocksScreen — the blocking-chain view."""

from __future__ import annotations

import unittest.mock
from typing import Any

from textual.app import App
from textual.widgets import DataTable, Static

from trovedb.config import ConnectionProfile, Driver
from trovedb.connectors.types import BlockingChain, Connection, ResultSet
from trovedb.screens.locks import LockKillModal, LocksScreen

# ---------------------------------------------------------------------------
# Fake connector + helpers
# ---------------------------------------------------------------------------


class FakeLockConnector:
    """In-memory connector returning canned BlockingChain data."""

    def __init__(
        self,
        chains: list[BlockingChain],
        *,
        fail: bool = False,
        kill_raises: Exception | None = None,
    ) -> None:
        self._chains = chains
        self._fail = fail
        self.kill_calls: list[tuple[int, bool]] = []
        self._kill_raises = kill_raises

    async def list_blocking_chains(self) -> list[BlockingChain]:
        if self._fail:
            raise RuntimeError("connection lost")
        return list(self._chains)

    async def kill_process(self, pid: int, force: bool = False) -> None:
        self.kill_calls.append((pid, force))
        if self._kill_raises is not None:
            raise self._kill_raises

    async def execute(self, sql: str, params: Any = None) -> ResultSet:
        return ResultSet(columns=["QUERY PLAN"], rows=[("Index Scan on trips",)], row_count=1)


def _make_chain(
    waiter_pid: int = 200,
    holder_pid: int = 100,
    depth: int = 1,
    waiter_user: str = "app_rw",
    holder_user: str = "app_rw",
    waiter_query: str = "DELETE FROM trips",
    holder_query: str = "UPDATE trips SET status='done'",
    lock_type: str = "ROW",
    object_name: str | None = "public.trips",
    waited_seconds: float = 1.5,
) -> BlockingChain:
    return BlockingChain(
        waiter_pid=waiter_pid,
        waiter_user=waiter_user,
        waiter_query=waiter_query,
        holder_pid=holder_pid,
        holder_user=holder_user,
        holder_query=holder_query,
        lock_type=lock_type,
        object_name=object_name,
        waited_seconds=waited_seconds,
        depth=depth,
    )


def _make_profile(driver: Driver = Driver.postgres) -> ConnectionProfile:
    return ConnectionProfile(
        name="test-conn",
        driver=driver,
        url="postgresql://localhost/test",
    )


def _make_connection(driver: str = "postgres") -> Connection:
    return Connection(driver=driver, dsn="postgresql://localhost/test", connected=True)


class _LocksApp(App[None]):
    """Minimal host app that pushes LocksScreen for testing."""

    def __init__(
        self,
        connector: Any,
        *,
        driver: str = "postgres",
        watch_interval: int = 2,
    ) -> None:
        super().__init__()
        self._connector = connector
        self._profile = _make_profile(
            Driver.postgres if driver == "postgres" else
            Driver.sqlite if driver == "sqlite" else Driver.mysql
        )
        self._connection = _make_connection(driver)
        self._watch_interval = watch_interval

    async def on_mount(self) -> None:
        await self.push_screen(
            LocksScreen(
                self._profile,
                self._connector,
                self._connection,
                watch_interval=self._watch_interval,
            )
        )


# ---------------------------------------------------------------------------
# Test: renders holder/waiter tree
# ---------------------------------------------------------------------------


async def test_locks_screen_renders_holder_waiter_tree() -> None:
    """Two waiters under one holder → 3 rows (1 holder + 2 waiters)."""
    chains = [
        _make_chain(waiter_pid=201, holder_pid=100, waited_seconds=0.9),
        _make_chain(waiter_pid=202, holder_pid=100, waited_seconds=0.3, waiter_user="reporter"),
    ]
    fake = FakeLockConnector(chains)
    app = _LocksApp(fake)
    async with app.run_test() as pilot:
        screen: LocksScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        table = screen.query_one("#locks-table", DataTable)
        assert table.row_count == 3  # 1 holder + 2 waiters


async def test_locks_screen_renders_two_holder_trees() -> None:
    """Two distinct holders → 2 holder rows + 1 waiter each = 4 rows total."""
    chains = [
        _make_chain(waiter_pid=201, holder_pid=100),
        _make_chain(waiter_pid=301, holder_pid=200, holder_user="backup"),
    ]
    fake = FakeLockConnector(chains)
    app = _LocksApp(fake)
    async with app.run_test() as pilot:
        screen: LocksScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        table = screen.query_one("#locks-table", DataTable)
        assert table.row_count == 4


# ---------------------------------------------------------------------------
# Test: color codes by role and depth
# ---------------------------------------------------------------------------


async def test_locks_screen_color_codes_by_role_and_depth() -> None:
    """Holder row has 'yellow' style; depth-1 waiter has 'red'; depth-2 has 'dim red'."""
    chains = [
        _make_chain(waiter_pid=201, holder_pid=100, depth=1),
        _make_chain(waiter_pid=202, holder_pid=100, depth=2),
    ]
    fake = FakeLockConnector(chains)
    app = _LocksApp(fake)
    async with app.run_test() as pilot:
        screen: LocksScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        # _display_rows: [(chain, is_holder), ...]
        # Index 0 = holder row, index 1 = depth-1 waiter, index 2 = depth-2 waiter
        assert screen._display_rows[0][1] is True   # is_holder
        assert screen._display_rows[1][0].depth == 1
        assert screen._display_rows[2][0].depth == 2


# ---------------------------------------------------------------------------
# Test: empty state
# ---------------------------------------------------------------------------


async def test_locks_screen_empty_state() -> None:
    """No chains → empty label visible, table hidden."""
    fake = FakeLockConnector([])
    app = _LocksApp(fake)
    async with app.run_test() as pilot:
        screen: LocksScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        empty = screen.query_one("#locks-empty", Static)
        assert empty.display is True
        assert "all clear" in str(empty.content).lower()

        table = screen.query_one("#locks-table", DataTable)
        assert table.display is False


# ---------------------------------------------------------------------------
# Test: SQLite info state
# ---------------------------------------------------------------------------


async def test_locks_screen_sqlite_info_state() -> None:
    """SQLite connector → informational notice shown, table hidden."""
    fake = FakeLockConnector([])
    app = _LocksApp(fake, driver="sqlite")
    async with app.run_test() as pilot:
        screen: LocksScreen = pilot.app.screen  # type: ignore[assignment]

        sqlite_notice = screen.query_one("#locks-sqlite", Static)
        assert sqlite_notice.display is True

        table = screen.query_one("#locks-table", DataTable)
        assert table.display is False


# ---------------------------------------------------------------------------
# Test: kill holder calls connector kill
# ---------------------------------------------------------------------------


async def test_locks_screen_kill_holder_calls_connector_kill() -> None:
    """K → LockKillModal → y → calls connector.kill_process(holder_pid, force=False)."""
    chain = _make_chain(waiter_pid=201, holder_pid=100)
    fake = FakeLockConnector([chain])
    app = _LocksApp(fake)
    async with app.run_test() as pilot:
        screen: LocksScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        # Suppress the run_worker so the kill is deterministic.
        with unittest.mock.patch.object(screen, "run_worker", return_value=None):
            await pilot.press("k")

        # Modal should now be pushed; confirm with soft kill 'y'.
        modal = pilot.app.screen
        assert isinstance(modal, LockKillModal)
        await pilot.press("y")

        assert fake.kill_calls == [(100, False)]


# ---------------------------------------------------------------------------
# Test: watch interval change
# ---------------------------------------------------------------------------


async def test_locks_screen_watch_interval_change() -> None:
    """Pressing '5' changes watch interval to 5s."""
    fake = FakeLockConnector([])
    app = _LocksApp(fake, watch_interval=2)
    async with app.run_test() as pilot:
        screen: LocksScreen = pilot.app.screen  # type: ignore[assignment]
        await pilot.press("5")
        assert screen._watch_interval == 5


async def test_locks_screen_toggle_watch() -> None:
    """W toggles watch mode on/off."""
    fake = FakeLockConnector([])
    app = _LocksApp(fake)
    async with app.run_test() as pilot:
        screen: LocksScreen = pilot.app.screen  # type: ignore[assignment]
        assert screen._watch_active is True
        await pilot.press("w")
        assert screen._watch_active is False
        await pilot.press("w")
        assert screen._watch_active is True


# ---------------------------------------------------------------------------
# Test: filter
# ---------------------------------------------------------------------------


async def test_locks_screen_filter_reduces_rows() -> None:
    """Typing a user filter hides chains that don't match."""
    chains = [
        _make_chain(waiter_pid=201, holder_pid=100, holder_user="app_rw", waiter_user="app_rw"),
        _make_chain(waiter_pid=301, holder_pid=200, holder_user="backup", waiter_user="backup"),
    ]
    fake = FakeLockConnector(chains)
    app = _LocksApp(fake)
    async with app.run_test() as pilot:
        screen: LocksScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        screen._filter_text = "backup"
        screen._render_table()

        table = screen.query_one("#locks-table", DataTable)
        # Only the "backup" holder + its waiter = 2 rows
        assert table.row_count == 2


# ---------------------------------------------------------------------------
# Test: error state
# ---------------------------------------------------------------------------


async def test_locks_screen_error_state() -> None:
    """Connector failure → error banner shown."""
    fake = FakeLockConnector([], fail=True)
    app = _LocksApp(fake)
    async with app.run_test() as pilot:
        screen: LocksScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._do_refresh()

        err = screen.query_one("#locks-error", Static)
        assert err.display is True
        assert "connection lost" in str(err.content).lower()
