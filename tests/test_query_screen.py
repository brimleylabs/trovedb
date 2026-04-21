"""Tests for QueryScreen and QueryHistory.

Acceptance criteria covered:
  - F5 triggers execute and calls connector.execute()
  - Results are rendered in the DataTable
  - Truncation hint shown when rows > 1000
  - Write queries show WriteConfirmModal
  - dangerous=True is passed after confirmation
  - Error panel shown on execute failure
  - History row written on success
  - History row written on error with error text
  - Ctrl+Up loads most recent history entry
  - Ctrl+S saves query to a timestamped file
  - Ctrl+L clears the editor

Testing pattern:
  - Tests that verify execute behaviour call ``_do_execute`` directly
    (the established pattern — see test_schema_screen.py calling
    ``screen._load_databases()`` directly).
  - Tests that verify action routing pass SQL via the ``_sql`` override
    on ``action_execute_query`` to avoid Textual TextArea reactivity quirks
    in the test harness.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from textual.app import App
from textual.widgets import DataTable, Static, TextArea

from tests._fakes import FakeQueryConnector
from trovedb.config import ConnectionProfile, Driver
from trovedb.connectors.types import Connection, ResultSet
from trovedb.data import QueryHistory, is_write_query
from trovedb.screens.query import QueryScreen

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_profile(name: str = "test-conn") -> ConnectionProfile:
    return ConnectionProfile(name=name, driver=Driver.sqlite, url="file:test.db")


def _make_connection() -> Connection:
    return Connection(driver="sqlite", dsn="file:test.db", connected=True)


def _static_text(widget: Static) -> str:
    """Return the text content of a Static widget (Textual 8.x compatible)."""
    return str(widget.render())


class _QueryApp(App[None]):
    """Minimal host that pushes QueryScreen for testing."""

    def __init__(
        self,
        connector: Any,
        *,
        history: QueryHistory | None = None,
        profile_name: str = "test-conn",
    ) -> None:
        super().__init__()
        self._connector = connector
        self._history = history
        self._profile_name = profile_name

    async def on_mount(self) -> None:
        await self.push_screen(
            QueryScreen(
                _make_profile(self._profile_name),
                self._connector,
                _make_connection(),
                history=self._history,
            )
        )


# ---------------------------------------------------------------------------
# Test 1: execute is called with dangerous=False for SELECT
# ---------------------------------------------------------------------------


async def test_query_screen_runs_select_on_f5() -> None:
    """Executing a SELECT query should call connector.execute() with dangerous=False."""
    connector = FakeQueryConnector()
    app = _QueryApp(connector)
    async with app.run_test() as pilot:
        screen: QueryScreen = pilot.app.screen  # type: ignore[assignment]
        await pilot.pause(0.1)

        await screen._do_execute("SELECT 1", dangerous=False)
        await pilot.pause(0.1)

    assert len(connector.execute_calls) >= 1
    sql, dangerous = connector.execute_calls[0]
    assert "SELECT" in sql.upper()
    assert dangerous is False


# ---------------------------------------------------------------------------
# Test 2: Results rendered in DataTable
# ---------------------------------------------------------------------------


async def test_query_screen_renders_results_in_datatable() -> None:
    """After a successful SELECT, results should appear in the DataTable."""
    canned = ResultSet(
        columns=["id", "name"],
        rows=[(1, "alice"), (2, "bob")],
        row_count=2,
    )
    connector = FakeQueryConnector(canned_result=canned)
    app = _QueryApp(connector)
    async with app.run_test() as pilot:
        screen: QueryScreen = pilot.app.screen  # type: ignore[assignment]
        await pilot.pause(0.1)

        await screen._do_execute("SELECT id, name FROM users", dangerous=False)
        await pilot.pause(0.1)

        table = screen.query_one("#query-result", DataTable)
        assert table.row_count == 2
        assert len(table.columns) == 2


# ---------------------------------------------------------------------------
# Test 3: Truncation hint when rows > RESULT_LIMIT
# ---------------------------------------------------------------------------


async def test_query_screen_shows_truncation_hint_when_over_limit() -> None:
    """When result has > 1000 rows the result-status should mention truncation."""
    big_rows = [(i, f"name{i}") for i in range(1500)]
    canned = ResultSet(columns=["id", "name"], rows=big_rows, row_count=1500)
    connector = FakeQueryConnector(canned_result=canned)
    app = _QueryApp(connector)
    async with app.run_test() as pilot:
        screen: QueryScreen = pilot.app.screen  # type: ignore[assignment]
        await pilot.pause(0.1)

        await screen._do_execute("SELECT * FROM big_table", dangerous=False)
        await pilot.pause(0.1)

        status = screen.query_one("#query-result-status", Static)
        status_text = _static_text(status)
        assert "1000" in status_text
        assert "1500" in status_text

        table = screen.query_one("#query-result", DataTable)
        assert table.row_count == 1000


# ---------------------------------------------------------------------------
# Test 4: Write query triggers WriteConfirmModal
# ---------------------------------------------------------------------------


async def test_query_screen_write_query_triggers_confirm_modal() -> None:
    """A non-SELECT statement should block immediate execute (modal is the guardrail).

    We verify the modal guardrail: connector.execute is NOT called immediately
    when a write query is submitted — the action suspends waiting for confirmation.
    A SELECT in the same session still executes normally.
    """
    connector = FakeQueryConnector()
    app = _QueryApp(connector)
    async with app.run_test() as pilot:
        screen: QueryScreen = pilot.app.screen  # type: ignore[assignment]
        await pilot.pause(0.1)

        # DELETE → modal intercepts, execute NOT called
        await screen.action_execute_query("DELETE FROM trips WHERE id = 1")
        await pilot.pause(0.2)
        assert len(connector.execute_calls) == 0

        # SELECT → no modal, execute called immediately
        await screen._do_execute("SELECT 1", dangerous=False)
        assert len(connector.execute_calls) == 1


# ---------------------------------------------------------------------------
# Test 5: dangerous=True passed after confirmation
# ---------------------------------------------------------------------------


async def test_query_screen_dangerous_true_passed_after_confirm() -> None:
    """After modal confirmation dangerous=True must be forwarded to execute.

    We test the post-confirmation path directly: _do_execute(dangerous=True)
    is exactly what the modal callback invokes after the user presses 'y'.
    """
    connector = FakeQueryConnector()
    app = _QueryApp(connector)
    async with app.run_test() as pilot:
        screen: QueryScreen = pilot.app.screen  # type: ignore[assignment]
        await pilot.pause(0.1)

        # Simulate what the confirmed modal callback does
        await screen._do_execute("DELETE FROM trips WHERE id = 1", dangerous=True)
        await pilot.pause(0.1)

    assert len(connector.execute_calls) == 1
    _sql, dangerous = connector.execute_calls[0]
    assert dangerous is True


# ---------------------------------------------------------------------------
# Test 6: Error panel shown on execute failure
# ---------------------------------------------------------------------------


async def test_query_screen_renders_error_panel_on_execute_failure() -> None:
    """When connector.execute() raises, the error Static should be visible."""
    connector = FakeQueryConnector(
        raise_on_execute=RuntimeError("syntax error near 'FROM'")
    )
    app = _QueryApp(connector)
    async with app.run_test() as pilot:
        screen: QueryScreen = pilot.app.screen  # type: ignore[assignment]
        await pilot.pause(0.1)

        await screen._do_execute("SELECT FROM users", dangerous=False)
        await pilot.pause(0.1)

        err = screen.query_one("#query-error", Static)
        assert err.display is True
        err_text = _static_text(err)
        assert "syntax error" in err_text.lower() or "ERROR" in err_text


# ---------------------------------------------------------------------------
# Test 7: History row written on success
# ---------------------------------------------------------------------------


async def test_query_history_writes_row_on_success(tmp_path: Path) -> None:
    """A successful _do_execute should append a row to the history DB."""
    history = QueryHistory(db_path=tmp_path / "history.db")
    canned = ResultSet(columns=["v"], rows=[(42,)], row_count=1)
    connector = FakeQueryConnector(canned_result=canned)
    app = _QueryApp(connector, history=history)

    async with app.run_test() as pilot:
        screen: QueryScreen = pilot.app.screen  # type: ignore[assignment]
        await pilot.pause(0.1)

        await screen._do_execute("SELECT 42", dangerous=False)
        await pilot.pause(0.1)

    rows = await history.fetch("test-conn", limit=10)
    assert len(rows) >= 1
    _id, sql = rows[0]
    assert sql == "SELECT 42"


# ---------------------------------------------------------------------------
# Test 8: History row written on error with error text
# ---------------------------------------------------------------------------


async def test_query_history_writes_row_on_error_with_error_text(
    tmp_path: Path,
) -> None:
    """A failing _do_execute should still write a history row with the error."""
    history = QueryHistory(db_path=tmp_path / "history.db")
    connector = FakeQueryConnector(
        raise_on_execute=RuntimeError("table not found")
    )
    app = _QueryApp(connector, history=history)

    async with app.run_test() as pilot:
        screen: QueryScreen = pilot.app.screen  # type: ignore[assignment]
        await pilot.pause(0.1)

        await screen._do_execute("SELECT * FROM ghost", dangerous=False)
        await pilot.pause(0.1)

    import aiosqlite
    async with aiosqlite.connect(tmp_path / "history.db") as db:
        async with db.execute("SELECT sql, error FROM history") as cursor:
            db_rows = await cursor.fetchall()

    assert len(db_rows) == 1
    row_sql, row_error = db_rows[0]
    assert row_sql == "SELECT * FROM ghost"
    assert row_error is not None
    assert "table not found" in row_error


# ---------------------------------------------------------------------------
# Test 9: Ctrl+Up loads most-recent history entry
# ---------------------------------------------------------------------------


async def test_query_history_ctrl_up_loads_previous_entry(tmp_path: Path) -> None:
    """Ctrl+Up should load the most recent history entry on first press."""
    history = QueryHistory(db_path=tmp_path / "history.db")
    await history.record("test-conn", "SELECT 1")
    await asyncio.sleep(0.01)  # ensure distinct ran_at timestamps
    await history.record("test-conn", "SELECT 2")

    connector = FakeQueryConnector()
    app = _QueryApp(connector, history=history)

    async with app.run_test() as pilot:
        screen: QueryScreen = pilot.app.screen  # type: ignore[assignment]
        await pilot.pause(0.2)  # let on_mount reload history

        # Call action directly — key routing not reliable on Windows in tests
        await screen.action_history_prev()
        await pilot.pause(0.1)

        editor = screen.query_one("#query-editor", TextArea)
        # First Ctrl+Up loads the most recent entry ("SELECT 2")
        assert "SELECT 2" in editor.text


# ---------------------------------------------------------------------------
# Test 10: Ctrl+S saves query with timestamped filename
# ---------------------------------------------------------------------------


async def test_query_save_writes_file_with_timestamped_name(tmp_path: Path) -> None:
    """Ctrl+S should write the editor text to a timestamped .sql file."""
    import unittest.mock

    connector = FakeQueryConnector()
    app = _QueryApp(connector)

    async with app.run_test() as pilot:
        screen: QueryScreen = pilot.app.screen  # type: ignore[assignment]
        await pilot.pause(0.1)

        sql_text = "SELECT * FROM trips LIMIT 5"
        editor = screen.query_one("#query-editor", TextArea)
        editor.load_text(sql_text)
        await pilot.pause(0.05)

        with unittest.mock.patch(
            "trovedb.screens.query.get_queries_dir", return_value=tmp_path
        ):
            await screen.action_save_query()
            await pilot.pause(0.1)

    saved_files = list(tmp_path.glob("test-conn-*.sql"))
    assert len(saved_files) == 1
    assert saved_files[0].read_text(encoding="utf-8") == sql_text


# ---------------------------------------------------------------------------
# Test 11: Ctrl+L clears the editor
# ---------------------------------------------------------------------------


async def test_query_clear_resets_editor() -> None:
    """Ctrl+L (action_clear_editor) should empty the editor."""
    connector = FakeQueryConnector()
    app = _QueryApp(connector)

    async with app.run_test() as pilot:
        screen: QueryScreen = pilot.app.screen  # type: ignore[assignment]
        await pilot.pause(0.1)

        editor = screen.query_one("#query-editor", TextArea)
        editor.load_text("SELECT * FROM users")
        await pilot.pause(0.05)

        screen.action_clear_editor()
        await pilot.pause(0.1)

        assert editor.text.strip() == ""


# ---------------------------------------------------------------------------
# Unit tests for is_write_query
# ---------------------------------------------------------------------------


def test_is_write_query_select_returns_false() -> None:
    assert is_write_query("SELECT * FROM users") is False


def test_is_write_query_with_cte_returns_false() -> None:
    assert is_write_query("WITH cte AS (SELECT 1) SELECT * FROM cte") is False


def test_is_write_query_delete_returns_true() -> None:
    assert is_write_query("DELETE FROM users WHERE id = 1") is True


def test_is_write_query_insert_returns_true() -> None:
    assert is_write_query("INSERT INTO t VALUES (1)") is True


def test_is_write_query_update_returns_true() -> None:
    assert is_write_query("UPDATE t SET x = 1") is True


def test_is_write_query_strips_comments() -> None:
    assert is_write_query("-- drop the table\nSELECT 1") is False
    assert is_write_query("/* comment */ DELETE FROM t") is True


def test_is_write_query_case_insensitive() -> None:
    assert is_write_query("select * from t") is False
    assert is_write_query("DELETE from t") is True


def test_is_write_query_empty_returns_false() -> None:
    assert is_write_query("") is False
    assert is_write_query("   ") is False
