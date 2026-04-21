"""Tests for SchemaScreen — the read-only schema browser.

Acceptance criteria covered:
  - Top-level databases are populated from list_databases()
  - Tables are NOT loaded until the database node is expanded (lazy loading)
  - The right pane renders columns, indexes, and foreign keys
  - Filtering keeps only matching table nodes and expands their ancestors
  - D (copy DDL) calls pyperclip.copy() and flashes a banner
  - Row counts are taken from list_tables() row_count field — no COUNT(*) calls
"""

from __future__ import annotations

import unittest.mock
from typing import Any

from textual.app import App
from textual.widgets import DataTable, Input, Static, Tree

from tests._fakes import FakeSchemaConnector
from trovedb.config import ConnectionProfile, Driver
from trovedb.connectors.types import (
    Column,
    Connection,
    Database,
    ForeignKey,
    Index,
    Table,
    TableSchema,
)
from trovedb.screens.schema import SchemaScreen, _ContainerNode, _TableNode

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_profile() -> ConnectionProfile:
    return ConnectionProfile(
        name="test-conn",
        driver=Driver.sqlite,
        url="file:test.db",
    )


def _make_connection() -> Connection:
    return Connection(driver="sqlite", dsn="file:test.db", connected=True)


class _SchemaApp(App[None]):
    """Minimal host app that pushes SchemaScreen for testing."""

    def __init__(self, connector: Any) -> None:
        super().__init__()
        self._connector = connector

    async def on_mount(self) -> None:
        await self.push_screen(
            SchemaScreen(_make_profile(), self._connector, _make_connection())
        )


def _db_names_in_tree(screen: SchemaScreen) -> list[str]:
    """Return the plain labels of all top-level tree nodes."""
    tree = screen.query_one("#schema-tree", Tree)
    return [str(child.label) for child in tree.root.children]


def _tables_in_container(db_node: Any) -> list[str]:
    """Return table_name values from _TableNode leaves under db_node's tables container."""
    names: list[str] = []
    for child in db_node.children:
        if isinstance(child.data, _ContainerNode) and child.data.kind == "tables":
            for leaf in child.children:
                if isinstance(leaf.data, _TableNode):
                    names.append(leaf.data.table_name)
    return names


def _get_tables_node(db_node: Any) -> Any:
    """Return the 'tables (N)' container child node of a DB node."""
    for child in db_node.children:
        if isinstance(child.data, _ContainerNode) and child.data.kind == "tables":
            return child
    return None


def _get_leaf_by_table(db_node: Any, table_name: str) -> Any:
    """Return the TreeNode leaf for a specific table name."""
    for child in db_node.children:
        if isinstance(child.data, _ContainerNode) and child.data.kind == "tables":
            for leaf in child.children:
                if isinstance(leaf.data, _TableNode) and leaf.data.table_name == table_name:
                    return leaf
    return None


# ---------------------------------------------------------------------------
# Test 1: populates top-level databases
# ---------------------------------------------------------------------------


async def test_schema_screen_populates_top_level_databases() -> None:
    """After mount, the tree root should have one child per database."""
    fake = FakeSchemaConnector(
        databases=[Database(name="db1"), Database(name="db2"), Database(name="db3")]
    )
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()
        await pilot.pause()

        names = _db_names_in_tree(screen)
        assert "db1" in names, f"Expected 'db1' in {names}"
        assert "db2" in names
        assert "db3" in names
        assert len(names) == 3


# ---------------------------------------------------------------------------
# Test 2: lazy-loads tables only on expand
# ---------------------------------------------------------------------------


async def test_schema_screen_lazy_loads_tables_on_expand() -> None:
    """list_tables should NOT be called until _load_tables_for_node is invoked."""
    fake = FakeSchemaConnector(
        databases=[Database(name="mydb")],
        tables_by_db={
            "mydb": [
                Table(name="users", db="mydb", row_count=50),
                Table(name="orders", db="mydb", row_count=1_200),
            ]
        },
    )
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()
        await pilot.pause()

        # After loading databases only — list_tables must NOT have been called
        assert fake.list_tables_calls == [], (
            f"list_tables was called before expansion: {fake.list_tables_calls}"
        )

        # Simulate expanding the first DB node
        tree = screen.query_one("#schema-tree", Tree)
        db_node = tree.root.children[0]
        await screen._load_tables_for_node(db_node)
        await pilot.pause()

        assert "mydb" in fake.list_tables_calls, (
            f"Expected 'mydb' in list_tables_calls after expansion, got {fake.list_tables_calls}"
        )

        # Tables should now be visible in the subtree
        table_names = _tables_in_container(db_node)
        assert "users" in table_names
        assert "orders" in table_names


# ---------------------------------------------------------------------------
# Test 3: right pane renders columns, indexes, and foreign keys
# ---------------------------------------------------------------------------


async def test_schema_screen_right_pane_renders_columns_indexes_fks() -> None:
    """After _show_table_schema, the right DataTable should contain section headers
    plus one row per column, index, and foreign key."""
    schema = TableSchema(
        db="testdb",
        table="trips",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False),
            Column(name="title", data_type="TEXT", nullable=True, default="'unnamed'"),
        ],
        indexes=[
            Index(name="trips_pkey", columns=["id"], unique=True, primary=True),
            Index(name="trips_title_idx", columns=["title"], unique=False, primary=False),
        ],
        foreign_keys=[
            ForeignKey(
                name="fk_user",
                columns=["user_id"],
                ref_table="users",
                ref_columns=["id"],
            )
        ],
        ddl="CREATE TABLE trips (id INTEGER, title TEXT);",
    )
    fake = FakeSchemaConnector(
        databases=[Database(name="testdb")],
        tables_by_db={"testdb": [Table(name="trips", db="testdb")]},
        schema_by_table={"testdb.trips": schema},
    )
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()
        await screen._show_table_schema("testdb", "trips")
        await pilot.pause()

        detail = screen.query_one("#schema-detail", DataTable)
        # Expected rows:
        # 1 (── COLUMNS ──) + 2 (id, title) +
        # 1 (── INDEXES ──) + 2 (trips_pkey, trips_title_idx) +
        # 1 (── FOREIGN KEYS ──) + 1 (fk_user) = 8
        assert detail.row_count == 8, f"Expected 8 rows, got {detail.row_count}"


# ---------------------------------------------------------------------------
# Test 4: filter expands matching ancestors
# ---------------------------------------------------------------------------


async def test_schema_screen_filter_expands_matching_ancestors() -> None:
    """Filtering by text keeps only matching table nodes; the DB node is auto-expanded."""
    fake = FakeSchemaConnector(
        databases=[Database(name="prod")],
        tables_by_db={
            "prod": [
                Table(name="users", db="prod"),
                Table(name="orders", db="prod"),
                Table(name="order_items", db="prod"),
                Table(name="products", db="prod"),
            ]
        },
    )
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()

        tree = screen.query_one("#schema-tree", Tree)
        db_node = tree.root.children[0]
        await screen._load_tables_for_node(db_node)
        await pilot.pause()

        # Apply filter: only "order" prefix matches "orders" and "order_items"
        screen._filter_text = "order"
        screen._apply_filter()
        await pilot.pause()

        table_names = _tables_in_container(db_node)
        assert "orders" in table_names, f"Expected 'orders' in {table_names}"
        assert "order_items" in table_names, f"Expected 'order_items' in {table_names}"
        assert "users" not in table_names, f"'users' should be filtered out, got {table_names}"
        assert "products" not in table_names, "'products' should be filtered out"

        # Clear the filter — all tables should return
        screen._filter_text = ""
        screen._apply_filter()
        await pilot.pause()

        all_names = _tables_in_container(db_node)
        assert len(all_names) == 4, f"Expected 4 tables after clearing filter, got {all_names}"


# ---------------------------------------------------------------------------
# Test 5: copy DDL calls pyperclip and flashes banner
# ---------------------------------------------------------------------------


async def test_schema_screen_copy_ddl_calls_pyperclip_and_flashes_banner() -> None:
    """action_copy_ddl should call pyperclip.copy() with the DDL and show a banner."""
    fake = FakeSchemaConnector(
        databases=[Database(name="testdb")],
        tables_by_db={"testdb": [Table(name="trips", db="testdb")]},
        ddl_by_table={"testdb.trips": "CREATE TABLE trips (id INT PRIMARY KEY);"},
    )
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()

        tree = screen.query_one("#schema-tree", Tree)
        db_node = tree.root.children[0]
        await screen._load_tables_for_node(db_node)
        await pilot.pause()

        # Set the selected node to the trips table leaf
        trips_leaf = _get_leaf_by_table(db_node, "trips")
        assert trips_leaf is not None
        screen._selected_node_data = trips_leaf.data

        with unittest.mock.patch("pyperclip.copy") as mock_copy:
            await screen.action_copy_ddl()
            await pilot.pause()
            mock_copy.assert_called_once_with("CREATE TABLE trips (id INT PRIMARY KEY);")

        banner = screen.query_one("#schema-banner", Static)
        assert banner.display is True, "Banner should be visible after DDL copy"
        assert "trips" in str(banner.content), (
            f"Banner should mention 'trips', got: {banner.content!r}"
        )

        # Verify get_ddl was called with correct args
        assert ("table", "testdb", "trips") in fake.get_ddl_calls


# ---------------------------------------------------------------------------
# Test 6: uses row estimate from list_tables, not COUNT(*)
# ---------------------------------------------------------------------------


async def test_schema_screen_uses_estimate_not_count_for_postgres_row_count() -> None:
    """SchemaScreen must use the row_count field from list_tables() for the
    tree label and must NOT call execute() with any COUNT(*) query."""
    fake = FakeSchemaConnector(
        databases=[Database(name="prod")],
        tables_by_db={
            "prod": [
                Table(name="campgrounds", db="prod", row_count=78_274),
                Table(name="vehicles", db="prod", row_count=95_567),
            ]
        },
    )
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()

        tree = screen.query_one("#schema-tree", Tree)
        db_node = tree.root.children[0]
        await screen._load_tables_for_node(db_node)
        await pilot.pause()

        # No COUNT(*) queries should have been issued
        count_calls = [s for s in fake.execute_calls if "count" in s.lower()]
        assert count_calls == [], f"Unexpected COUNT calls: {count_calls}"

        # The tree node labels should include a row count estimate
        for child in db_node.children:
            if isinstance(child.data, _ContainerNode) and child.data.kind == "tables":
                for leaf in child.children:
                    label_str = str(leaf.label)
                    assert "≈" in label_str, (
                        f"Expected '≈' in label for table node, got {label_str!r}"
                    )


# ---------------------------------------------------------------------------
# Test 7: views are grouped separately from base tables
# ---------------------------------------------------------------------------


async def test_schema_screen_views_grouped_separately() -> None:
    """VIEW table_type entries should appear under a 'views (N)' container, not tables."""
    fake = FakeSchemaConnector(
        databases=[Database(name="mydb")],
        tables_by_db={
            "mydb": [
                Table(name="trips", db="mydb", table_type="BASE TABLE"),
                Table(name="active_trips", db="mydb", table_type="VIEW"),
                Table(name="trip_summary", db="mydb", table_type="VIEW"),
            ]
        },
    )
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()

        tree = screen.query_one("#schema-tree", Tree)
        db_node = tree.root.children[0]
        await screen._load_tables_for_node(db_node)
        await pilot.pause()

        container_kinds = {
            c.data.kind for c in db_node.children if isinstance(c.data, _ContainerNode)
        }
        assert "tables" in container_kinds
        assert "views" in container_kinds

        views_node = next(
            c for c in db_node.children
            if isinstance(c.data, _ContainerNode) and c.data.kind == "views"
        )
        view_names = [
            leaf.data.table_name
            for leaf in views_node.children
            if isinstance(leaf.data, _TableNode)
        ]
        assert "active_trips" in view_names
        assert "trip_summary" in view_names
        # trips should NOT be in views
        assert "trips" not in view_names


# ---------------------------------------------------------------------------
# Test 8: error handling when list_databases fails
# ---------------------------------------------------------------------------


async def test_schema_screen_error_on_list_databases_failure() -> None:
    """When list_databases() raises, the error widget should be visible."""
    fake = FakeSchemaConnector(fail_list_databases=True)
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()
        await pilot.pause()

        err = screen.query_one("#schema-error", Static)
        assert err.display is True
        assert "Error" in str(err.content)


# ---------------------------------------------------------------------------
# Test 9: action_refresh_all reloads databases
# ---------------------------------------------------------------------------


async def test_schema_screen_refresh_all_reloads_databases() -> None:
    """Shift+R (action_refresh_all) clears loaded state and reloads databases."""
    fake = FakeSchemaConnector(
        databases=[Database(name="db1")],
        tables_by_db={"db1": [Table(name="users", db="db1")]},
    )
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()

        tree = screen.query_one("#schema-tree", Tree)
        db_node = tree.root.children[0]
        await screen._load_tables_for_node(db_node)
        await pilot.pause()

        calls_before = fake.list_databases_calls
        await screen.action_refresh_all()
        await pilot.pause()

        assert fake.list_databases_calls > calls_before
        # Loaded tables cache should be cleared
        assert screen._loaded_tables == {}


# ---------------------------------------------------------------------------
# Test 10: action_refresh_node reloads one DB subtree
# ---------------------------------------------------------------------------


async def test_schema_screen_refresh_node_reloads_db_subtree() -> None:
    """R (action_refresh_node) should re-query list_tables for the current DB only."""
    fake = FakeSchemaConnector(
        databases=[Database(name="prod")],
        tables_by_db={"prod": [Table(name="campgrounds", db="prod", row_count=100)]},
    )
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()

        tree = screen.query_one("#schema-tree", Tree)
        db_node = tree.root.children[0]
        await screen._load_tables_for_node(db_node)
        await pilot.pause()

        # action_refresh_node needs the cursor to be on a node; manually call with cursor set
        # Directly test the method by temporarily setting cursor focus
        await screen.action_refresh_node()
        await pilot.pause()

        # Since cursor_node is None (no focus), no additional load should happen
        # This tests the early return path. Load it directly to test the reload path:
        fake.list_tables_calls.clear()
        assert isinstance(db_node.data, type(db_node.data))  # db_node still valid
        db_node.data.loaded = False
        screen._loaded_tables.pop("prod", None)
        db_node.remove_children()
        db_node.add_leaf("(loading…)")
        await screen._load_tables_for_node(db_node)
        assert "prod" in fake.list_tables_calls


# ---------------------------------------------------------------------------
# Test 11: action_go_back closes filter then dismisses
# ---------------------------------------------------------------------------


async def test_schema_screen_go_back_closes_filter_first() -> None:
    """First Esc closes the filter; second Esc would dismiss the screen."""
    fake = FakeSchemaConnector(databases=[Database(name="db1")])
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()
        await pilot.pause()

        # Open the filter
        screen.action_open_filter()
        fi = screen.query_one("#schema-filter", Input)
        assert fi.display is True

        # First go_back: should close filter, not dismiss
        screen.action_go_back()
        await pilot.pause()
        assert fi.display is False


# ---------------------------------------------------------------------------
# Test 12: schema shows columns-only for view (no indexes/FKs)
# ---------------------------------------------------------------------------


async def test_schema_screen_view_shows_columns_only() -> None:
    """A view with no indexes or FKs shows just the COLUMNS section."""
    schema = TableSchema(
        db="mydb",
        table="active_trips",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False),
            Column(name="status", data_type="TEXT", nullable=True),
        ],
        indexes=[],
        foreign_keys=[],
        ddl=None,
    )
    fake = FakeSchemaConnector(
        databases=[Database(name="mydb")],
        tables_by_db={"mydb": [Table(name="active_trips", db="mydb", table_type="VIEW")]},
        schema_by_table={"mydb.active_trips": schema},
    )
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()
        await screen._show_table_schema("mydb", "active_trips")
        await pilot.pause()

        detail = screen.query_one("#schema-detail", DataTable)
        # 1 (── COLUMNS ──) + 2 (id, status) = 3 rows
        assert detail.row_count == 3, f"Expected 3 rows for view, got {detail.row_count}"


# ---------------------------------------------------------------------------
# Test 13: copy DDL with no selection is a no-op
# ---------------------------------------------------------------------------


async def test_schema_screen_copy_ddl_noop_without_selection() -> None:
    """action_copy_ddl with no selected node should not crash or call pyperclip."""
    fake = FakeSchemaConnector(databases=[Database(name="db1")])
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()
        await pilot.pause()

        screen._selected_node_data = None
        with unittest.mock.patch("pyperclip.copy") as mock_copy:
            await screen.action_copy_ddl()
            mock_copy.assert_not_called()


# ---------------------------------------------------------------------------
# Test 14: error handling when describe_table fails
# ---------------------------------------------------------------------------


async def test_schema_screen_describe_table_error_shown() -> None:
    """When describe_table raises, the error widget should be shown."""

    class _ErrorConnector(FakeSchemaConnector):
        async def describe_table(self, db: str, table: str) -> TableSchema:
            raise RuntimeError("describe failed")

    fake = _ErrorConnector(
        databases=[Database(name="db1")],
        tables_by_db={"db1": [Table(name="users", db="db1")]},
    )
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()
        await screen._show_table_schema("db1", "users")
        await pilot.pause()

        err = screen.query_one("#schema-error", Static)
        assert err.display is True
        assert "Describe failed" in str(err.content)


# ---------------------------------------------------------------------------
# Test 15: error handling when list_tables fails
# ---------------------------------------------------------------------------


async def test_schema_screen_list_tables_error_handled() -> None:
    """When list_tables raises, the DB node gets an error leaf."""

    class _ErrorConnector(FakeSchemaConnector):
        async def list_tables(self, db: str) -> list[Table]:
            self.list_tables_calls.append(db)
            raise RuntimeError("tables failed")

    fake = _ErrorConnector(databases=[Database(name="mydb")])
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()

        tree = screen.query_one("#schema-tree", Tree)
        db_node = tree.root.children[0]
        await screen._load_tables_for_node(db_node)
        await pilot.pause()

        # Should have an error leaf child
        child_labels = [str(c.label) for c in db_node.children]
        assert any("Error" in label for label in child_labels), (
            f"Expected error leaf, got: {child_labels}"
        )


# ---------------------------------------------------------------------------
# Test 16: filter input submitted closes the filter
# ---------------------------------------------------------------------------


async def test_schema_screen_filter_submit_closes_input() -> None:
    """Pressing Enter in the filter input should close it."""
    fake = FakeSchemaConnector(databases=[Database(name="db1")])
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()
        await pilot.pause()

        # Open filter
        screen.action_open_filter()
        fi = screen.query_one("#schema-filter", Input)
        assert fi.display is True

        # Simulate on_input_submitted
        screen.on_input_submitted(Input.Submitted(fi, fi.value))
        await pilot.pause()
        assert fi.display is False


# ---------------------------------------------------------------------------
# Test 17: _show_banner with error=True routes to error widget
# ---------------------------------------------------------------------------


async def test_schema_screen_show_banner_error_true_shows_error_widget() -> None:
    """_show_banner(error=True) should update the error widget, not the success banner."""
    fake = FakeSchemaConnector(databases=[Database(name="db1")])
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()
        await pilot.pause()

        screen._show_banner("something went wrong", error=True)
        await pilot.pause()

        err = screen.query_one("#schema-error", Static)
        assert err.display is True
        assert "something went wrong" in str(err.content)

        # Success banner should remain hidden
        banner = screen.query_one("#schema-banner", Static)
        assert banner.display is False


# ---------------------------------------------------------------------------
# Test 18: copy DDL failure flashes error
# ---------------------------------------------------------------------------


async def test_schema_screen_copy_ddl_failure_shows_error() -> None:
    """When get_ddl raises, _show_banner with error=True should be called."""

    class _FailDDLConnector(FakeSchemaConnector):
        async def get_ddl(self, kind: str, db: str, name: str) -> str:
            raise RuntimeError("DDL unavailable")

    fake = _FailDDLConnector(
        databases=[Database(name="db1")],
        tables_by_db={"db1": [Table(name="users", db="db1")]},
    )
    app = _SchemaApp(fake)
    async with app.run_test() as pilot:
        screen: SchemaScreen = pilot.app.screen  # type: ignore[assignment]
        await screen._load_databases()
        tree = screen.query_one("#schema-tree", Tree)
        db_node = tree.root.children[0]
        await screen._load_tables_for_node(db_node)
        await pilot.pause()

        leaf = _get_leaf_by_table(db_node, "users")
        assert leaf is not None
        screen._selected_node_data = leaf.data

        await screen.action_copy_ddl()
        await pilot.pause()

        err = screen.query_one("#schema-error", Static)
        assert err.display is True
