"""SchemaScreen — read-only schema browser for trovedb.

Displays a two-pane layout:

  Left pane  — Textual Tree showing databases → tables / views (lazy-loaded)
  Right pane — DataTable showing columns, indexes, and foreign keys for the
               currently highlighted table or view.

Navigation:
  /        Open filter (substring match on table names)
  Enter    Focus the right detail pane (when on a table/view node)
  Esc      Return focus to the tree (or dismiss screen if tree is focused)
  D        Copy DDL for the selected table/view to clipboard
  R        Refresh the current database subtree only
  Shift+R  Full refresh — reload all databases
  q        Quit
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import pyperclip
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.screen import Screen
from textual.widgets import DataTable, Input, Static, Tree
from textual.widgets.tree import TreeNode

from trovedb.config import ConnectionProfile
from trovedb.connectors.types import Connection, Table, TableSchema
from trovedb.widgets._format import format_row_count, truncate

logger = logging.getLogger(__name__)

_HINT = (
    "Enter: select  /: filter  D: copy DDL  R: refresh  Shift+R: full refresh"
    "  :: query  ?: help  Esc: back  q: quit"
)


# ---------------------------------------------------------------------------
# Node data types stored in TreeNode.data
# ---------------------------------------------------------------------------


@dataclass
class _DbNode:
    """Data attached to a database-level tree node."""

    db_name: str
    loaded: bool = False


@dataclass
class _ContainerNode:
    """Data attached to a 'tables (N)' or 'views (N)' group node."""

    kind: str  # "tables" | "views"
    db_name: str


@dataclass
class _TableNode:
    """Data attached to an individual table or view leaf node."""

    db_name: str
    table_name: str
    table_type: str  # "BASE TABLE" | "VIEW"


# ---------------------------------------------------------------------------
# SchemaScreen
# ---------------------------------------------------------------------------


class SchemaScreen(Screen[None]):
    """Read-only schema browser: databases → tables/views → columns/indexes/FKs."""

    DEFAULT_CSS = """
    SchemaScreen #schema-status {
        dock: top;
        height: 1;
        background: $primary-darken-2;
        color: $text;
        padding: 0 1;
    }
    SchemaScreen #schema-banner {
        dock: top;
        height: 1;
        background: $success;
        color: $text;
        padding: 0 1;
    }
    SchemaScreen #schema-error {
        dock: top;
        height: 1;
        background: $error;
        color: $text;
        padding: 0 1;
    }
    SchemaScreen #schema-main {
        height: 1fr;
    }
    SchemaScreen #schema-tree {
        width: 35%;
        border-right: solid $primary;
    }
    SchemaScreen #schema-detail {
        width: 1fr;
    }
    SchemaScreen #schema-hint {
        dock: bottom;
        height: 1;
        background: $primary-darken-2;
        color: $text-muted;
        padding: 0 1;
    }
    SchemaScreen #schema-filter {
        dock: bottom;
        height: 3;
    }
    """

    BINDINGS = [
        Binding("r", "refresh_node", "Refresh", show=False),
        Binding("shift+r", "refresh_all", "Full Refresh", show=False),
        Binding("d", "copy_ddl", "Copy DDL", show=False),
        Binding("colon", "open_query", "Query", show=False),
        Binding("slash", "open_filter", "Filter", show=False),
        Binding("escape", "go_back", "Back", show=False),
        Binding("q", "quit", "Quit", show=False),
    ]

    def __init__(
        self,
        profile: ConnectionProfile,
        connector: Any,
        connection: Connection,
    ) -> None:
        super().__init__()
        self._profile = profile
        self._connector = connector
        self._connection = connection
        self._filter_text = ""
        # Cache: db_name → list[Table] (populated on lazy-load)
        self._loaded_tables: dict[str, list[Table]] = {}
        # Currently highlighted/selected table node (for DDL copy)
        self._selected_node_data: _TableNode | None = None
        self._banner_timer: Any = None

    # ------------------------------------------------------------------
    # Compose
    # ------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        yield Static("", id="schema-status")
        yield Static("", id="schema-banner")
        yield Static("", id="schema-error")
        with Horizontal(id="schema-main"):
            yield Tree("Databases", id="schema-tree")
            yield DataTable(id="schema-detail", zebra_stripes=True, cursor_type="row")
        yield Input(
            placeholder="Filter: type to search, Esc to close",
            id="schema-filter",
        )
        yield Static(_HINT, id="schema-hint")

    # ------------------------------------------------------------------
    # Mount
    # ------------------------------------------------------------------

    async def on_mount(self) -> None:
        """Set up the detail table columns, hide transient widgets, load databases."""
        detail = self.query_one("#schema-detail", DataTable)
        detail.add_column("name", key="name", width=24)
        detail.add_column("type / kind", key="type", width=18)
        detail.add_column("nullable", key="nullable", width=8)
        detail.add_column("default", key="default", width=18)
        detail.add_column("notes", key="notes", width=12)

        self.query_one("#schema-banner", Static).display = False
        self.query_one("#schema-error", Static).display = False
        self.query_one("#schema-filter", Input).display = False

        self._update_status("Loading databases…")
        await self._load_databases()

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    async def _load_databases(self) -> None:
        """Fetch the list of databases and populate the tree root."""
        tree = self.query_one("#schema-tree", Tree)
        tree.root.remove_children()
        try:
            databases = await self._connector.list_databases()
        except Exception as exc:
            logger.exception("list_databases failed")
            self._show_error(f"Error loading databases: {exc}")
            return

        for db in databases:
            node = tree.root.add(db.name, data=_DbNode(db_name=db.name))
            # Add a placeholder so the node is expandable before loading
            node.add_leaf("(loading…)")

        tree.root.expand()
        count = len(databases)
        self._update_status(
            f"Schema — {self._profile.name} — {count} database{'s' if count != 1 else ''}"
        )

    async def _load_tables_for_node(self, db_node: TreeNode) -> None:  # type: ignore[type-arg]
        """Lazy-load tables and views under a database node.

        No-ops if the node has already been loaded (``node.data.loaded is True``).
        """
        node_data: _DbNode = db_node.data  # type: ignore[assignment]
        if node_data.loaded:
            return

        db_name = node_data.db_name
        # Remove the placeholder "(loading…)" child
        db_node.remove_children()

        try:
            tables = await self._connector.list_tables(db_name)
        except Exception as exc:
            logger.exception("list_tables(%r) failed", db_name)
            db_node.add_leaf(f"Error: {exc}")
            return

        self._loaded_tables[db_name] = tables
        node_data.loaded = True

        self._build_db_subtree(db_node, tables, filter_text="")

    def _build_db_subtree(
        self,
        db_node: TreeNode,  # type: ignore[type-arg]
        tables: list[Table],
        *,
        filter_text: str,
    ) -> None:
        """Populate *db_node* children from *tables*, applying optional filter."""
        q = filter_text.lower()
        db_name: str = db_node.data.db_name  # type: ignore[union-attr]

        if q:
            visible_tables = [t for t in tables if q in t.name.lower()]
        else:
            visible_tables = tables

        base_tables = [t for t in visible_tables if t.table_type == "BASE TABLE"]
        views = [t for t in visible_tables if t.table_type == "VIEW"]

        # Tables container
        tables_label = f"tables ({len(base_tables)})"
        tables_node = db_node.add(
            tables_label,
            data=_ContainerNode(kind="tables", db_name=db_name),
        )
        for t in base_tables:
            rc = format_row_count(t.row_count)
            label = f"{t.name} ({rc})" if rc else t.name
            tables_node.add_leaf(
                label,
                data=_TableNode(
                    db_name=db_name,
                    table_name=t.name,
                    table_type="BASE TABLE",
                ),
            )
        tables_node.expand()

        # Views container (only if there are views)
        if views:
            views_node = db_node.add(
                f"views ({len(views)})",
                data=_ContainerNode(kind="views", db_name=db_name),
            )
            for v in views:
                views_node.add_leaf(
                    v.name,
                    data=_TableNode(
                        db_name=db_name,
                        table_name=v.name,
                        table_type="VIEW",
                    ),
                )

        # Expand the db node if there are matches (useful during filtering)
        if visible_tables and q:
            db_node.expand()

    # ------------------------------------------------------------------
    # Tree events — lazy loading and right-pane update
    # ------------------------------------------------------------------

    async def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:  # type: ignore[type-arg]
        """Trigger lazy-load when a database node is expanded for the first time."""
        node = event.node
        if isinstance(node.data, _DbNode) and not node.data.loaded:
            await self._load_tables_for_node(node)

    async def on_tree_node_highlighted(self, event: Tree.NodeHighlighted) -> None:  # type: ignore[type-arg]
        """Update the right detail pane when cursor moves to a table/view node."""
        node = event.node
        if isinstance(node.data, _TableNode):
            self._selected_node_data = node.data
            await self._show_table_schema(node.data.db_name, node.data.table_name)

    async def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:  # type: ignore[type-arg]
        """Move focus to the detail pane when Enter is pressed on a table/view."""
        node = event.node
        if isinstance(node.data, _TableNode):
            self._selected_node_data = node.data
            self.query_one("#schema-detail", DataTable).focus()

    # ------------------------------------------------------------------
    # Right-pane rendering
    # ------------------------------------------------------------------

    async def _show_table_schema(self, db: str, table: str) -> None:
        """Fetch ``describe_table`` and render columns / indexes / FKs."""
        detail = self.query_one("#schema-detail", DataTable)
        detail.clear()
        try:
            schema: TableSchema = await self._connector.describe_table(db, table)
        except Exception as exc:
            logger.exception("describe_table(%r, %r) failed", db, table)
            self._show_error(f"Describe failed: {exc}")
            return

        # Build a set of column names that are part of the PK
        pk_cols: set[str] = set()
        for idx in schema.indexes:
            if idx.primary:
                pk_cols.update(idx.columns)

        # ── COLUMNS ───────────────────────────────────────────────────
        detail.add_row(
            Text("── COLUMNS ──", style="bold"),
            Text("", style=""),
            Text("", style=""),
            Text("", style=""),
            Text("", style=""),
            key=f"_hdr_cols_{db}_{table}",
        )
        for col in schema.columns:
            notes = "PK" if col.name in pk_cols else ""
            detail.add_row(
                Text(col.name, style="bright_white"),
                Text(col.data_type, style="cyan"),
                Text("YES" if col.nullable else "NO",
                     style="yellow" if col.nullable else "green"),
                Text(truncate(col.default or "", 16), style="dim"),
                Text(notes, style="bold yellow"),
            )

        # ── INDEXES ───────────────────────────────────────────────────
        if schema.indexes:
            detail.add_row(
                Text("── INDEXES ──", style="bold"),
                Text("", style=""),
                Text("", style=""),
                Text("", style=""),
                Text("", style=""),
                key=f"_hdr_idx_{db}_{table}",
            )
            for idx in schema.indexes:
                if idx.primary:
                    kind = "PK"
                elif idx.unique:
                    kind = "UNIQUE"
                else:
                    kind = "INDEX"
                detail.add_row(
                    Text(idx.name, style="bright_white"),
                    Text(kind, style="cyan"),
                    Text(", ".join(idx.columns), style="dim"),
                    Text("", style=""),
                    Text("", style=""),
                )

        # ── FOREIGN KEYS ──────────────────────────────────────────────
        if schema.foreign_keys:
            detail.add_row(
                Text("── FOREIGN KEYS ──", style="bold"),
                Text("", style=""),
                Text("", style=""),
                Text("", style=""),
                Text("", style=""),
                key=f"_hdr_fk_{db}_{table}",
            )
            for fk in schema.foreign_keys:
                local_cols = ", ".join(fk.columns)
                ref = f"{fk.ref_table}({', '.join(fk.ref_columns)})"
                detail.add_row(
                    Text(fk.name, style="bright_white"),
                    Text(local_cols, style="cyan"),
                    Text("→", style="dim"),
                    Text(ref, style="dim"),
                    Text("", style=""),
                )

    # ------------------------------------------------------------------
    # Filter
    # ------------------------------------------------------------------

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "schema-filter":
            self._filter_text = event.value
            self._apply_filter()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "schema-filter":
            self.query_one("#schema-filter", Input).display = False
            self.query_one("#schema-tree", Tree).focus()

    def _apply_filter(self) -> None:
        """Rebuild loaded DB subtrees keeping only table/view nodes that match."""
        tree = self.query_one("#schema-tree", Tree)
        for db_node in tree.root.children:
            if not isinstance(db_node.data, _DbNode):
                continue
            db_data: _DbNode = db_node.data
            if not db_data.loaded:
                continue  # skip unloaded nodes — nothing to filter yet
            tables = self._loaded_tables.get(db_data.db_name, [])
            db_node.remove_children()
            self._build_db_subtree(db_node, tables, filter_text=self._filter_text)

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    async def action_refresh_node(self) -> None:
        """Re-load the database subtree for the currently selected DB node."""
        tree = self.query_one("#schema-tree", Tree)
        node = tree.cursor_node
        if node is None:
            return
        db_node = self._find_db_node(node)
        if db_node is None:
            return
        db_data: _DbNode = db_node.data  # type: ignore[assignment]
        db_data.loaded = False
        self._loaded_tables.pop(db_data.db_name, None)
        db_node.remove_children()
        db_node.add_leaf("(loading…)")
        await self._load_tables_for_node(db_node)

    async def action_refresh_all(self) -> None:
        """Clear the tree and reload all databases from scratch."""
        self._loaded_tables.clear()
        self._selected_node_data = None
        await self._load_databases()

    async def action_copy_ddl(self) -> None:
        """Copy the DDL for the selected table/view to the clipboard."""
        if self._selected_node_data is None:
            return
        nd = self._selected_node_data
        kind = "table" if nd.table_type == "BASE TABLE" else "view"
        try:
            ddl = await self._connector.get_ddl(kind, nd.db_name, nd.table_name)
            pyperclip.copy(ddl)
            self._show_banner(f"Copied DDL for {nd.table_name}")
        except Exception as exc:
            logger.warning("get_ddl failed: %s", exc)
            self._show_banner(f"Copy failed: {exc}", error=True)

    def action_open_filter(self) -> None:
        fi = self.query_one("#schema-filter", Input)
        fi.display = True
        fi.focus()

    def action_open_query(self) -> None:
        """Push the SQL editor QueryScreen (`:` hotkey)."""
        from trovedb.screens.query import QueryScreen  # lazy import to avoid cycles

        self.app.push_screen(
            QueryScreen(
                self._profile,
                self._connector,
                self._connection,
            )
        )

    def action_go_back(self) -> None:
        """Close filter if open; shift focus to tree if detail is focused; else dismiss."""
        fi = self.query_one("#schema-filter", Input)
        detail = self.query_one("#schema-detail", DataTable)
        if fi.display:
            fi.value = ""
            self._filter_text = ""
            fi.display = False
            self.query_one("#schema-tree", Tree).focus()
        elif detail.has_focus:
            self.query_one("#schema-tree", Tree).focus()
        else:
            self.dismiss()

    def action_quit(self) -> None:
        self.app.exit()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _find_db_node(self, node: TreeNode) -> TreeNode | None:  # type: ignore[type-arg]
        """Walk up the tree to find the nearest _DbNode ancestor (or self)."""
        current: TreeNode | None = node  # type: ignore[type-arg]
        while current is not None:
            if isinstance(current.data, _DbNode):
                return current
            current = current.parent
        return None

    def _update_status(self, msg: str) -> None:
        self.query_one("#schema-status", Static).update(msg)

    def _show_error(self, msg: str) -> None:
        err = self.query_one("#schema-error", Static)
        err.update(msg)
        err.display = True

    def _show_banner(self, msg: str, *, error: bool = False) -> None:
        if error:
            self._show_error(msg)
            return
        banner = self.query_one("#schema-banner", Static)
        banner.update(msg)
        banner.display = True
        if self._banner_timer is not None:
            self._banner_timer.stop()
        self._banner_timer = self.set_timer(3.0, self._hide_banner)

    def _hide_banner(self) -> None:
        self.query_one("#schema-banner", Static).display = False
