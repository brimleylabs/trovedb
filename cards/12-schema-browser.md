---
card_id: trovedb-12-schema-browser
difficulty: moderate
stack: python
registered_at: 2026-04-21
---

# `:schema` — read-only schema browser

## Goal

A tree-navigable schema browser keyed off `list_databases()` /
`list_tables()` / `describe_table()` — all already implemented on each
connector. Operators need this for "is that column even an integer?"
reality checks during an incident, without alt-tabbing to DBeaver.

Read-only. Editing DDL is not in scope.

## Acceptance criteria

1. New screen `screens/schema.py`, accessible from proclist / locks
   via `:` -> type `schema` -> Enter, and from hotkey `S`.
2. Left pane: Textual `Tree` widget populated from:
   ```
   {database}
    ├─ tables (N)
    │   ├─ {table_name}
    │   ...
    ├─ views (N)
    │   └─ {view_name}
    └─ indexes (N) — optional, collapsed by default
   ```
   Use `list_databases()` for the top level, `list_tables(db)` for
   the middle level. Lazy-load: don't query all tables until a
   `database` node is expanded.
3. Right pane: a `DataTable` that renders the current selection's
   `TableSchema`:
   - For a table: columns (name, type, nullable, default, PK),
     followed by indexes (name, columns, unique?), followed by FKs
     (name, local cols → ref table/cols).
   - For a view: columns only, plus the view definition in a
     collapsible text panel.
4. `Enter` on a table/view node: switches focus to the right pane.
   `Esc` returns focus to the tree.
5. `/` on the tree filters nodes (case-insensitive substring match
   on name); matching nodes auto-expand their ancestors.
6. `D` on a table/view copies the result of `get_ddl("table", db,
   name)` (or `"view"`) to clipboard via `pyperclip`. Flash banner
   `Copied DDL for {name}`.
7. Row count hint: when a table node is rendered, show the cheap
   estimate (Postgres: `pg_class.reltuples`; MySQL:
   `information_schema.tables.table_rows`; SQLite: `COUNT(*)` since
   no estimate exists). Suffix the tree label: `trips (≈120k)`.
   Don't `COUNT(*)` on Postgres/MySQL — use the estimate.
8. Refresh `R` re-queries the current database node only (not the
   whole tree). Full refresh is `Shift+R`.
9. Hint bar: `Enter: select  /: filter  D: copy DDL  R: refresh  Esc: back  q: quit`.
10. Tests (pilot + fake connector with canned databases/tables/schemas):
    - `test_schema_screen_populates_top_level_databases`
    - `test_schema_screen_lazy_loads_tables_on_expand` — assert
      `list_tables('db1')` not called until `db1` node is expanded.
    - `test_schema_screen_right_pane_renders_columns_indexes_fks`
    - `test_schema_screen_filter_expands_matching_ancestors`
    - `test_schema_screen_copy_ddl_calls_pyperclip_and_flashes_banner`
    - `test_schema_screen_uses_estimate_not_count_for_postgres_row_count`
      — mock the connector; assert the estimate path was exercised
      and `SELECT COUNT(*)` was NOT run.
11. Coverage of `screens/schema.py` ≥ 85%.

## Notes

- Reuse `widgets/_format.py` from card 11 for row-count formatting
  (`≈120k`, `≈1.2M`, `≈3`).
- When a table has many columns (>50), render in pages — Textual's
  `DataTable` handles this natively with its scrollable container.
- For `Shift+R`: clear the `Tree`, rebuild from `list_databases()`.
- The fake connector used in tests should live in `tests/_fakes.py`
  (new module, shared across screen tests) — card 9 probably already
  started one; centralize it here if not.

Registered before execution. Not edited after running.
