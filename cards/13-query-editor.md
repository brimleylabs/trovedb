---
card_id: trovedb-13-query-editor
difficulty: moderate
stack: python
registered_at: 2026-04-21
---

# `:query` — single-tab SQL editor

## Goal

A utility SQL editor for the operator queries that don't fit the
fixed views: `SELECT pg_terminate_backend(123)`, `SHOW SLAVE STATUS`,
ad-hoc `SELECT ... WHERE id IN (...)` to inspect rows referenced by a
proclist entry.

Deliberately not a harlequin replacement — single tab, no
autocomplete, no schema-aware linting. Run → result → next query.

## Acceptance criteria

1. New screen `screens/query.py`, accessible via `:query` and hotkey
   `Q`.
2. Top half: a Textual `TextArea` (SQL syntax highlighting on),
   editable, multiline, starts empty on first entry.
3. Bottom half: a `DataTable` result grid, populated from the last
   successful `connector.execute(sql)` call. Shows the first 1000
   rows; if truncated, a hint row at the top says `Showing first
   1000 of {total} — raise with :set result_limit N`.
4. `F5` / `Ctrl+Enter`: execute the query. During execution, show a
   spinner in the result area; disable `F5` so double-press doesn't
   queue two runs.
5. Read-only by default: the editor runs `execute(sql, dangerous=False)`.
   If the query is non-SELECT, show a confirm modal
   `This looks like a write query. Run anyway? [y/N]`. `y` re-runs
   with `dangerous=True`. (Single tap; no double-tap here — the
   modal itself is the guardrail.)
6. Error display: on error, replace the result grid with a red error
   panel showing `ERROR: {message}` plus the offending SQL line (if
   the driver returns a position).
7. **Query history** persisted to `~/.local/share/trovedb/history.db`
   (sqlite, created on first run). Schema:
   ```sql
   CREATE TABLE IF NOT EXISTS history (
       id INTEGER PRIMARY KEY,
       profile TEXT NOT NULL,
       sql TEXT NOT NULL,
       ran_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       duration_ms INTEGER,
       error TEXT  -- null on success
   );
   ```
   Every execute — success or error — appends a row.
8. `Ctrl+↑` / `Ctrl+↓`: walk history for the current profile, oldest
   to newest. Loads into the editor, replacing current contents.
   `Ctrl+R`: opens an inline history search (last 100 entries, most
   recent first, `/`-style filter).
9. `Ctrl+S`: save current query text to
   `~/.local/share/trovedb/queries/{profile}-{YYYYMMDD-HHMMSS}.sql`.
   Flash banner with the saved filename.
10. `Ctrl+L`: clear the editor. `Ctrl+Shift+L`: clear both editor
    and result grid.
11. Copy support on the result grid: `C` copies the highlighted
    cell's value; `Shift+C` copies the highlighted row as TSV.
12. Hint bar: `F5: run  Ctrl+R: history  Ctrl+S: save  Ctrl+L: clear  Esc: back  q: quit`.
13. Tests (pilot + fake connector + tmp history DB):
    - `test_query_screen_runs_select_on_f5`
    - `test_query_screen_renders_results_in_datatable`
    - `test_query_screen_shows_truncation_hint_when_over_limit`
    - `test_query_screen_write_query_triggers_confirm_modal`
    - `test_query_screen_dangerous_true_passed_after_confirm`
    - `test_query_screen_renders_error_panel_on_execute_failure`
    - `test_query_history_writes_row_on_success`
    - `test_query_history_writes_row_on_error_with_error_text`
    - `test_query_history_ctrl_up_loads_previous_entry`
    - `test_query_save_writes_file_with_timestamped_name`
    - `test_query_clear_resets_editor`
14. Coverage of `screens/query.py` + `query_history.py` ≥ 85%.

## Notes

- Textual's `TextArea` supports `language="sql"` for basic highlighting
  — that's enough; no tree-sitter needed.
- History DB lives at `platformdirs.user_data_dir("trovedb")` — this
  is the `~/.local/share/trovedb/` path on Linux, roamed AppData on
  Windows. `trovedb/data.py` should expose `get_history_db_path()`
  for reuse.
- "Looks like a write" heuristic: if the first SQL keyword
  (case-insensitive, after stripping leading comments and whitespace)
  is NOT in `{SELECT, WITH, SHOW, EXPLAIN, DESCRIBE, DESC, PRAGMA,
  VALUES, TABLE}` — treat as write.
- Don't reimplement SQL parsing — naive keyword-prefix is enough and
  matches what pgcli/mycli do.
- The result grid is already factored in proclist/schema; lift the
  shared renderer into `widgets/result_grid.py` if it isn't already.

Registered before execution. Not edited after running.
