---
card_id: trovedb-09-proclist-live
difficulty: hard
stack: python
registered_at: 2026-04-21
---

# PROCLIST — live session table (headline view)

## Goal

Replace the `ProclistScreen` placeholder with the **real headline view**:
a live, refreshing table of server sessions sourced from
`connector.list_processes()`. This is the first view of trovedb that
delivers its operator pitch — "see what's running right now, across
Postgres / MySQL / sqlite, in one TUI".

No kill yet, no watch-mode timer yet, no locks drill-down yet. Those
are card 10 and 11. This card is strictly about rendering the table
beautifully and correctly from live connector data, with a single
manual `R` keystroke to refresh.

## Acceptance criteria

1. After a successful connect in `ConnectionPickerScreen`, the app
   pushes `ProclistScreen(profile, connector, connection)` with the
   open `Connection` object (don't reconnect — reuse what the picker
   already opened).
2. `ProclistScreen` renders a Textual `DataTable` with these columns
   and widths tuned for a typical 120-char terminal:
   `pid` (right-aligned, 6ch) · `user` (10ch) · `database` (14ch) ·
   `state` (9ch) · `runtime` (8ch, human-readable: `2.4s`, `4m 12s`,
   `1h 03m`) · `wait_event` (14ch, blank for None) · `query` (remainder,
   truncated with ellipsis — full text visible on selection in a
   footer panel).
3. A one-line **footer detail panel** below the table shows the full
   (untruncated) query for the currently-highlighted row, with
   `RuntimeError`-safe line wrapping. If the row has no query, show
   `(idle)`.
4. Top status bar shows `trovedb — {profile.name} · {host or url} ·
   {driver} · {N} sessions · last refresh: {HH:MM:SS}`. Title only,
   no extra widgets above the table.
5. Hint bar (dock bottom, one line) shows the bindings visible on this
   screen: `R: refresh  /: filter  Esc: back  q: quit`. Watch-mode /
   kill hints come in card 10; do not show them yet.
6. `R` (or `F5`) calls `connector.list_processes()` and re-renders the
   table. The refresh must feel instant on a healthy LAN connection
   (sub-100ms for the render portion; the DB round-trip dominates).
7. `/` opens an inline filter prompt. As the user types, filter rows
   client-side by substring match (case-insensitive) across `user`,
   `database`, `state`, and `query`. `Esc` or `Enter` closes the
   prompt; the filter persists until cleared.
8. Handle the **empty-result** case: if `list_processes()` returns
   zero rows, show centered muted text `No active sessions. Press R
   to refresh.` — don't render an empty DataTable.
9. Handle the **disconnect / server-gone** case: if the refresh raises
   a connection error, show an error banner at the top of the screen
   `Connection lost: {message}. Press R to retry, Esc to go back.`
   Do not crash the app; do not close the Connection object.
10. `Process.runtime_seconds` may be `None` for idle sessions — render
    as `—` (em dash) in that case, not `0s`.
11. **Do not** block the event loop on the DB round-trip. Use
    `asyncio.create_task` or `run_worker`; show a subtle spinner in
    the status bar (`... refreshing`) during a refresh.
12. Tests use Textual's pilot harness + a fake in-memory connector:
    - `test_proclist_renders_rows_from_connector` — feed 3 fake
      processes, assert table has 3 rows with correct pid/user/query
      truncation.
    - `test_proclist_empty_state` — connector returns `[]`, assert
      centered "No active sessions" message visible, no table.
    - `test_proclist_detail_footer_shows_full_query_on_selection` —
      feed a row with a 500-char query; assert truncation in the
      table cell AND full text in the footer panel for the selected
      row.
    - `test_proclist_refresh_key_calls_list_processes` — press `R`,
      assert `list_processes()` was awaited a second time.
    - `test_proclist_filter_narrows_rows` — feed 5 rows, press `/`,
      type `app_rw`, assert only rows where `user == 'app_rw'`
      remain visible.
    - `test_proclist_handles_connection_error` — fake connector
      raises on second `list_processes()` call; press `R`; assert
      error banner visible, table retains previously-rendered rows,
      app did not crash.
    - `test_proclist_runtime_formatting` — parametrized over
      `(seconds, expected)`: `(None, "—")`, `(0.4, "0.4s")`,
      `(2.4, "2.4s")`, `(61, "1m 01s")`, `(4*60+12, "4m 12s")`,
      `(3720, "1h 02m")`.
    - `test_proclist_idle_rows_render_dash_for_runtime_and_paren_idle_for_query`.
13. Coverage of the new `screens/proclist.py` (and any helper module
    it pulls in, e.g. `widgets/proclist_table.py`) ≥ 85%.

## Notes

- Keep the placeholder's `Esc: back` binding — on back, the picker
  reappears and the old connection is closed cleanly.
- Don't introduce a timer yet — watch-mode is card 10. Manual `R`
  only. This keeps card scope honest.
- Row highlighting follows Textual defaults (blue); don't override.
- Query truncation: use `…` (single char) not `...` (three dots) —
  saves precious columns.
- If the connector returns `blocked_by` on a row, add a leading `▶`
  marker in a zero-width "gutter" column so blocked rows stand out
  visually — but don't build the locks drill-down here, that's card 11.
- Consider factoring the row-to-cell formatting into a small pure
  function so card 11's locks view can reuse the runtime / query
  formatting helpers.

Registered before execution. Not edited after running.
