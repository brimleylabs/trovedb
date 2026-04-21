---
card_id: trovedb-10-watch-mode-kill
difficulty: hard
stack: python
registered_at: 2026-04-21
---

# PROCLIST — watch mode + kill + EXPLAIN

## Goal

Turn the static PROCLIST from card 9 into a **live operator console**:
auto-refresh on a timer, kill the highlighted session (with a safety
confirm), run EXPLAIN on its query. This is the keystroke-driven kill
loop that `k9s` makes feel effortless.

## Acceptance criteria

1. **Watch mode (default ON).** On screen mount, start a timer that
   calls `connector.list_processes()` every 2 seconds. The status bar
   shows `⏱ watch 2s` when active; `⏸ paused` when off. `W` toggles.
2. **Configurable interval.** `2` / `5` / `10` / `30` keys set the
   watch interval to 2s / 5s / 10s / 30s respectively; selection
   persists for the session.
3. **Manual refresh still works.** `R` / `F5` triggers an immediate
   refresh regardless of watch state, without disturbing the timer.
4. **Row identity on refresh.** When rows are replaced, preserve the
   cursor on the same `pid` if it's still present; if it's gone, snap
   to the nearest-by-index row. No flicker, no cursor jumping to row 0.
5. **`K` — kill.** On the highlighted row, open a small centered
   modal: `Kill PID {pid}?  [C]ancel  [y]: cancel query  [Y]: terminate session`.
   Lowercase `y` calls `connector.kill_process(pid, force=False)`
   (cancel only); uppercase `Y` calls `kill_process(pid, force=True)`
   (terminate). `C` or `Esc` dismisses.
6. **Post-kill feedback.** After a successful kill, flash a one-line
   success banner (`Cancelled query on pid {pid}` or `Terminated
   session {pid}`) that auto-dismisses after 3s. Trigger an immediate
   refresh so the change is visible.
7. **Kill errors are handled gracefully.** `OperationalError`
   ("permission denied", "pid not found") → red error banner, no
   crash, no refresh.
8. **`E` — EXPLAIN.** On the highlighted row with a non-empty query,
   run `connector.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql)` for
   Postgres, `EXPLAIN FORMAT=TREE {sql}` for MySQL, `EXPLAIN QUERY PLAN
   {sql}` for SQLite. Show the output in a scrollable modal
   (`ModalScreen`). `Esc` closes. Don't run EXPLAIN on idle rows —
   disable the key visually and show a hint.
9. **`C` — copy SQL.** On the highlighted row, copy the full (untruncated)
   query text to the system clipboard via `pyperclip`. Flash a
   one-line success banner.
10. **Kill confirmation guardrail.** `Y` (force) must require double-tap
    within 2s — a single `Y` press shows the modal; only the second
    confirms. Cancel query (`y`) works on first press. This matches
    `k9s` single-vs-double-tap semantics.
11. **Safety rail.** Never allow `K` on the current session's own
    backend pid (Postgres: `pg_backend_pid()`, MySQL: `CONNECTION_ID()`).
    Show `Cannot kill the trovedb session itself` banner.
12. **Hint bar** now reads:
    `W: watch  R: refresh  K: kill  E: explain  C: copy  /: filter  Esc: back  q: quit`
13. Tests (pilot harness + fake connector):
    - `test_watch_mode_default_on_calls_list_processes_repeatedly` — assert ≥2 refreshes within 2.5s with a fake 1s interval.
    - `test_watch_mode_toggle_stops_refresh` — press `W`; sleep 2.5s; assert `list_processes` was not called again.
    - `test_interval_key_changes_refresh_rate` — press `5`; assert status-bar shows `watch 5s`.
    - `test_cursor_persists_on_refresh_by_pid`.
    - `test_cursor_snaps_when_selected_pid_disappears`.
    - `test_kill_lowercase_y_cancels_query` — highlight row, press `K` then `y`; assert `kill_process(pid=X, force=False)` called.
    - `test_kill_uppercase_y_requires_double_tap` — single `Y` does not call `kill_process`; second `Y` within 2s does, with `force=True`.
    - `test_kill_cancel_dismisses_modal_without_calling`.
    - `test_kill_permission_denied_renders_error_banner_no_crash`.
    - `test_explain_runs_connector_execute_with_driver_specific_prefix` — parametrize over postgres/mysql/sqlite; assert the executed SQL starts with the right `EXPLAIN ...` prefix.
    - `test_explain_disabled_on_idle_rows`.
    - `test_copy_sql_calls_pyperclip_with_full_query_text`.
    - `test_kill_self_session_is_blocked`.
14. Coverage of `screens/proclist.py` + any new helpers ≥ 85%.

## Notes

- Use Textual's `set_interval` for the watch timer; stash the handle
  so `W` can pause/resume.
- EXPLAIN modal: reuse Textual's built-in `ModalScreen` + `TextArea`
  in read-only mode for scrolling.
- Double-tap: keep a small `datetime` of the last `Y` keypress on the
  screen; compare on each press; reset on any other key.
- `pyperclip` is cross-platform and already a common TUI dep; add it
  to `pyproject.toml` as a runtime dep.
- Kill-self guardrail: query the backend pid once at connect time,
  stash on the `Connection` object, so the screen can compare without
  an extra round-trip per keypress.

Registered before execution. Not edited after running.
