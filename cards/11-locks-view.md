---
card_id: trovedb-11-locks-view
difficulty: hard
stack: python
registered_at: 2026-04-21
---

# `:locks` — blocking chain view

## Goal

Give the operator a single screen that answers "who is blocking whom
right now?" across Postgres (`pg_locks` + `pg_stat_activity`) and
MySQL (`INFORMATION_SCHEMA.INNODB_TRX` + `INNODB_LOCK_WAITS` on 5.7 /
`performance_schema.data_lock_waits` on 8.0+). SQLite has no true
blocking beyond a single writer — show a one-line informational state.

This is the view that earns trovedb its "incident tool" reputation.

## Acceptance criteria

1. Extend the `Connector` Protocol with:
   ```python
   async def list_blocking_chains(self) -> list[BlockingChain]: ...
   ```
   where `BlockingChain` is a new domain type in `trovedb/connectors/types.py`:
   ```python
   @dataclass(frozen=True, slots=True)
   class BlockingChain:
       waiter_pid: int
       waiter_user: str
       waiter_query: str
       holder_pid: int
       holder_user: str
       holder_query: str
       lock_type: str           # "ROW", "TABLE", "ADVISORY", ...
       object_name: str | None  # e.g. "public.trips"
       waited_seconds: float
       depth: int               # 1 for direct block, >1 for transitive
   ```
2. Implement `list_blocking_chains()` on each real connector:
   - **Postgres**: recursive CTE over `pg_locks` + `pg_blocking_pids()`
     joined to `pg_stat_activity`. Populate `depth` from the CTE.
   - **MySQL 8.0+**: join `performance_schema.data_lock_waits` +
     `performance_schema.data_locks` + `INFORMATION_SCHEMA.PROCESSLIST`.
   - **MySQL 5.7**: fall back to `INFORMATION_SCHEMA.INNODB_LOCK_WAITS`
     + `INNODB_TRX` + `PROCESSLIST`. Branch on `SELECT VERSION()` once
     at connect time and stash the branch choice.
   - **SQLite**: return `[]`. Screen renders informational text.
3. New screen `screens/locks.py` accessible from proclist via `:` ->
   type `locks` -> Enter, and from a hotkey `L` on proclist directly.
4. Rendering: a tree-style table, not a flat one.
   ```
   ▶ holder: pid 1247  app_rw   UPDATE trips SET ...        (4.2s)
     ├─ waiter: pid 1251  app_rw  DELETE FROM trips WHERE.. (0.9s)
     └─ waiter: pid 1263  reporter SELECT * FROM trips      (0.3s)
   ▶ holder: pid 1248  backup   COPY (SELECT * FROM ...)   (4m 12s)
     └─ waiter: pid 1290  app_rw  UPDATE users SET ...     (1.4s)
   ```
5. Color coding (Textual CSS):
   - holder row: yellow foreground
   - direct waiter: red foreground
   - transitive waiter (depth ≥ 2): dim red
   - blocking chain > 5s total: background flashes once on appearance
6. Same keybindings philosophy as proclist: `R` refresh, `W` watch
   (default 2s, configurable 2/5/10/30), `K` kill holder (with double-
   tap `Y` for force), `E` EXPLAIN on holder or highlighted waiter,
   `C` copy SQL, `/` filter, `Esc` back, `q` quit.
7. Empty state when no blocking: centered muted `No blocking chains —
   all clear.`
8. SQLite state: show single-line `SQLite is single-writer; use
   .proclist to see the active writer.`
9. Tests:
   - `test_postgres_list_blocking_chains_detects_direct_block` —
     start session A holding row lock; session B waits; call from
     session C; assert one `BlockingChain` with matching pids and
     `depth=1`. Uses the env-var Postgres (skip if unset — same
     pattern as card 6 v2).
   - `test_postgres_list_blocking_chains_detects_transitive_depth_2` —
     three-session chain.
   - `test_mysql_list_blocking_chains_8_0_path` — version-branch path
     for 8.0+. Skip if unset or wrong version.
   - `test_mysql_list_blocking_chains_5_7_fallback_path` — skip if
     version not 5.7.x. Use `monkeypatch` on the version detection
     to force the 5.7 branch on any MySQL — that's enough for
     coverage without requiring a real 5.7 server.
   - `test_sqlite_list_blocking_chains_returns_empty`.
   - Screen tests (pilot + fake connector with three canned chains):
     · `test_locks_screen_renders_holder_waiter_tree`
     · `test_locks_screen_color_codes_by_role_and_depth`
     · `test_locks_screen_empty_state`
     · `test_locks_screen_kill_holder_calls_connector_kill`
     · `test_locks_screen_watch_interval_change`
10. Coverage of `screens/locks.py` + the per-connector `list_blocking_chains`
    ≥ 85%.

## Notes

- Postgres's `pg_blocking_pids()` returns an array; expand with
  `unnest` and join back to `pg_stat_activity` — that's the clean way
  to get `depth`.
- MySQL 5.7 lock views are deprecated but still present; guard with
  `if version < 8` at branch-selection time.
- The Textual `Tree` widget is the natural fit; if it proves too
  constraining, a plain `DataTable` with indented text + explicit
  depth column is acceptable.
- Don't fetch blocking chains twice per refresh — memoize per
  refresh tick if the locks view shares a refresh cycle with proclist.
- Factor out the `fmt_runtime` / `fmt_query_truncated` helpers from
  card 9 into `widgets/_format.py`; both screens use them.

Registered before execution. Not edited after running.
