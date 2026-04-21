<p align="center">
  <strong>trovedb</strong>
</p>

<p align="center">
  <em>A live operator console for SQL databases.</em><br>
  Watch what's actually happening — sessions, locks, replication, slow queries —
  across Postgres / MySQL / SQLite from one terminal UI.
</p>

<p align="center">
  Inspired by <strong>k9s for Kubernetes</strong>: not an editor, an operator console.
</p>

---

## What it is

`trovedb` is a keyboard-driven full-screen TUI that focuses on what's
hard to see today: the **live state of a running database**.

Sessions in flight. Locks and blocking chains. Replica lag. Slow queries.
One keystroke to kill, watch, diff, inspect. Inspired by `k9s` for
Kubernetes — k9s isn't a YAML editor, it's a live-state console.
trovedb is the same idea for databases.

## What it isn't

`trovedb` is **not another SQL IDE**. The query-editor space is owned by
[harlequin](https://harlequin.sh) — full-screen TUI, tabbed buffers,
schema-aware autocomplete, plugin ecosystem. If you want the IDE, use
harlequin. They're complementary: harlequin is the SQL IDE, trovedb is
the `top` / `htop`.

trovedb does ship a single-tab query view — operators need to run ad-hoc
SQL — but it's a utility, not the headline feature.

## Headline view: PROCLIST

```
┌─ trovedb ── prod-pg @ db.internal:5432 (15ms) ── ⏱ watch 2s ── ⚡ 8 sessions ─┐
│ <p>roclist  <l>ocks  <r>eplication  <s>chema  <q>uery  <d>iff   ?: help     │
├──────────────────────────────────────────────────────────────────────────────┤
│ pid    user      db        state    waited  query                            │
│ ▶ 1247 app_rw    cc_prod   active   2.4s    SELECT * FROM trips WHERE ...   │
│   1248 app_ro    cc_prod   idle     —       (last) SELECT 1                 │
│ ▶ 1249 app_rw    cc_prod   active   12.1s   UPDATE waypoints SET stop_typ.. │
│   1250 backup    cc_prod   active   4m 12s  COPY (SELECT * FROM users) TO   │
│ ▶ 1251 app_rw    cc_prod   waiting  0.3s    DELETE FROM trips WHERE id IN.. │
├──────────────────────────────────────────────────────────────────────────────┤
│ K: kill  E: explain  T: trace blockers  C: copy SQL  W: watch  /: filter    │
└──────────────────────────────────────────────────────────────────────────────┘
```

Auto-refresh every 2s. `K` kills. `T` shows the blocking chain. `E`
runs `EXPLAIN (ANALYZE, BUFFERS)` on the selected query.

## Built by Winston

Every commit in this repo is the output of a [Winston](https://github.com/brimleylabs/winston)
card execution. The cards (specifications) are pre-registered in
[`cards/`](cards/) before any agent run. Each PR description names the
card and links to the Winston run that produced it. The full build is
auditable, end to end. This is the project that ships if Winston works.

## Install

```bash
pip install trovedb
```

Or from source (development):

```bash
git clone https://github.com/brimleylabs/trovedb
cd trovedb
pip install -e ".[dev]"
```

## Usage

```bash
# Show version
trovedb --version

# Connect to a named connection from ~/.config/trovedb/connections.toml
trovedb connect prod-pg

# Show all options
trovedb --help
```

## Status

Pre-MVP. Card 1 (project skeleton) complete. See [`ARCHITECTURE.md`](ARCHITECTURE.md) for locked
design decisions and [`cards/`](cards/) for the full build backlog.

## License

MIT.
