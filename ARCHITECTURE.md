# trovedb — architecture

> **A live operator console for SQL databases.** Watch what's actually
> happening — sessions, locks, replication, slow queries — across
> Postgres / MySQL / SQLite from one terminal UI. Kill, diff, and
> inspect as easily as you query.
>
> Inspired by **k9s for Kubernetes**: k9s isn't a YAML editor, it's a
> live-state operator console. trovedb is the same idea for databases.
>
> Built autonomously by the [Winston](https://github.com/brimleylabs/winston)
> coding agent under human direction — every commit is a Winston card
> execution, auditable in the project's `cards/` folder.

## What we're building (and what we're explicitly NOT)

**We are building** a TUI that focuses on what's hard to see today:
the live state of a running database. Sessions, locks, replication,
slow queries, blocking chains. One keystroke to kill, watch, diff.

**We are NOT building** "another SQL IDE with tabs and autocomplete."
That space is owned by [harlequin](https://harlequin.sh) (Textual-based,
6k stars, very good). Position trovedb as **complementary**: harlequin
is the SQL IDE, trovedb is the `top` / `htop` for your database.

A query editor still exists in trovedb — operators need to run ad-hoc
SQL — but it's a single-tab utility view, not the headline feature.
Anyone who wants the IDE experience should use harlequin.

### The trovedb headline view: PROCLIST

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
│   1252 reporter  cc_prod   active   31s     SELECT campgrounds.name, COUN.. │
│                                                                              │
├──────────────────────────────────────────────────────────────────────────────┤
│ K: kill  E: explain  T: trace blockers  C: copy SQL  W: watch  /: filter    │
└──────────────────────────────────────────────────────────────────────────────┘
```

Live updates every 2s by default. K kills a runaway. T shows the
blocking chain (who's waiting on whom). E runs `EXPLAIN (ANALYZE, BUFFERS)`
on the selected query.

### Other operator-first views

- **`:locks`** — `pg_locks` / InnoDB lock waits. Color-coded blocking chains.
- **`:replication`** — replica lag, WAL position, sync status. Watch mode.
- **`:diff`** — schema diff between two connections. CREATE/ALTER/DROP plan.
- **`:slow`** — slow-query log digest, sorted by total time / call count.
- **`:schema`** — schema browser (tables, views, indexes, FKs). For
  navigation, not editing.
- **`:query`** — single-tab SQL editor. F5 to run. For ad-hoc operator
  queries (`SELECT pg_terminate_backend(123)`, `SHOW SLAVE STATUS`, etc.).

Vim-style ergonomics throughout: `/` filter, `:` jump, `j/k` move, `g/G`
top/bottom, `Esc` back. No mouse needed; mouse works if you want it.

## Locked architectural decisions

### Async throughout

- **TUI:** Textual (async-native).
- **DB drivers:** psycopg (>=3, async), aiomysql, aiosqlite. One bundled
  per supported DB. Adding a new DB later = one new connector class +
  one driver dependency.
- **No SQLAlchemy.** Direct driver use keeps performance-sensitive paths
  fast and avoids forcing every contributor to learn an extra layer.
  Schema introspection is done through the per-driver `information_schema`
  / `pg_catalog` queries, abstracted by the `Connector` interface.

### Pluggable connector contract

Each DB ships as a `Connector` implementation living in
`trovedb/connectors/<dbname>.py`. The contract:

```python
class Connector(Protocol):
    name: str                                 # "postgres", "mysql", "sqlite"
    default_port: int | None
    async def connect(self, dsn: ConnectionProfile) -> Connection: ...
    async def list_databases(self) -> list[Database]: ...
    async def list_tables(self, db: str) -> list[Table]: ...
    async def describe_table(self, db: str, table: str) -> TableSchema: ...
    async def execute(self, sql: str, params: dict | None = None) -> ResultSet: ...
    async def list_processes(self) -> list[Process]: ...
    async def kill_process(self, pid: int) -> None: ...
    async def get_ddl(self, kind: str, db: str, name: str) -> str: ...
```

MVP ships **postgres**, **mysql**, **sqlite**. Extension via this contract;
no core changes needed for snowflake/clickhouse/duckdb later.

### Connection profiles in TOML

```
~/.config/trovedb/connections.toml
~/.config/trovedb/keybindings.toml      (overrides; optional)
~/.config/trovedb/theme.toml            (overrides; optional)
~/.local/share/trovedb/history.db       (sqlite — query history)
```

Connection profile shape:

```toml
[connections.prod-mysql]
driver = "mysql"
host = "db.internal"
port = 3306
user = "readonly"
database = "campcommand"
password_env = "TROVEDB_PROD_MYSQL_PASSWORD"  # never plaintext password in toml

[connections.local-pg]
driver = "postgres"
url = "postgresql://postgres:postgres@localhost/dev"
```

Two ways to specify creds: discrete fields + env-var indirection, OR a
DSN URL. Plaintext passwords in the file are flagged on startup with a
warning.

### Theme + keybindings hot-reloadable

Edit the TOML, save, see the change. No restart. (Implemented via a
filesystem watcher on the config dir.)

### Single binary — `pip install trovedb`

Bundled connectors for postgres / mysql / sqlite. Other DBs lazy-loaded
when the connector is first invoked, with a clear "install `trovedb[snowflake]`"
message if missing.

### Tests

- `pytest` + `pytest-asyncio`
- Connector tests use disposable Docker containers via `testcontainers-python`
  (postgres + mysql); sqlite tests use temp files.
- TUI tests use Textual's built-in `pilot` testing harness.
- Goal: a card cannot land green unless its tests cover every new
  Connector method or widget added.

### Telemetry

Zero by default. If we add opt-in telemetry later (anonymous usage stats
to inform feature priorities), it's behind a config flag, off by default,
with a one-time prompt on first run.

## What's deliberately NOT in MVP

- Multi-account / team profile sync (Pro feature, maybe never)
- Plugin system (v2)
- Cloud-hosted variant (no thanks)
- AI-assisted query writing (later, after the basics are tight)

## Project layout

```
trovedb/
├── pyproject.toml                  Hatch + ruff + pytest config
├── README.md
├── ARCHITECTURE.md                 (this file)
├── trovedb/
│   ├── __init__.py
│   ├── cli.py                      Typer entrypoint
│   ├── app.py                      Textual App subclass
│   ├── config.py                   Pydantic models + TOML loader
│   ├── connectors/
│   │   ├── __init__.py             Connector Protocol + registry
│   │   ├── postgres.py
│   │   ├── mysql.py
│   │   └── sqlite.py
│   ├── widgets/
│   │   ├── connection_picker.py
│   │   ├── schema_tree.py
│   │   ├── table_inspector.py
│   │   ├── query_editor.py
│   │   ├── result_grid.py
│   │   ├── proclist.py
│   │   └── help_overlay.py
│   ├── screens/
│   │   ├── main.py
│   │   ├── query.py
│   │   ├── proclist.py
│   │   └── diff.py
│   └── theme/
│       └── default.tcss            Textual CSS
└── tests/
    ├── connectors/
    │   ├── test_postgres.py
    │   ├── test_mysql.py
    │   └── test_sqlite.py
    └── widgets/...
```

## Built by Winston

Most of the build was produced by autonomous [Winston](https://github.com/brimleylabs/winston)
card runs against specs pre-registered in `cards/`. Each run's full
JSONL tool-call journal is committed under
[`cards/journals/`](cards/journals/) and indexed in
[`cards/JOURNAL_INDEX.md`](cards/JOURNAL_INDEX.md) (card → spec →
journal → merge commit).

Cards were merged directly to `main` from the agent workspace (no PRs);
the commit log is the primary audit trail. The `JOURNAL_INDEX` also
enumerates the handful of human hot-fix commits that are *not* card
runs and what each one addressed — keeping the autonomous /
human-intervention line honest.

## License

MIT. Same as Winston.
