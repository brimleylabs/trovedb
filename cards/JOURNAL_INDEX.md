# Card journals

Every card spec in `cards/*.md` was executed by the [Winston](https://github.com/brimleylabs/winston)
coding agent. Each run wrote a JSONL journal of every tool call (Read,
Bash, Edit, Write, Skill, Agent, MCP call) and every session event
(`session_start` / `pre_tool` / `post_tool` / `session_end`). Those
journals are committed below so the full build is auditable — not just
the code that shipped, but the reasoning and tool use behind it.

| Card | Spec | Journal | Merge commit | Events | Tool calls |
|------|------|---------|--------------|-------:|-----------:|
| 1 | [01-bootstrap-pyproject.md](01-bootstrap-pyproject.md) | [01-bootstrap-pyproject.jsonl](journals/01-bootstrap-pyproject.jsonl) | [`21f96a4`](https://github.com/brimleylabs/trovedb/commit/21f96a4) | 59 | 28 |
| 2 | [02-config-loader.md](02-config-loader.md) | [02-config-loader.jsonl](journals/02-config-loader.jsonl) | [`3aa1f53`](https://github.com/brimleylabs/trovedb/commit/3aa1f53) | 65 | 32 |
| 3 | [03-connector-protocol.md](03-connector-protocol.md) | [03-connector-protocol.jsonl](journals/03-connector-protocol.jsonl) | [`efbe5b2`](https://github.com/brimleylabs/trovedb/commit/efbe5b2) | 54 | 26 |
| 4 | [04-sqlite-connector.md](04-sqlite-connector.md) | [04-sqlite-connector.jsonl](journals/04-sqlite-connector.jsonl) | [`0e8382f`](https://github.com/brimleylabs/trovedb/commit/0e8382f) | 71 | 35 |
| 5 | [05-app-shell-textual.md](05-app-shell-textual.md) | [05-app-shell-textual.jsonl](journals/05-app-shell-textual.jsonl) | [`aefaac8`](https://github.com/brimleylabs/trovedb/commit/aefaac8) | 71 | 35 |
| 6 | [06-postgres-connector-v2.md](06-postgres-connector-v2.md) | [06-postgres-connector.jsonl](journals/06-postgres-connector.jsonl) | [`55f4d1b`](https://github.com/brimleylabs/trovedb/commit/55f4d1b) | 98 | 48 |
| 7 | [07-mysql-connector-v2.md](07-mysql-connector-v2.md) | [07-mysql-connector.jsonl](journals/07-mysql-connector.jsonl) | [`5aaca80`](https://github.com/brimleylabs/trovedb/commit/5aaca80) | 134 | 66 |
| 8 | [08-connection-picker.md](08-connection-picker.md) | [08-connection-picker.jsonl](journals/08-connection-picker.jsonl) | [`83715f0`](https://github.com/brimleylabs/trovedb/commit/83715f0) | 110 | 54 |
| 9 | [09-proclist-live.md](09-proclist-live.md) | [09-proclist-live.jsonl](journals/09-proclist-live.jsonl) | [`4dde116`](https://github.com/brimleylabs/trovedb/commit/4dde116) | 140 | 69 |
| 10 | [10-watch-mode-kill.md](10-watch-mode-kill.md) | [10-watch-mode-kill.jsonl](journals/10-watch-mode-kill.jsonl) | [`80d0465`](https://github.com/brimleylabs/trovedb/commit/80d0465) | 115 | 57 |
| 11 | [11-locks-view.md](11-locks-view.md) | [11-locks-view.jsonl](journals/11-locks-view.jsonl) | [`bc4f732`](https://github.com/brimleylabs/trovedb/commit/bc4f732) | 235 | 117 |
| 12 | [12-schema-browser.md](12-schema-browser.md) | [12-schema-browser.jsonl](journals/12-schema-browser.jsonl) | [`4acec7f`](https://github.com/brimleylabs/trovedb/commit/4acec7f) | 292 | 144 |
| 13 | [13-query-editor.md](13-query-editor.md) | [13-query-editor.jsonl](journals/13-query-editor.jsonl) | [`d9f3517`](https://github.com/brimleylabs/trovedb/commit/d9f3517) | 270 | 133 |

## Not everything shipped is a Winston card

The commit log also contains **human hot-fixes** that were not themselves
card runs. They exist because the autonomous build hit edges the cards
didn't cover:

- [`c93f9bc`](https://github.com/brimleylabs/trovedb/commit/c93f9bc) — *Fix Windows psycopg event loop + eager-import connectors.* Card 9 exercised the runtime for the first time and exposed two things cards 1–8 never hit: (a) `psycopg` async refuses to run on Windows' default `ProactorEventLoop`, and (b) the connector registry stays empty unless the modules are imported somewhere.
- [`5a19b3c`](https://github.com/brimleylabs/trovedb/commit/5a19b3c) — *Restore `TroveApp(conn_name=, conn_url=)` kwargs dropped by card 9 merge.* Card 9's rewrite of `TroveApp` silently broke the `--conn NAME` CLI path because no test exercised the actual entrypoint. `PROJECT_MEMORY.md` now records this as a lesson for future screen-touching cards.
- [`edd6dfc`](https://github.com/brimleylabs/trovedb/commit/edd6dfc) and [`666f417`](https://github.com/brimleylabs/trovedb/commit/666f417) — *Fix Postgres `list_tables` / `describe_table` / `get_ddl` to open per-database scratch connections.* Card 6's Postgres connector treated the `db` parameter as a schema name (correct for one connected database), but `:schema` in card 12 passes actual Postgres database names. A `psycopg` connection is scoped to one database, so cross-database introspection needs a short-lived scratch connection per request.

These fixes are labelled as fixes, not cards — keeping the autonomous
vs human-intervention line honest. Both are part of the story of what it
took to ship trovedb autonomously.

## Card 12 needed three tries

`cards/journals/12-schema-browser.jsonl` contains **four** `session_start`
events, not one. The first three died instantly with
`FileNotFoundError: [WinError 206] The filename or extension is too long`
because `PROJECT_MEMORY.md` (appended to every system prompt) had grown
past 21 KB, pushing the full system_prompt over Windows' 32,767-char
command-line cap. Truncating `PROJECT_MEMORY.md` from 21 KB → 4.5 KB
unblocked the run. The failed attempts are kept in the journal on
purpose — they're part of the honest build log.

## Format

Each line of a `*.jsonl` file is a JSON object. Common `phase` values:

- `session_start` — builder, complexity, branch
- `pre_tool` — one per tool call, with `tool` and `input`
- `post_tool` — one per tool result, with `result_preview` (trimmed)
- `session_end` — status (`complete` / `error`) and any error text

Open one in your favourite JSON-aware tool (`jq -c 'select(.tool != null) | .tool' 11-locks-view.jsonl | sort | uniq -c` gives you a per-card tool histogram).
