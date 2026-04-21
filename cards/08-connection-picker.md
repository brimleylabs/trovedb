---
card_id: trovedb-08-connection-picker
difficulty: moderate
stack: python
registered_at: 2026-04-21
---

# Connection picker — first user-facing screen

## Goal

When `trovedb` launches with no `--conn` argument, show a **connection
picker** as the initial screen. List every profile from
`~/.config/trovedb/connections.toml` plus an entry for "Open by URL..."
(prompts for an ad-hoc DSN). Selecting a profile attempts to connect;
on success, transitions to the **PROCLIST screen** (the headline view —
to be built in a later card; for now show a placeholder that confirms
"Connected to {profile.name}").

Why this is the first user-facing screen: it's the only "before" view
the operator ever sees, and getting it right makes trovedb feel polished
from second one.

## Acceptance criteria

1. `trovedb` (no args) launches the app and immediately shows the
   `ConnectionPickerScreen`.
2. The picker shows a list of profiles, one per row, with columns:
   name, driver, host:port (or `local file` for sqlite), database.
3. Pressing `j/k` or arrows navigates; `Enter` connects to the highlighted
   profile; `Esc` quits the app.
4. `n` opens an inline "New connection" prompt (DSN URL only — saving to
   TOML is a later card).
5. `/` filters the list as the user types (case-insensitive, matches
   profile name + database name).
6. On `Enter`, the picker shows a "Connecting..." spinner; on success,
   pushes the placeholder PROCLIST screen showing only `Connected to
   {profile.name} ({driver})` for now. On failure, shows an inline error
   (don't crash the app) and stays on the picker.
7. `trovedb --conn <name>` skips the picker and connects directly. If
   `<name>` doesn't exist in the TOML, exit with a helpful error and
   non-zero status.
8. `trovedb postgres://user@localhost/db` (positional URL) skips the
   picker, connects directly to the URL.
9. Tests use Textual's pilot harness:
   - launches with no args, picker visible
   - presses `j` once and `Enter`, asserts placeholder screen visible
   - presses `/`, types "prod", asserts list filtered
   - presses `n`, asserts inline DSN prompt appears
   - asserts `--conn nonexistent` exits with helpful error (subprocess test)
10. Empty connections file → picker shows "No saved connections. Press
    `n` to add one, or pass a URL: `trovedb postgres://...`"

## Notes

- Use `Screen` and `ListView` from Textual.
- The connection attempt is async; show a `LoadingIndicator` overlay
  during the attempt so the UI never appears frozen.
- Connection errors should classify clean from confusing — at minimum
  detect "auth failed", "host unreachable", "database does not exist",
  fall through to "connection failed: <raw error>" for unknown.
- Future card will add `e` to edit and `D` to delete profiles. This card
  is read + connect only.

Registered before execution. Not edited after running.
