---
card_id: trovedb-05-app-shell-textual
difficulty: moderate
stack: python
registered_at: 2026-04-21
---

# Textual app shell + top status bar + help overlay

## Goal

Build the empty-but-running TUI shell. `trovedb` (no args) launches a
Textual app with:
- A persistent **top status bar** showing app name, version, and
  "(no connection)" placeholder.
- A persistent **bottom hint bar** showing `?: help  q: quit`.
- An **empty main area** (a `Static` widget displaying "Welcome to trovedb").
- A working **`?` keybinding** that overlays a `HelpOverlay` showing the
  current keymap. Press `?` or `Esc` to dismiss.
- A working **`q` keybinding** that exits the app.

No database connection logic in this card. The app is structurally
complete but functionally empty — proves Textual is wired correctly and
the top/bottom chrome rendering works.

## Acceptance criteria

1. `trovedb` launches a Textual app and renders without error.
2. Top status bar visible, shows `trovedb 0.0.1 — (no connection)`.
3. Bottom hint bar visible, shows the two keybinding hints.
4. Pressing `?` opens the help overlay; pressing `?` or `Esc` closes it.
5. Pressing `q` cleanly exits (exit code 0).
6. Tests use Textual's `app.run_test()` pilot harness:
   - assert top bar text
   - assert bottom bar text
   - press `?`, assert overlay visible
   - press `Esc`, assert overlay dismissed
   - press `q`, assert app exited
7. Default theme is dark; defined in `trovedb/theme/default.tcss`.
8. `trovedb/app.py` exposes `TroveApp(App)` for use in tests.

## Notes

- Use `App.compose()` for the layout.
- Status bar is its own `Widget` subclass so we can update it
  reactively when a connection lands.
- Keep the help overlay content static for now — list the four bindings
  you implement here. Subsequent cards add bindings + extend it.

Registered before execution. Not edited after running.
