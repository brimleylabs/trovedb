---
card_id: trovedb-01-bootstrap
difficulty: trivial
stack: python
registered_at: 2026-04-21
---

# Bootstrap project skeleton

## Goal

Create the Python project skeleton: `pyproject.toml`, `trovedb/__init__.py`,
`trovedb/cli.py`, `tests/`, `README.md`, `LICENSE` (MIT). Set up
ruff + pytest + pytest-asyncio. Editable install (`pip install -e .[dev]`)
must succeed and `trovedb --version` must print the package version.

## Acceptance criteria

1. `pip install -e ".[dev]"` succeeds in a fresh venv.
2. `trovedb --version` exits 0 and prints `trovedb 0.0.1` (or current pyproject version).
3. `trovedb --help` exits 0 and shows at least one subcommand placeholder.
4. `pytest -q` exits 0 with at least one passing smoke test (e.g.
   asserts the CLI imports without error).
5. `ruff check trovedb tests` exits 0.
6. README has a one-paragraph project pitch + install + usage snippet.

## Notes

Use **typer** for the CLI (matches Winston's choice and is well-known).
Python 3.11+. Pin Textual >=1.0, psycopg >=3, aiomysql, aiosqlite — but
do not yet import them in cli.py; this card is structure only.

Registered before execution. Not edited after running.
