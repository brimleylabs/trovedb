"""Root pytest configuration.

On Windows, psycopg (>=3) requires a SelectorEventLoop rather than the
default ProactorEventLoop.  The ``asyncio_mode = "auto"`` setting in
pyproject.toml honours ``asyncio_event_loop_policy`` at the session level,
but the simplest portable fix is to override the policy here.
"""

from __future__ import annotations

import asyncio
import sys


def pytest_configure(config: object) -> None:  # noqa: ARG001
    """Switch to SelectorEventLoop on Windows so psycopg async works."""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
