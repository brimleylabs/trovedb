"""Pure formatting helpers shared across trovedb TUI screens."""

from __future__ import annotations


def format_runtime(seconds: float | None) -> str:
    """Return a human-readable elapsed-time string.

    Examples:
        ``None``  → ``"—"``
        ``2.4``   → ``"2.4s"``
        ``61``    → ``"1m 01s"``
        ``3720``  → ``"1h 02m"``
    """
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours, mins = divmod(minutes, 60)
    return f"{hours}h {mins:02d}m"


def truncate(text: str, max_width: int) -> str:
    """Truncate *text* to *max_width* chars, appending ``…`` if needed."""
    if len(text) <= max_width:
        return text
    return text[: max_width - 1] + "…"
