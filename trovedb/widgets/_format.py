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


def format_row_count(n: int | None) -> str:
    """Format a row count as a compact human-readable estimate.

    Examples:
        ``None`` or negative → ``""``
        ``0``    → ``"≈0"``
        ``42``   → ``"≈42"``
        ``1500`` → ``"≈2k"``
        ``78274`` → ``"≈78k"``
        ``1_200_000`` → ``"≈1.2M"``
    """
    if n is None or n < 0:
        return ""
    if n < 1_000:
        return f"≈{n}"
    if n < 1_000_000:
        k = round(n / 1_000)
        return f"≈{k}k"
    return f"≈{n / 1_000_000:.1f}M"
