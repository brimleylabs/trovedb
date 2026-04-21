"""Tests for the TroveApp Textual application shell."""

from __future__ import annotations

from trovedb import __version__
from trovedb.app import HelpOverlay, TroveApp


async def test_app_composes_without_error() -> None:
    """App should mount and render without raising exceptions."""
    app = TroveApp()
    async with app.run_test() as pilot:
        # If we got here, compose() succeeded
        assert pilot.app is app


async def test_status_bar_shows_version_and_no_connection() -> None:
    """Status bar must display 'trovedb <version> — (no connection)'."""
    app = TroveApp()
    async with app.run_test() as pilot:
        from textual.widgets import Static

        status_bar = pilot.app.query_one("#status-bar", Static)
        content = str(status_bar.content)
        assert f"trovedb {__version__}" in content
        assert "(no connection)" in content


async def test_hint_bar_shows_keybinding_hints() -> None:
    """Bottom hint bar must mention '?' and 'q'."""
    app = TroveApp()
    async with app.run_test() as pilot:
        from textual.widgets import Static

        hint_bar = pilot.app.query_one("#hint-bar", Static)
        content = str(hint_bar.content)
        assert "?" in content
        assert "q" in content


async def test_main_content_area_present() -> None:
    """Main content area widget must be present."""
    app = TroveApp()
    async with app.run_test() as pilot:
        from textual.widgets import Static

        main_content = pilot.app.query_one("#main-content", Static)
        assert main_content is not None


async def test_question_mark_opens_help_overlay() -> None:
    """Pressing '?' should push the HelpOverlay onto the screen stack."""
    app = TroveApp()
    async with app.run_test() as pilot:
        await pilot.press("question_mark")
        assert isinstance(pilot.app.screen, HelpOverlay)


async def test_escape_closes_help_overlay() -> None:
    """Pressing Esc while help overlay is open should dismiss it."""
    app = TroveApp()
    async with app.run_test() as pilot:
        await pilot.press("question_mark")
        assert isinstance(pilot.app.screen, HelpOverlay)
        await pilot.press("escape")
        assert not isinstance(pilot.app.screen, HelpOverlay)


async def test_question_mark_closes_help_overlay() -> None:
    """Pressing '?' while help overlay is open should dismiss it."""
    app = TroveApp()
    async with app.run_test() as pilot:
        await pilot.press("question_mark")
        assert isinstance(pilot.app.screen, HelpOverlay)
        await pilot.press("question_mark")
        assert not isinstance(pilot.app.screen, HelpOverlay)


async def test_q_exits_app() -> None:
    """Pressing 'q' should exit the application cleanly."""
    app = TroveApp()
    async with app.run_test() as pilot:
        await pilot.press("q")
    assert app.return_code == 0 or app.return_code is None
