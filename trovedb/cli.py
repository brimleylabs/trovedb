"""trovedb CLI entrypoint."""

from __future__ import annotations

import logging
from typing import Annotated

import typer

from trovedb import __version__

logger = logging.getLogger(__name__)

app = typer.Typer(
    name="trovedb",
    help="A live operator console for SQL databases.",
    invoke_without_command=True,
)


def _version_callback(value: bool) -> None:  # noqa: FBT001
    if value:
        typer.echo(f"trovedb {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    ctx: typer.Context,
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            "-V",
            callback=_version_callback,
            is_eager=True,
            help="Show version and exit.",
        ),
    ] = None,
    conn: Annotated[
        str | None,
        typer.Option(
            "--conn",
            help=(
                "Named connection profile from "
                "~/.config/trovedb/connections.toml. "
                "Skips the picker and connects directly."
            ),
        ),
    ] = None,
    conn_url: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Ad-hoc DSN URL (e.g. postgres://user@localhost/db). "
                "Skips the picker and connects directly."
            ),
            metavar="URL",
        ),
    ] = None,
) -> None:
    """trovedb \u2014 live operator console for SQL databases."""
    if ctx.invoked_subcommand is None:
        from trovedb.app import TroveApp

        TroveApp(conn_name=conn, conn_url=conn_url).run()


if __name__ == "__main__":
    app()
