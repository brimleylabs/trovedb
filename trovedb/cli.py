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
) -> None:
    """trovedb — live operator console for SQL databases."""
    if ctx.invoked_subcommand is None:
        from trovedb.app import TroveApp

        TroveApp().run()


@app.command()
def connect(
    connection: Annotated[
        str | None,
        typer.Argument(help="Connection name from ~/.config/trovedb/connections.toml"),
    ] = None,
) -> None:
    """Connect to a database and open the operator console."""
    logger.info("connect called with connection=%s", connection)
    typer.echo("connect: not yet implemented")
    raise typer.Exit(1)


if __name__ == "__main__":
    app()
