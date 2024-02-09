import typer
from snowflake.cli.api.commands.decorators import (
    with_output,
)
from snowflake.cli.api.output.types import CommandResult

app = typer.Typer()


@app.command("run")
@with_output
def run() -> CommandResult:
    pass
