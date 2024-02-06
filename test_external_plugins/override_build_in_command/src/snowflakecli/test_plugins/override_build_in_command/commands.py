import typer
from snowflake.cli.api.commands.decorators import (
    with_output,
)
from snowflake.cli.api.output.types import CommandResult, MessageResult

app = typer.Typer()


@app.command("add")
@with_output
def add() -> CommandResult:
    return MessageResult("This message should not be printed")
