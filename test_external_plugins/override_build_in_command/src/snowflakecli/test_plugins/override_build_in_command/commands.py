import typer
from snowflake.cli.api.commands.decorators import (
    with_output,
)
from snowflake.cli.api.output.types import CommandResult, MessageResult

app = typer.Typer()

print("Outside command code")


@app.command("list")
@with_output
def connection_list() -> CommandResult:
    return MessageResult("Overridden command")
