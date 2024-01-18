import typer
from snowflake.cli.api.commands.decorators import (
    global_options_with_connection,
    with_output,
)
from snowflake.cli.api.output.types import CommandResult, SingleQueryResult
from snowflakecli.test_plugins.snowpark_hello.manager import SnowparkHelloManager

app = typer.Typer(name="hello")


@app.command("hello")
@with_output
@global_options_with_connection
def hello(
    name: str = typer.Argument(help="Your name"),
    **options,
) -> CommandResult:
    """
    Says hello
    """
    hello_manager = SnowparkHelloManager()
    cursor = hello_manager.say_hello(name)
    return SingleQueryResult(cursor)
