import typer
from snowcli.api.output.decorators import with_output
from snowcli.api.output.types import CommandResult, SingleQueryResult
from snowcli.cli.common.decorators import global_options_with_connection
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
