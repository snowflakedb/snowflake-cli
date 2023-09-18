import typer

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, ConnectionOption
from snowcli.cli.snowpark.registry.manager import get_token
from snowcli.output.decorators import with_output
from snowcli.output.types import ObjectResult

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="registry",
    help="Manages Snowpark registries.",
)


@app.command("token")
@with_output
@global_options_with_connection
def token(environment: str = ConnectionOption, **options):
    """
    Gets the token from environment to use for authenticating with the registry.
    """
    return ObjectResult(get_token(environment))
