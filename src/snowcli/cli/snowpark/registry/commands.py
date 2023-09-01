import typer

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, ConnectionOption
from snowcli.cli.snowpark.registry.manager import get_token
from snowcli.output.decorators import with_output
from snowcli.output.formats import OutputFormat
from snowcli.output.printing import OutputData

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS, name="registry", help="Manage registry"
)


@app.command("token")
@with_output
@global_options_with_connection
def token(environment: str = ConnectionOption, **options):
    """
    Get token to authenticate with registry.
    """
    return OutputData.from_list([get_token(environment)])
