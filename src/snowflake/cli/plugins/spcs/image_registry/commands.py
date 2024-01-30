import typer
from snowflake.cli.api.commands.decorators import (
    global_options_with_connection,
    with_output,
)
from snowflake.cli.api.commands.flags import DEFAULT_CONTEXT_SETTINGS
from snowflake.cli.api.output.types import ObjectResult
from snowflake.cli.plugins.spcs.image_registry.manager import RegistryManager

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="image-registry",
    help="Manages Snowpark registries.",
    rich_markup_mode="markdown",
)


@app.command("token")
@with_output
@global_options_with_connection
def token(**options) -> ObjectResult:
    """Gets the token from environment to use for authenticating with the registry."""
    return ObjectResult(RegistryManager().get_token())
