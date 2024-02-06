import typer
from click import ClickException
from snowflake.cli.api.commands.decorators import (
    global_options_with_connection,
    with_output,
)
from snowflake.cli.api.commands.flags import DEFAULT_CONTEXT_SETTINGS
from snowflake.cli.api.output.types import MessageResult, ObjectResult
from snowflake.cli.plugins.spcs.image_registry.manager import (
    NoImageRepositoriesFoundError,
    RegistryManager,
)

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
    """Gets the token from environment to use for authenticating with the registry. Note that this token is specific
    to your current user and will not grant access to any repositories that your current user cannot access."""
    return ObjectResult(RegistryManager().get_token())


@app.command()
@with_output
@global_options_with_connection
def url(**options) -> MessageResult:
    """Gets the image registry URL for the current account. Must be called from a role that can view at least one image repository in the image registry."""
    try:
        return MessageResult(RegistryManager().get_registry_url())
    except NoImageRepositoriesFoundError:
        raise ClickException(
            "No image repository found. To get the registry url, please switch to a role with read access to at least one image repository or create a new image repository first."
        )
