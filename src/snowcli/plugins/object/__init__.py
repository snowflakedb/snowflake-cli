import typer
from snowcli.api.commands.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.plugins.object.commands import app as show_app
from snowcli.plugins.object.stage.commands import app as stage_app

app = typer.Typer(
    name="object",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manages Snowflake objects like warehouses and stages",
)

app.add_typer(stage_app)  # type: ignore
app.add_typer(show_app)  # type: ignore
