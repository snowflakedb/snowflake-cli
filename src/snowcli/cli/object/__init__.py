import typer
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.object.commands import app as show_app
from snowcli.cli.object.stage.commands import app as stage_app

app = typer.Typer(
    name="object",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manages Snowflake objects like warehouses and stages",
)

app.add_typer(stage_app)  # type: ignore
app.add_typer(show_app)  # type: ignore
