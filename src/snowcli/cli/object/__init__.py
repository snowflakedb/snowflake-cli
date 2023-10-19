import typer

from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.object.stage.commands import app as stage_app
from snowcli.cli.object.warehouse.commands import app as warehouse_app

app = typer.Typer(
    name = "object",
    context_settings= DEFAULT_CONTEXT_SETTINGS,
    help="Manages Snowflake objects, like warehouses and stages"
)

app.add_typer(stage_app) # type: ignore
app.add_typer(warehouse_app) # type: ignore

