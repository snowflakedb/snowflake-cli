from snowflake.cli._plugins.object.commands import app as show_app
from snowflake.cli._plugins.stage.commands import app as stage_app
from snowflake.cli.api.commands.snow_typer import SnowTyper

app = SnowTyper(
    name="object",
    help="Manages Snowflake objects like warehouses and stages",
)

app.add_typer(stage_app)  # type: ignore
app.add_typer(show_app)  # type: ignore
