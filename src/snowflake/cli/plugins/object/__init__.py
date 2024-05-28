from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.plugins.object.commands import app as show_app

app = SnowTyper(
    name="object",
    help="Manages Snowflake objects like warehouses and stages",
)

app.add_typer(show_app)  # type: ignore
