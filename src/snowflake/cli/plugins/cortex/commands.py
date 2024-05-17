from snowflake.cli.api.commands.snow_typer import SnowTyper

app = SnowTyper(
    name="cortex",
    help="Provides access to Snowflake Cortex LLM.",
)
