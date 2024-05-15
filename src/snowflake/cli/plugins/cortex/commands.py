from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.feature_flags import FeatureFlag

app = SnowTyper(
    name="cortex",
    help="Provides access to Snowflake Cortex LLM.",
    hidden=FeatureFlag.ENABLE_CORTEX.is_disabled(),
)
