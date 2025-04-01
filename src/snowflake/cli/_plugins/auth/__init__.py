from snowflake.cli._plugins.auth.keypair.commands import app as keypair_app
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.feature_flags import FeatureFlag

app = SnowTyperFactory(
    name="auth",
    help="Manages authentication methods.",
    is_hidden=lambda: FeatureFlag.ENABLE_AUTH_KEYPAIR.is_disabled(),
)

app.add_typer(keypair_app)
