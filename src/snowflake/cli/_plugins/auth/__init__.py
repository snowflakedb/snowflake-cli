from snowflake.cli._plugins.auth.keypair.commands import app as keypair_app
from snowflake.cli._plugins.auth.workload_identity.commands import (
    app as workload_identity_app,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory

app = SnowTyperFactory(
    name="auth",
    help="Manages authentication methods.",
)

app.add_typer(keypair_app)
app.add_typer(workload_identity_app)
