from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.plugins.spcs.compute_pool.commands import (
    app as compute_pools_app,
)
from snowflake.cli.plugins.spcs.image_registry.commands import app as registry_app
from snowflake.cli.plugins.spcs.image_repository.commands import (
    app as image_repository_app,
)
from snowflake.cli.plugins.spcs.jobs.commands import app as jobs_app
from snowflake.cli.plugins.spcs.services.commands import app as services_app

app = SnowTyper(
    name="spcs",
    help="Manages Snowpark services, pools, image registries, and image repositories.",
)

app.add_typer(compute_pools_app)  # type: ignore
app.add_typer(services_app)  # type: ignore
app.add_typer(jobs_app)  # type: ignore
app.add_typer(registry_app)  # type: ignore
app.add_typer(image_repository_app)
