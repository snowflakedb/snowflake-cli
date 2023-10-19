import typer

from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.snowpark.function.commands import app as function_app
from snowcli.cli.snowpark.package.commands import app as package_app
from snowcli.cli.snowpark.procedure.commands import app as procedure_app
from snowcli.cli.snowpark.compute_pool.commands import (
    app as compute_pools_app,
    app_cp as cp_app,
    app_compute_pool as app_compute_pool,
)
from snowcli.cli.snowpark.services.commands import app as services_app
from snowcli.cli.snowpark.jobs.commands import app as jobs_app
from snowcli.cli.snowpark.registry.commands import app as registry_app

app = typer.Typer(
    name="snowpark",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manages functions, procedures, and Snowpark objects.",
)

app.add_typer(function_app)  # type: ignore
app.add_typer(package_app)  # type: ignore
app.add_typer(procedure_app)  # type: ignore
app.add_typer(compute_pools_app)  # type: ignore
app.add_typer(cp_app)  # type: ignore
app.add_typer(app_compute_pool)  # type: ignore
app.add_typer(services_app)  # type: ignore
app.add_typer(jobs_app)  # type: ignore
app.add_typer(registry_app)  # type: ignore
