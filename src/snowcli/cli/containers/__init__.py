import typer
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.containers.compute_pool.commands import app as compute_pools_app
from snowcli.cli.containers.jobs.commands import app as jobs_app
from snowcli.cli.containers.services.commands import app as services_app

app = typer.Typer(
    name="containers",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manages Snowpark services, pools and jobs.",
)

app.add_typer(compute_pools_app)  # type: ignore
app.add_typer(services_app)  # type: ignore
app.add_typer(jobs_app)  # type: ignore
