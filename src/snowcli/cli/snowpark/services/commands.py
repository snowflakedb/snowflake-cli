import sys
from pathlib import Path

import typer
from snowcli.output.formats import OutputFormat
from snowcli.cli.common.decorators import global_options_with_connection, GLOBAL_OPTIONS
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.snowpark.common import print_log_lines
from snowcli.cli.snowpark.services.manager import ServiceManager
from snowcli.output.decorators import with_output
from snowcli.output.types import (
    QueryResult,
    SingleQueryResult,
    QueryJsonValueResult,
    CommandResult,
)
from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="services",
    help="Manages Snowpark services.",
)


@app.command()
@with_output
@global_options_with_connection
def create(
    name: str = typer.Option(..., "--name", "-n", help="Job Name"),
    compute_pool: str = typer.Option(..., "--compute_pool", "-p", help="Compute Pool"),
    spec_path: Path = typer.Option(
        ...,
        "--spec_path",
        "-s",
        help="Spec Path",
        file_okay=True,
        dir_okay=False,
        exists=True,
    ),
    num_instances: int = typer.Option(
        1, "--num_instances", "-num", help="Number of instances"
    ),
    **options,
) -> CommandResult:
    """
    Creates a new Snowpark Container Services service in the current schema.
    """

    cursor = ServiceManager().create(
        service_name=name,
        num_instances=num_instances,
        compute_pool=compute_pool,
        spec_path=spec_path,
    )
    return SingleQueryResult(cursor)


@app.command()
@with_output
@global_options_with_connection
def desc(
    name: str = typer.Argument(..., help="Service Name"), **options
) -> CommandResult:
    """
    Describes the properties of a Snowpark Container Services service.
    """
    cursor = ServiceManager().desc(service_name=name)
    return SingleQueryResult(cursor)


@app.command()
@with_output
@global_options_with_connection
def status(
    name: str = typer.Argument(..., help="Name of the service."), **options
) -> CommandResult:
    """
    Retrieves status of a Snowpark Container Services service.
    """
    cursor = ServiceManager().status(service_name=name)
    return QueryJsonValueResult(cursor)


@app.command()
@with_output
@global_options_with_connection
def list(**options) -> CommandResult:
    """
    Lists the services for which you have access privileges.
    """
    cursor = ServiceManager().show()
    return QueryResult(cursor)


@app.command()
@with_output
@global_options_with_connection
def drop(
    name: str = typer.Argument(..., help="Name of the service to remove."), **options
) -> CommandResult:
    """
    Removes the specified service from the current or specified schema.
    """
    cursor = ServiceManager().drop(service_name=name)
    return SingleQueryResult(cursor)


@app.command()
@global_options_with_connection
def logs(
    name: str = typer.Argument(..., help="Name of the service."),
    container_name: str = typer.Option(
        ..., "--container_name", "-n", help="Name of the container."
    ),
    **options,
):
    """
    Retrieves local logs from a Snowpark Container Services service container.
    """
    results = ServiceManager().logs(service_name=name, container_name=container_name)
    cursor = results.fetchone()
    logs = next(iter(cursor)).split("\n")
    print_log_lines(sys.stdout, name, "0", logs)
