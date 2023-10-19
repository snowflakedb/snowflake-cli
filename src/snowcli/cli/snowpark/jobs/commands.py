import sys
from pathlib import Path

import typer

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.snowpark.common import print_log_lines
from snowcli.cli.snowpark.jobs.manager import JobManager
from snowcli.output.decorators import with_output

from snowcli.output.types import SingleQueryResult, CommandResult

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS, name="jobs", help="Manage jobs"
)


@app.command()
@with_output
@global_options_with_connection
def create(
    compute_pool: str = typer.Option(
        ..., "--compute-pool", help="Name of the pool in which to run the job."
    ),
    spec_path: Path = typer.Option(
        ...,
        "--spec-path",
        help="Path to the `spec.yaml` file containing the job details.",
        file_okay=True,
        dir_okay=False,
        exists=True,
    ),
    **options,
) -> CommandResult:
    """
    Creates a job to run in a compute pool.
    """
    cursor = JobManager().create(compute_pool=compute_pool, spec_path=spec_path)
    return SingleQueryResult(cursor)


@app.command()
@with_output
@global_options_with_connection
def desc(id: str = typer.Argument(..., help="Job id"), **options) -> CommandResult:
    """
    Gets the description of a job.
    """
    cursor = JobManager().desc(job_name=id)
    return SingleQueryResult(cursor)


@app.command()
@global_options_with_connection
def logs(
    id: str = typer.Argument(..., help="Job id"),
    container_name: str = typer.Option(
        ..., "--container-name", help="Name of the container."
    ),
    **options,
):
    """
    Retrieves local logs from a job container.
    """
    results = JobManager().logs(job_name=id, container_name=container_name)
    cursor = results.fetchone()
    logs = next(iter(cursor)).split("\n")
    print_log_lines(sys.stdout, id, "0", logs)


@app.command()
@with_output
@global_options_with_connection
def status(
    id: str = typer.Argument(..., help="ID of the job."), **options
) -> CommandResult:
    """
    Returns the status of a named Snowpark Container Services job.
    """
    cursor = JobManager().status(job_name=id)
    return SingleQueryResult(cursor)


@app.command()
@with_output
@global_options_with_connection
def drop(
    id: str = typer.Argument(..., help="ID of the job."), **options
) -> CommandResult:
    """
    Deletes a job from all compute pools in a warehouse.
    """
    cursor = JobManager().drop(job_name=id)
    return SingleQueryResult(cursor)
