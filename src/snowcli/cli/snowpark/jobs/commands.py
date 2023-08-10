import sys
from pathlib import Path

import typer

from snowcli.cli.common.decorators import global_options
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.snowpark.common import print_log_lines
from snowcli.cli.snowpark.jobs.manager import JobManager
from snowcli.cli.stage.manager import StageManager
from snowcli.output.decorators import with_output

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS, name="jobs", help="Manage jobs"
)


@app.command()
@with_output
@global_options
def create(
    compute_pool: str = typer.Option(..., "--compute-pool", "-c", help="Compute Pool"),
    spec_path: Path = typer.Option(
        ...,
        "--spec-path",
        "-s",
        help="Spec.yaml file path",
        file_okay=True,
        dir_okay=False,
        exists=True,
    ),
    stage: str = typer.Option("SOURCE_STAGE", "--stage", "-l", help="Stage name"),
    **options,
):
    """
    Create Job
    """
    stage_manager = StageManager()
    stage_manager.create(stage_name=stage)
    stage_manager.put(local_path=str(spec_path), stage_path=stage, overwrite=True)

    return JobManager().create(
        compute_pool=compute_pool, spec_path=spec_path, stage=stage
    )


@app.command()
@with_output
@global_options
def desc(id: str = typer.Argument(..., help="Job id"), **options):
    """
    Desc Service
    """
    return JobManager().desc(job_name=id)


@app.command()
@global_options
def logs(
    id: str = typer.Argument(..., help="Job id"),
    container_name: str = typer.Option(
        ..., "--container-name", "-c", help="Container Name"
    ),
    **options,
):
    """
    Logs Service
    """
    results = JobManager().logs(job_name=id, container_name=container_name)
    cursor = results.fetchone()
    logs = next(iter(cursor)).split("\n")
    print_log_lines(sys.stdout, id, "0", logs)


@app.command()
@with_output
@global_options
def status(id: str = typer.Argument(..., help="Job id"), **options):
    """
    Returns status of a job.
    """
    return JobManager().status(job_name=id)


@app.command()
@with_output
@global_options
def drop(id: str = typer.Argument(..., help="Job id"), **options):
    """
    Drop Service
    """
    return JobManager().drop(job_name=id)
