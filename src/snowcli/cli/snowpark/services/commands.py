import sys
from pathlib import Path

import typer

from snowcli.cli.common.decorators import global_options
from snowcli.cli.common.flags import ConnectionOption, DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.snowpark.common import print_log_lines
from snowcli.cli.snowpark.services.manager import ServiceManager
from snowcli.cli.stage.manager import StageManager
from snowcli.output.decorators import with_output

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS, name="services", help="Manage services"
)


@app.command()
@with_output
@global_options
def create(
    name: str = typer.Option(..., "--name", "-n", help="Job Name"),
    compute_pool: str = typer.Option(..., "--compute_pool", "-c", help="Compute Pool"),
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
    stage: str = typer.Option("SOURCE_STAGE", "--stage", "-l", help="Stage name"),
    **options,
):
    """
    Create service
    """
    stage_manager = StageManager()
    stage_manager.create(stage_name=stage)
    stage_manager.put(local_path=str(spec_path), stage_path=stage, overwrite=True)

    return ServiceManager().create(
        service_name=name,
        num_instances=num_instances,
        compute_pool=compute_pool,
        spec_path=spec_path,
        stage=stage,
    )


@app.command()
def desc(
    environment: str = ConnectionOption,
    name: str = typer.Argument(..., help="Service Name"),
):
    """
    Desc Service
    """
    return ServiceManager().desc(service_name=name)


@app.command()
@with_output
@global_options
def status(name: str = typer.Argument(..., help="Service Name"), **options):
    """
    Logs Service
    """
    return ServiceManager().status(service_name=name)


@app.command()
@with_output
@global_options
def list(**options):
    """
    List Service
    """
    return ServiceManager().show()


@app.command()
@with_output
@global_options
def drop(name: str = typer.Argument(..., help="Service Name"), **options):
    """
    Drop Service
    """
    return ServiceManager().drop(service_name=name)


@app.command()
@global_options
def logs(
    name: str = typer.Argument(..., help="Service Name"),
    container_name: str = typer.Option(
        ..., "--container_name", "-c", help="Container Name"
    ),
    **options,
):
    """
    Logs Service
    """
    results = ServiceManager().logs(service_name=name, container_name=container_name)
    cursor = results.fetchone()
    logs = next(iter(cursor)).split("\n")
    print_log_lines(sys.stdout, name, "0", logs)
