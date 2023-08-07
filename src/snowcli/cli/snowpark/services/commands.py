import sys
from pathlib import Path

import typer
from typing import TextIO
from typing_extensions import Annotated

from snowcli.cli.common.decorators import global_options
from snowcli.cli.common.flags import ConnectionOption, DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.snowpark.services.manager import ServiceManager
from snowcli.cli.stage.manager import StageManager
from snowcli.output.decorators import with_output

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS, name="services", help="Manage services"
)

if not sys.stdout.closed and sys.stdout.isatty():
    GREEN = "\033[32m"
    BLUE = "\033[34m"
    ORANGE = "\033[38:2:238:76:44m"
    GRAY = "\033[2m"
    ENDC = "\033[0m"
else:
    GREEN = ""
    ORANGE = ""
    BLUE = ""
    GRAY = ""
    ENDC = ""


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
    num_instances: Annotated[
        int, typer.Option("--num_instances", "-num", help="Number of instances")
    ] = 1,
    stage: str = typer.Option("SOURCE_STAGE", "--stage", "-l", help="Stage name"),
    **options,
):
    """
    Create service
    """
    stage_manager = StageManager()
    stage_manager.create(stage_name=stage)
    stage_manager.put(local_path=str(spec_path), stage_name=stage, overwrite=True)

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


def _prefix_line(prefix: str, line: str) -> str:
    """
    _prefix_line ensure the prefix is still present even when dealing with return characters
    """
    if "\r" in line:
        line = line.replace("\r", f"\r{prefix}")
    if "\n" in line[:-1]:
        line = line[:-1].replace("\n", f"\n{prefix}") + line[-1:]
    if not line.startswith("\r"):
        line = f"{prefix}{line}"
    return line


def print_log_lines(file: TextIO, name, id, logs):
    prefix = f"{GREEN}{name}/{id}{ENDC} "
    logs = logs[0:-1]
    for log in logs:
        print(_prefix_line(prefix, log + "\n"), file=file, end="", flush=True)


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
