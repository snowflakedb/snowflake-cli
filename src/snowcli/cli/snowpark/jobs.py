import hashlib
import os
import sys
from pathlib import Path

import typer
from typing import TextIO

from snowcli.cli.common.decorators import global_options
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.stage.manager import StageManager
from snowcli.output.decorators import with_output

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS, name="jobs", help="Manage jobs"
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


class JobManager(SqlExecutionMixin):
    def create(self, compute_pool: str, spec_path: Path, stage: str):
        spec_filename = os.path.basename(spec_path)
        file_hash = hashlib.md5(open(spec_path, "rb").read()).hexdigest()
        stage_dir = os.path.join("jobs", file_hash)
        return self._execute_query(
            f"""\
        EXECUTE SERVICE
        COMPUTE_POOL =  {compute_pool}
        spec=@{stage}/{stage_dir}/{spec_filename};
        """
        )

    def desc(self, job_name: str):
        return self._execute_query(f"desc service {job_name}")

    def status(self, job_name: str):
        return self._execute_query(f"CALL SYSTEM$GET_JOB_STATUS('{job_name}')")

    def drop(self, job_name: str):
        return self._execute_query(f"CALL SYSTEM$CANCEL_JOB('{job_name}')")

    def logs(self, job_name: str, container_name: str):
        return self._execute_query(
            f"call SYSTEM$GET_JOB_LOGS('{job_name}', '{container_name}')"
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
    stage_manager.put(local_path=str(spec_path), stage_name=stage, overwrite=True)

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
