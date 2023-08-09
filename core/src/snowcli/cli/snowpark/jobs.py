import sys
import typer
from typing import TextIO


from snowcli.cli.common.flags import ConnectionOption, DEFAULT_CONTEXT_SETTINGS
from snowcli.snow_connector import connect_to_snowflake
from snowcli.output.printing import print_db_cursor

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


@app.command()
def create(
    environment: str = ConnectionOption,
    compute_pool: str = typer.Option(..., "--compute_pool", "-c", help="Compute Pool"),
    spec_path: str = typer.Option(..., "--spec_path", "-s", help="Spec.yaml file path"),
    stage: str = typer.Option("SOURCE_STAGE", "--stage", "-l", help="Stage name"),
):
    """
    Create Job
    """
    conn = connect_to_snowflake(connection_name=environment)

    results = conn.create_job(
        database=conn.ctx.database,
        schema=conn.ctx.schema,
        role=conn.ctx.role,
        warehouse=conn.ctx.warehouse,
        compute_pool=compute_pool,
        spec_path=spec_path,
        stage=stage,
    )
    print_db_cursor(results)


@app.command()
def desc(
    environment: str = ConnectionOption,
    id: str = typer.Argument(..., help="Job id"),
):
    """
    Desc Service
    """
    conn = connect_to_snowflake(connection_name=environment)

    results = conn.desc_job(
        database=conn.ctx.database,
        schema=conn.ctx.schema,
        role=conn.ctx.role,
        warehouse=conn.ctx.warehouse,
        id=id,
    )
    print_db_cursor(results)


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
def logs(
    environment: str = ConnectionOption,
    id: str = typer.Argument(..., help="Job id"),
    container_name: str = typer.Option(
        ..., "--container-name", "-c", help="Container Name"
    ),
):
    """
    Logs Service
    """
    conn = connect_to_snowflake(connection_name=environment)

    results = conn.logs_job(
        database=conn.ctx.database,
        schema=conn.ctx.schema,
        role=conn.ctx.role,
        warehouse=conn.ctx.warehouse,
        id=id,
        container_name=container_name,
    )
    cursor = results.fetchone()
    logs = next(iter(cursor)).split("\n")
    print_log_lines(sys.stdout, id, "0", logs)


@app.command()
def status(
    environment: str = ConnectionOption,
    id: str = typer.Argument(..., help="Job id"),
):
    """
    Logs Service
    """
    conn = connect_to_snowflake(connection_name=environment)

    results = conn.status_job(
        database=conn.ctx.database,
        schema=conn.ctx.schema,
        role=conn.ctx.role,
        warehouse=conn.ctx.warehouse,
        id=id,
    )
    print_db_cursor(results)


@app.command()
def drop(
    environment: str = ConnectionOption,
    id: str = typer.Argument(..., help="Job id"),
):
    """
    Drop Service
    """
    conn = connect_to_snowflake(connection_name=environment)

    results = conn.drop_job(
        database=conn.ctx.database,
        schema=conn.ctx.schema,
        role=conn.ctx.role,
        warehouse=conn.ctx.warehouse,
        id=id,
    )
    print_db_cursor(results)
