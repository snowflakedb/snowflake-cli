import sys
import json
import typer
from snowcli import config
from typing import TextIO
from datetime import datetime

from snowcli.cli import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.common.flags import ConnectionOption
from snowcli.config import connect_to_snowflake
from snowcli.output.printing import print_db_cursor, print_data

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
    name: str = typer.Option(..., "--name", "-n", help="Job Name"),
    image: str = typer.Option(..., "--image", "-i", help="Image"),
    compute_pool: str = typer.Option(..., "--compute_pool", "-c", help="Compute Pool"),
    num: int = typer.Option(..., "--num", "-d", help="Num Instances"),
):
    """
    Create Job
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = config.snowflake_connection.create_job(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=name,
            image=image,
            compute_pool=compute_pool,
            num_instances=num,
        )
        print_db_cursor(results)


@app.command()
def desc(
    environment: str = ConnectionOption,
    name: str = typer.Argument(..., help="Job Name"),
):
    """
    Desc Service
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = config.snowflake_connection.desc_job(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=name,
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
    name: str = typer.Argument(..., help="Job Name"),
):
    """
    Logs Service
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = config.snowflake_connection.logs_job(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=name,
        )
        cursor = results.fetchone()
        service_logs = json.loads(next(iter(cursor)))
        for service_id, log_str in service_logs.items():
            logs = log_str.split("\n")
            print_log_lines(sys.stdout, name, service_id, logs)


@app.command()
def status(
    environment: str = ConnectionOption,
    name: str = typer.Argument(..., help="Job Name"),
):
    """
    Logs Service
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = config.snowflake_connection.status_job(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=name,
        )
        print_db_cursor(results)


@app.command()
def list(environment: str = ConnectionOption):
    """
    List Service
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = config.snowflake_connection.list_job(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
        )
        data = []
        for row in results.fetchall():
            data = json.loads(row[0])
        cols = set()
        for row in data:
            row["name"] = f"{row['name']}_{row['run_id']}"
            del row["run_id"]
            timestamp = int(row["created_on"])
            date = datetime.fromtimestamp(timestamp / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            row["created_on"] = date
            cols |= set(row.keys())
        cols_list = [col for col in cols]
        print_data(data=data, columns=cols_list)


@app.command()
def drop(
    environment: str = ConnectionOption,
    name: str = typer.Argument(..., help="Job Name"),
):
    """
    Drop Service
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = config.snowflake_connection.drop_job(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=name,
        )
        print_db_cursor(results)
