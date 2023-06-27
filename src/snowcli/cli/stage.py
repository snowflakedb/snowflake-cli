from __future__ import annotations

from pathlib import Path

import typer

from snowcli import config
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, ConnectionOption
from snowcli.config import connect_to_snowflake
from snowcli.output.printing import print_db_cursor

app = typer.Typer(
    name="stage",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage stages",
)


@app.command("list")
def stage_list(
    environment: str = ConnectionOption,
    name=typer.Argument(None, help="Name of stage"),
):
    """
    List stage contents
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        if name:
            results = conn.list_stage(
                database=conn.ctx.database,
                schema=conn.ctx.schema,
                role=conn.ctx.role,
                warehouse=conn.ctx.warehouse,
                name=name,
            )
            print_db_cursor(results)
        else:
            results = conn.list_stages(
                database=conn.ctx.database,
                schema=conn.ctx.schema,
                role=conn.ctx.role,
                warehouse=conn.ctx.warehouse,
            )
            print_db_cursor(results)


@app.command("get")
def stage_get(
    environment: str = ConnectionOption,
    name: str = typer.Argument(..., help="Stage name"),
    path: Path = typer.Argument(
        Path.cwd(),
        exists=False,
        file_okay=True,
        dir_okay=True,
        writable=True,
        resolve_path=True,
        help="Directory location to store downloaded files",
    ),
):
    """
    Download files from a stage to a local client
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = conn.get_stage(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=name,
            path=str(path),
        )
        print_db_cursor(results)


@app.command("put")
def stage_put(
    environment: str = ConnectionOption,
    path: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=True,
        writable=True,
        resolve_path=True,
        help="File or directory to upload to stage",
    ),
    name: str = typer.Argument(..., help="Stage name"),
    overwrite: bool = typer.Option(
        False,
        help="Overwrite existing files in stage",
    ),
    parallel: int = typer.Option(
        4,
        help="Number of parallel threads to use for upload",
    ),
):
    """
    Upload files to a stage from a local client
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        filepath = str(path)
        if path.is_dir():
            filepath = str(path) + "/*"

        results = conn.put_stage(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=name,
            path=str(filepath),
            overwrite=overwrite,
            parallel=parallel,
        )
        print_db_cursor(results)


@app.command("create")
def stage_create(
    environment: str = ConnectionOption,
    name: str = typer.Argument(..., help="Stage name"),
):
    """
    Create stage if not exists
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = conn.create_stage(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=name,
        )
        print_db_cursor(results)


@app.command("drop")
def stage_drop(
    environment: str = ConnectionOption,
    name: str = typer.Argument(..., help="Stage name"),
):
    """
    Drop stage
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = conn.drop_stage(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=name,
        )
        print_db_cursor(results)


@app.command("remove")
def stage_remove(
    environment: str = ConnectionOption,
    stage_name: str = typer.Argument(..., help="Stage name"),
    file_name: str = typer.Argument(..., help="File name"),
):
    """
    Remove file from stage
    """

    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        config.connect_to_snowflake()
        results = conn.remove_from_stage(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=stage_name,
            path=file_name,
        )
        print_db_cursor(results)
