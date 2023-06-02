#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path

import typer

from snowcli import config
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.config import AppConfig
from snowcli.output.printing import print_db_cursor

app = typer.Typer(
    name="stage",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage stages",
)
EnvironmentOption = typer.Option("dev", help="Environment name")


@app.command("list")
def stage_list(
    environment: str = EnvironmentOption,
    name=typer.Argument(None, help="Name of stage"),
):
    """
    List stage contents
    """
    env_conf = AppConfig().config.get(environment)

    if config.is_auth():
        config.connect_to_snowflake()
        if name:
            results = config.snowflake_connection.list_stage(
                database=env_conf.get("database"),
                schema=env_conf.get("schema"),
                role=env_conf.get("role"),
                warehouse=env_conf.get("warehouse"),
                name=name,
            )
            print_db_cursor(results)
        else:
            results = config.snowflake_connection.list_stages(
                database=env_conf.get("database"),
                schema=env_conf.get("schema"),
                role=env_conf.get("role"),
                warehouse=env_conf.get("warehouse"),
            )
            print_db_cursor(results)


@app.command("get")
def stage_get(
    environment: str = EnvironmentOption,
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
    env_conf = AppConfig().config.get(environment)

    if config.is_auth():
        config.connect_to_snowflake()
        results = config.snowflake_connection.get_stage(
            database=env_conf.get("database"),
            schema=env_conf.get("schema"),
            role=env_conf.get("role"),
            warehouse=env_conf.get("warehouse"),
            name=name,
            path=str(path),
        )
        print_db_cursor(results)


@app.command("put")
def stage_put(
    environment: str = EnvironmentOption,
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
    env_conf = AppConfig().config.get(environment)

    if config.is_auth():
        config.connect_to_snowflake()
        filepath = str(path)
        if path.is_dir():
            filepath = str(path) + "/*"

        results = config.snowflake_connection.put_stage(
            database=env_conf.get("database"),
            schema=env_conf.get("schema"),
            role=env_conf.get("role"),
            warehouse=env_conf.get("warehouse"),
            name=name,
            path=str(filepath),
            overwrite=overwrite,
            parallel=parallel,
        )
        print_db_cursor(results)


@app.command("create")
def stage_create(
    environment: str = EnvironmentOption,
    name: str = typer.Argument(..., help="Stage name"),
):
    """
    Create stage if not exists
    """
    env_conf = AppConfig().config.get(environment)

    if config.is_auth():
        config.connect_to_snowflake()
        results = config.snowflake_connection.create_stage(
            database=env_conf.get("database"),
            schema=env_conf.get("schema"),
            role=env_conf.get("role"),
            warehouse=env_conf.get("warehouse"),
            name=name,
        )
        print_db_cursor(results)


@app.command("drop")
def stage_drop(
    environment: str = EnvironmentOption,
    name: str = typer.Argument(..., help="Stage name"),
):
    """
    Drop stage
    """
    env_conf = AppConfig().config.get(environment)

    if config.is_auth():
        config.connect_to_snowflake()
        results = config.snowflake_connection.drop_stage(
            database=env_conf.get("database"),
            schema=env_conf.get("schema"),
            role=env_conf.get("role"),
            warehouse=env_conf.get("warehouse"),
            name=name,
        )
        print_db_cursor(results)
