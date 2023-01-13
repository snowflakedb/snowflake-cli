#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path

import typer

from snowcli import config
from snowcli.config import AppConfig
from snowcli.utils import print_db_cursor

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})
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

    if config.isAuth():
        config.connectToSnowflake()
        if name:
            results = config.snowflake_connection.listStage(
                database=env_conf.get("database"),
                schema=env_conf.get("schema"),
                role=env_conf.get("role"),
                warehouse=env_conf.get("warehouse"),
                name=name,
            )
            print_db_cursor(results)
        else:
            results = config.snowflake_connection.listStages(
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
        Path("."),
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

    if config.isAuth():
        config.connectToSnowflake()
        results = config.snowflake_connection.getStage(
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

    if config.isAuth():
        config.connectToSnowflake()
        filepath = str(path)
        if path.is_dir():
            filepath = str(path) + "/*"

        results = config.snowflake_connection.putStage(
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
