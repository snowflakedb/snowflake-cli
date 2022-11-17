#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path

import typer
from rich import print
from snowcli import config
from snowcli.config import AppConfig
from snowcli.utils import print_db_cursor

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})
EnvironmentOption = typer.Option("dev", help='Environment name')


@app.command("list")
def streamlit_list(environment: str = EnvironmentOption):
    """
    List streamlit apps.
    """
    env_conf = AppConfig().config.get(environment)

    if config.isAuth():
        config.connectToSnowflake()
        results = config.snowflake_connection.listStreamlits(
            database=env_conf.get('database'),
            schema=env_conf.get('schema'),
            role=env_conf.get('role'),
            warehouse=env_conf.get('warehouse'),
        )
        print_db_cursor(results)


@app.command("describe")
def streamlit_describe(
    environment: str = EnvironmentOption,
    name: str = typer.Argument(..., help='Name of streamlit to be deployed.'),
):
    """
    Describe a streamlit app.
    """
    env_conf = AppConfig().config.get(environment)

    if config.isAuth():
        config.connectToSnowflake()
        description, url = config.snowflake_connection.describeStreamlit(
            name,
            database=env_conf.get('database'),
            schema=env_conf.get('schema'),
            role=env_conf.get('role'),
            warehouse=env_conf.get('warehouse'),
        )
        print_db_cursor(description)
        print_db_cursor(url)


@app.command("create")
def streamlit_create(
    environment: str = EnvironmentOption,
    name: str = typer.Argument(..., help='Name of streamlit to be created.'),
    file: Path = typer.Option(
        'streamlit_app.py',
        exists=True,
        readable=True,
        file_okay=True,
        help='Path to streamlit file',
    ),
):
    """
    Create a streamlit app named NAME.
    """
    env_conf = AppConfig().config.get(environment)

    if config.isAuth():
        config.connectToSnowflake()
        results = config.snowflake_connection.createStreamlit(
            database=env_conf.get('database'),
            schema=env_conf.get('schema'),
            role=env_conf.get('role'),
            warehouse=env_conf.get('warehouse'),
            name=name,
            file=str(file),
        )
        print_db_cursor(results)


@app.command("deploy")
def streamlit_deploy(
    environment: str = EnvironmentOption,
    name: str = typer.Argument(..., help='Name of streamlit to be deployed.'),
    file: Path = typer.Option(
        'streamlit_app.py',
        exists=True,
        readable=True,
        file_okay=True,
        help='Path to streamlit file',
    ),
    open_: bool = typer.Option(
        False, "--open", "-o", help='Open streamlit in browser.',
    ),
):
    """
    Deploy streamlit with NAME.
    """
    env_conf = AppConfig().config.get(environment)

    if config.isAuth():
        config.connectToSnowflake()
        results = config.snowflake_connection.deployStreamlit(
            name=name, file_path=str(file), stage_path='/',
            role=env_conf.get('role'), database=env_conf.get('database'),
            schema=env_conf.get('schema'),
            overwrite=True,
        )

        url = results.fetchone()[0]
        if open_:
            typer.launch(url)
        else:
            print(url)
