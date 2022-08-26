#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
from rich import print
from rich.console import Console
from rich.table import Table
import tempfile
import typer

import click
import prettytable
import toml

from snowcli import config, utils
from snowcli.config import AppConfig
from snowcli.snowsql_config import SnowsqlConfig

console = Console()

_global_options = [
    click.option('--environment', '-e', help='Environment name', default='dev')
]

def global_options(func):
    for option in reversed(_global_options):
        func = option(func)
    return func

@click.group()
def streamlit():
    pass

app = typer.Typer()

EnvironmentOption = typer.Option("dev", help='Environment name')

def print_db_cursor(cursor):
    if cursor.description:
        table = Table(*[col[0] for col in cursor.description])
        for row in cursor.fetchall():
            table.add_row(*[str(c) for c in row])
        print(table)

@app.command()
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
            warehouse=env_conf.get('warehouse'))
        print_db_cursor(results)

typer_click_object = typer.main.get_command(app)
streamlit.add_command(typer_click_object, "list")

@click.command("create")
@global_options
@click.option('--name', '-n', help='Name of streamlit to be created.', required=True)
@click.option('--file', '-f', help='Path to streamlit file', default='streamlit_app.py', required=True)
def streamlit_create(environment, name, file):
    env_conf = AppConfig().config.get(environment)

    if config.isAuth():
        config.connectToSnowflake()
        results = config.snowflake_connection.createStreamlit(
            database=env_conf.get('database'),
            schema=env_conf.get('schema'),
            role=env_conf.get('role'),
            warehouse=env_conf.get('warehouse'),
            name=name,
            file=file)
        table = prettytable.from_db_cursor(results)
        click.echo(table)

@click.command("deploy")
@global_options
@click.option('--name', '-n', help='Name of streamlit to be deployed', required=True)
@click.option('--file', '-f', help='Path to streamlit file', default='streamlit_app.py', required=True)
@click.option('--open/--no-open', '-o', 'open_', help='Open streamlit in browser', default=False, required=False)
def streamlit_deploy(environment, name, file, open_):
    env_conf = AppConfig().config.get(environment)

    if config.isAuth():
        config.connectToSnowflake()
        results = config.snowflake_connection.deployStreamlit(
            name=name, file_path=file, stage_path='/',
            role=env_conf.get('role'), overwrite=True)

        url = results.fetchone()[0]
        if open_:
            click.launch(url)
        else:
            click.echo(url)

streamlit.add_command(streamlit_create, 'create')
streamlit.add_command(streamlit_deploy, 'deploy')
