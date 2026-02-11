# Copyright (c) 2026 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import typer
from click import ClickException
from snowflake.cli._plugins.snowpark.project.manager import SnowflakeProjectManager
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import MessageResult, QueryResult

app = SnowTyperFactory(
    name="project",
    help="Manages Snowpark projects.",
)


@app.command("create", requires_connection=True)
def create(
    name: str = typer.Argument(
        None,
        help="Name of the Snowpark project.",
        show_default=False,
    ),
    stage: str = typer.Option(
        None,
        "--stage",
        help="The stage containing the project files.",
        show_default=False,
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="Overwrite the project if it already exists.",
    ),
    set_session_config: bool = typer.Option(
        False,
        "--set-session-config",
        help="Set the session config for Snowpark project.",
    ),
    **options,
):
    """
    Creates a Snowpark project.
    """
    if not name:
        raise ClickException("Project name is required.")
    if not stage:
        raise ClickException("Stage is required.")

    manager = SnowflakeProjectManager(set_session_config=set_session_config)
    return MessageResult(manager.create(name=name, stage=stage, overwrite=overwrite))


@app.command("drop", requires_connection=True)
def drop(
    name: str = typer.Argument(
        None,
        help="Name of the Snowpark project.",
        show_default=False,
    ),
    set_session_config: bool = typer.Option(
        False,
        "--set-session-config",
        help="Set the session config for Snowpark project.",
    ),
    **options,
):
    """
    Drops a Snowpark project.
    """
    if not name:
        raise ClickException("Project name is required.")

    manager = SnowflakeProjectManager(set_session_config=set_session_config)
    return MessageResult(manager.drop(name=name))


@app.command("list", requires_connection=True)
def list_projects(
    set_session_config: bool = typer.Option(
        False,
        "--set-session-config",
        help="Set the session config for Snowpark project.",
    ),
    **options,
):
    """
    Lists all Snowpark projects.
    """
    manager = SnowflakeProjectManager(set_session_config=set_session_config)
    return QueryResult(manager.list_projects())


@app.command("execute", requires_connection=True)
def execute(
    name: str = typer.Argument(
        None,
        help="Name of the Snowpark project.",
        show_default=False,
    ),
    entrypoint: str = typer.Option(
        None,
        "--entrypoint",
        metavar="STAGE FILE PATH",
        help="The path on the project stage to the entrypoint file for the Snowpark project.",
        show_default=False,
    ),
    set_session_config: bool = typer.Option(
        False,
        "--set-session-config",
        help="Set the session config for Snowpark project.",
    ),
    **options,
):
    """
    Executes a Snowpark project.
    """
    if not name:
        raise ClickException("Project name is required.")
    if not entrypoint:
        raise ClickException("Entrypoint is required.")
    manager = SnowflakeProjectManager(set_session_config=set_session_config)
    return QueryResult(manager.execute(name=name, entrypoint=entrypoint))
