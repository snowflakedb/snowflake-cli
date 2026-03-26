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


from typing import List, Optional

import typer
from click import ClickException
from snowflake.cli._plugins.notebook.project.manager import NotebookProjectManager
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import MessageResult, QueryResult

app = SnowTyperFactory(
    name="project",
    help="Manages notebook projects in Snowflake.",
)


@app.command("create", requires_connection=True)
def create(
    name: str = typer.Argument(
        None,
        help="Name of the Snowpark project.",
        show_default=False,
    ),
    source: str = typer.Option(
        None,
        "--source",
        help="Source location of the notebook project. Supports stage path (starting with '@') or workspace path (starting with 'snow://workspace/').",
        show_default=False,
    ),
    comment: Optional[str] = typer.Option(
        None,
        "--comment",
        help="Comment for the notebook project.",
        show_default=False,
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="Overwrite the notebook project if it already exists.",
        show_default=False,
    ),
    skip_if_exists: bool = typer.Option(
        False,
        "--skip-if-exists",
        help="Skip the creation of the notebook project if it already exists.",
        show_default=False,
    ),
    **options,
):
    """Creates a notebook project in Snowflake."""
    if not name:
        raise ClickException("Name is required.")
    if not source:
        raise ClickException("Source is required.")
    if overwrite and skip_if_exists:
        raise ClickException("overwrite and skip_if_exists cannot be used together")
    manager = NotebookProjectManager()
    processed_source = manager.process_source(source)
    return MessageResult(
        manager.create(name, processed_source, comment, overwrite, skip_if_exists)
    )


@app.command("list", requires_connection=True)
def list_projects(**options):
    """Lists notebook projects in Snowflake."""
    manager = NotebookProjectManager()
    return QueryResult(manager.list_projects())


@app.command(requires_connection=True)
def drop(
    name: str = typer.Argument(
        None, help="Name of the notebook project.", show_default=False
    ),
    **options,
):
    """Drops a notebook project in Snowflake."""
    if not name:
        raise ClickException("Name is required.")
    manager = NotebookProjectManager()
    return MessageResult(manager.drop(name))


@app.command(requires_connection=True)
def execute(
    name: str = typer.Argument(
        None, help="Name of the notebook project.", show_default=False
    ),
    arguments: Optional[List[str]] = typer.Argument(
        None,
        metavar="ARGUMENTS",
        help="Arguments to pass to the notebook project.",
        show_default=False,
    ),
    main_file: str = typer.Option(
        None,
        "--main-file",
        help="Main file of the notebook project.",
        show_default=False,
    ),
    compute_pool: Optional[str] = typer.Option(
        None,
        "--compute-pool",
        help="Compute pool to run the notebook project on.",
        show_default=False,
    ),
    query_warehouse: Optional[str] = typer.Option(
        None,
        "--query-warehouse",
        help="Query warehouse to run the notebook project on.",
        show_default=False,
    ),
    runtime: Optional[str] = typer.Option(
        None,
        "--runtime",
        help="Runtime to run the notebook project on.",
        show_default=False,
    ),
    requirements_file: Optional[str] = typer.Option(
        None,
        "--requirements-file",
        help="Requirements file to use for the notebook project.",
        show_default=False,
    ),
    external_access_integrations: Optional[List[str]] = typer.Option(
        None,
        "--external-access-integrations",
        help="External access integrations to use for the notebook project.",
        show_default=False,
    ),
    **options,
):
    """Executes a notebook project in Snowflake."""
    if not name:
        raise ClickException("Name is required.")
    if not main_file:
        raise ClickException("Main file is required.")
    manager = NotebookProjectManager()
    return MessageResult(
        manager.execute(
            name,
            arguments,
            main_file,
            compute_pool,
            query_warehouse,
            runtime,
            requirements_file,
            external_access_integrations,
        )
    )
