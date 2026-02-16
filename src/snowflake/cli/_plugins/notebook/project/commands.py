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


from typing import Optional

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
    **options,
):
    """Creates a notebook project in Snowflake."""
    if not name:
        raise ClickException("Name is required.")
    if not source:
        raise ClickException("Source is required.")
    manager = NotebookProjectManager()
    return MessageResult(manager.create(name, source, comment))


@app.command("list", requires_connection=True)
def list_projects(**options):
    """Lists notebook projects in Snowflake."""
    manager = NotebookProjectManager()
    return QueryResult(manager.list_projects())


@app.command(requires_connection=True)
def delete(**options):
    """Deletes a notebook project in Snowflake."""
    pass


@app.command(requires_connection=True)
def execute(**options):
    """Executes a notebook project in Snowflake."""
    pass
