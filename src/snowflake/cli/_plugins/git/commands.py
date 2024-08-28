# Copyright (c) 2024 Snowflake Inc.
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

from __future__ import annotations

import itertools
import logging
from os import path
from pathlib import Path
from typing import List, Optional

import typer
from click import ClickException
from snowflake.cli._plugins.git.manager import GitManager
from snowflake.cli._plugins.object.command_aliases import (
    add_object_command_aliases,
    scope_option,
)
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli.api.commands.common import OnErrorType
from snowflake.cli.api.commands.flags import (
    ExecuteVariablesOption,
    OnErrorOption,
    PatternOption,
    identifier_argument,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.output.types import CollectionResult, CommandResult, QueryResult
from snowflake.cli.api.utils.path_utils import is_stage_path

app = SnowTyperFactory(
    name="git",
    help="Manages git repositories in Snowflake.",
)
log = logging.getLogger(__name__)


def _repo_path_argument_callback(path):
    # All repository paths must start with repository scope:
    # "@repo_name/tag/example_tag/*"
    if not is_stage_path(path) or path.count("/") < 3:
        raise ClickException(
            "REPOSITORY_PATH should be a path to git repository stage with scope provided."
            " Path to the repository root must end with '/'."
            " For example: @my_repo/branches/main/"
        )

    return path


RepoNameArgument = identifier_argument(sf_object="git repository", example="my_repo")
RepoPathArgument = typer.Argument(
    metavar="REPOSITORY_PATH",
    help=(
        "Path to git repository stage with scope provided."
        " Path to the repository root must end with '/'."
        " For example: @my_repo/branches/main/"
    ),
    callback=_repo_path_argument_callback,
    show_default=False,
)
add_object_command_aliases(
    app=app,
    object_type=ObjectType.GIT_REPOSITORY,
    name_argument=RepoNameArgument,
    like_option=like_option(
        help_example='`list --like "my%"` lists all git repositories with name that begin with “my”',
    ),
    scope_option=scope_option(help_example="`list --in database my_db`"),
)

from snowflake.cli.api.identifiers import FQN


def _assure_repository_does_not_exist(om: ObjectManager, repository_name: FQN) -> None:
    if om.object_exists(
        object_type=ObjectType.GIT_REPOSITORY.value.cli_name, fqn=repository_name
    ):
        raise ClickException(f"Repository '{repository_name}' already exists")


def _validate_origin_url(url: str) -> None:
    if not url.startswith("https://"):
        raise ClickException("Url address should start with 'https'")


@app.command("setup", requires_connection=True)
def setup(
    repository_name: FQN = RepoNameArgument,
    **options,
) -> CommandResult:
    """
    Sets up a git repository object.

    ## Usage notes

    You will be prompted for:

    * url - address of repository to be used for git clone operation

    * secret - Snowflake secret containing authentication credentials. Not needed if origin repository does not require
    authentication for RO operations (clone, fetch)

    * API integration - object allowing Snowflake to interact with git repository.
    """
    manager = GitManager()
    om = ObjectManager()
    _assure_repository_does_not_exist(om, repository_name)

    url = typer.prompt("Origin url")
    _validate_origin_url(url)

    secret_needed = typer.confirm("Use secret for authentication?")
    should_create_secret = False
    secret_name = None
    if secret_needed:
        secret_name = f"{repository_name}_secret"
        secret_name = typer.prompt(
            "Secret identifier (will be created if not exists)", default=secret_name
        )
        secret_fqn = FQN.from_string(secret_name)
        if om.object_exists(
            object_type=ObjectType.SECRET.value.cli_name, fqn=secret_fqn
        ):
            cli_console.step(f"Using existing secret '{secret_name}'")
        else:
            should_create_secret = True
            cli_console.step(f"Secret '{secret_name}' will be created")
            secret_username = typer.prompt("username")
            secret_password = typer.prompt("password/token", hide_input=True)

    api_integration = f"{repository_name}_api_integration"
    api_integration = typer.prompt(
        "API integration identifier (will be created if not exists)",
        default=api_integration,
    )
    api_integration_fqn = FQN.from_string(api_integration)

    if should_create_secret:
        manager.create_password_secret(
            name=secret_fqn, username=secret_username, password=secret_password
        )
        cli_console.step(f"Secret '{secret_name}' successfully created.")

    if not om.object_exists(
        object_type=ObjectType.INTEGRATION.value.cli_name, fqn=api_integration_fqn
    ):
        manager.create_api_integration(
            name=api_integration_fqn,
            api_provider="git_https_api",
            allowed_prefix=url,
            secret=secret_name,
        )
        cli_console.step(f"API integration '{api_integration}' successfully created.")
    else:
        cli_console.step(f"Using existing API integration '{api_integration}'.")

    return QueryResult(
        manager.create(
            repo_name=repository_name,
            url=url,
            api_integration=api_integration,
            secret=secret_name,
        )
    )


@app.command(
    "list-branches",
    requires_connection=True,
)
def list_branches(
    repository_name: FQN = RepoNameArgument,
    like=like_option(
        help_example='`list-branches --like "%_test"` lists all branches that end with "_test"'
    ),
    **options,
) -> CommandResult:
    """
    List all branches in the repository.
    """
    return QueryResult(
        GitManager().show_branches(repo_name=repository_name.identifier, like=like)
    )


@app.command(
    "list-tags",
    requires_connection=True,
)
def list_tags(
    repository_name: FQN = RepoNameArgument,
    like=like_option(
        help_example='`list-tags --like "v2.0%"` lists all tags that start with "v2.0"'
    ),
    **options,
) -> CommandResult:
    """
    List all tags in the repository.
    """
    return QueryResult(
        GitManager().show_tags(repo_name=repository_name.identifier, like=like)
    )


@app.command(
    "list-files",
    requires_connection=True,
)
def list_files(
    repository_path: str = RepoPathArgument,
    pattern=PatternOption,
    **options,
) -> CommandResult:
    """
    List files from given state of git repository.
    """
    return QueryResult(
        GitManager().list_files(stage_name=repository_path, pattern=pattern)
    )


@app.command(
    "fetch",
    requires_connection=True,
)
def fetch(
    repository_name: FQN = RepoNameArgument,
    **options,
) -> CommandResult:
    """
    Fetch changes from origin to Snowflake repository.
    """
    return QueryResult(GitManager().fetch(fqn=repository_name))


@app.command(
    "copy",
    requires_connection=True,
)
def copy(
    repository_path: str = RepoPathArgument,
    destination_path: str = typer.Argument(
        help="Target path for copy operation. Should be a path to a directory on remote stage or local file system.",
        show_default=False,
    ),
    parallel: int = typer.Option(
        4,
        help="Number of parallel threads to use when downloading files.",
    ),
    **options,
):
    """
    Copies all files from given state of repository to local directory or stage.

    If the source path ends with '/', the command copies contents of specified directory.
    Otherwise, it creates a new directory or file in the destination directory.
    """
    is_copy = is_stage_path(destination_path)
    if is_copy:
        return QueryResult(
            GitManager().copy_files(
                source_path=repository_path, destination_path=destination_path
            )
        )
    return get(
        source_path=repository_path,
        destination_path=destination_path,
        parallel=parallel,
    )


@app.command("execute", requires_connection=True)
def execute(
    repository_path: str = RepoPathArgument,
    on_error: OnErrorType = OnErrorOption,
    variables: Optional[List[str]] = ExecuteVariablesOption,
    **options,
):
    """
    Execute immediate all files from the repository path. Files can be filtered with glob like pattern,
    e.g. `@my_repo/branches/main/*.sql`, `@my_repo/branches/main/dev/*`. Only files with `.sql`
    extension will be executed.
    """
    results = GitManager().execute(
        stage_path=repository_path, on_error=on_error, variables=variables
    )
    return CollectionResult(results)


def get(source_path: str, destination_path: str, parallel: int):
    target = Path(destination_path).resolve()

    cursors = GitManager().get_recursive(
        stage_path=source_path, dest_path=target, parallel=parallel
    )
    results = [list(QueryResult(c).result) for c in cursors]
    flattened_results = list(itertools.chain.from_iterable(results))
    sorted_results = sorted(
        flattened_results,
        key=lambda e: (path.dirname(e["file"]), path.basename(e["file"])),
    )
    return CollectionResult(sorted_results)
