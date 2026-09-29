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
from typing import Dict, List, Optional

import typer
from click import ClickException
from snowflake.cli._plugins.git.manager import GitManager
from snowflake.cli._plugins.object.command_aliases import (
    add_object_command_aliases,
    scope_option,
)
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli.api.commands.command_docs import (
    PYTHON_EXECUTE_VERSION_SUPPORT,
    CommandDocs,
    Example,
    bullet,
    bullet_list,
    code,
    link,
    plain_text,
)
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
from snowflake.connector import DictCursor

_GIT_RELATED = (
    link("/developer-guide/snowflake-cli/index"),
    link(
        "/developer-guide/snowflake-cli/command-reference/overview",
        "Snowflake CLI command reference",
    ),
    link("/developer-guide/snowflake-cli/command-reference/git-commands/overview"),
)
_GIT_COPY_RELATED = _GIT_RELATED + (
    link("/developer-guide/snowflake-cli/git/copy-files"),
)
_GIT_EXECUTE_RELATED = _GIT_RELATED + (
    link("/developer-guide/snowflake-cli/git/execute-sql"),
)
_OBJECT_ALIAS_DOCS = CommandDocs(
    related=_GIT_RELATED,
    usage_notes=(plain_text("None."),),
)

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
    list_docs=_OBJECT_ALIAS_DOCS,
    describe_docs=_OBJECT_ALIAS_DOCS,
    drop_docs=_OBJECT_ALIAS_DOCS,
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
    if "'" in url:
        raise ClickException("Url address must not contain single-quote characters")


def _unique_new_object_name(
    om: ObjectManager, object_type: ObjectType, proposed_fqn: FQN
) -> str:
    existing_objects: List[Dict] = om.show(
        object_type=object_type.value.cli_name,
        like=f"{proposed_fqn.name}%",
        cursor_class=DictCursor,
    ).fetchall()
    existing_names = set(o["name"].upper() for o in existing_objects)

    result = proposed_fqn.name
    i = 1
    while result.upper() in existing_names:
        result = proposed_fqn.name + str(i)
        i += 1
    return result


@app.command(
    "setup",
    requires_connection=True,
    docs=CommandDocs(
        related=(
            link("/developer-guide/snowflake-cli/index"),
            link(
                "/developer-guide/snowflake-cli/command-reference/overview",
                "Snowflake CLI command reference",
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/git-commands/overview"
            ),
        ),
        usage_notes=(
            plain_text(
                "The ",
                code("snow git setup"),
                " command prompts for the following information:",
            ),
            bullet_list(
                bullet(
                    "URL: address of repository to use for ",
                    code("git clone"),
                    " operation.",
                ),
                bullet(
                    "Secret: Snowflake secret containing authentication credentials. "
                    "Not needed if origin repository does not require authentication "
                    "for read-only operations, such as clone and fetch."
                ),
                bullet(
                    "API integration: object allowing Snowflake to interact with a "
                    "Git repository."
                ),
            ),
            plain_text(
                "If the role or user specified in your ",
                link(
                    "/developer-guide/snowflake-cli/connecting/configure-connections",
                    "connection",
                ),
                " has not been granted, executing this command generates an error "
                "similar to the following: ",
                code(
                    "003001 (42501): 01b2f095-0508-c66d-0001-c1be009a66ee: SQL access "
                    "control error: Insufficient privileges to operate on account XXX"
                ),
                ". In this situation, you should check your connection configuration "
                "or ask your account administrator to give you the necessary privileges "
                "or to create the integration for you. For more information, see ",
                link("/developer-guide/git/git-setting-up"),
                ".",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "Create a repository that requires a secret and credentials"
                ),
                command=(
                    "$ snow git setup " + "snow" + "cli_git\n"
                    "Origin url: https://github.com/snowflakedb/snowflake-cli.git\n"
                    "Use secret for authentication? [y/N]: y\n"
                    "Secret identifier (will be created if not exists) "
                    "[" + "snow" + "cli_git_secret]: new_secret\n"
                    "Secret 'new_secret' will be created\n"
                    "username: john_doe\n"
                    "password/token: ****\n"
                    "API integration identifier (will be created if not exists) "
                    "[" + "snow" + "cli_git_api_integration]:"
                ),
                output=(
                    "Secret 'new_secret' successfully created.\n"
                    "API integration "
                    + "snow"
                    + "cli_git_api_integration successfully created.\n"
                    "+------------------------------------------------------+\n"
                    "| status                                               |\n"
                    "|------------------------------------------------------|\n"
                    "| Git Repository SNOWCLI_GIT was successfully created. |\n"
                    "+------------------------------------------------------+"
                ),
            ),
            Example(
                description=plain_text(
                    "Create a repository without a secret and an existing API "
                    "integration ID"
                ),
                command=(
                    "$ snow git setup " + "snow" + "cli_git\n"
                    "Origin url: https://github.com/snowflakedb/snowflake-cli.git\n"
                    "Use secret for authentication [y/N]: n\n"
                    "API integration identifier (will be created if not exists) "
                    "[" + "snow" + "cli_git_api_integration]: EXISTING_INTEGRATION"
                ),
                output=(
                    "Using existing API integration 'EXISTING_INTEGRATION'.\n"
                    "+------------------------------------------------------+\n"
                    "| status                                               |\n"
                    "|------------------------------------------------------|\n"
                    "| Git Repository SNOWCLI_GIT was successfully created. |\n"
                    "+------------------------------------------------------+"
                ),
            ),
        ),
    ),
)
def setup(
    repository_name: FQN = RepoNameArgument,
    **options,
) -> CommandResult:
    """Sets up a git repository object."""
    manager = GitManager()
    om = ObjectManager()
    _assure_repository_does_not_exist(om, repository_name)

    url = typer.prompt("Origin url")
    _validate_origin_url(url)

    secret_needed = typer.confirm("Use secret for authentication?")
    should_create_secret = False
    secret_name = None
    if secret_needed:
        default_secret_name = (
            FQN.from_string(f"{repository_name.name}_secret")
            .set_schema(repository_name.schema)
            .set_database(repository_name.database)
        )
        default_secret_name.set_name(
            _unique_new_object_name(
                om, object_type=ObjectType.SECRET, proposed_fqn=default_secret_name
            ),
        )
        secret_name = FQN.from_string(
            typer.prompt(
                "Secret identifier (will be created if not exists)",
                default=default_secret_name.name,
            )
        )
        if not secret_name.database:
            secret_name.set_database(repository_name.database)
        if not secret_name.schema:
            secret_name.set_schema(repository_name.schema)

        if om.object_exists(
            object_type=ObjectType.SECRET.value.cli_name, fqn=secret_name
        ):
            cli_console.step(f"Using existing secret '{secret_name}'")
        else:
            should_create_secret = True
            cli_console.step(f"Secret '{secret_name}' will be created")
            secret_username = typer.prompt("username")
            secret_password = typer.prompt("password/token", hide_input=True)

    # API integration is an account-level object
    api_integration = FQN.from_string(f"{repository_name.name}_api_integration")
    api_integration.set_name(
        typer.prompt(
            "API integration identifier (will be created if not exists)",
            default=_unique_new_object_name(
                om,
                object_type=ObjectType.INTEGRATION,
                proposed_fqn=api_integration,
            ),
        )
    )

    if should_create_secret:
        manager.create_password_secret(
            name=secret_name, username=secret_username, password=secret_password
        )
        cli_console.step(f"Secret '{secret_name}' successfully created.")

    if not om.object_exists(
        object_type=ObjectType.INTEGRATION.value.cli_name, fqn=api_integration
    ):
        manager.create_api_integration(
            name=api_integration,
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
    docs=CommandDocs(
        related=_GIT_RELATED,
        usage_notes=(plain_text("None."),),
        examples=(
            Example(
                description=plain_text(
                    "For example, to list all of the branches in a repository named ",
                    code("my_snow_git"),
                    ", enter the following command:",
                ),
                command="snow git list-branches my_snow_git",
                output="show git branches in my_snow_git\n+--------------------------------------------------------------------------------------------------------------------------------------------+\n| name                                     | path                                     | checkouts | commit_hash                              |\n|------------------------------------------+------------------------------------------+-----------+------------------------------------------|\n| SNOW-1011750-service-create-options      | /branches/SNOW-1011750-service-create-op |           | 729855df0104c8d0ef1c7a3e8f79fe50c6c8d2fa |\n|                                          | tions                                    |           |                                          |\n| SNOW-1011775-containers-to-spcs-int-test | /branches/SNOW-1011775-containers-to-spc |           | e81b00de6b0eb73a99a7baaa39b0afa5ea1202d0 |\n| s                                        | s-int-tests                              |           |                                          |\n| SNOW-1105629-git-integration-tests       | /branches/SNOW-1105629-git-integration-t |           | 712b07b5e692624c34caabe07d64801615ce5f0f |\n+--------------------------------------------------------------------------------------------------------------------------------------------+",
            ),
        ),
    ),
)
def list_branches(
    repository_name: FQN = RepoNameArgument,
    like=like_option(
        help_example='`list-branches --like "%_test"` lists all branches that end with "_test"'
    ),
    **options,
) -> CommandResult:
    """List all branches in the repository."""
    return QueryResult(
        GitManager().show_branches(repo_name=repository_name.identifier, like=like)
    )


@app.command(
    "list-tags",
    requires_connection=True,
    docs=CommandDocs(
        related=_GIT_RELATED,
        usage_notes=(plain_text("None."),),
        examples=(
            Example(
                description=plain_text(
                    "For example, to list all of the tags in a repository named ",
                    code("my_snow_git"),
                    ", enter the following command:",
                ),
                command="snow git list-tags my_snow_git",
                output="show git tags in my_snow_git\n+--------------------------------------------------------------------------------------------------------------+\n| name           | path                 | commit_hash                 | author                       | message |\n|----------------+----------------------+-----------------------------+------------------------------+---------|\n| v2.0.0rc3      | /tags/v2.0.0rc3      | 2b019d2841da823d8001f23c6f3 | None                         | None    |\n|                |                      | 064e5899142a0               |                              |         |\n| v2.1.0-rc0     | /tags/v2.1.0-rc0     | 829887b758b43b86959611dd612 | None                         | None    |\n|                |                      | 7638da75cf871               |                              |         |\n| v2.1.0-rc1     | /tags/v2.1.0-rc1     | b7efe1fe9c0925b95ba214e233b | None                         | None    |\n|                |                      | 18924fa0404b3               |                              |         |\n+--------------------------------------------------------------------------------------------------------------+",
            ),
        ),
    ),
)
def list_tags(
    repository_name: FQN = RepoNameArgument,
    like=like_option(
        help_example='`list-tags --like "v2.0%"` lists all tags that start with "v2.0"'
    ),
    **options,
) -> CommandResult:
    """List all tags in the repository."""
    return QueryResult(
        GitManager().show_tags(repo_name=repository_name.identifier, like=like)
    )


@app.command(
    "list-files",
    requires_connection=True,
    docs=CommandDocs(
        related=_GIT_RELATED,
        usage_notes=(plain_text("None."),),
        examples=(
            Example(
                description=plain_text(
                    "The following example lists all of the files in the ",
                    code("tests/"),
                    " directory of the ",
                    code("my_snow_git"),
                    " repository marked with the ",
                    code("v2.0.0"),
                    " tag:",
                ),
                command='snow git list-files @my_snow_git/tags/v2.0.0/tests --pattern ".*\\.toml"',
                output=(
                    "ls @"
                    + "snow"
                    + "cli_git/tags/v2.0.0/tests pattern = '.*\\.toml'\n"
                    "+-----------------------------------------------------------------------------------------------------------------------------------------+\n"
                    "| name                                            | size | md5  | sha1                                     | last_modified                |\n"
                    "|-------------------------------------------------+------+------+------------------------------------------+------------------------------|\n"
                    "| "
                    + "snow"
                    + "cli_git/tags/v2.0.0/tests/empty_config.toml | 0    | None | e69de29bb2d1d6434b8b29ae775ad8c2e48c5391 | Mon, 5 Feb 2024 13:16:25 GMT |\n"
                    "| "
                    + "snow"
                    + "cli_git/tags/v2.0.0/tests/test.toml         | 381  | None | 45f1c00f16eba1b7bc7b4ab2982afe95d0161e7f | Mon, 5 Feb 2024 13:16:25 GMT |\n"
                    "+-----------------------------------------------------------------------------------------------------------------------------------------+"
                ),
            ),
        ),
    ),
)
def list_files(
    repository_path: str = RepoPathArgument,
    pattern=PatternOption,
    **options,
) -> CommandResult:
    """List files from given state of git repository."""
    return QueryResult(
        GitManager().list_files(stage_name=repository_path, pattern=pattern)
    )


@app.command(
    "fetch",
    requires_connection=True,
    docs=CommandDocs(
        related=_GIT_RELATED,
        usage_notes=(plain_text("None."),),
        examples=(
            Example(
                description=plain_text(
                    "The following example refreshes a repository named ",
                    code("my_snow_git"),
                    ":",
                ),
                command="snow git fetch my_snow_git",
                output="alter Git repository my_snow_git fetch\n+-------------------------------------------------------------------+\n| status                                                            |\n|-------------------------------------------------------------------|\n| Git Repository MY_SNOW_GIT is up to date. No change was fetched.. |\n+-------------------------------------------------------------------+",
            ),
        ),
    ),
)
def fetch(
    repository_name: FQN = RepoNameArgument,
    **options,
) -> CommandResult:
    """Fetch changes from origin to Snowflake repository."""
    return QueryResult(GitManager().fetch(fqn=repository_name))


@app.command(
    "copy",
    requires_connection=True,
    docs=CommandDocs(
        related=_GIT_COPY_RELATED,
        usage_notes=(plain_text("None."),),
        examples=(
            Example(
                description=plain_text(
                    "This example creates a ",
                    code("snow" + "cli" + "2.0/"),
                    " directory on stage ",
                    code("@public"),
                    " and copies all files from the commit marked with tag ",
                    code("v2.0.0"),
                    " into that directory:",
                ),
                command="snow git copy @my_snow_git/tags/v2.0.0/ @public/"
                + "snow"
                + "cli"
                + "2.0/",
            ),
            Example(
                description=plain_text(
                    "The following example creates a ",
                    code("plugin_tests"),
                    " directory in the local file system and downloads the contents of the ",
                    code("tests/plugin"),
                    " directory into it.",
                ),
                command="snow git copy @"
                + "snow"
                + "cli_git/branches/main/tests/plugin plugin_tests/",
            ),
        ),
    ),
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


@app.command(
    "execute",
    requires_connection=True,
    docs=CommandDocs(
        related=_GIT_EXECUTE_RELATED,
        usage_notes=(
            PYTHON_EXECUTE_VERSION_SUPPORT,
            plain_text(
                "You can use glob-like patterns to filter the files, such as ",
                code("@my_repo/branches/main/*.sql"),
                " and ",
                code("@my_repo/branches/main/dev/*"),
                ". The command only executes files with a ",
                code(".sql"),
                " extension.",
            ),
            plain_text(
                "When using Jinja templates for the SQL files, you can pass template variables using ",
                code("-D"),
                " or ",
                code("--variable"),
                " option, such as ",
                code('-D "<key>=<value>"'),
                ". You must enclose string values in single quotes (",
                code("''"),
                ").",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "The following example shows how to execute SQL commands in all files within the ",
                    code("project"),
                    " directory that match a regular expression.",
                ),
                command='snow git execute "@git_test/branches/main/projects/script*.sql"',
                output="SUCCESS - git_test/branches/main/projects/script1.sql\nSUCCESS - git_test/branches/main/projects/script2.sql\nSUCCESS - git_test/branches/main/projects/script3.sql\n+---------------------------------------------------------------+\n| File                                        | Status  | Error |\n|---------------------------------------------+---------+-------|\n| git_test/branches/main/projects/script1.sql | SUCCESS | None  |\n| git_test/branches/main/projects/script2.sql | SUCCESS | None  |\n| git_test/branches/main/projects/script3.sql | SUCCESS | None  |\n+---------------------------------------------------------------+",
            ),
        ),
    ),
)
def execute(
    repository_path: str = RepoPathArgument,
    on_error: OnErrorType = OnErrorOption,
    variables: Optional[List[str]] = ExecuteVariablesOption,
    **options,
):
    """
    Execute immediate all files from the repository path. Files can be filtered with a glob-like pattern,
    e.g. `@my_repo/branches/main/*.sql`, `@my_repo/branches/main/dev/*`. Only files with `.sql`
    or `.py` extension will be executed.
    """
    results = GitManager().execute(
        stage_path_str=repository_path,
        on_error=on_error,
        variables=variables,
        requires_temporary_stage=True,
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
