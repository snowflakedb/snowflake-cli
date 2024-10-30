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
from dataclasses import dataclass
from os import path
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
    IdentifierType,
    OnErrorOption,
    PatternOption,
    identifier_argument,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import IncompatibleParametersError
from snowflake.cli.api.output.types import CollectionResult, CommandResult, QueryResult
from snowflake.cli.api.utils.path_utils import is_stage_path
from snowflake.connector import DictCursor
from typer.models import OptionInfo

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
OriginUrlOption: OptionInfo = typer.Option(
    None,
    "--url",
    help="Origin URL.",
    show_default=False,
)
NoSecretOption: OptionInfo = typer.Option(
    False,
    "--no-secret",
    help="Mark that your repository does not require a secret.",
    is_flag=True,
    show_default=False,
)
SecretIdentifierOption: OptionInfo = typer.Option(
    None,
    "--secret",
    help="The identifier of the secret (will be created if not exists).",
    show_default=False,
    click_type=IdentifierType(),
)
NewSecretDefaultNameOption: OptionInfo = typer.Option(
    False,
    "--new-secret-default-name",
    help="Use a default name for a newly created secret.",
    is_flag=True,
    show_default=False,
)
NewSecretUserOption: OptionInfo = typer.Option(
    None,
    "--new-secret-user",
    help="An user being a part of a new secret definition.",
    show_default=False,
)
NewSecretPasswordOption: OptionInfo = typer.Option(
    None,
    "--new-secret-password",
    "--new-secret-token",
    help="A password or a token being a part of new a secret definition.",
    show_default=False,
)
ApiIntegrationIdentifierOption: OptionInfo = typer.Option(
    None,
    "--api-integration",
    help="The identifier of the API integration (will be created if not exists).",
    show_default=False,
    click_type=IdentifierType(),
)
NewApiIntegrationDefaultNameOption: OptionInfo = typer.Option(
    False,
    "--new-api-integration-default-name",
    help="Use a default name for a newly created API integration.",
    is_flag=True,
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


def _validate_exclusivity_of_git_setup_params(
    use_no_secret: Optional[bool],
    provided_secret_identifier: Optional[FQN],
    use_new_secret_default_name: Optional[bool],
    new_secret_user: Optional[str],
    new_secret_password: Optional[str],
    provided_api_integration_identifier: Optional[FQN],
    use_new_api_integration_default_name: Optional[bool],
):
    def validate_params_incompatibility(
        param1: Tuple[OptionInfo, Optional[Any]],
        param2: Tuple[OptionInfo, Optional[Any]],
    ):
        def1: OptionInfo
        def2: OptionInfo
        (def1, value1) = param1
        (def2, value2) = param2
        if value1 and value2:
            raise IncompatibleParametersError(
                [def1.param_decls[0], def2.param_decls[0]]
            )

    def validate_incompability_with_no_secret(
        param_def: OptionInfo, param_value: Optional[Any]
    ):
        validate_params_incompatibility(
            param1=(param_def, param_value), param2=(NoSecretOption, use_no_secret)
        )

    validate_incompability_with_no_secret(
        SecretIdentifierOption, provided_secret_identifier
    )
    validate_incompability_with_no_secret(
        NewSecretDefaultNameOption, use_new_secret_default_name
    )
    validate_incompability_with_no_secret(NewSecretUserOption, new_secret_user)
    validate_incompability_with_no_secret(NewSecretPasswordOption, new_secret_password)
    validate_params_incompatibility(
        param1=(SecretIdentifierOption, provided_secret_identifier),
        param2=(NewSecretDefaultNameOption, use_new_secret_default_name),
    )
    validate_params_incompatibility(
        param1=(ApiIntegrationIdentifierOption, provided_api_integration_identifier),
        param2=(
            NewApiIntegrationDefaultNameOption,
            use_new_api_integration_default_name,
        ),
    )


def _collect_git_setup_secret_details(
    om: ObjectManager,
    repository_name: FQN,
    use_no_secret: Optional[bool],
    provided_secret_identifier: Optional[FQN],
    use_new_secret_default_name: Optional[bool],
    new_secret_user: Optional[str],
    new_secret_password: Optional[str],
) -> _GitSetupSecretDetails:
    secret_needed = (
        False
        if use_no_secret
        else True
        if provided_secret_identifier or use_new_secret_default_name
        else typer.confirm("Use secret for authentication?")
    )
    should_create_secret = False
    secret_name = None
    secret_username = None
    secret_password = None
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
        forced_default_secret_name: Optional[FQN] = (
            default_secret_name if use_new_secret_default_name else None
        )
        secret_name = (
            provided_secret_identifier
            or forced_default_secret_name
            or FQN.from_string(
                typer.prompt(
                    "Secret identifier (will be created if not exists)",
                    default=default_secret_name.name,
                )
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
            secret_username = new_secret_user or typer.prompt("username")
            secret_password = new_secret_password or typer.prompt(
                "password/token", hide_input=True
            )
    return _GitSetupSecretDetails(
        secret_needed=secret_needed,
        should_create_secret=should_create_secret,
        secret_name=secret_name,
        secret_username=secret_username,
        secret_password=secret_password,
    )


def _collect_api_integration_details(
    om: ObjectManager,
    repository_name: FQN,
    provided_api_integration_identifier: Optional[FQN],
    use_new_api_integration_default_name: Optional[bool],
) -> FQN:
    # API integration is an account-level object
    if provided_api_integration_identifier:
        api_integration = provided_api_integration_identifier
    else:
        api_integration = FQN.from_string(f"{repository_name.name}_api_integration")

        def generate_api_integration_name():
            return _unique_new_object_name(
                om,
                object_type=ObjectType.INTEGRATION,
                proposed_fqn=api_integration,
            )

        forced_default_api_integration_name: Optional[str] = (
            generate_api_integration_name()
            if use_new_api_integration_default_name
            else None
        )
        api_integration.set_name(
            forced_default_api_integration_name
            or typer.prompt(
                "API integration identifier (will be created if not exists)",
                default=generate_api_integration_name(),
            )
        )
    return api_integration


def _create_secret_and_api_integration_objects_if_needed(
    om: ObjectManager,
    manager: GitManager,
    url: str,
    secret_details: _GitSetupSecretDetails,
    api_integration: FQN,
):
    if secret_details.should_create_secret:
        manager.create_password_secret(
            name=secret_details.secret_name,
            username=secret_details.secret_username,
            password=secret_details.secret_password,
        )
        cli_console.step(f"Secret '{secret_details.secret_name}' successfully created.")

    if not om.object_exists(
        object_type=ObjectType.INTEGRATION.value.cli_name, fqn=api_integration
    ):
        manager.create_api_integration(
            name=api_integration,
            api_provider="git_https_api",
            allowed_prefix=url,
            secret=secret_details.secret_name,
        )
        cli_console.step(f"API integration '{api_integration}' successfully created.")
    else:
        cli_console.step(f"Using existing API integration '{api_integration}'.")


@app.command("setup", requires_connection=True)
def setup(
    repository_name: FQN = RepoNameArgument,
    url: Optional[str] = OriginUrlOption,
    use_no_secret: Optional[bool] = NoSecretOption,
    provided_secret_identifier: Optional[FQN] = SecretIdentifierOption,
    use_new_secret_default_name: Optional[bool] = NewSecretDefaultNameOption,
    new_secret_user: Optional[str] = NewSecretUserOption,
    new_secret_password: Optional[str] = NewSecretPasswordOption,
    provided_api_integration_identifier: Optional[FQN] = ApiIntegrationIdentifierOption,
    use_new_api_integration_default_name: Optional[
        bool
    ] = NewApiIntegrationDefaultNameOption,
    **options,
) -> CommandResult:
    """
    Sets up a git repository object.

    ## Usage notes

    You can use options to specify details, otherwise you will be prompted for:

    * url - address of repository to be used for git clone operation

    * secret - Snowflake secret containing authentication credentials. Not needed if origin repository does not require
    authentication for RO operations (clone, fetch)

    * API integration - object allowing Snowflake to interact with git repository.
    """
    _validate_exclusivity_of_git_setup_params(
        use_no_secret=use_no_secret,
        provided_secret_identifier=provided_secret_identifier,
        use_new_secret_default_name=use_new_secret_default_name,
        new_secret_user=new_secret_user,
        new_secret_password=new_secret_password,
        provided_api_integration_identifier=provided_api_integration_identifier,
        use_new_api_integration_default_name=use_new_api_integration_default_name,
    )

    manager = GitManager()
    om = ObjectManager()
    _assure_repository_does_not_exist(om, repository_name)

    url = url or typer.prompt("Origin url")
    _validate_origin_url(url)

    secret_details = _collect_git_setup_secret_details(
        om=om,
        repository_name=repository_name,
        use_no_secret=use_no_secret,
        provided_secret_identifier=provided_secret_identifier,
        use_new_secret_default_name=use_new_secret_default_name,
        new_secret_user=new_secret_user,
        new_secret_password=new_secret_password,
    )
    api_integration = _collect_api_integration_details(
        om=om,
        repository_name=repository_name,
        provided_api_integration_identifier=provided_api_integration_identifier,
        use_new_api_integration_default_name=use_new_api_integration_default_name,
    )
    _create_secret_and_api_integration_objects_if_needed(
        om=om,
        manager=manager,
        url=url,
        secret_details=secret_details,
        api_integration=api_integration,
    )

    return QueryResult(
        manager.create(
            repo_name=repository_name,
            url=url,
            api_integration=api_integration,
            secret=secret_details.secret_name,
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


@dataclass
class _GitSetupSecretDetails:
    secret_needed: bool = False
    should_create_secret: bool = False
    secret_name: Optional[FQN] = None
    secret_username: Optional[str] = None
    secret_password: Optional[str] = None
