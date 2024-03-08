import logging
from pathlib import Path

import typer
from click import ClickException
from snowflake.cli.api.commands.flags import identifier_argument
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.output.types import CommandResult, QueryResult
from snowflake.cli.api.utils.path_utils import is_stage_path
from snowflake.cli.plugins.git.manager import GitManager
from snowflake.cli.plugins.object.manager import ObjectManager
from snowflake.connector import ProgrammingError

app = SnowTyper(
    name="git",
    help="Manages git repositories in Snowflake.",
    hidden=True,
)
log = logging.getLogger(__name__)


def _repo_path_argument_callback(path):
    if not is_stage_path(path):
        raise ClickException(
            "REPOSITORY_PATH should be a path to git repository stage with scope provided."
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
)


def _object_exists(object_type, identifier):
    try:
        ObjectManager().describe(
            object_type=object_type.value.cli_name,
            name=identifier,
        )
        return True
    except ProgrammingError:
        return False


def _assure_repository_does_not_exist(repository_name: str) -> None:
    if _object_exists(ObjectType.GIT_REPOSITORY, repository_name):
        raise ClickException(f"Repository '{repository_name}' already exists")


def _validate_origin_url(url: str) -> None:
    if not url.startswith("https://"):
        raise ClickException("Url address should start with 'https'")


@app.command("setup", requires_connection=True)
def setup(
    repository_name: str = RepoNameArgument,
    **options,
) -> CommandResult:
    """
    Sets up a git repository object.

    You will be prompted for:

    * url - address of repository to be used for git clone operation

    * secret - Snowflake secret containing authentication credentials. Not needed if origin repository does not require
    authentication for RO operations (clone, fetch)

    * API integration - object allowing Snowflake to interact with git repository.
    """
    _assure_repository_does_not_exist(repository_name)
    manager = GitManager()

    url = typer.prompt("Origin url")
    _validate_origin_url(url)

    secret = {}
    secret_needed = typer.confirm("Use secret for authentication?")
    if secret_needed:
        secret_name = f"{repository_name}_secret"
        secret_name = typer.prompt(
            "Secret identifier (will be created if not exists)", default=secret_name
        )
        secret = {"name": secret_name}
        if not _object_exists(ObjectType.SECRET, secret_name):
            cli_console.step(f"Secret '{secret_name}' will be created")
            secret["username"] = typer.prompt("username")
            secret["password"] = typer.prompt("password/token", hide_input=True)

    api_integration = f"{repository_name}_api_integration"
    api_integration = typer.prompt(
        "API integration identifier (will be created if not exists)",
        default=api_integration,
    )

    if "username" in secret:
        manager.create_password_secret(**secret)
        secret_name = secret["name"]
        cli_console.step(f"Secret '{secret_name}' successfully created")

    if not _object_exists(ObjectType.INTEGRATION, api_integration):
        manager.create_api_integration(
            name=api_integration,
            api_provider="git_https_api",
            allowed_prefix=url,
            secret=secret.get("name"),
        )
        cli_console.step(f"API integration '{api_integration}' successfully created.")
    else:
        cli_console.step(f"Using existing API integration '{api_integration}'")

    return QueryResult(
        manager.create(
            repo_name=repository_name,
            url=url,
            api_integration=api_integration,
            secret=secret.get("name"),
        )
    )


@app.command(
    "list-branches",
    requires_connection=True,
)
def list_branches(
    repository_name: str = RepoNameArgument,
    **options,
) -> CommandResult:
    """
    List all branches in the repository.
    """
    return QueryResult(GitManager().show_branches(repo_name=repository_name))


@app.command(
    "list-tags",
    requires_connection=True,
)
def list_tags(
    repository_name: str = RepoNameArgument,
    **options,
) -> CommandResult:
    """
    List all tags in the repository.
    """
    return QueryResult(GitManager().show_tags(repo_name=repository_name))


@app.command(
    "list-files",
    requires_connection=True,
)
def list_files(
    repository_path: str = RepoPathArgument,
    **options,
) -> CommandResult:
    """
    List files from given state of git repository.
    """
    return QueryResult(GitManager().show_files(repo_path=repository_path))


@app.command(
    "fetch",
    requires_connection=True,
)
def fetch(
    repository_name: str = RepoNameArgument,
    **options,
) -> CommandResult:
    """
    Fetch changes from origin to snowflake repository.
    """
    return QueryResult(GitManager().fetch(repo_name=repository_name))


@app.command(
    "copy",
    requires_connection=True,
)
def copy(
    repository_path: str = RepoPathArgument,
    destination_path: str = typer.Argument(
        help="Target path for copy operation. Should be a path to a directory on remote stage or local file system.",
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
        cursor = GitManager().copy_files(
            source_path=repository_path, destination_path=destination_path
        )
    else:
        cursor = GitManager().get(
            stage_path=repository_path,
            dest_path=Path(destination_path).resolve(),
            parallel=parallel,
        )
    return QueryResult(cursor)
