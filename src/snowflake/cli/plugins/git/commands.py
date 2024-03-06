import logging
from pathlib import Path

import typer
from click import ClickException
from snowflake.cli.api.commands.flags import identifier_argument
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import CommandResult, QueryResult
from snowflake.cli.api.utils.path_utils import is_stage_path
from snowflake.cli.plugins.git.manager import GitManager

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


def _assert_repository_path_is_stage(argument_name, path):
    if not is_stage_path(path):
        raise ClickException(
            f"{argument_name} should be a path to git repository stage with scope provided."
            " For example: @my_repo/branches/main"
        )
