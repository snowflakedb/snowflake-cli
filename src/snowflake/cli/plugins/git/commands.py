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

RepoNameArgument = identifier_argument(sf_object="git repository", example="my_repo")
RepoPathArgument = typer.Argument(
    help="Path to git repository stage with scope provided. For example: @my_repo/branches/main"
)


@app.command(
    "list-branches",
    help="List all branches in the repository.",
    requires_connection=True,
)
def list_branches(
    repository_name: str = RepoNameArgument,
    **options,
) -> CommandResult:
    return QueryResult(GitManager().show_branches(repo_name=repository_name))


@app.command(
    "list-tags",
    help="List all tags in the repository.",
    requires_connection=True,
)
def list_tags(
    repository_name: str = RepoNameArgument,
    **options,
) -> CommandResult:
    return QueryResult(GitManager().show_tags(repo_name=repository_name))


@app.command(
    "list-files",
    help="List files from given state of git repository.",
    requires_connection=True,
)
def list_files(
    repository_path: str = RepoPathArgument,
    **options,
) -> CommandResult:
    _assert_repository_path_is_stage("REPOSITORY_PATH", path=repository_path)
    return QueryResult(GitManager().show_files(repo_path=repository_path))


@app.command(
    "fetch",
    help="Fetch changes from origin to snowflake repository.",
    requires_connection=True,
)
def fetch(
    repository_name: str = RepoNameArgument,
    **options,
) -> CommandResult:
    return QueryResult(GitManager().fetch(repo_name=repository_name))


@app.command(
    "copy",
    help="Copies all files from given state of repository to local directory or stage.",
    requires_connection=True,
)
def copy(
    repository_path: str = RepoPathArgument,
    destination_path: str = typer.Argument(
        help="Target path for copy operation. Should be stage or local file path.",
    ),
    parallel: int = typer.Option(
        4,
        help="Number of parallel threads to use when downloading files.",
    ),
    **options,
):
    _assert_repository_path_is_stage("REPOSITORY_PATH", path=repository_path)
    is_copy = is_stage_path(destination_path)
    if is_copy:
        cursor = GitManager().copy(
            repo_path=repository_path, destination_path=destination_path
        )
    else:
        target = Path(destination_path).resolve()
        cursor = GitManager().get(
            repo_path=repository_path, target_path=target, parallel=parallel
        )
    return QueryResult(cursor)


def _assert_repository_path_is_stage(argument_name, path):
    if not is_stage_path(path):
        raise ClickException(
            f"{argument_name} should be a path to git repository stage with scope provided."
            " For example: @my_repo/branches/main"
        )
