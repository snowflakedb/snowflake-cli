import logging

from snowflake.cli.api.commands.flags import identifier_argument
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import CommandResult, QueryResult
from snowflake.cli.plugins.git.manager import GitManager

app = SnowTyper(
    name="git",
    help="Manages git repositories in Snowflake.",
    hidden=True,
)
log = logging.getLogger(__name__)

RepoNameArgument = identifier_argument(sf_object="git repository", example="my_repo")


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
