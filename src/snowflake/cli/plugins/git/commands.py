import logging

import typer
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import CommandResult, QueryResult
from snowflake.cli.plugins.git.manager import GitManager

app = SnowTyper(
    name="git",
    help="Manages git repositories in Snowflake.",
)
log = logging.getLogger(__name__)

RepoNameArgument = typer.Argument(help="name of the git repository")


@app.command("list-branches", help="List all branches in the repository.")
def list_branches(
    repository_name: str = RepoNameArgument,
    **options,
) -> CommandResult:
    return QueryResult(
        GitManager().show(object_type="branches", repo_name=repository_name)
    )


@app.command("list-tags", help="List all tags in the repository.")
def list_tags(
    repository_name: str = RepoNameArgument,
    **options,
) -> CommandResult:
    return QueryResult(GitManager().show(object_type="tags", repo_name=repository_name))
