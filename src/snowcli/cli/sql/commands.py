from pathlib import Path
from typing import Optional

import typer

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.sql.manager import SqlManager
from snowcli.output.decorators import with_output
from snowcli.output.types import QueryResult, CommandResult, MultipleResults

# simple Typer with defaults because it won't become a command group as it contains only one command
app = typer.Typer()


@app.command(name="sql")
@with_output
@global_options_with_connection
def execute_sql(
    query: Optional[str] = typer.Option(
        None,
        "--query",
        "-q",
        help="Query to execute.",
    ),
    file: Optional[Path] = typer.Option(
        None,
        "--filename",
        "-f",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="File to execute.",
    ),
    **options
) -> CommandResult:
    """
    Executes Snowflake query.

    Query to execute can be specified using query option, filename option (all queries from file will be executed)
    or via stdin by piping output from other command. For example `cat my.sql | snow sql`.
    """
    cursors = SqlManager().execute(query, file)
    if len(cursors) > 1:
        result = MultipleResults()
        for curr in cursors:
            result.add(QueryResult(curr))
    else:
        result = QueryResult(cursors[0])
    return result
