from pathlib import Path
from typing import Optional

import typer
from snowflake.cli.api.commands.decorators import (
    global_options_with_connection,
    with_output,
)
from snowflake.cli.api.output.types import CommandResult, MultipleResults, QueryResult
from snowflake.cli.plugins.sql.manager import SqlManager

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
    std_in: Optional[bool] = typer.Option(
        False,
        "--stdin",
        "-i",
        help="Read the query from standard input. Use it when piping input to this command.",
    ),
    **options
) -> CommandResult:
    """
    Executes Snowflake query.

    Query to execute can be specified using query option, filename option (all queries from file will be executed)
    or via stdin by piping output from other command. For example `cat my.sql | snow sql -i`.
    """
    single_statement, cursors = SqlManager().execute(query, file, std_in)
    if single_statement:
        return QueryResult(next(cursors))
    return MultipleResults((QueryResult(c) for c in cursors))
