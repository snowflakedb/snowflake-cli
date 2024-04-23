from pathlib import Path

import typer
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import CommandResult, MultipleResults, QueryResult
from snowflake.cli.plugins.sql.manager import SqlManager

# simple Typer with defaults because it won't become a command group as it contains only one command
app = SnowTyper()


@app.command(name="sql", requires_connection=True)
def execute_sql(
    query: str
    | None = typer.Option(
        None,
        "--query",
        "-q",
        help="Query to execute.",
    ),
    file: Path
    | None = typer.Option(
        None,
        "--filename",
        "-f",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="File to execute.",
    ),
    std_in: bool
    | None = typer.Option(
        False,
        "--stdin",
        "-i",
        help="Read the query from standard input. Use it when piping input to this command.",
    ),
    **options,
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
