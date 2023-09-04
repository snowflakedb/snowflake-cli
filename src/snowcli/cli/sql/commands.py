from pathlib import Path
from typing import Optional

import typer

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.sql.manager import SqlManager
from snowcli.output.decorators import with_output
from snowcli.output.printing import OutputData


@with_output
@global_options_with_connection
def execute_sql(
    query: Optional[str] = typer.Option(
        None,
        "-q",
        "--query",
        help="Query to execute.",
    ),
    file: Optional[Path] = typer.Option(
        None,
        "-f",
        "--filename",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="File to execute.",
    ),
    **options
) -> OutputData:
    """
    Executes Snowflake query.

    Query to execute can be specified using query option, filename option (all queries from file will be executed)
    or via stdin by piping output from other command. For example `cat my.sql | snow sql`.
    """
    cursors = SqlManager().execute(query, file)
    if len(cursors) > 1:
        return OutputData(stream=(OutputData.from_cursor(cur) for cur in cursors))
    return OutputData.from_cursor(cursors[0])
