import sys
from pathlib import Path
from typing import Optional, List, Union

import typer
from click import UsageError
from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.output.decorators import with_output
from snowcli.output.printing import OutputData


class SqlManager(SqlExecutionMixin):
    def execute(
        self, query: Optional[str], file: Optional[Path]
    ) -> List[SnowflakeCursor]:
        sys_input = None

        if query and file:
            raise UsageError("Both query and file provided, please specify only one.")

        if not sys.stdin.isatty():
            sys_input = sys.stdin.read()

        if sys_input and (query or file):
            raise UsageError(
                "Can't use stdin input together with query or filename option."
            )

        if not query and not file and not sys_input:
            raise UsageError("Provide either query or filename argument")
        elif sys_input:
            sql = sys_input
        else:
            sql = query if query else file.read_text()  # type: ignore
        return self._execute_queries(sql)


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
