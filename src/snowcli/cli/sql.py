import sys
from functools import partial
from pathlib import Path
from typing import Optional

import typer
from rich import box
from rich.live import Live
from rich.table import Table
from snowflake.connector.cursor import SnowflakeCursor

from snowcli import config
from snowcli.utils import conf_callback

EnvironmentOption = typer.Option(
    "dev",
    help="Environment name",
    callback=conf_callback,
    is_eager=True,
)


class LiveOutput:
    def __init__(self, table: Table, live: Live):
        self.table = table
        self.live = live

    def add_row(self, *args):
        self.table.add_row(*args)
        self.live.refresh()

    def add_result_row(self, result):
        self.table.add_row(str(result))
        self.live.refresh()


class LoggingCursor(SnowflakeCursor):
    def __init__(self, live_output: LiveOutput, *args, **kwargs):
        super().__init__(*args, use_dict_result=True, **kwargs)
        self.live_output = live_output

    def execute(self, command: str, *args, **kwargs):
        self.live_output.add_row(command)
        result = super(LoggingCursor, self).execute(command, *args, **kwargs)
        for result in result.fetchall():
            self.live_output.add_result_row(result)


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
    connection: Optional[str] = typer.Option(
        None, "-c", "--connection", help="Connection to be used"
    ),
    account: Optional[str] = typer.Option(
        None,
        "-a",
        "--accountname",
        "--account",
        help="Name assigned to your Snowflake account.",
    ),
    user: Optional[str] = typer.Option(
        None, "-u", "--username", "--user", help="Username to connect to Snowflake."
    ),
    database: Optional[str] = typer.Option(
        None, "-d", "--dbname", "--database", help="Database to use."
    ),
    schema: Optional[str] = typer.Option(
        None, "-s", "--schemaname", "--schema", help=" Schema in the database to use."
    ),
    role: Optional[str] = typer.Option(
        None, "-r", "--rolename", "--role", help="Role to be used"
    ),
    warehouse: Optional[str] = typer.Option(
        None, "-w", "--warehouse", help="Warehouse to use."
    ),
    verbose: Optional[bool] = typer.Option(
        True, "-v", "--verbose", help="Prints information about executed queries"
    ),
):
    """
    Executes Snowflake query.

    Query to execute can be specified using query option, filename option (all queries from file will be executed)
    or via stdin by piping output from other command. For example `snow render template my.sql | snow sql`.
    """
    sys_input = None

    if query and file:
        raise ValueError("Both query and file provided, please specify only one.")

    if not sys.stdin.isatty():
        sys_input = sys.stdin.read()

    if sys_input and (query or file):
        raise ValueError(
            "Can't use stdin input together with query or filename option."
        )

    if not query and not file and not sys_input:
        raise ValueError("Provide either query or filename argument")
    elif sys_input:
        sql = sys_input
    else:
        sql = query if query else file.read_text()  # type: ignore

    if not config.isAuth():
        raise ValueError("Not authorize")

    config.connectToSnowflake(
        connection,
        account=account,
        user=user,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )

    table = Table(show_lines=True, box=box.ASCII, width=120)
    table.add_column("Query")

    if verbose:
        with Live(table, auto_refresh=False) as live:
            config.snowflake_connection.ctx.execute_string(
                sql_text=sql,
                remove_comments=True,
                cursor_class=partial(LoggingCursor, LiveOutput(table, live)),
            )
    else:
        config.snowflake_connection.ctx.execute_string(
            sql_text=sql,
            remove_comments=True,
        )
