import sys
from pathlib import Path
from typing import Optional, List

from click import UsageError
from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.common.sql_execution import SqlExecutionMixin


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
