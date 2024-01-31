import sys
from io import StringIO
from pathlib import Path
from typing import Iterable, Optional, Tuple

from click import UsageError
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.util_text import split_statements


class SqlManager(SqlExecutionMixin):
    def execute(
        self, query: Optional[str], file: Optional[Path], std_in: bool
    ) -> Tuple[int, Iterable[SnowflakeCursor]]:
        inputs = [query, file, std_in]
        if not any(inputs):
            raise UsageError("Use either query, filename or input option.")

        # Check if any two inputs were provided simultaneously
        if len([i for i in inputs if i]) > 1:
            raise UsageError(
                "Multiple input sources specified. Please specify only one."
            )

        if std_in:
            query = sys.stdin.read()
        elif file:
            query = file.read_text()

        statements = tuple(
            statement
            for statement, _ in split_statements(StringIO(query), remove_comments=True)
        )
        single_statement = len(statements) == 1

        return single_statement, self._execute_string("\n".join(statements))
