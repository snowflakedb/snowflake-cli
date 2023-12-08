import sys
from itertools import combinations, starmap
from pathlib import Path
from typing import List, Optional

from click import UsageError
from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class SqlManager(SqlExecutionMixin):
    def execute(
        self, query: Optional[str], file: Optional[Path], std_in: bool
    ) -> List[SnowflakeCursor]:
        inputs = [query, file, std_in]
        if not any(inputs):
            raise UsageError("Use either query, filename or input option.")

        # Check if any two inputs were provided simultaneously
        if any(starmap(lambda *t: all(t), combinations(inputs, 2))):
            raise UsageError(
                "Multiple input sources specified. Please specify only one."
            )

        if std_in:
            query = sys.stdin.read()
        elif file:
            query = file.read_text()

        return self._execute_queries(query)
