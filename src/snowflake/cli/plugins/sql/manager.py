from __future__ import annotations

import sys
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

from click import ClickException, UsageError
from jinja2 import UndefinedError
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.utils.rendering import snowflake_cli_jinja_render
from snowflake.cli.plugins.sql.snowsql_templating import transpile_snowsql_templates
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.util_text import split_statements


class SqlManager(SqlExecutionMixin):
    def execute(
        self,
        query: Optional[str],
        file: Optional[Path],
        std_in: bool,
        data: Dict | None = None,
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
            query = SecurePath(file).read_text(file_size_limit_mb=UNLIMITED)

        if data:
            # Do rendering if any data was provided
            try:
                query = transpile_snowsql_templates(query)
                query = snowflake_cli_jinja_render(content=query, data=data)
            except UndefinedError as err:
                raise ClickException(f"SQL template rendering error: {err}")

        statements = tuple(
            statement
            for statement, _ in split_statements(StringIO(query), remove_comments=True)
        )
        single_statement = len(statements) == 1

        return single_statement, self._execute_string("\n".join(statements))
