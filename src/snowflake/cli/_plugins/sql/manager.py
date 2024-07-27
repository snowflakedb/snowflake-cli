# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import sys
from io import StringIO
from itertools import chain
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from click import ClickException, UsageError
from jinja2 import UndefinedError
from snowflake.cli._plugins.sql.snowsql_templating import transpile_snowsql_templates
from snowflake.cli.api.rendering.sql_templates import snowflake_sql_jinja_render
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin, VerboseCursor
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.util_text import split_statements

IsSingleStatement = bool


class SqlManager(SqlExecutionMixin):
    def execute(
        self,
        query: str | None,
        files: List[Path] | None,
        std_in: bool,
        data: Dict | None = None,
    ) -> Tuple[IsSingleStatement, Iterable[SnowflakeCursor]]:
        inputs = [query, files, std_in]
        # Check if any two inputs were provided simultaneously
        if len([i for i in inputs if i]) > 1:
            raise UsageError(
                "Multiple input sources specified. Please specify only one."
            )

        if std_in:
            query = sys.stdin.read()
        if query:
            return self._execute_single_query(query=query, data=data)

        if files:
            # Multiple files
            results = []
            single_statement = False
            for file in files:
                query_from_file = SecurePath(file).read_text(
                    file_size_limit_mb=UNLIMITED
                )
                single_statement, result = self._execute_single_query(
                    query=query_from_file, data=data
                )
                results.append(result)

            # Use single_statement if there's only one, otherwise this is multi statement result
            single_statement = len(files) == 1 and single_statement
            return single_statement, chain.from_iterable(results)

        # At that point, no stdin, query or files were provided
        raise UsageError("Use either query, filename or input option.")

    def _execute_single_query(
        self, query: str, data: Dict | None = None
    ) -> Tuple[IsSingleStatement, Iterable[SnowflakeCursor]]:
        try:
            query = transpile_snowsql_templates(query)
            query = snowflake_sql_jinja_render(content=query, data=data)
        except UndefinedError as err:
            raise ClickException(f"SQL template rendering error: {err}")

        statements = tuple(
            statement
            for statement, _ in split_statements(StringIO(query), remove_comments=True)
        )
        single_statement = len(statements) == 1

        return single_statement, self._execute_string(
            "\n".join(statements), cursor_class=VerboseCursor
        )
