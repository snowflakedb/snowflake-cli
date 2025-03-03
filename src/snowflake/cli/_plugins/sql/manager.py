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

import logging
import sys
from functools import partial
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from click import ClickException, UsageError
from snowflake.cli._plugins.sql.reader import SQLReader
from snowflake.cli._plugins.sql.snowsql_templating import transpile_snowsql_templates
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.rendering.sql_templates import snowflake_sql_jinja_render
from snowflake.cli.api.sql_execution import SqlExecutionMixin, VerboseCursor
from snowflake.connector.cursor import SnowflakeCursor

IsSingleStatement = bool

logger = logging.getLogger(__name__)


class SqlManager(SqlExecutionMixin):
    def execute(
        self,
        query: str | None,
        files: List[Path] | None,
        std_in: bool,
        data: Dict | None = None,
        retain_comments: bool = False,
    ) -> Tuple[IsSingleStatement, Iterable[SnowflakeCursor]]:
        """Reads, transforms and execute statements from input.

        Only one input can be consumed at a time. If multiple inputs are provided, the
        order of precedence is as follows:
        - stdin
        - query
        - files

        When no compilation errors are detected, the sequence on queries
        in executed and returned as tuple.
        """
        inputs = (query, files, std_in)
        if len([i for i in inputs if i]) > 1:
            raise UsageError(
                "Multiple input sources specified. Please specify only one."
            )

        query = sys.stdin.read() if std_in else query

        stmt_reader = SQLReader(query, files, not retain_comments)
        stmt_operator_funcs = (
            transpile_snowsql_templates,
            partial(snowflake_sql_jinja_render, data=data),
        )
        errors, stmt_count, compiled_statements = stmt_reader.compile_statements(
            stmt_operator_funcs
        )
        if not any((errors, stmt_count, compiled_statements)):
            raise UsageError("Use either query, filename or input option.")

        if errors:
            for error in errors:
                logger.info("Statement compilation error: %s", error)
                cli_console.warning(error)
            raise ClickException("SQL rendering error")

        is_single_statement = not (stmt_count > 1)
        return is_single_statement, self._execute_string(
            "\n".join(compiled_statements),
            cursor_class=VerboseCursor,
        )
