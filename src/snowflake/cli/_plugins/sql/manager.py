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

from snowflake.cli._app.printing import print_result
from snowflake.cli._plugins.sql.snowsql_templating import transpile_snowsql_templates
from snowflake.cli._plugins.sql.statement_reader import (
    CompiledStatement,
    compile_statements,
    files_reader,
    query_reader,
)
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliArgumentError, CliSqlError
from snowflake.cli.api.output.types import CollectionResult
from snowflake.cli.api.rendering.sql_templates import snowflake_sql_jinja_render
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin, VerboseCursor
from snowflake.connector.cursor import SnowflakeCursor

ExpectedResultsCount = int

logger = logging.getLogger(__name__)


class SqlManager(SqlExecutionMixin):
    def execute(
        self,
        query: str | None,
        files: List[Path] | None,
        std_in: bool,
        data: Dict | None = None,
        retain_comments: bool = False,
        single_transaction: bool = False,
    ) -> Tuple[ExpectedResultsCount, Iterable[SnowflakeCursor]]:
        """Reads, transforms and execute statements from input.

        Only one input can be consumed at a time.
        When no compilation errors are detected, the sequence on queries
        in executed and returned as tuple.

        Throws an exception ff multiple inputs are provided.
        """
        query = sys.stdin.read() if std_in else query

        stmt_operators = (
            transpile_snowsql_templates,
            partial(snowflake_sql_jinja_render, data=data),
        )
        remove_comments = not retain_comments

        if query:
            stmt_reader = query_reader(query, stmt_operators, remove_comments)
        elif files:
            secured_files = [SecurePath(f) for f in files]
            stmt_reader = files_reader(secured_files, stmt_operators, remove_comments)
        else:
            raise CliArgumentError("Use either query, filename or input option.")

        errors, expected_results_cnt, compiled_statements = compile_statements(
            stmt_reader
        )
        if not any((errors, expected_results_cnt, compiled_statements)):
            raise CliArgumentError("Use either query, filename or input option.")

        if errors:
            for error in errors:
                logger.info("Statement compilation error: %s", error)
                cli_console.warning(error)
            raise CliSqlError("SQL rendering error")

        if single_transaction:
            logger.info("disabling AUTOCOMMIT")
            self.disable_autocommit()
            compiled_statements = [
                CompiledStatement(statement="BEGIN;"),
                *compiled_statements,
                CompiledStatement(statement="COMMIT;"),
            ]
            expected_results_cnt = len(compiled_statements)

        cursor_class = SnowflakeCursor if get_cli_context().is_repl else VerboseCursor
        return expected_results_cnt, self._execute_compiled_statements(
            compiled_statements,
            cursor_class=cursor_class,
        )

    def _execute_compiled_statements(
        self, compiled_statements: List[CompiledStatement], cursor_class
    ) -> Iterable[SnowflakeCursor]:
        for stmt in compiled_statements:
            if stmt.execute_async:
                cursor = self._conn.cursor(cursor_class=cursor_class)
                cursor.execute(stmt.statement, _no_results=True)
                # only log query ID for consistency with SnowSQL
                logger.info("Async execution id: %s", cursor.sfqid)
                print_result(CollectionResult([{"scheduled query ID": cursor.sfqid}]))
            elif stmt.statement:
                yield from self.execute_string(
                    stmt.statement, cursor_class=cursor_class
                )
            if stmt.command:
                stmt.command.execute(self._conn)
