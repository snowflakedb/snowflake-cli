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
import re
import sys
from functools import partial
from io import StringIO, TextIOWrapper
from itertools import chain
from pathlib import Path
from typing import Callable, Dict, Generator, Iterable, List, Sequence, Tuple

from click import ClickException, UsageError
from jinja2 import UndefinedError
from snowflake.cli._plugins.sql.snowsql_templating import transpile_snowsql_templates
from snowflake.cli.api.rendering.sql_templates import snowflake_sql_jinja_render
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin, VerboseCursor
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.util_text import split_statements

IsSingleStatement = bool
StatementGenerator = Generator[str, None, None]

SOURCE_PATTERN = re.compile(
    r"!source\s+[\"']?(.*?)[\"']?\s*(?:;|$)",
    flags=re.IGNORECASE,
)

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
        inputs = [query, files, std_in]
        # Check if any two inputs were provided simultaneously
        if len([i for i in inputs if i]) > 1:
            raise UsageError(
                "Multiple input sources specified. Please specify only one."
            )

        if std_in:
            query = sys.stdin.read()
        if query:
            return self._execute_single_query(
                query=query, data=data, retain_comments=retain_comments
            )

        if files:
            # Multiple files
            results = []
            single_statement = False
            for file in files:
                query_from_file = SecurePath(file).read_text(
                    file_size_limit_mb=UNLIMITED
                )
                single_statement, result = self._execute_single_query(
                    query=query_from_file, data=data, retain_comments=retain_comments
                )
                results.append(result)

            # Use single_statement if there's only one, otherwise this is multi statement result
            single_statement = len(files) == 1 and single_statement
            return single_statement, chain.from_iterable(results)

        # At that point, no stdin, query or files were provided
        raise UsageError("Use either query, filename or input option.")

    def _execute_single_query(
        self, query: str, data: Dict | None = None, retain_comments: bool = False
    ) -> Tuple[IsSingleStatement, Iterable[SnowflakeCursor]]:
        try:
            query = transpile_snowsql_templates(query)
            query = snowflake_sql_jinja_render(content=query, data=data)
        except UndefinedError as err:
            raise ClickException(f"SQL template rendering error: {err}")

        statements = tuple(
            statement
            for statement, _ in split_statements(
                StringIO(query), remove_comments=not retain_comments
            )
        )
        single_statement = len(statements) == 1

        return single_statement, self._execute_string(
            "\n".join(statements), cursor_class=VerboseCursor
        )

    def execute_(
        self,
        query: str | None,
        files: List[Path] | None,
        std_in: bool,
        data: Dict | None = None,
        retain_comments: bool = False,
    ) -> Tuple[IsSingleStatement, Iterable[SnowflakeCursor]]:
        """New execute."""

        query = sys.stdin.read() if std_in else query

        stmt_reader = SQLReader(query, files)
        stmt_operators = (
            transpile_snowsql_templates,
            partial(snowflake_sql_jinja_render, data=data),
        )
        errors, stmt_count, compiled_statements = stmt_reader.compile_statements(
            stmt_operators
        )
        if errors:
            for error in errors:
                logger.info("Statement compilation error: %s", error)
            raise ClickException("Errors during SQL compilation")

        is_single_statement = not (stmt_count > 1)
        return is_single_statement, self._execute_string(
            "\n".join(compiled_statements),
            cursor_class=VerboseCursor,
        )


StrFunction = Callable[[str], str]
OperatorFunctions = Sequence[StrFunction]
SQLFiles = Sequence[Path]
StatementCompilationErrors = Sequence[str]
StatementsCount = int
CompiledStatements = Sequence[str]
StatementCompilationResult = Tuple[
    StatementCompilationErrors,
    StatementsCount,
    CompiledStatements,
]
Error = str
Statement = str
StatementAccumulator = int
IS_COMMAND: StatementAccumulator = 0
IS_STATEMENT: StatementAccumulator = 1
RawStatementGenerator = Generator[
    Tuple[Error | None, StatementAccumulator, Statement | None], None, None
]


class SQLReader:
    def __init__(
        self,
        query: str | None,
        files: Sequence[Path] | None,
    ):
        self._query = query
        self._files = files
        self._raw_statements = self._dispatch_input()

    def _dispatch_input(self):
        if self._query:
            return self._input_reader
        elif self._files:
            return self._file_reader
        else:
            raise ClickException("No input provided")

    def compile_statements(
        self,
        operators: OperatorFunctions,
    ) -> StatementCompilationResult:
        errors = []
        stmt_count = 0
        compiled_statements = []

        for read_error, stmt_value, raw_statement in self._raw_statements:
            stmt_count += stmt_value
            if read_error:
                errors.append(str(read_error))
                continue

            try:
                compiled_statement = self._apply_operators(raw_statement, operators)
                compiled_statements.append(compiled_statement)

            except UndefinedError as err:
                errors.append(str(err))

        return errors, stmt_count, compiled_statements

    @staticmethod
    def _apply_operators(statement: str, operators: OperatorFunctions) -> str:
        if not operators:
            return statement
        for operator in operators:
            statement = operator(statement)
        return statement

    @property
    def _input_reader(self) -> RawStatementGenerator:
        payload = StringIO(self._query)
        for statement, _ in split_statements(payload):
            yield from self._command_dispatcher(statement)

    @property
    def _file_reader(self) -> RawStatementGenerator:
        if not self._files:
            return

        for path in self._files:
            yield from self._recursive_file_reader(SecurePath(path), set())

    @staticmethod
    def _check_for_source_command(statement: str) -> tuple[bool, SecurePath | None]:
        split_result = SOURCE_PATTERN.split(statement.strip(), maxsplit=1)

        match split_result:
            case ("", file_path, "") if SecurePath(file_path.strip()).exists():
                result = True, SecurePath(file_path.strip())
            case _:
                result = False, None
        return result

    def _command_dispatcher(
        self,
        statement: str,
        seen_files: set | None = None,
    ) -> RawStatementGenerator:
        if seen_files is None:
            seen_files = set()

        is_source, source_path = self._check_for_source_command(statement)
        if is_source and isinstance(source_path, SecurePath):
            yield from self._recursive_file_reader(source_path, seen_files)
        else:
            yield None, IS_STATEMENT, statement

    def _recursive_file_reader(
        self,
        file: SecurePath,
        seen_files: set,
    ) -> RawStatementGenerator:
        try:
            if file.path in seen_files:
                yield (
                    f"Recursion detected for file {file.path.as_posix()}",
                    IS_COMMAND,
                    None,
                )

            else:
                seen_files.add(file.path)

                with file.open("rb", read_file_limit_mb=UNLIMITED) as fh:
                    payload = TextIOWrapper(fh)
                    for statement, _ in split_statements(payload):
                        yield from self._command_dispatcher(statement, seen_files)
        finally:
            if file.path in seen_files:
                seen_files.remove(file.path)
