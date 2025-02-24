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

import re
import sys
from io import StringIO, TextIOWrapper
from itertools import chain
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Tuple

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

SOURCE_PATTERN = re.compile(r"!source", flags=re.IGNORECASE)


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

    @staticmethod
    def check_for_source_command(statement: str) -> tuple[bool, SecurePath | None]:
        split_result = SOURCE_PATTERN.split(statement.strip(), maxsplit=1)

        match split_result:
            case ("", file_path) if SecurePath(file_path.strip()).exists():
                result = True, SecurePath(file_path.strip())
            case _:
                result = False, None
        return result

    @staticmethod
    def _recursive_file_reader(file: SecurePath, seen_files: set) -> StatementGenerator:
        try:
            if file.path in seen_files:
                raise RecursionError(
                    f"Recursive file inclusion detected {file.path.as_posix()}"
                )

            seen_files.add(file.path)

            with file.open("rb", read_file_limit_mb=UNLIMITED) as fh:
                payload = TextIOWrapper(fh)
                for statement, _ in split_statements(payload):
                    yield from SqlManager.source_dispatcher(statement, seen_files)

        finally:
            if file in seen_files:
                seen_files.remove(file.path)

    @staticmethod
    def source_dispatcher(statement: str, seen_files: set) -> StatementGenerator:
        is_source, source_path = SqlManager.check_for_source_command(statement)
        if is_source:
            yield from SqlManager._recursive_file_reader(source_path, seen_files)
        else:
            yield statement

    @staticmethod
    def input_reader(source: str) -> StatementGenerator:
        payload = StringIO(source)
        for statement, _ in split_statements(payload):
            yield from SqlManager.source_dispatcher(statement, set())

    @staticmethod
    def file_reader(source_paths: list[Path]) -> StatementGenerator:
        for path in source_paths:
            yield from SqlManager._recursive_file_reader(SecurePath(path), set())
