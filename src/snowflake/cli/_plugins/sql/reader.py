from __future__ import annotations

import re
from io import StringIO, TextIOWrapper
from pathlib import Path
from typing import Callable, Generator, Sequence, Tuple

from click import ClickException
from jinja2.exceptions import UndefinedError
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath
from snowflake.connector.util_text import split_statements

SOURCE_PATTERN = re.compile(
    r"!source\s+[\"']?(.*?)[\"']?\s*(?:;|$)",
    flags=re.IGNORECASE,
)

Error = str
Statement = str
StatementAccumulator = int
StatementsCount = int
SqlTransofrmFunc = Callable[[str], str]
OperatorFunctions = Sequence[SqlTransofrmFunc]
SQLFiles = Sequence[Path]
StatementCompilationErrors = Sequence[str]
CompiledStatements = Sequence[str]
StatementCompilationResult = Tuple[
    StatementCompilationErrors,
    StatementsCount,
    CompiledStatements,
]
RawStatementGenerator = Generator[
    Tuple[Error | None, StatementAccumulator, Statement | None], None, None
]

IS_COMMAND: StatementAccumulator = 0
IS_STATEMENT: StatementAccumulator = 1


class SQLReader:
    def __init__(
        self,
        query: str | None,
        files: Sequence[Path] | None,
        remove_comments: bool = True,
    ):
        self._query = query
        self._files = files
        self._raw_statements = self._dispatch_input()
        self._remove_comments = remove_comments

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
    def remove_comments(self):
        """Should comments be removed from statemetns."""
        return self._remove_comments

    @property
    def _input_reader(self) -> RawStatementGenerator:
        payload = StringIO(self._query)
        for statement, _ in split_statements(
            buf=payload,
            remove_comments=self.remove_comments,
        ):
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
                    for statement, _ in split_statements(
                        buf=payload,
                        remove_comments=self.remove_comments,
                    ):
                        yield from self._command_dispatcher(statement, seen_files)
        finally:
            if file.path in seen_files:
                seen_files.remove(file.path)
