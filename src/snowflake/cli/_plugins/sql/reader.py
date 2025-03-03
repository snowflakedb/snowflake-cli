from __future__ import annotations

import re
from enum import Enum
from functools import cached_property
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


STATEMENT = "STATEMENT"
COMMAND = "COMMAND"
EMPTY = "EMPTY"


class StatementType(Enum):
    """Class for labeling statement types.

    Each type has a score property that is used to calculate total
    count of statements.

    Only SQL statements have score of 1.
    All other types have score of 0.
    """

    STATEMENT = STATEMENT
    COMMAND = COMMAND
    EMPTY = EMPTY

    @cached_property
    def score(self) -> int:
        scores = {STATEMENT: 1}
        return scores.get(self.value, 0)


Error = str
Statement = str
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
    Tuple[Error | None, StatementType, Statement | None], None, None
]


class SQLReader:
    """Ingests statements from provided input and prepares them for execution.

    Accepts query as a string or files as a list of paths.

    Each input is divided into separate statements for further processing.
    Each statement is scanned for source command that triggers recursive file read.
    Each statement is transformed by operator functions before being returned
    as a result in form of RawStatementGenerator.
    """

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
        """Maps provided inputs to common attribute for other methods to consume."""
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
        """Transforms raw statements into list of compiled.

        Calculates total number of statements by summing score for each StatementType.

        Returns tuple of errors, statement count and compiled statements.
        """
        errors = []
        stmt_count = 0
        compiled_statements = []

        for read_error, stmt_type, raw_statement in self._raw_statements:
            stmt_count += stmt_type.score
            if read_error:
                errors.append(str(read_error))
                continue

            try:
                compiled_statement = self._apply_operators(raw_statement, operators)
                if compiled_statement:
                    compiled_statements.append(compiled_statement)

            except UndefinedError as err:
                errors.append(f"SQL template rendering error: {err}")

        return errors, stmt_count, compiled_statements

    @staticmethod
    def _apply_operators(statement: str, operators: OperatorFunctions) -> str:
        """Executes operator function against statement.

        Each operator function accepts single parameter as string
        and must return a transformed statement as string.
        Operator function must raise UndefinedError if it encounters error.

        Operators that require additional context should be
        handled with functools.partial.
        """
        if not operators:
            return statement
        for operator in operators:
            statement = operator(statement)
        return statement

    @property
    def remove_comments(self):
        """Should comments be removed from statements."""
        return self._remove_comments

    @property
    def _input_reader(self) -> RawStatementGenerator:
        """Ingests statements provided by query or stdin.

        Returns tuple generator of errors, statement type and statement.
        """
        payload = StringIO(self._query)
        for statement, _ in split_statements(
            buf=payload,
            remove_comments=self.remove_comments,
        ):
            yield from self._command_dispatcher(statement)

    @property
    def _file_reader(self) -> RawStatementGenerator:
        """Ingests statements provided with files.

        Returns tuple generator of errors, statement type and statement."""
        if not self._files:
            return

        for path in self._files:
            yield from self._recursive_file_reader(SecurePath(path), set())

    @staticmethod
    def _check_for_source_command(statement: str) -> tuple[bool, SecurePath | None]:
        """Detects if statement contains source command.

        Returns tuple of boolean and file path if command is found and file exists.
        Otherwise returns tuple of False and None.

        Uses regex to split statement into 3 parts (left, middle, right):
        - left part is an empty string if command is found
          otherwise it contains the original statement
        - middle part contains command if found
          otherwise is an empty string
        - left part contains possible file path to include if match
          otherwise an empty string
        """
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
        """Parses raw statements one by one in search for commands.

        Returns tuple generator of errors, statement type and statement.

        Each statement is labeled with StatementType enum. If source command
        is found recursive file reader is used to ingest statements
        before continuing.

        """
        if seen_files is None:
            seen_files = set()

        is_source, source_path = self._check_for_source_command(statement)
        if is_source and isinstance(source_path, SecurePath):
            yield from self._recursive_file_reader(source_path, seen_files)
        else:
            stmt_score = (
                StatementType.STATEMENT if statement.strip() else StatementType.EMPTY
            )
            yield None, stmt_score, statement

    def _recursive_file_reader(
        self,
        file: SecurePath,
        seen_files: set,
    ) -> RawStatementGenerator:
        """Reads content of the file and tracks for recursive reads.

        Read content is divided into separate statements for further processing.
        """
        try:
            if file.path in seen_files:
                yield (
                    f"Recursion detected for file {file.path.as_posix()}",
                    StatementType.COMMAND,
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
