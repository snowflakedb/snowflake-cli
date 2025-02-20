from __future__ import annotations

import io
import re
import typing as t
from pathlib import Path
from textwrap import dedent

import pytest
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath
from snowflake.connector.util_text import split_statements

UserInput = str
SQLFiles = t.Sequence[Path]
FilePath = str
UserFiles = SQLFiles
Statement = str
StatementSource = UserInput | UserFiles
StatementCount = int
SecurePathFileHandler = t.IO[t.Any]
StatementGenerator = t.Generator[Statement, None, None]
NGStatementGenerator = t.Generator[t.Tuple[StatementCount, Statement], None, None]
CommandCheckerResult = t.Tuple[str, str]
CommandChecker = t.Callable[[Statement], CommandCheckerResult]

SOURCE_PATTERN = re.compile(r"!source", flags=re.IGNORECASE)


def test_split_statement_input(tmp_path_factory):
    f1 = tmp_path_factory.mktemp("statement_reader") / "f1.sql"
    f2 = tmp_path_factory.mktemp("statement_reader") / "f2.sql"
    f1.write_text(
        dedent(
            f"""
        f1: select 1;
        !source {f2.as_posix()};
        f1: select 2;
    """
        )
    )
    f2.write_text(
        dedent(
            """
        f2: select 1;
        f2: select 2;
    """
        )
    )
    user_input = dedent(
        """
    f1: select 1;
    f2: select 1;
    f2: select 2;
    f1: select 2;
    """
    )

    def is_source(stmt) -> str:
        split_result = SOURCE_PATTERN.split(stmt, maxsplit=1)
        if len(split_result) == 1:
            return ""
        return split_result[1].strip()

    def sreader(content: io.TextIOBase) -> StatementGenerator:
        for stmt, _ in split_statements(content):
            if file_path := is_source(stmt):
                for stmt in sreader(freader(file_path)):
                    yield stmt
            else:
                yield stmt

    def freader(file_path) -> io.TextIOWrapper:
        return io.TextIOWrapper(SecurePath(file_path).path.open("rb"))

    content = freader(f1)
    content = io.StringIO(user_input)
    reader = sreader(content)

    assert next(reader) == "f1: select 1;"
    assert next(reader) == "f2: select 1;"
    assert next(reader) == "f2: select 2;"
    assert next(reader) == "f1: select 2;"

    with pytest.raises(StopIteration):
        next(reader)


def is_source_command(statement: Statement) -> FilePath:
    match SOURCE_PATTERN.split(statement, maxsplit=1):
        case ("", file_path) if len(file_path) > 0:
            file_path = file_path.strip()
        case _:
            file_path = ""

    return file_path


class NGStatementReader:
    def __init__(self, /, source: StatementSource):
        match source:
            case UserInput():
                self._stmt_enumerator = enumerate(
                    self.user_input_reader(source), start=1
                )
            case list() | tuple() as seq if all(isinstance(item, Path) for item in seq):
                self._stmt_enumerator = enumerate(self.file_reader(source), start=1)

    # def __init__(self, /, user_input: UserInput, files: UserFiles):
    #     if user_input is not None and files is not None:
    #         raise ValueError("Cannot have both user_input and files")
    #
    #     if user_input:
    #         self._user_input = user_input
    #         self._statements_enumerator = enumerate(self.user_input_reader, start=1)
    #     elif files:
    #         self._initial_sql_files = files
    #         self._satements = enumerate(self.file_reader, start=1)

    # @classmethod
    # def from_user_input(cls, user_input: UserInput):
    #     return cls(user_input=user_input, files=None)
    #
    # @classmethod
    # def from_user_files(cls, user_files: UserFiles):
    #     return cls(user_input=None, files=user_files)

    @property
    def statements(self) -> NGStatementGenerator:
        yield from self._stmt_enumerator

    def user_input_reader(self, source: UserInput) -> StatementGenerator:
        content = self._user_input_io_wrapper(source)
        for statement in self._statement_reader(content):
            yield statement

    def file_reader(self, source: UserFiles) -> StatementGenerator:
        for file in source:
            secure_path = SecurePath(file)
            with secure_path.open("rb", read_limit=UNLIMITED) as secure_fh:
                data = self._file_handler_io_wrapper(secure_fh)
                for statement in self._statement_reader(data):
                    yield statement

    def _statement_reader(self, data: io.TextIOBase) -> StatementGenerator:
        for statement, _ in split_statements(data):
            if file_path := is_source_command(statement):
                with SecurePath(file_path).open(
                    mode="rb", read_file_limit_mb=UNLIMITED
                ) as secure_fh:
                    source_file = self._file_handler_io_wrapper(secure_fh)
                    for statement in self._statement_reader(source_file):
                        yield statement
            else:
                yield statement

    @staticmethod
    def _file_handler_io_wrapper(
        file_handler: SecurePathFileHandler,
    ) -> io.TextIOWrapper:
        match file_handler:
            case io.TextIOWrapper():
                return file_handler
            case io.BufferedIOBase():
                return io.TextIOWrapper(file_handler)
            case io.BufferedReader() | io.BufferedWriter() | io.BytesIO():
                return io.TextIOWrapper(file_handler)
            case _:
                raise TypeError("Unsupported io type")

    @staticmethod
    def _user_input_io_wrapper(user_input: UserInput) -> io.StringIO:
        return io.StringIO(user_input)

    # @staticmethod
    # def check_command(
    #     statement: Statement,
    #     command_checkers: t.Iterable[CommandChecker],
    # ) -> CommandCheckerResult:
    #     """runs checkers against a statement and returns first matched command as CommandCheckerResult"""
    #     for command_checker in command_checkers:
    #         command, command_payload = command_checker(statement)
    #         if command:
    #             return command, command_payload
    #     return "", ""


def test_class_based_reader_from_user_input():
    user_input = "select 1; select 2;"

    reader = NGStatementReader(user_input)

    assert next(reader.statements) == (1, "select 1;")
    assert next(reader.statements) == (2, "select 2;")

    with pytest.raises(StopIteration):
        next(reader.statements)
