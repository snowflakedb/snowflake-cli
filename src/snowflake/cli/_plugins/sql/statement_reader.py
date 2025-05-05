import enum
import io
import re
import urllib.error
from dataclasses import dataclass
from typing import Any, Callable, Generator, List, Literal, Sequence, Tuple
from urllib.request import urlopen

from jinja2 import UndefinedError
from snowflake.cli._plugins.sql.snowsql_commands import (
    SnowSQLCommand,
    compile_snowsql_command,
)
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath
from snowflake.connector.util_text import split_statements

COMMAND_PATTERN = re.compile(
    r"^!(\w+)\s*[\"']?(.*?)[\"']?\s*(?:;|$)",
    flags=re.IGNORECASE,
)
URL_PATTERN = re.compile(r"^(\w+?):\/(\/.*)", flags=re.IGNORECASE)

ASYNC_SUFFIX = ";>"


SplitedStatements = Generator[
    tuple[str, bool | None] | tuple[str, Literal[False]],
    Any,
    None,
]

SqlTransformFunc = Callable[[str], str]
OperatorFunctions = Sequence[SqlTransformFunc]


class StatementType(enum.Enum):
    FILE = "file"
    QUERY = "query"
    UNKNOWN = "unknown"
    URL = "url"
    SNOWSQL_COMMAND = "snowsql_command"


class ParsedStatement:
    """Container for parsed statement.

    Holds:
      - source: statement on command content
      - source_type: type of source
      - source_path: in case of URL or FILE path of the origin
      - error: optional message
    """

    __slots__ = ("statement", "statement_type", "source_path", "error")
    __match_args__ = ("statement_type", "error")

    statement: io.StringIO
    statement_type: StatementType | None
    source_path: str | None
    error: str | None

    def __init__(
        self,
        statement: str,
        source_type: StatementType,
        source_path: str | None,
        error: str | None = None,
    ):
        self.statement = io.StringIO(statement)
        self.statement_type = source_type
        self.source_path = source_path
        self.error = error

    def __bool__(self):
        return not self.error

    def __eq__(self, other):
        result = (
            self.statement_type == other.statement_type,
            self.source_path == other.source_path,
            self.error == other.error,
            self.statement.read() == other.statement.read(),
        )
        self.statement.seek(0)
        other.statement.seek(0)
        return all(result)

    def __repr__(self):
        return f"{self.__class__.__name__}(statement_type={self.statement_type}, source_path={self.source_path}, error={self.error})"

    @classmethod
    def from_url(cls, path_part: str, raw_source: str) -> "ParsedStatement":
        """Constructor for loading from URL."""
        try:
            payload = urlopen(path_part, timeout=10.0).read().decode()
            return cls(payload, StatementType.URL, path_part)

        except urllib.error.HTTPError as err:
            error = f"Could not fetch {path_part}: {err}"
            return cls(path_part, StatementType.URL, raw_source, error)

    @classmethod
    def from_file(cls, path_part: str, raw_source: str) -> "ParsedStatement":
        """Constructor for loading from file."""
        path = SecurePath(path_part)

        if path.is_file():
            payload = path.read_text(file_size_limit_mb=UNLIMITED)
            return cls(payload, StatementType.FILE, path.as_posix())

        error_msg = f"Could not read: {path_part}"
        return cls(path_part, StatementType.FILE, raw_source, error_msg)


RecursiveStatementReader = Generator[ParsedStatement, Any, Any]


def parse_statement(source: str, operators: OperatorFunctions) -> ParsedStatement:
    """Evaluates templating and source commands.

    Returns parsed source according to origin."""
    try:
        statement = source
        for operator in operators:
            statement = operator(statement)
    except UndefinedError as e:
        error_msg = f"SQL template rendering error: {e}"
        return ParsedStatement(source, StatementType.UNKNOWN, source, error_msg)

    split_result = COMMAND_PATTERN.split(statement, maxsplit=1)
    split_result = [p.strip() for p in split_result]

    if len(split_result) == 1:
        return ParsedStatement(statement, StatementType.QUERY, None)

    _, command, command_args, *_ = split_result
    _path_match = URL_PATTERN.split(command_args.lower())

    match command.lower(), _path_match:
        # load content from an URL
        case "source" | "load", ("", "http" | "https", *_):
            return ParsedStatement.from_url(command_args, statement)

        # load content from a local file
        case "source" | "load", (str(),):
            return ParsedStatement.from_file(command_args, statement)

        case "source" | "load", _:
            return ParsedStatement(
                statement,
                StatementType.UNKNOWN,
                command_args,
                f"Unknown source: {command_args}",
            )

        case "queries" | "result" | "abort", (str(),):
            return ParsedStatement(statement, StatementType.SNOWSQL_COMMAND, None)

        case _:
            error_msg = f"Unknown command: {command}"

    return ParsedStatement(statement, StatementType.UNKNOWN, None, error_msg)


def recursive_statement_reader(
    source: SplitedStatements,
    seen_files: list,
    operators: OperatorFunctions,
    remove_comments: bool,
) -> RecursiveStatementReader:
    """Based on detected source command reads content of the source and tracks for recursion cycles."""
    for stmt, _ in source:
        if not stmt:
            continue
        parsed_source = parse_statement(stmt, operators)

        match parsed_source:
            case ParsedStatement(StatementType.FILE | StatementType.URL, None):
                if parsed_source.source_path in seen_files:
                    error = f"Recursion detected: {' -> '.join(seen_files)}"
                    parsed_source.error = error
                    yield parsed_source
                    continue

                seen_files.append(parsed_source.source_path)

                yield from recursive_statement_reader(
                    split_statements(parsed_source.statement, remove_comments),
                    seen_files,
                    operators,
                    remove_comments,
                )

                seen_files.pop()

            case ParsedStatement(StatementType.URL, error) if error:
                yield parsed_source

            case _:
                yield parsed_source
    return


def files_reader(
    paths: Sequence[SecurePath],
    operators: OperatorFunctions,
    remove_comments: bool = False,
) -> RecursiveStatementReader:
    """Entry point for reading statements from files.

    Returns a generator with statements."""
    for path in paths:
        with path.open(read_file_limit_mb=UNLIMITED) as f:
            stmts = split_statements(io.StringIO(f.read()), remove_comments)
            yield from recursive_statement_reader(
                stmts,
                [path.as_posix()],
                operators,
                remove_comments,
            )


def query_reader(
    source: str,
    operators: OperatorFunctions,
    remove_comments: bool = False,
) -> RecursiveStatementReader:
    """Entry point for reading statements from query.

    Returns a generator with statements."""
    # known issue of split_statements (doesn't work in SnowSQL either):
    # when the line starts with a command:
    # '!queries amount=3; select 3;'
    # it is treated as a single statement
    stmts = split_statements(io.StringIO(source), remove_comments)
    yield from recursive_statement_reader(stmts, [], operators, remove_comments)


@dataclass
class CompiledStatement:
    statement: str | None = None
    execute_async: bool = False
    command: SnowSQLCommand | None = None


def _is_empty_statement(statement: str) -> bool:
    # checks whether all lines from the statement are empty or start with comment
    for line in statement.splitlines():
        if line.strip() and not line.lstrip().startswith("--"):
            # nonempty uncommented line
            return False
    return True


def compile_statements(
    source: RecursiveStatementReader,
) -> Tuple[List[str], int, List[CompiledStatement]]:
    """Tracks statements evaluation and collects errors."""
    errors = []
    expected_results_cnt = 0
    compiled = []

    for stmt in source:
        if stmt.statement_type == StatementType.QUERY:
            statement = stmt.statement.read()
            if not stmt.error and not _is_empty_statement(statement):
                is_async = statement.endswith(ASYNC_SUFFIX)
                compiled.append(
                    CompiledStatement(
                        statement=statement.removesuffix(ASYNC_SUFFIX),
                        execute_async=is_async,
                    )
                )
                if not is_async:
                    expected_results_cnt += 1

        if stmt.statement_type == StatementType.SNOWSQL_COMMAND:
            if not stmt.error:
                cmd = (
                    stmt.statement.read()
                    .removesuffix(ASYNC_SUFFIX)
                    .removesuffix(";")
                    .split()
                )
                parsed_command = compile_snowsql_command(
                    command=cmd[0], cmd_args=cmd[1:]
                )
                if parsed_command.error_message:
                    errors.append(parsed_command.error_message)
                else:
                    compiled.append(CompiledStatement(command=parsed_command.command))

        if stmt.error:
            errors.append(stmt.error)

    return errors, expected_results_cnt, compiled
