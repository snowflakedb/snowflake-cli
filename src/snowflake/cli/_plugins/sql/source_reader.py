import enum
import io
import re
import urllib.error
from dataclasses import dataclass
from typing import Any, Callable, Generator, Literal, Sequence
from urllib.request import urlopen

from jinja2 import UndefinedError
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath
from snowflake.connector.util_text import split_statements

COMMAND_PATTERN = re.compile(
    r"^!(source|load|queries)\s*[\"']?(.*?)[\"']?\s*(?:;|$)",
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


class SourceType(enum.Enum):
    FILE = "file"
    QUERY = "query"
    UNKNOWN = "unknown"
    URL = "url"


class ParsedStatement:
    """Container for parsed statement.

    Holds:
      - source: statement on command content
      - source_type: type of source
      - source_path: in case of URL or FILE path of the origin
      - error: optional message
    """

    __slots__ = ("source", "source_type", "source_path", "error")
    __match_args__ = ("source_type", "error")

    source: io.StringIO
    source_type: SourceType | None
    source_path: str | None
    error: str | None

    def __init__(
        self,
        source: str,
        source_type: SourceType,
        source_path: str | None,
        error: str | None = None,
    ):
        self.source = io.StringIO(source)
        self.source_type = source_type
        self.source_path = source_path
        self.error = error

    def __bool__(self):
        return not self.error

    def __eq__(self, other):
        result = (
            self.source_type == other.source_type,
            self.source_path == other.source_path,
            self.error == other.error,
            self.source.read() == other.source.read(),
        )
        self.source.seek(0)
        other.source.seek(0)
        return all(result)

    def __repr__(self):
        return f"{self.__class__.__name__}(source_type={self.source_type}, source_path={self.source_path}, error={self.error})"

    @classmethod
    def from_url(cls, path_part: str, raw_source: str) -> "ParsedStatement":
        """Constructor for loading from URL."""
        try:
            payload = urlopen(path_part, timeout=10.0).read().decode()
            return cls(payload, SourceType.URL, path_part)

        except urllib.error.HTTPError as err:
            error = f"Could not fetch {path_part}: {err}"
            return cls(path_part, SourceType.URL, raw_source, error)

    @classmethod
    def from_file(cls, path_part: str, raw_source: str) -> "ParsedStatement":
        """Constructor for loading from file."""
        path = SecurePath(path_part)

        if path.is_file():
            payload = path.read_text(file_size_limit_mb=UNLIMITED)
            return cls(payload, SourceType.FILE, path.as_posix())

        error_msg = f"Could not read: {path_part}"
        return cls(path_part, SourceType.FILE, raw_source, error_msg)


RecursiveStatementReader = Generator[ParsedStatement, Any, Any]


@dataclass
class ParseCommandResult:
    query: str = ""
    error_message: str | None = None


def _parse_command_queries(args, passed_kwargs):
    expected_kwargs = {
        "amount": "25",
        "status": None,
        "warehouse": None,
        "user": None,
        "start": None,
        "end": None,
        "type": None,
        "duration": None,
    }

    # validate args
    for key in passed_kwargs:
        if key not in expected_kwargs:
            return ParseCommandResult(
                error_message=f"Unrecognized argument for command 'query': '{key}'"
            )
    for arg in args:
        if arg not in ["session", "help"]:
            return ParseCommandResult(
                error_message=f"Unrecognized argument for command 'query': '{arg}'"
            )
    expected_kwargs.update(**passed_kwargs)
    kwargs = expected_kwargs

    # parse query
    if "help" in args:
        raise NotImplementedError

    conditions = ["true"]
    if "session" in args or len(passed_kwargs) == 0:
        conditions.append("session_id = CURRENT_SESSION()")
    if kwargs["status"]:
        status = kwargs["status"].upper()
        if status not in [
            "RUNNING",
            "SUCCEEDED",
            "FAILED",
            "BLOCKED",
            "QUEUED",
            "ABORTED",
        ]:
            return ParseCommandResult(
                error_message=f"Invalid argument passed to status filter: {status}"
            )
        conditions.append(f"execution_status = '{status}'")
        #
        # while arg != "":
        #     x = arg.split(" ", 1)
        #     s = x[0]
        #     arg = x[1] if arg != x[0] else arg
        #     if s == "session":  # all options that dont need an arg are below here
        #         session = cli.sqlexecute.session_id
        #     else:
        #         s = s.split("=", 1)
        #         if len(s) == 1:
        #             cli.output(
        #                 "Invalid argument passed to queries command: {s}".format(
        #                     s=s[0]
        #                 ),
        #                 err=True,
        #                 fg="red",
        #             )
        #             return []
        #         if (
        #             s[1].startswith('"')
        #             and s[1].endswith('"')
        #             and not s[1].endswith('\\"')
        #         ):
        #             s[1] = s[1][1:-1]
        #         elif s[1].startswith('"') and (
        #             (not s[1].endswith('"')) or s[1].endswith('\\"')
        #         ):
        #             s[1] = s[1][1:]
        #             x = arg.split('"', 1)
        #             if len(x) <= 1:
        #                 cli.output("Invalid quoting", err=True, fg="red")
        #                 return []
        #             s[1] += " " + x[0]
        #             arg = x[1]
        #
        #         elif s[0] == "warehouse":
        #             warehouse = s[1]
        #         elif s[0] == "user":
        #             user = s[1].upper()
        #         elif s[0] == "start":
        #             start_time = s[1]
        #         elif s[0] == "end":
        #             end_time = s[1]
        #         elif s[0] == "type":
        #             accepted = [
        #                 "ANY",
        #                 "SELECT",
        #                 "INSERT",
        #                 "UPDATE",
        #                 "DELETE",
        #                 "MERGE",
        #                 "MULTI_TABLE_INSERT",
        #                 "COPY",
        #                 "COMMIT",
        #                 "ROLLBACK",
        #                 "BEGIN_TRANSACTION",
        #                 "SHOW",
        #                 "GRANT",
        #                 "CREATE",
        #                 "ALTER",
        #             ]
        #             stmt_type = s[1].replace(" ", "_").upper()
        #             if stmt_type not in accepted:
        #                 cli.output(
        #                     "Invalid argument passed to type filter: {stmt_type}".format(
        #                         stmt_type=stmt_type
        #                     ),
        #                     err=True,
        #                     fg="red",
        #                 )
        #                 return []
        #         elif s[0] == "duration":
        #             min_duration = s[1]
        #         else:
        #
        #             cli.output(
        #                 "Invalid argument passed to queries command: {s}".format(
        #                     s=s[0]
        #                 ),
        #                 err=True,
        #                 fg="red",
        #             )
        #             return []
        #     if arg == x[0]:
        #         break

    query = f"""SELECT
      query_id as "QUERY ID",
      query_text as "SQL TEXT",
      execution_status as STATUS,
      total_elapsed_time as DURATION_MS
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE {" AND ".join(conditions)}
    ORDER BY start_time
    LIMIT {kwargs['amount']}"""

    return ParseCommandResult(query=query)


def _parse_command(command: str, cmd_args: str):
    """Parses command into SQL query"""
    args = []
    kwargs = {}
    for cmd_arg in cmd_args.split():
        if "=" not in cmd_arg:
            args.append(cmd_arg)
        else:
            key, val = cmd_arg.split("=", maxsplit=1)
            if key in kwargs:
                return ParseCommandResult(
                    error_message=f"duplicated argument '{key}' for command '{command}'",
                )
            kwargs[key] = val

    match command.lower():
        case "queries":
            return _parse_command_queries(args, kwargs)
        case _:
            return ParseCommandResult(error_message=f"Unknown command '{command}'")


def parse_statement(source: str, operators: OperatorFunctions) -> ParsedStatement:
    """Evaluates templating and source commands.

    Returns parsed source according to origin."""
    try:
        statement = source
        for operator in operators:
            statement = operator(statement)
    except UndefinedError as e:
        error_msg = f"SQL template rendering error: {e}"
        return ParsedStatement(source, SourceType.UNKNOWN, source, error_msg)

    split_result = COMMAND_PATTERN.split(statement, maxsplit=1)
    split_result = [p.strip() for p in split_result]

    if len(split_result) == 1:
        return ParsedStatement(statement, SourceType.QUERY, None)

    _, command, command_args, suffix = split_result
    _path_match = URL_PATTERN.split(command_args.lower())

    match command.lower(), _path_match:
        # load content from an URL
        case "source" | "load", ("", "http" | "https", *_):
            return ParsedStatement.from_url(command_args, statement)

        # load content from a local file
        case "source" | "load", (str(),):
            return ParsedStatement.from_file(command_args, statement)

        # translate command into SQL query
        case "queries", (str(),):
            translation = _parse_command(command=command, cmd_args=command_args)
            return ParsedStatement(
                translation.query + f";{suffix}",
                SourceType.QUERY,
                None,
                translation.error_message,
            )

        case _:
            error_msg = f"Unknown command: {source}"

    return ParsedStatement(statement, SourceType.UNKNOWN, source, error_msg)


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
            case ParsedStatement(SourceType.FILE | SourceType.URL, None):
                if parsed_source.source_path in seen_files:
                    error = f"Recursion detected: {' -> '.join(seen_files)}"
                    parsed_source.error = error
                    yield parsed_source
                    continue

                seen_files.append(parsed_source.source_path)

                yield from recursive_statement_reader(
                    split_statements(parsed_source.source, remove_comments),
                    seen_files,
                    operators,
                    remove_comments,
                )

                seen_files.pop()

            case ParsedStatement(SourceType.URL, error) if error:
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
    stmts = split_statements(io.StringIO(source), remove_comments)
    yield from recursive_statement_reader(stmts, [], operators, remove_comments)


@dataclass
class CompiledStatement:
    statement: str
    execute_async: bool


def compile_statements(source: RecursiveStatementReader):
    """Tracks statements evaluation and collects errors."""
    errors = []
    expected_results_cnt = 0
    compiled = []

    for stmt in source:
        if stmt.source_type == SourceType.QUERY:
            if not stmt.error:
                statement = stmt.source.read()
                is_async = statement.endswith(ASYNC_SUFFIX)
                compiled.append(
                    CompiledStatement(
                        statement=statement.rstrip(ASYNC_SUFFIX),
                        execute_async=is_async,
                    )
                )
                if not is_async:
                    expected_results_cnt += 1

        if stmt.error:
            errors.append(stmt.error)

    return errors, expected_results_cnt, compiled
