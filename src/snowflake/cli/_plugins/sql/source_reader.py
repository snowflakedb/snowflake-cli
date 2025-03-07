import enum
import io
import re
import urllib.error
from typing import Any, Callable, Generator, Literal, Sequence
from urllib.parse import urlparse
from urllib.request import urlopen

from jinja2 import UndefinedError
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath
from snowflake.connector.util_text import split_statements

SOURCE_PATTERN = re.compile(
    r"!(source|load)\s+[\"']?(.*?)[\"']?\s*(?:;|$)",
    flags=re.IGNORECASE,
)

SplitedStatements = Generator[
    tuple[str, bool | None] | tuple[str, Literal[False]],
    Any,
    None,
]

SqlTransofrmFunc = Callable[[str], str]
OperatorFunctions = Sequence[SqlTransofrmFunc]


class SourceType(enum.Enum):
    FILE = "file"
    QUERY = "query"
    UNKNOWN = "unknown"
    URL = "url"


class ParsedSource:
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
        # TODO: render each SourceType.QUERY to handle variables resolution
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
    def from_url(cls, path_part: str, raw_source: str) -> "ParsedSource":
        try:
            payload = urlopen(path_part, timeout=10.0).read().decode()
            return cls(payload, SourceType.URL, path_part)

        except urllib.error.HTTPError as err:
            error = f"Could not fetch {path_part}: {err}"
            return cls(path_part, SourceType.URL, raw_source, error)

    @classmethod
    def from_file(cls, path_part: str, raw_source: str) -> "ParsedSource":
        path = SecurePath(path_part)

        if path.is_file():
            payload = path.read_text(file_size_limit_mb=UNLIMITED)
            return cls(payload, SourceType.FILE, path.as_posix())

        error_msg = f"Could not read: {path_part}"
        return cls(path_part, SourceType.FILE, raw_source, error_msg)


RecursiveStatementReader = Generator[ParsedSource, Any, Any]


def parse_source(source: str, operators: OperatorFunctions) -> ParsedSource:
    try:
        statement = source
        for operator in operators:
            statement = operator(statement)
    except UndefinedError as e:
        error_msg = f"SQL template rendering error: {e}"
        return ParsedSource(source, SourceType.UNKNOWN, source, error_msg)

    split_result = SOURCE_PATTERN.split(statement, maxsplit=1)
    split_result = [p.strip() for p in split_result]

    if len(split_result) == 1:
        return ParsedSource(statement, SourceType.QUERY, None)

    _, command, source_path, *_ = split_result

    match command.lower(), urlparse(source_path):
        # load content from an URL
        case "source" | "load", ("http" | "https", netloc, path, *_) if netloc and path:
            return ParsedSource.from_url(source_path, statement)

        # load content from a local file
        case "source" | "load", ("", "", path, *_) if path:
            return ParsedSource.from_file(path, statement)

        case _:
            error_msg = f"Unknown source: {source_path}"

    return ParsedSource(source_path, SourceType.UNKNOWN, source, error_msg)


def recursive_source_reader(
    source: SplitedStatements,
    seen_files: list,
    operators: OperatorFunctions,
    remove_comments: bool,
) -> RecursiveStatementReader:
    for stmt, _ in source:
        if not stmt:
            continue
        parsed_source = parse_source(stmt, operators)

        match parsed_source:
            case ParsedSource(SourceType.FILE | SourceType.URL, None):
                if parsed_source.source_path in seen_files:
                    error = f"Recursion detected: {' -> '.join(seen_files)}"
                    parsed_source.error = error
                    yield parsed_source
                    continue

                seen_files.append(parsed_source.source_path)

                yield from recursive_source_reader(
                    split_statements(parsed_source.source, remove_comments),
                    seen_files,
                    operators,
                    remove_comments,
                )

                seen_files.pop()

            # case ParsedSource(SourceType.URL, None):
            #     if parsed_source.source_path in seen_files:
            #         error = f"Recursion detected: {' -> '.join(seen_files)}"
            #         parsed_source.error = error
            #         yield parsed_source
            #         continue
            #
            #     seen_files.append(parsed_source.source_path)
            #
            #     yield from recursive_source_reader(
            #         split_statements(parsed_source.source, remove_comments),
            #         seen_files,
            #         operators,
            #         remove_comments,
            #     )
            #
            #     seen_files.pop()

            case ParsedSource(SourceType.URL, error) if error:
                yield parsed_source

            case _:
                yield parsed_source
    return


def files_reader(
    paths: Sequence[SecurePath],
    operators: OperatorFunctions,
    remove_comments: bool = False,
) -> RecursiveStatementReader:
    for path in paths:
        with path.open(read_file_limit_mb=UNLIMITED) as f:
            stmts = split_statements(io.StringIO(f.read()), remove_comments)
            yield from recursive_source_reader(
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
    stmts = split_statements(io.StringIO(source), remove_comments)
    yield from recursive_source_reader(stmts, [], operators, remove_comments)


def compile_statements(source: RecursiveStatementReader):
    errors = []
    cnt = 0
    compiled = []

    for stmt in source:
        if stmt.source_type == SourceType.QUERY:
            cnt += 1
            if not stmt.error:
                compiled.append(stmt.source.read())
        if stmt.error:
            errors.append(stmt.error)

    return errors, cnt, compiled
