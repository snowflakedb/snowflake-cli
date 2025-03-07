import enum
import io
import re
import urllib.error
from typing import Any, Callable, Generator, Literal, Sequence
from urllib.parse import urlparse
from urllib.request import urlopen

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


RecursiveStatementReader = Generator[ParsedSource, Any, Any]


def parse_source(source: str) -> ParsedSource:
    split_result = SOURCE_PATTERN.split(source, maxsplit=1)
    split_result = [p.strip() for p in split_result]

    if len(split_result) == 1:
        return ParsedSource(source, SourceType.QUERY, None)

    _, command, source_path, *_ = split_result

    match command.lower(), urlparse(source_path):
        # load content from an URL
        case "source" | "load", ("http" | "https", netloc, path, *_) if netloc and path:
            try:
                payload = urlopen(source_path, timeout=10.0).read().decode()
                return ParsedSource(payload, SourceType.URL, source_path)
            except urllib.error.HTTPError as err:
                error = f"Could not fetch {source_path}: {err}"
                return ParsedSource(source_path, SourceType.URL, source, error)

        # load content from a local file
        case "source" | "load", ("", "", path, *_) if SecurePath(path).is_file():
            _path = SecurePath(path)
            payload = _path.read_text(file_size_limit_mb=UNLIMITED)
            return ParsedSource(payload, SourceType.FILE, _path.as_posix())

        # load content from a non existing local file
        case "source" | "load", ("", "", path, *_) if not SecurePath(path).is_file():
            error_msg = f"Could not read: {source_path}"
            return ParsedSource(source_path, SourceType.FILE, source, error_msg)

        case _:
            error_msg = f"Unknown source: {source_path}"

    return ParsedSource(source_path, SourceType.UNKNOWN, source, error_msg)


def recursive_source_reader(
    source: SplitedStatements,
    seen_files: list,
) -> RecursiveStatementReader:
    for stmt, _ in source:
        parsed_source = parse_source(stmt)

        match parsed_source:
            case ParsedSource(SourceType.FILE, None):
                if parsed_source.source_path in seen_files:
                    error = f"Recursion detected: {' -> '.join(seen_files)}"
                    parsed_source.error = error
                    yield parsed_source
                    continue

                seen_files.append(parsed_source.source_path)

                yield from recursive_source_reader(
                    split_statements(parsed_source.source), seen_files
                )

                seen_files.pop()

            case ParsedSource(SourceType.URL, None):
                if parsed_source.source_path in seen_files:
                    error = f"Recursion detected: {' -> '.join(seen_files)}"
                    parsed_source.error = error
                    yield parsed_source
                    continue

                seen_files.append(parsed_source.source_path)

                yield from recursive_source_reader(
                    split_statements(parsed_source.source), seen_files
                )

                seen_files.pop()

            case ParsedSource(SourceType.URL, error) if error:
                yield parsed_source

            case _:
                yield parsed_source
    return


def file_reader(paths: Sequence[SecurePath]) -> RecursiveStatementReader:
    for path in paths:
        with path.open(read_file_limit_mb=UNLIMITED) as f:
            stmts = split_statements(io.StringIO(f.read()))
            yield from recursive_source_reader(stmts, [path.as_posix()])


def query_reader(source: str) -> RecursiveStatementReader:
    stmts = split_statements(io.StringIO(source))
    yield from recursive_source_reader(stmts, [])


def compile_statements(source: RecursiveStatementReader, operators: OperatorFunctions):
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
