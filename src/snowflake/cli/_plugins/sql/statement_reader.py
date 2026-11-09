import enum
import io
import re
import urllib.error
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Generator, List, Literal, Sequence, Tuple
from urllib.request import HTTPRedirectHandler, build_opener

from jinja2 import UndefinedError
from snowflake.cli._plugins.sql.repl_commands import (
    ReplCommand,
    UnknownCommandError,
    compile_repl_command,
    is_registered_command,
)
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.exceptions import FileTooLargeError
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector.util_text import split_statements

# Bounds for !source / !load expansion (DoS: unbounded recursion and reads).
SQL_SOURCE_MAX_DEPTH = 32
SQL_SOURCE_MAX_INCLUDES = 32
SQL_SOURCE_SIZE_LIMIT_MB = DEFAULT_SIZE_LIMIT_MB
_URL_READ_CHUNK_SIZE = 65536

COMMAND_PATTERN = re.compile(
    r"^!(\w+)\s*[\"']?(.*?)[\"']?\s*(?:;|$)",
    flags=re.IGNORECASE,
)
URL_PATTERN = re.compile(r"^(\w+?):\/(\/.*)", flags=re.IGNORECASE)

ASYNC_SUFFIX = ";>"


class _PlaintextRedirectError(urllib.error.URLError):
    """Raised when a redirect would request a non-https URL."""


class _HttpsOnlyRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not str(newurl).lower().startswith("https://"):
            raise _PlaintextRedirectError(
                f"Plaintext http:// sources are not allowed: {newurl}. "
                "Use https:// or a local file."
            )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def urlopen(url, timeout=10.0):
    """Fetch *url*, refusing a redirect whose Location is not https://."""
    return build_opener(_HttpsOnlyRedirectHandler()).open(url, timeout=timeout)


# Regex that recognises SQL tokens whose contents must not be scanned for
# comment syntax.  Alternatives are tried left-to-right:
#   1. single-quoted string  'text' with '' escaping
#   2. $$-dollar-quoted string
#   3. opening of a block comment  /*  (content handled by depth-counting loop)
#   4. line comment  -- …  (everything up to but not including the newline)
_SQL_TOKEN_RE = re.compile(
    r"""
      '[^']*(?:''[^']*)*'          # single-quoted string ('' escape, no backslash)
    | \$\$.*?\$\$                  # $$-dollar-quoted string
    | (?P<block_start> /\* )       # block comment opening → depth-counted below
    | (?P<line_comment> --[^\n]* ) # line comment text (newline NOT consumed)
    """,
    re.VERBOSE | re.DOTALL,
)


@dataclass
class _SavedComments:
    """Stores SQL comment text replaced by inert placeholders.

    Placeholders contain a random nonce so they cannot collide with user SQL.
    """

    _comments: list = field(default_factory=list)
    _nonce: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    def save(self, text: str) -> str:
        """Replace *text* with a placeholder and remember the original."""
        idx = len(self._comments)
        self._comments.append(text)
        return f"__SQLC_{self._nonce}_{idx}__"

    def restore(self, sql: str) -> str:
        """Put all saved comment texts back into *sql*."""
        for i, comment in enumerate(self._comments):
            sql = sql.replace(f"__SQLC_{self._nonce}_{i}__", comment)
        return sql


def _protect_sql_comments(sql: str) -> tuple[str, _SavedComments]:
    """Replace SQL comments in *sql* with inert placeholders.

    Returns ``(placeholder_sql, saved)`` where ``saved.restore(rendered)``
    puts the original comment text back after Jinja has run.

    Handles:
    - ``--`` line comments
    - ``/* … */`` block comments, including Snowflake-style **nested** block
      comments (e.g. ``/* outer /* inner */ … */``)
    - Single-quoted strings and ``$$``-dollar-quoted strings are skipped so
      that comment-like syntax inside string literals is never mis-classified.

    Used inside ``_jinja_pre_render`` so that Jinja-like syntax inside comments
    (e.g. ``-- {{ var }}``) is not evaluated by Jinja.  Comment removal or
    retention is then left entirely to ``split_statements(remove_comments=…)``.
    """
    saved = _SavedComments()
    result: list[str] = []
    pos = 0

    while pos < len(sql):
        m = _SQL_TOKEN_RE.search(sql, pos)
        if m is None:
            break

        if m.group("line_comment"):
            result.append(sql[pos : m.start()])
            result.append(saved.save(m.group()))
            pos = m.end()

        elif m.group("block_start"):
            # Depth-count to find the matching */ for nested block comments.
            depth = 1
            j = m.end()
            while j < len(sql) and depth > 0:
                if sql[j : j + 2] == "/*":
                    depth += 1
                    j += 2
                elif sql[j : j + 2] == "*/":
                    depth -= 1
                    j += 2
                else:
                    j += 1
            result.append(sql[pos : m.start()])
            result.append(saved.save(sql[m.start() : j]))
            pos = j

        else:
            # String literal — emit as-is and advance past it.
            result.append(sql[pos : m.end()])
            pos = m.end()

    result.append(sql[pos:])
    return "".join(result), saved


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
    REPL_COMMAND = "repl_command"


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

    @staticmethod
    def drop_comments_from_path_parts(path_part: str) -> str:
        """Clean up path_part from trailing comments."""
        uncommented, _ = next(
            split_statements(io.StringIO(path_part), remove_comments=True)
        )
        return uncommented

    @classmethod
    def from_url(cls, path_part: str, raw_source: str) -> "ParsedStatement":
        """Constructor for loading from URL."""
        stripped_comments_path_part = cls.drop_comments_from_path_parts(path_part)
        response = None
        try:
            response = urlopen(stripped_comments_path_part, timeout=10.0)
            final_url = response.geturl()
            if not final_url.lower().startswith("https://"):
                error = (
                    f"Plaintext http:// sources are not allowed: {final_url}. "
                    "Use https:// or a local file."
                )
                return cls(path_part, StatementType.UNKNOWN, raw_source, error)
            payload_bytes = _read_url_bytes(response, _source_size_limit_bytes())
            if payload_bytes is None:
                error = f"Could not fetch {path_part}: payload exceeds {SQL_SOURCE_SIZE_LIMIT_MB} MB limit"
                return cls(path_part, StatementType.URL, raw_source, error)
            payload = payload_bytes.decode()
            return cls(payload, StatementType.URL, stripped_comments_path_part)

        # _PlaintextRedirectError subclasses URLError, so it must come first.
        except _PlaintextRedirectError as err:
            return cls(path_part, StatementType.UNKNOWN, raw_source, str(err.reason))
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as err:
            error = f"Could not fetch {path_part}: {err}"
            return cls(path_part, StatementType.URL, raw_source, error)
        finally:
            if response is not None:
                response.close()

    @classmethod
    def from_disabled_url(cls, path_part: str, raw_source: str) -> "ParsedStatement":
        """Constructor used when URL sources are disabled by configuration."""
        stripped_comments_path_part = cls.drop_comments_from_path_parts(path_part)
        error = (
            "Loading SQL from URLs is disabled (--local-only): "
            f"{stripped_comments_path_part}"
        )
        return cls(path_part, StatementType.URL, raw_source, error)

    @classmethod
    def from_file(cls, path_part: str, raw_source: str) -> "ParsedStatement":
        """Constructor for loading from file."""
        stripped_comments_path_part = cls.drop_comments_from_path_parts(path_part)
        path = SecurePath(stripped_comments_path_part)

        if path.is_file():
            try:
                payload = path.read_text(file_size_limit_mb=SQL_SOURCE_SIZE_LIMIT_MB)
            except FileTooLargeError:
                error_msg = f"Could not read: {path_part} (exceeds {SQL_SOURCE_SIZE_LIMIT_MB} MB limit)"
                return cls(path_part, StatementType.FILE, raw_source, error_msg)
            return cls(payload, StatementType.FILE, path.as_posix())

        error_msg = f"Could not read: {path_part}"
        return cls(
            path_part,
            StatementType.FILE,
            raw_source,
            error_msg,
        )


def _source_size_limit_bytes() -> int:
    return SQL_SOURCE_SIZE_LIMIT_MB * 1024 * 1024


def _read_url_bytes(response, max_bytes: int) -> bytes | None:
    """Read at most *max_bytes* from *response*. Return None if the body is larger."""
    chunks: list[bytes] = []
    total = 0
    while True:
        to_read = min(_URL_READ_CHUNK_SIZE, max_bytes + 1 - total)
        if to_read <= 0:
            return None
        chunk = response.read(to_read)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


def _canonical_source_path(parsed: ParsedStatement) -> str:
    """Stable identity for cycle detection (resolved files; raw URL strings)."""
    source_path = parsed.source_path or ""
    if parsed.statement_type == StatementType.FILE:
        return str(Path(source_path).resolve())
    return source_path


RecursiveStatementReader = Generator[ParsedStatement, Any, Any]


def parse_statement(
    source: str,
    operators: OperatorFunctions,
    disable_url_sources: bool = False,
    *,
    load_sources: bool = True,
) -> ParsedStatement:
    """Evaluates templating and source commands.

    Returns parsed source according to origin.

    When ``load_sources`` is False, ``!source`` / ``!load`` of files and URLs
    are classified only (path/URL recorded, no I/O). Callers that enforce
    depth and inclusion caps can then load via ``_load_classified_source``.
    """
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
        case "source" | "load", ("", "http", *_):
            return ParsedStatement(
                statement,
                StatementType.UNKNOWN,
                command_args,
                f"Plaintext http:// sources are not allowed: {command_args}. "
                "Use https:// or a local file.",
            )

        case "source" | "load", ("", "https", *_):
            if disable_url_sources:
                return ParsedStatement.from_disabled_url(command_args, statement)
            if not load_sources:
                stripped = ParsedStatement.drop_comments_from_path_parts(command_args)
                return ParsedStatement("", StatementType.URL, stripped)
            return ParsedStatement.from_url(command_args, statement)

        # load content from a local file
        case "source" | "load", (str(),):
            if not load_sources:
                stripped = ParsedStatement.drop_comments_from_path_parts(command_args)
                return ParsedStatement(
                    "", StatementType.FILE, SecurePath(stripped).as_posix()
                )
            return ParsedStatement.from_file(command_args, statement)

        case "source" | "load", _:
            return ParsedStatement(
                statement,
                StatementType.UNKNOWN,
                command_args,
                f"Unknown source: {command_args}",
            )

        case "edit", (str(),):
            return ParsedStatement(statement, StatementType.REPL_COMMAND, command_args)

        case cmd, (str(),) if is_registered_command(f"!{cmd}"):
            return ParsedStatement(statement, StatementType.REPL_COMMAND, None)

        case _:
            error_msg = f"Unknown command: {command}"

    return ParsedStatement(statement, StatementType.UNKNOWN, None, error_msg)


def _load_classified_source(
    parsed: ParsedStatement, raw_source: str
) -> ParsedStatement:
    """Fetch or read a ``!source`` / ``!load`` classified without I/O."""
    path = parsed.source_path or ""
    if parsed.statement_type == StatementType.URL:
        return ParsedStatement.from_url(path, raw_source)
    return ParsedStatement.from_file(path, raw_source)


def recursive_statement_reader(
    source: SplitedStatements,
    seen_files: list,
    operators: OperatorFunctions,
    remove_comments: bool,
    pre_render: SqlTransformFunc | None = None,
    disable_url_sources: bool = False,
    *,
    depth: int = 0,
    include_count: list[int] | None = None,
) -> RecursiveStatementReader:
    """Based on detected source command reads content of the source and tracks for recursion cycles."""
    if include_count is None:
        include_count = [0]

    for stmt, _ in source:
        if not stmt:
            continue
        parsed_source = parse_statement(
            stmt, operators, disable_url_sources, load_sources=False
        )

        match parsed_source:
            case ParsedStatement(StatementType.FILE | StatementType.URL, None):
                identity = _canonical_source_path(parsed_source)
                if identity in seen_files:
                    error = f"Recursion detected: {' -> '.join(seen_files)}"
                    parsed_source.error = error
                    yield parsed_source
                    continue

                if depth >= SQL_SOURCE_MAX_DEPTH:
                    parsed_source.error = f"Maximum !source nesting depth ({SQL_SOURCE_MAX_DEPTH}) exceeded"
                    yield parsed_source
                    continue

                if include_count[0] >= SQL_SOURCE_MAX_INCLUDES:
                    parsed_source.error = f"Maximum number of !source/!load inclusions ({SQL_SOURCE_MAX_INCLUDES}) exceeded"
                    yield parsed_source
                    continue

                include_count[0] += 1
                parsed_source = _load_classified_source(parsed_source, stmt)
                if parsed_source.error:
                    yield parsed_source
                    continue

                seen_files.append(identity)

                content = parsed_source.statement.read()
                if pre_render:
                    content = pre_render(content)

                yield from recursive_statement_reader(
                    split_statements(io.StringIO(content), remove_comments),
                    seen_files,
                    operators,
                    remove_comments,
                    pre_render,
                    disable_url_sources,
                    depth=depth + 1,
                    include_count=include_count,
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
    pre_render: SqlTransformFunc | None = None,
    disable_url_sources: bool = False,
) -> RecursiveStatementReader:
    """Entry point for reading statements from files.

    Returns a generator with statements."""
    for path in paths:
        try:
            with path.open(read_file_limit_mb=SQL_SOURCE_SIZE_LIMIT_MB) as f:
                content = f.read()
        except FileTooLargeError:
            yield ParsedStatement(
                "",
                StatementType.FILE,
                path.as_posix(),
                f"Could not read: {path.as_posix()} (exceeds {SQL_SOURCE_SIZE_LIMIT_MB} MB limit)",
            )
            continue
        if pre_render:
            content = pre_render(content)
        stmts = split_statements(io.StringIO(content), remove_comments)
        # per -f file; include budget is not shared across paths
        yield from recursive_statement_reader(
            stmts,
            [str(path.path.resolve())],
            operators,
            remove_comments,
            pre_render,
            disable_url_sources,
        )


def query_reader(
    source: str,
    operators: OperatorFunctions,
    remove_comments: bool = False,
    pre_render: SqlTransformFunc | None = None,
    disable_url_sources: bool = False,
) -> RecursiveStatementReader:
    """Entry point for reading statements from query.

    Returns a generator with statements."""
    # known issue of split_statements (doesn't work in SnowSQL either):
    # when the line starts with a command:
    # '!queries amount=3; select 3;'
    # it is treated as a single statement
    content = source
    if pre_render:
        content = pre_render(content)
    stmts = split_statements(io.StringIO(content), remove_comments)
    yield from recursive_statement_reader(
        stmts, [], operators, remove_comments, pre_render, disable_url_sources
    )


@dataclass
class CompiledStatement:
    statement: str | None = None
    execute_async: bool = False
    command: ReplCommand | None = None


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

        if stmt.statement_type == StatementType.REPL_COMMAND:
            if not stmt.error:
                command_text = (
                    stmt.statement.read()
                    .removesuffix(ASYNC_SUFFIX)
                    .removesuffix(";")
                    .strip()
                )
                try:
                    parsed_command = compile_repl_command(command_text)
                    if parsed_command.error_message:
                        errors.append(parsed_command.error_message)
                    else:
                        compiled.append(
                            CompiledStatement(command=parsed_command.command)
                        )
                except UnknownCommandError as e:
                    errors.append(str(e))
                except Exception as e:
                    errors.append(f"Error parsing command: {e}")

        if stmt.error:
            errors.append(stmt.error)

    return errors, expected_results_cnt, compiled
