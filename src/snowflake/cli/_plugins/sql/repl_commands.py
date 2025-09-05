import abc
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Generator, Iterable, List, Tuple, Type
from urllib.parse import urlencode

import click
from snowflake.cli._app.printing import print_result
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.output.types import CollectionResult, QueryResult
from snowflake.connector import SnowflakeConnection

# Command pattern to detect REPL commands
COMMAND_PATTERN = re.compile(r"^(![\w]+)(?:\s+(.*))?$")

log = logging.getLogger(__name__)

VALID_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)

# Registry for auto-discovery of commands
_COMMAND_REGISTRY: Dict[str, Type["ReplCommand"]] = {}


class UnknownCommandError(CliError):
    """Raised when a command pattern matches but no registered command is found."""

    def __init__(self, command_name: str):
        self.command_name = command_name
        super().__init__(f"Unknown command '{command_name}'")


def register_command(command_name: str | List[str]):
    """Decorator to register a command class."""

    def decorator(cls):
        command_names = (
            command_name if isinstance(command_name, list) else [command_name]
        )
        for cmd_name in command_names:
            _COMMAND_REGISTRY[cmd_name.lower()] = cls
        return cls

    return decorator


class ReplCommand(abc.ABC):
    """Base class for REPL commands."""

    @abc.abstractmethod
    def execute(self, connection: SnowflakeConnection):
        """Executes command and prints the result."""
        ...

    @classmethod
    @abc.abstractmethod
    def from_args(cls, raw_args, kwargs=None) -> "CompileCommandResult":
        """Parses raw argument string and creates command ready for execution."""
        ...

    @classmethod
    def _parse_args(cls, raw_args: str) -> tuple[List[str], Dict[str, Any]]:
        """Parse raw argument string into positional args and keyword arguments.

        This is a helper method that commands can use for standard argument parsing.
        Commands can override this if they need custom parsing logic.
        """
        if not raw_args:
            return [], {}

        args = []
        kwargs = {}
        cmd_args = raw_args.split()

        for cmd_arg in cmd_args:
            if "=" not in cmd_arg:
                args.append(cmd_arg)
            else:
                key, val = cmd_arg.split("=", maxsplit=1)
                if key in kwargs:
                    raise ValueError(f"Duplicated argument '{key}'")
                kwargs[key] = val

        return args, kwargs


def _print_result_to_stdout(headers: Iterable[str], rows: Iterable[Iterable[Any]]):
    formatted_rows: Generator[Dict[str, Any], None, None] = (
        {key: value for key, value in zip(headers, row)} for row in rows
    )
    print_result(CollectionResult(formatted_rows))


@dataclass
class CompileCommandResult:
    command: ReplCommand | None = None
    error_message: str | None = None


@register_command("!queries")
@dataclass
class QueriesCommand(ReplCommand):
    """Command to query and display query history."""

    help_mode: bool = False
    from_current_session: bool = False
    amount: int = 25
    user: str | None = None
    warehouse: str | None = None
    start_timestamp_ms: int | None = None
    end_timestamp_ms: int | None = None
    duration: str | None = None
    stmt_type: str | None = None
    status: str | None = None

    def execute(self, connection: SnowflakeConnection) -> None:
        if self.help_mode:
            self._execute_help()
        else:
            self._execute_queries(connection)

    def _execute_help(self):
        headers = ["FILTER", "ARGUMENT", "DEFAULT"]
        filters = [
            ["amount", "integer", "25"],
            ["status", "string", "any"],
            ["warehouse", "string", "any"],
            ["user", "string", "any"],
            [
                "start_date",
                "datetime in ISO format (for example YYYY-MM-DDTHH:mm:ss.sss)",
                "any",
            ],
            [
                "end_date",
                "datetime in ISO format (for example YYYY-MM-DDTHH:mm:ss.sss)",
                "any",
            ],
            ["start", "timestamp in milliseconds (integer)", "any"],
            ["end", "timestamp in milliseconds (integer)", "any"],
            ["type", "string", "any"],
            ["duration", "time in milliseconds", "any"],
            ["session", "No arguments", "any"],
        ]
        _print_result_to_stdout(headers, filters)

    def _execute_queries(self, connection: SnowflakeConnection) -> None:
        url_parameters = {
            "_dc": f"{time.time()}",
            "includeDDL": "false",
            "max": self.amount,
        }
        if self.user:
            url_parameters["user"] = self.user
        if self.warehouse:
            url_parameters["wh"] = self.warehouse
        if self.start_timestamp_ms:
            url_parameters["start"] = self.start_timestamp_ms
        if self.end_timestamp_ms:
            url_parameters["end"] = self.end_timestamp_ms
        if self.duration:
            url_parameters["min_duration"] = self.duration
        if self.from_current_session:
            url_parameters["session_id"] = connection.session_id
        if self.status:
            url_parameters["subset"] = self.status
        if self.stmt_type:
            url_parameters["stmt_type"] = self.stmt_type

        url = "/monitoring/queries?" + urlencode(url_parameters)
        ret = connection.rest.request(url=url, method="get", client="rest")
        if ret.get("data") and ret["data"].get("queries"):
            _result: Generator[Tuple[str, str, str, str], None, None] = (
                (
                    query["id"],
                    query["sqlText"],
                    query["state"],
                    query["totalDuration"],
                )
                for query in ret["data"]["queries"]
            )
            _print_result_to_stdout(
                ["QUERY ID", "SQL TEXT", "STATUS", "DURATION_MS"], _result
            )

    @classmethod
    def from_args(cls, raw_args, kwargs=None) -> CompileCommandResult:
        """Parse arguments and create QueriesCommand instance.

        Supports both calling patterns:
        - New pattern: from_args("amount=3 user=jdoe")
        - Old pattern: from_args(["session"], {"amount": "3"})
        """
        if isinstance(raw_args, str):
            try:
                args, kwargs = cls._parse_args(raw_args)
            except ValueError as e:
                return CompileCommandResult(error_message=str(e))
        else:
            args, kwargs = raw_args, kwargs or {}

        return cls._from_parsed_args(args, kwargs)

    @classmethod
    def _from_parsed_args(
        cls, args: List[str], kwargs: Dict[str, Any]
    ) -> CompileCommandResult:
        if "help" in args:
            return CompileCommandResult(command=cls(help_mode=True))

        # "session" is set by default if no other arguments are provided
        from_current_session = "session" in args or not kwargs
        amount = kwargs.pop("amount", "25")
        if not amount.isdigit():
            return CompileCommandResult(
                error_message=f"Invalid argument passed to 'amount' filter: {amount}"
            )
        user = kwargs.pop("user", None)
        warehouse = kwargs.pop("warehouse", None)
        duration = kwargs.pop("duration", None)

        start_timestamp_ms = kwargs.pop("start", None)
        if start_timestamp_ms:
            try:
                start_timestamp_ms = int(start_timestamp_ms)
            except ValueError:
                return CompileCommandResult(
                    error_message=f"Invalid argument passed to 'start' filter: {start_timestamp_ms}"
                )
        end_timestamp_ms = kwargs.pop("end", None)
        if end_timestamp_ms:
            try:
                end_timestamp_ms = int(end_timestamp_ms)
            except ValueError:
                return CompileCommandResult(
                    error_message=f"Invalid argument passed to 'end' filter: {end_timestamp_ms}"
                )

        start_date = kwargs.pop("start_date", None)
        if start_date:
            if start_timestamp_ms:
                return CompileCommandResult(
                    error_message="'start_date' filter cannot be used with 'start' filter"
                )
            try:
                seconds = datetime.fromisoformat(start_date).timestamp()
                start_timestamp_ms = int(seconds * 1000)  # convert to milliseconds
            except ValueError:
                return CompileCommandResult(
                    error_message=f"Invalid date format passed to 'start_date' filter: {start_date}"
                )
        end_date = kwargs.pop("end_date", None)
        if end_date:
            if end_timestamp_ms:
                return CompileCommandResult(
                    error_message="'end_date' filter cannot be used with 'end' filter"
                )
            try:
                seconds = datetime.fromisoformat(end_date).timestamp()
                end_timestamp_ms = int(seconds * 1000)  # convert to milliseconds
            except ValueError:
                return CompileCommandResult(
                    error_message=f"Invalid date format passed to 'end_date' filter: {end_date}"
                )

        stmt_type = kwargs.pop("type", None)
        if stmt_type:
            stmt_type = stmt_type.upper()
            if stmt_type not in [
                "ANY",
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "MERGE",
                "MULTI_TABLE_INSERT",
                "COPY",
                "COMMIT",
                "ROLLBACK",
                "BEGIN_TRANSACTION",
                "SHOW",
                "GRANT",
                "CREATE",
                "ALTER",
            ]:
                return CompileCommandResult(
                    error_message=f"Invalid argument passed to 'type' filter: {stmt_type}"
                )

        status = kwargs.pop("status", None)
        if status:
            status = status.upper()
            if status not in [
                "RUNNING",
                "SUCCEEDED",
                "FAILED",
                "BLOCKED",
                "QUEUED",
                "ABORTED",
            ]:
                return CompileCommandResult(
                    error_message=f"Invalid argument passed to 'status' filter: {status}"
                )

        for arg in args:
            if arg.lower() not in ["session", "help"]:
                return CompileCommandResult(
                    error_message=f"Invalid argument passed to 'queries' command: {arg}"
                )

        kwargs_error = _validate_kwargs_empty("queries", kwargs)
        if kwargs_error:
            return CompileCommandResult(error_message=kwargs_error)

        return CompileCommandResult(
            command=cls(
                help_mode=False,
                from_current_session=from_current_session,
                amount=int(amount),
                user=user,
                warehouse=warehouse,
                start_timestamp_ms=start_timestamp_ms,
                end_timestamp_ms=end_timestamp_ms,
                duration=duration,
                stmt_type=stmt_type,
                status=status,
            )
        )


def _validate_kwargs_empty(command_name: str, kwargs: Dict[str, Any]) -> str | None:
    """Validate that kwargs is empty and return comprehensive error message if not."""
    if not kwargs:
        return None

    invalid_args = [f"{key}={value}" for key, value in kwargs.items()]
    if len(invalid_args) == 1:
        return f"Invalid argument passed to '{command_name}' command: {invalid_args[0]}"
    else:
        args_str = ", ".join(invalid_args)
        return f"Invalid arguments passed to '{command_name}' command: {args_str}"


def _validate_only_arg_is_query_id(
    command_name: str, args: List[str], kwargs: Dict[str, Any]
) -> str | None:
    kwargs_error = _validate_kwargs_empty(command_name, kwargs)
    if kwargs_error:
        return kwargs_error
    if len(args) != 1:
        amount = "Too many" if args else "No"
        return f"{amount} arguments passed to '{command_name}' command. Usage: `!{command_name} <query id>`"

    qid = args[0]
    if not VALID_UUID_RE.match(qid):
        return f"Invalid query ID passed to '{command_name}' command: {qid}"

    return None


@register_command("!result")
@dataclass
class ResultCommand(ReplCommand):
    """Command to retrieve and display query results by ID."""

    query_id: str

    def execute(self, connection: SnowflakeConnection):
        cursor = connection.cursor()
        cursor.query_result(self.query_id)
        print_result(QueryResult(cursor=cursor))

    @classmethod
    def from_args(cls, raw_args, kwargs=None) -> CompileCommandResult:
        """Parse arguments and create ResultCommand instance.

        Supports both calling patterns:
        - New pattern: from_args("00000000-0000-0000-0000-000000000000")
        - Old pattern: from_args(["query_id"], {})
        """
        if isinstance(raw_args, str):
            try:
                args, kwargs = cls._parse_args(raw_args)
            except ValueError as e:
                return CompileCommandResult(error_message=str(e))
        else:
            args, kwargs = raw_args, kwargs or {}

        return cls._from_parsed_args(args, kwargs)

    @classmethod
    def _from_parsed_args(cls, args, kwargs) -> CompileCommandResult:
        error_msg = _validate_only_arg_is_query_id("result", args, kwargs)
        if error_msg:
            return CompileCommandResult(error_message=error_msg)
        return CompileCommandResult(command=cls(args[0]))


@register_command("!abort")
@dataclass
class AbortCommand(ReplCommand):
    """Command to abort a running query by ID."""

    query_id: str

    def execute(self, connection: SnowflakeConnection):
        cursor = connection.cursor()
        cursor.execute("SELECT SYSTEM$CANCEL_QUERY(%s)", (self.query_id,))
        print_result(QueryResult(cursor=cursor))

    @classmethod
    def from_args(cls, raw_args, kwargs=None) -> CompileCommandResult:
        """Parse arguments and create AbortCommand instance.

        Supports both calling patterns:
        - New pattern: from_args("00000000-0000-0000-0000-000000000000")
        - Old pattern: from_args(["query_id"], {})
        """
        if isinstance(raw_args, str):
            try:
                args, kwargs = cls._parse_args(raw_args)
            except ValueError as e:
                return CompileCommandResult(error_message=str(e))
        else:
            args, kwargs = raw_args, kwargs or {}

        return cls._from_parsed_args(args, kwargs)

    @classmethod
    def _from_parsed_args(cls, args, kwargs) -> CompileCommandResult:
        error_msg = _validate_only_arg_is_query_id("abort", args, kwargs)
        if error_msg:
            return CompileCommandResult(error_message=error_msg)
        return CompileCommandResult(command=cls(args[0]))


@register_command("!edit")
@dataclass
class EditCommand(ReplCommand):
    """Command to edit SQL statements using an external editor."""

    sql_content: str = ""

    def execute(self, connection: SnowflakeConnection):
        """Execute the edit command.

        Flow:
        1. Validate REPL mode and EDITOR environment variable
        2. Get content to edit (provided args or last command from history)
        3. Open editor with content using click.edit()
        4. Inject edited content back into REPL prompt for execution
        """
        if not get_cli_context().is_repl:
            raise CliError("The edit command can only be used in interactive mode.")

        editor = os.environ.get("EDITOR")
        if not editor:
            raise CliError(
                "No editor is set. Please set the EDITOR environment variable."
            )

        content_to_edit = self.sql_content
        if not content_to_edit:
            content_to_edit = self._get_last_command_from_history()

        edited_content = click.edit(
            text=content_to_edit, editor=editor, extension=".sql", require_save=False
        )

        if edited_content is None:
            log.debug("Editor closed without changes")
            return

        edited_content = edited_content.strip()

        if edited_content:
            log.debug("Editor returned content, length: %d", len(edited_content))

            if repl := get_cli_context().repl:
                repl.set_next_input(edited_content)
            else:
                log.warning("REPL instance not found in context")

            cli_console.message(
                "[green]✓ Edited SQL loaded into prompt. Modify as needed or press Enter to execute.[/green]"
            )
        else:
            log.debug("Editor returned empty content")
            cli_console.message("[yellow]Editor closed with no content.[/yellow]")

    def _get_last_command_from_history(self) -> str:
        """Get the last command from the REPL history."""
        repl = get_cli_context().repl
        if repl and repl.history:
            history_entries = list(repl.history.get_strings())
            for entry in reversed(history_entries):
                entry = entry.strip()
                is_repl_command = entry and entry.startswith("!")
                if not is_repl_command:
                    return entry

        return ""

    @classmethod
    def from_args(cls, raw_args, kwargs=None) -> CompileCommandResult:
        """Parse arguments and create EditCommand instance.

        Supports both calling patterns:
        - New pattern: from_args("SELECT * FROM table WHERE id = 1")
        - Old pattern: from_args(["SELECT", "*", "FROM", "table"], {})
        """
        if isinstance(raw_args, str):
            try:
                args, kwargs = cls._parse_args(raw_args)
            except ValueError as e:
                return CompileCommandResult(error_message=str(e))
        else:
            args, kwargs = raw_args, kwargs or {}

        return cls._from_parsed_args(args, kwargs)

    @classmethod
    def _parse_args(cls, raw_args: str) -> tuple[List[str], Dict[str, Any]]:
        """Parse raw argument string for EditCommand.

        Unlike other commands, EditCommand treats all arguments as SQL content,
        not as key-value pairs. This allows SQL with equals signs to work correctly.
        """
        return [raw_args] if raw_args else [], {}

    @classmethod
    def _from_parsed_args(cls, args, kwargs) -> CompileCommandResult:
        """Create EditCommand from parsed arguments.

        Since EditCommand's custom _parse_args always returns empty kwargs,
        we only need to handle the args to reconstruct the SQL content.
        """
        sql_content = " ".join(args) if args else ""
        return CompileCommandResult(command=cls(sql_content=sql_content))


def detect_command(input_text: str) -> tuple[str, str] | None:
    """Detect if input text matches a command pattern.

    Returns:
        tuple[command_name, raw_args] if command pattern is detected, None otherwise
    """
    match = COMMAND_PATTERN.match(input_text.strip())
    if match:
        command_name = match.group(1)  # The !command part
        raw_args = match.group(2) or ""  # Everything after the command
        return command_name, raw_args
    return None


def is_registered_command(command_name: str) -> bool:
    """Check if a command name is registered."""
    return command_name.lower() in _COMMAND_REGISTRY


def get_command_class(command_name: str) -> Type[ReplCommand] | None:
    """Get the command class for a given command name."""
    return _COMMAND_REGISTRY.get(command_name.lower())


def compile_repl_command(input_text: str) -> CompileCommandResult:
    """Detect and compile a REPL command from input text.

    This function handles:
    1. Command pattern detection
    2. Command registration checking
    3. Delegation to command-specific parsing
    """
    # Step 1: Detect if this is a command
    detection_result = detect_command(input_text)
    if not detection_result:
        log.info("Input does not match command pattern")
        return CompileCommandResult(error_message="Not a command")

    command_name, raw_args = detection_result
    log.debug("Detected command: %s", command_name)

    # Step 2: Check if command is registered
    if not is_registered_command(command_name):
        log.info("Unknown command: %s", command_name)
        raise UnknownCommandError(command_name)

    # Step 3: Get command class and delegate parsing
    command_class = get_command_class(command_name)
    if command_class is None:
        # This should never happen since we already checked registration
        raise RuntimeError(f"Command class not found for {command_name}")

    log.debug("Found command class: %s", command_class.__name__)
    return command_class.from_args(raw_args)


def get_available_commands() -> List[str]:
    """Returns a list of all registered command names."""
    return list(_COMMAND_REGISTRY.keys())
