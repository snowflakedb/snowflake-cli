import enum
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Generator, Iterable, List, Tuple
from urllib.parse import urlencode

from snowflake.cli._app.printing import print_result
from snowflake.cli.api.output.types import CollectionResult, QueryResult
from snowflake.connector import SnowflakeConnection

VALID_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


class CommandType(enum.Enum):
    QUERIES = "queries"
    UNKNOWN = "unknown"
    URL = "url"


class SnowSQLCommand:
    def execute(self, connection: SnowflakeConnection):
        """Executes command and prints the result."""
        raise NotImplementedError

    @classmethod
    def from_args(cls, args, kwargs) -> "CompileCommandResult":
        """Validates arguments and creates command ready for execution."""
        raise NotImplementedError


def _print_result_to_stdout(headers: Iterable[str], rows: Iterable[Iterable[Any]]):
    formatted_rows: Generator[Dict[str, Any], None, None] = (
        {key: value for key, value in zip(headers, row)} for row in rows
    )
    print_result(CollectionResult(formatted_rows))


@dataclass
class CompileCommandResult:
    command: SnowSQLCommand | None = None
    error_message: str | None = None


@dataclass
class QueriesCommand(SnowSQLCommand):
    help_mode: bool = False
    from_current_session: bool = False
    amount: int = 25
    user: str | None = None
    warehouse: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    duration: str | None = None
    stmt_type: str | None = None
    status: str | None = None

    def execute(self, connection: SnowflakeConnection) -> None:
        if self.help_mode:
            self._execute_help()
        else:
            self._execute_queries(connection)

    def _execute_help(self):
        filters = []
        headers = ["FILTER", "ARGUMENT", "DEFAULT"]
        filters.append(["amount", "integer", "25"])
        filters.append(["status", "string", "any"])
        filters.append(["warehouse", "string", "any"])
        filters.append(["user", "string", "any"])
        filters.append(
            [
                "start_date",
                "date in ISO format (for example YYYY-MM-DDTHH:mm:ss.sss)",
                "any",
            ]
        )
        filters.append(
            [
                "end_date",
                "date in ISO format (for example YYYY-MM-DDTHH:mm:ss.sss)",
                "any",
            ]
        )
        filters.append(["start", "timestamp in milliseconds", "any"])
        filters.append(["end", "timestamp in milliseconds", "any"])
        filters.append(["type", "string", "any"])
        filters.append(["duration", "time in milliseconds", "any"])
        filters.append(["session", "No arguments", "any"])
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
        if self.start_time:
            url_parameters["start"] = self.start_time
        if self.end_time:
            url_parameters["end"] = self.end_time
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
    def from_args(cls, args: List[str], kwargs: Dict[str, Any]) -> CompileCommandResult:
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
        start_timestamp = kwargs.pop("start", None)
        end_timestamp = kwargs.pop("end", None)
        duration = kwargs.pop("duration", None)

        start_date = kwargs.pop("start_date", None)
        if start_date:
            if start_timestamp:
                return CompileCommandResult(
                    error_message=f"'start_date' filter cannot be used with 'start' filter"
                )
            try:
                seconds = datetime.fromisoformat(start_date).timestamp()
                start_time = seconds * 1000  # convert to milliseconds
            except ValueError:
                return CompileCommandResult(
                    error_message=f"Invalid date format passed to 'start_date' filter: {start_date}"
                )
        end_date = kwargs.pop("end_date", None)
        if end_date:
            if end_timestamp:
                return CompileCommandResult(
                    error_message=f"'end_date' filter cannot be used with 'end' filter"
                )
            try:
                seconds = datetime.fromisoformat(end_date).timestamp()
                end_time = seconds * 1000  # convert to milliseconds
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
        if kwargs:
            key, value = kwargs.popitem()
            return CompileCommandResult(
                error_message=f"Invalid argument passed to 'queries' command: {key}={value}"
            )

        return CompileCommandResult(
            command=cls(
                help_mode=False,
                from_current_session=from_current_session,
                amount=int(amount),
                user=user,
                warehouse=warehouse,
                start_time=start_timestamp,
                end_time=end_timestamp,
                duration=duration,
                stmt_type=stmt_type,
                status=status,
            )
        )


def _validate_only_arg_is_query_id(
    command_name: str, args: List[str], kwargs: Dict[str, Any]
) -> str | None:
    if kwargs:
        key, value = kwargs.popitem()
        return f"Invalid argument passed to '{command_name}' command: {key}={value}"
    if len(args) != 1:
        amount = "Too many" if args else "No"
        return f"{amount} arguments passed to '{command_name}' command. Usage: `!{command_name} <query id>`"

    qid = args[0]
    if not VALID_UUID_RE.match(qid):
        return f"Invalid query ID passed to '{command_name}' command: {qid}"

    return None


@dataclass
class ResultCommand(SnowSQLCommand):
    query_id: str

    def execute(self, connection: SnowflakeConnection):
        cursor = connection.cursor()
        cursor.query_result(self.query_id)
        print_result(QueryResult(cursor=cursor))

    @classmethod
    def from_args(cls, args, kwargs) -> CompileCommandResult:
        error_msg = _validate_only_arg_is_query_id("result", args, kwargs)
        if error_msg:
            return CompileCommandResult(error_message=error_msg)
        return CompileCommandResult(command=cls(args[0]))


@dataclass
class AbortCommand(SnowSQLCommand):
    query_id: str

    def execute(self, connection: SnowflakeConnection):
        cursor = connection.cursor()
        cursor.execute(f"SELECT SYSTEM$CANCEL_QUERY('{self.query_id}')")
        print_result(QueryResult(cursor=cursor))

    @classmethod
    def from_args(cls, args, kwargs) -> CompileCommandResult:
        error_msg = _validate_only_arg_is_query_id("abort", args, kwargs)
        if error_msg:
            return CompileCommandResult(error_message=error_msg)
        return CompileCommandResult(command=cls(args[0]))


def compile_snowsql_command(command: str, cmd_args: List[str]):
    """Parses command into SQL query"""
    args = []
    kwargs = {}
    for cmd_arg in cmd_args:
        if "=" not in cmd_arg:
            args.append(cmd_arg)
        else:
            key, val = cmd_arg.split("=", maxsplit=1)
            if key in kwargs:
                return CompileCommandResult(
                    error_message=f"duplicated argument '{key}' for command '{command}'",
                )
            kwargs[key] = val

    match command.lower():
        case "!queries":
            return QueriesCommand.from_args(args, kwargs)
        case "!result":
            return ResultCommand.from_args(args, kwargs)
        case "!abort":
            return AbortCommand.from_args(args, kwargs)
        case _:
            return CompileCommandResult(error_message=f"Unknown command '{command}'")
