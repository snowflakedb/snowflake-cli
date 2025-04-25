import enum
import time
from dataclasses import dataclass
from typing import Any, Dict
from urllib.parse import urlencode

from snowflake.connector import SnowflakeConnection


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


@dataclass
class CompileCommandResult:
    command: SnowSQLCommand | None = None
    error_message: str | None = None


class QueriesCommand(SnowSQLCommand):
    def __init__(
        self,
        parameters: Dict[str, Any],
        filter_session: bool = False,
        help_mode: bool = False,
    ) -> None:
        self.help_mode = help_mode
        self.parameters = parameters
        self.filter_session = filter_session
        pass

    def run(self, connection: SnowflakeConnection):
        url_parameters = {
            "_dc": "{time}".format(time=time.time()),
            "includeDDL": "false",
        }
        url_parameters.update(**self.parameters)
        if self.filter_session:
            url_parameters["session_id"] = connection.session_id
        url = "/monitoring/queries?" + urlencode(url_parameters)
        ret = connection.rest.request(url=url, method="get", client="rest")
        if ret.get("data") and ret["data"].get("queries"):
            for query in ret["data"]["queries"]:
                yield [
                    query["id"],
                    query["sqlText"],
                    query["state"],
                    query["totalDuration"],
                ]

    @classmethod
    def from_args(cls, args, kwargs) -> CompileCommandResult:
        if "help" in args:
            return CompileCommandResult(command=cls({}, help_mode=True))

        amount = kwargs.pop("amount", "25")
        if not amount.isdigit():
            return CompileCommandResult(
                error_message=f"Non-integer argument passed to 'amount' parameter."
            )
        parameters = {"max": int(amount)}
        filter_session = "session" in args or not kwargs
        if user := kwargs.pop("user", None):
            parameters["user"] = user
        if warehouse := kwargs.pop("warehouse", None):
            parameters["wh"] = warehouse
        if start_time := kwargs.pop("start", None):
            parameters["start"] = start_time
        if end_time := kwargs.pop("end", None):
            parameters["end"] = end_time
        if min_duration := kwargs.pop("duration", None):
            parameters["min_duration"] = min_duration

        if stmt_type := kwargs.pop("type", None):
            accepted = [
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
            ]
            if stmt_type.upper() not in accepted:
                return CompileCommandResult(
                    error_message=f"Invalid argument passed to 'type' filter: {stmt_type}"
                )
            parameters["stmt_type"] = stmt_type.upper()

        if status := kwargs.pop("status", None):
            accepted = [
                "RUNNING",
                "SUCCEEDED",
                "FAILED",
                "BLOCKED",
                "QUEUED",
                "ABORTED",
            ]
            if stmt_type.upper() not in accepted:
                return CompileCommandResult(
                    error_message=f"Invalid argument passed to 'status' filter: {status}"
                )
            parameters["subset"] = status.upper()

        # todo: incorrect args/kwargs error

        return CompileCommandResult(
            command=cls(parameters, filter_session=filter_session, help_mode=False)
        )


def compile_snowsql_command(statement: str):
    """Parses command into SQL query"""
    args = []
    kwargs = {}
    cmd = statement.split()
    command = cmd[0]
    for cmd_arg in cmd[1:]:
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
        case _:
            return CompileCommandResult(error_message=f"Unknown command '{command}'")
