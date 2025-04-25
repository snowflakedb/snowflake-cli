import enum
from dataclasses import dataclass

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
    def __init__(self, filters, help_mode):
        self.help_mode = help_mode
        pass

    def run(self, connection: SnowflakeConnection):
        connection.cursor()

    @classmethod
    def from_args(cls, args, kwargs) -> CompileCommandResult:
        if "help" in args:
            return CompileCommandResult(command=cls(None, help_mode=True))
        return CompileCommandResult(error_message="Not implemented")


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
