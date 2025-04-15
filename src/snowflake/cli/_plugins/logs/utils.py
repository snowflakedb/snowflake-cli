from datetime import datetime
from typing import List, NamedTuple, Optional, Tuple

from snowflake.cli.api.exceptions import CliArgumentError, CliSqlError
from snowflake.connector.cursor import SnowflakeCursor

LOG_LEVELS = ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"]

LogsQueryRow = NamedTuple(
    "LogsQueryRow",
    [
        ("timestamp", datetime),
        ("database_name", str),
        ("schema_name", str),
        ("object_name", str),
        ("log_level", str),
        ("log_message", str),
    ],
)


def sanitize_logs(logs: SnowflakeCursor | List[Tuple]) -> List[LogsQueryRow]:
    try:
        return [LogsQueryRow(*log) for log in logs]
    except TypeError:
        raise CliSqlError(
            "Logs table has incorrect format. Please check the logs_table in your database"
        )


def get_timestamp_query(from_time: Optional[datetime], to_time: Optional[datetime]):
    if from_time and to_time and from_time > to_time:
        raise CliArgumentError(
            "From_time cannot be later than to_time. Please check the values"
        )
    query = []

    if from_time is not None:
        query.append(f"AND timestamp >= TO_TIMESTAMP_LTZ('{from_time.isoformat()}')\n")

    if to_time is not None:
        query.append(f"AND timestamp <= TO_TIMESTAMP_LTZ('{to_time.isoformat()}')\n")

    return "".join(query)


def get_log_levels(log_level: str):
    if log_level.upper() not in LOG_LEVELS and log_level != "":
        raise CliArgumentError(
            f"Invalid log level. Please choose from {', '.join(LOG_LEVELS)}"
        )

    if log_level == "":
        log_level = "INFO"

    return LOG_LEVELS[LOG_LEVELS.index(log_level.upper()) :]


def parse_log_levels_for_query(log_level: str):
    return ", ".join(f"'{level}'" for level in get_log_levels(log_level))
