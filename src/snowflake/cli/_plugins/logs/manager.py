import re
import time
from datetime import datetime
from textwrap import dedent
from typing import Iterable, List, Optional

from snowflake.cli._plugins.logs.utils import (
    LogsQueryRow,
    get_timestamp_query,
    parse_log_levels_for_query,
    sanitize_logs,
)
from snowflake.cli._plugins.object.commands import NameArgument, ObjectArgument
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import escape_like_pattern
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


def _logs_string_literal(value: str) -> str:
    """Quote ``value`` so ``execute_stream`` keeps it in one string literal.

    ``split_statements`` reads ``\\'`` as an escaped quote. An odd run of
    backslashes before ``''`` therefore ends the literal early. One extra
    backslash makes that run even. Even runs already stay in the literal;
    leaving them unchanged keeps the query matched to that object name.
    """

    def _pad_odd_run(match: re.Match[str]) -> str:
        slashes = match.group(1)
        if len(slashes) % 2 == 1:
            slashes += "\\"
        return slashes + "''"

    escaped = re.sub(r"(\\*)'", _pad_odd_run, value)
    if (len(escaped) - len(escaped.rstrip("\\"))) % 2 == 1:
        escaped += "\\"
    return f"'{escaped}'"


class LogsManager(SqlExecutionMixin):
    def stream_logs(
        self,
        refresh_time: int,
        object_type: str = ObjectArgument,
        object_name: FQN = NameArgument,
        from_time: Optional[datetime] = None,
        event_table: Optional[str] = None,
        log_level: Optional[str] = "INFO",
        partial_match: bool = False,
    ) -> Iterable[List[LogsQueryRow]]:
        try:
            previous_end = from_time

            while True:
                raw_logs = self.get_raw_logs(
                    object_type=object_type,
                    object_name=object_name,
                    from_time=previous_end,
                    to_time=None,
                    event_table=event_table,
                    log_level=log_level,
                    partial_match=partial_match,
                ).fetchall()

                if raw_logs:
                    result = self.sanitize_logs(raw_logs)
                    yield result
                    if result:
                        previous_end = result[-1].timestamp
                time.sleep(refresh_time)

        except KeyboardInterrupt:
            return

    def get_logs(
        self,
        object_type: str = ObjectArgument,
        object_name: FQN = NameArgument,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
        event_table: Optional[str] = None,
        log_level: Optional[str] = "INFO",
        partial_match: bool = False,
    ) -> Iterable[LogsQueryRow]:
        """
        Basic function to get a single batch of logs from the server
        """

        logs = self.get_raw_logs(
            object_type=object_type,
            object_name=object_name,
            from_time=from_time,
            to_time=to_time,
            event_table=event_table,
            log_level=log_level,
            partial_match=partial_match,
        )

        return sanitize_logs(logs)

    def get_raw_logs(
        self,
        object_type: str = ObjectArgument,
        object_name: FQN = NameArgument,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
        event_table: Optional[str] = None,
        log_level: Optional[str] = "INFO",
        partial_match: bool = False,
    ) -> SnowflakeCursor:

        table = event_table if event_table else "SNOWFLAKE.TELEMETRY.EVENTS"

        object_name_text = str(object_name)
        if partial_match:
            like_pattern = (
                "%" + escape_like_pattern(object_name_text, escape_sequence="\\") + "%"
            )
            object_condition = f"object_name ILIKE {_logs_string_literal(like_pattern)}"
        else:
            object_condition = f"object_name = {_logs_string_literal(object_name_text)}"

        query = dedent(
            f"""
            SELECT
                timestamp,
                resource_attributes:"snow.database.name"::string as database_name,
                resource_attributes:"snow.schema.name"::string as schema_name,
                resource_attributes:"snow.{object_type}.name"::string as object_name,
                record:severity_text::string as log_level,
                value::string as log_message
            FROM {table}
            WHERE record_type = 'LOG'
            AND (record:severity_text IN ({parse_log_levels_for_query((log_level))}) or record:severity_text is NULL )
            AND {object_condition}
            {get_timestamp_query(from_time, to_time)}
            ORDER BY timestamp;
"""
        ).strip()

        result = self.execute_query(query)

        return result
