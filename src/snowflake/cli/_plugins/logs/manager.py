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
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class LogsManager(SqlExecutionMixin):
    def stream_logs(
        self,
        refresh_time: int,
        object_type: str = ObjectArgument,
        object_name: FQN = NameArgument,
        from_time: Optional[datetime] = None,
        event_table: Optional[str] = None,
        log_level: Optional[str] = "INFO",
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
    ) -> SnowflakeCursor:

        table = event_table if event_table else "SNOWFLAKE.TELEMETRY.EVENTS"

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
            AND object_name = '{object_name}'
            {get_timestamp_query(from_time, to_time)}
            ORDER BY timestamp;
"""
        ).strip()

        result = self.execute_query(query)

        return result
