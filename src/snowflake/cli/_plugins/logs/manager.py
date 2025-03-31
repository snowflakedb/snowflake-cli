import functools
import time
from datetime import datetime
from textwrap import dedent
from typing import Iterable, List, NamedTuple, Optional, Tuple

from click import ClickException
from snowflake.cli._plugins.object.commands import NameArgument, ObjectArgument
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor

LogsTableQueryResult = NamedTuple(
    "LogsTableQueryResult",
    [
        ("key", str),
        ("table_name", str),
        ("default", str),
        ("level", str),
        ("description", str),
        ("type", str),
    ],
)

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


class LogsManager(SqlExecutionMixin):
    def stream_logs(
        self,
        refresh_time: int,
        object_type: str = ObjectArgument,
        object_name: FQN = NameArgument,
        from_time: Optional[datetime] = None,
    ) -> Iterable[List[LogsQueryRow]]:
        try:
            previous_end = from_time

            while True:
                raw_logs = self.get_raw_logs(
                    object_type, object_name, previous_end, None
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
    ) -> Iterable[LogsQueryRow]:
        """
        Basic function to get a single batch of logs from the server
        """

        logs = self.get_raw_logs(object_type, object_name, from_time, to_time)

        return self.sanitize_logs(logs)

    def get_raw_logs(
        self,
        object_type: str = ObjectArgument,
        object_name: FQN = NameArgument,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
    ) -> SnowflakeCursor:
        query = dedent(
            f"""
            SELECT
                timestamp,
                resource_attributes:"snow.database.name"::string as database_name,
                resource_attributes:"snow.schema.name"::string as schema_name,
                resource_attributes:"snow.{object_type}.name"::string as object_name,
                record:severity_text::string as log_level,
                value::string as log_message
            FROM {self.logs_table}
            WHERE record_type = 'LOG'
            AND (record:severity_text = 'INFO' or record:severity_text is NULL )
            AND object_name = '{object_name}'
            {self._get_timestamp_query(from_time, to_time)}
            ORDER BY timestamp;
"""
        ).strip()

        result = self.execute_query(query)

        return result

    @functools.cached_property
    def logs_table(self) -> str:
        """
        Get the table where logs are."""
        query_result = self.execute_query(
            f"SHOW PARAMETERS LIKE 'event_table' IN ACCOUNT;"
        ).fetchone()

        try:
            logs_table_query_result = LogsTableQueryResult(*query_result)
        except TypeError:
            raise ClickException(
                "Encountered error while querying for logs table. Please check if your account has an event_table"
            )
        return logs_table_query_result.table_name

    def _get_timestamp_query(
        self, from_time: Optional[datetime], to_time: Optional[datetime]
    ):
        if from_time and to_time and from_time > to_time:
            raise ClickException(
                "From_time cannot be later than to_time. Please check the values"
            )
        query = []

        if from_time is not None:
            query.append(
                f"AND timestamp >= TO_TIMESTAMP_LTZ('{from_time.isoformat()}')\n"
            )

        if to_time is not None:
            query.append(
                f"AND timestamp <= TO_TIMESTAMP_LTZ('{to_time.isoformat()}')\n"
            )

        return "".join(query)

    def sanitize_logs(self, logs: SnowflakeCursor | List[Tuple]) -> List[LogsQueryRow]:
        try:
            return [LogsQueryRow(*log) for log in logs]
        except TypeError:
            raise ClickException(
                "Logs table has incorrect format. Please check the logs_table in your database"
            )
