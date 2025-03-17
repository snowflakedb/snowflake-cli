import functools
import re
import time
from datetime import datetime
from textwrap import dedent
from typing import Iterable, List, Optional

from click import ClickException
from snowflake.cli._plugins.object.commands import NameArgument, ObjectArgument
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DATETIME_FORMAT_IN_OUTPUT = "%d/%b/%Y %H:%M:%S"
DATE_PATTERN = r"\[(\d{2}/\w{3}/\d{4} \d{2}:\d{2}:\d{2})\]"


class LogsManager(SqlExecutionMixin):
    def stream_logs(
        self,
        refresh_time: int,
        object_type: str = ObjectArgument,
        object_name: FQN = NameArgument,
        from_time: Optional[datetime] = None,
    ):
        try:
            previous_end = from_time

            while True:
                logs = [
                    log
                    for log in self.get_raw_logs(
                        object_type, object_name, previous_end, None
                    )
                ]
                if logs:
                    yield logs
                    previous_end = get_timestamps_from_log_messages(logs[-1])[0]
                time.sleep(refresh_time)

        except KeyboardInterrupt:
            return

    def get_logs(
        self,
        object_type: str = ObjectArgument,
        object_name: FQN = NameArgument,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
    ) -> Iterable[str]:
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
        """ """
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
            ORDER BY timestamp
"""
        ).strip()

        return self.execute_query(query + ";")

    @functools.cached_property
    def logs_table(self) -> str:  # Maybe this should be a cached property?
        """
        Get the table where logs are.
        The query returns a tuple with fields:
        0: Key ("EVENT_TABLE")
        1: table name <- this is where the logs are stored
        2: default (where the logs are stored by default)
        3: level (log level currently set, by default is NULL)
        4: description
        5: type
        """
        return self.execute_query(
            f"SHOW PARAMETERS LIKE 'event_table' IN ACCOUNT;"
        ).fetchone()[1]

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
                f"AND timestamp >= TO_TIMESTAMP_LTZ('{from_time.strftime(DATETIME_FORMAT)}')\n"
            )

        if to_time is not None:
            query.append(
                f"AND timestamp <= TO_TIMESTAMP_LTZ('{to_time.strftime(DATETIME_FORMAT)}')\n"
            )

        return "".join(query)

    def sanitize_logs(self, logs: SnowflakeCursor) -> List[str]:
        if [metadata.name for metadata in logs.description] != [
            "TIMESTAMP",
            "DATABASE_NAME",
            "SCHEMA_NAME",
            "OBJECT_NAME",
            "LOG_LEVEL",
            "LOG_MESSAGE",
        ]:
            raise ClickException(
                "Logs table has incorrect format. Please check the logs_table in your database"
            )

        if logs:
            return [log[5] for log in logs]
        else:
            return []


def get_timestamps_from_log_messages(log_message: str) -> List[datetime]:
    return [
        datetime.strptime(date, DATETIME_FORMAT_IN_OUTPUT)
        for date in re.findall(DATE_PATTERN, log_message)
    ]
