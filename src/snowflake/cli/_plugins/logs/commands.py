# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import itertools
from datetime import datetime
from typing import Generator, Iterable, Optional, cast

import typer
from click import ClickException
from snowflake.cli._plugins.logs.manager import LogsManager
from snowflake.cli._plugins.logs.utils import LOG_LEVELS, LogsQueryRow
from snowflake.cli._plugins.object.commands import NameArgument, ObjectArgument
from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    Example,
    code,
    link,
    plain_text,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.exceptions import CliArgumentError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
    StreamResult,
)

app = SnowTyperFactory()


@app.command(
    name="logs",
    requires_connection=True,
    docs=CommandDocs(
        related=(
            link("/developer-guide/snowflake-cli/index"),
            link(
                "/developer-guide/snowflake-cli/command-reference/overview",
                "Snowflake CLI command reference",
            ),
        ),
        usage_notes=(
            plain_text(
                "The ",
                code("snow logs"),
                " command accesses an event table and retrieves ",
                link("/developer-guide/logging-tracing/logging", "logs"),
                " for a specified entity. By default, the command looks for the logs "
                "in the default event table, which is SNOWFLAKE.TELEMETRY.EVENTS; "
                "however, you can select a different table with the ",
                code("--table"),
                " option. For more information about event tables and default values, "
                "see ",
                link("#label-logging-event-table-custom-create"),
                ".",
            ),
            plain_text(
                "You can use the ",
                code("--from"),
                " and ",
                code("-to"),
                " options to filter the period during which to retrieve the logs. You "
                "can use one or both of these option, but if you use both, the ",
                code("--from"),
                " time must be earlier than the ",
                code("-to"),
                " time. The values for times you provide must comply with the ",
                link(
                    "https://www.iso.org/iso-8601-date-and-time-format.html",
                    "ISO 8601 standard",
                ),
                ". For more information, you can also check the Python ",
                link(
                    "https://docs.python.org/3/library/datetime.html#datetime.datetime.fromisoformat",
                    "datetime.fromisoformat()",
                ),
                " method documentation.",
            ),
            plain_text(
                "The ",
                code("--log-level"),
                " option lets you filter message by ",
                link("#label-event-table-schema", "severity level"),
                ". Some logs do not include a severity level. In those cases, messages "
                "are display for all ",
                code("--log-level"),
                " values.",
            ),
            plain_text(
                "The ",
                code("--partial"),
                " option lets you retrieve logs that contain a specific string using "
                "a case-insensitive match. For example, if you searched for logs "
                "containing ",
                code("myDb"),
                " with this option, the results would include logs for databases named ",
                code("mydb"),
                ", ",
                code("MYDB"),
                ", and ",
                code("MyDb"),
                ". Without this option, it would return only logs for databases named "
                "exactly ",
                code("myDb"),
                ".",
            ),
            plain_text(
                "If you want continuous updates for the logs, you can use the ",
                code("--refresh"),
                " option and provide the number of seconds between retrievals. You "
                "cannot use both the ",
                code("--refresh"),
                " and ",
                code("--to"),
                " options together. To stop streaming the logs, use your system's "
                "default ",
                code("Keyboardinterrupt"),
                " key, such as ",
                code("CTRL-c"),
                " in a Mac Terminal.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "Display the compute pool logs for a period from a specified "
                    "starting time to now:"
                ),
                command=(
                    "snow logs compute_pool MY_COMPUTE_POOL --from '2025-04-01 09:00:31'"
                ),
                output=(
                    '10.12.71.201 - - [01/Apr/2025 09:46:07] "GET /healthcheck HTTP/1.1" 200 -\n'
                    '10.12.71.201 - - [01/Apr/2025 09:46:09] "GET /healthcheck HTTP/1.1" 200 -\n'
                    '10.12.71.201 - - [01/Apr/2025 09:46:14] "GET /healthcheck HTTP/1.1" 200 -\n'
                    '10.12.71.201 - - [01/Apr/2025 09:46:19] "GET /healthcheck HTTP/1.1" 200 -\n'
                    '10.12.71.201 - - [01/Apr/2025 09:46:24] "GET /healthcheck HTTP/1.1" 200 -\n'
                    '10.12.71.201 - - [01/Apr/2025 09:46:29] "GET /healthcheck HTTP/1.1" 200 -\n'
                    '10.12.71.201 - - [01/Apr/2025 09:46:34] "GET /healthcheck HTTP/1.1" 200 -'
                ),
            ),
            Example(
                description=plain_text("Display the logs for a specific event table:"),
                command=(
                    "snow logs compute_pool SNOWCLI_COMPUTE_POOL "
                    '--table "my_db.my_schema.my_events"'
                ),
            ),
            Example(
                description=plain_text(
                    "Display the logs for all databases that contain ",
                    code("myDb"),
                    " using a case-insensitive partial match:",
                ),
                command="snow logs database myDb --partial",
            ),
            Example(
                description=plain_text(
                    "Display the logs for a time range where the from time is later "
                    "than the to time, which causes an error:"
                ),
                command=(
                    "snow logs compute_pool SNOWCLI_COMPUTE_POOL "
                    "--from '2025-03-24 12:00:31' --to \"2024-01-03 00:00:00\""
                ),
                output=(
                    "╭─ Error ─────────────────────────────────────────────────────────\n"
                    "│ From_time cannot be later than to_time. Please check the values\n"
                    "╰─────────────────────────────────────────────────────────────────"
                ),
            ),
        ),
    ),
)
def get_logs(
    object_type: str = ObjectArgument,
    object_name: FQN = NameArgument,
    from_: Optional[str] = typer.Option(
        None,
        "--from",
        help="The start time of the logs to retrieve. Accepts all ISO8061 formats",
    ),
    to: Optional[str] = typer.Option(
        None,
        "--to",
        help="The end time of the logs to retrieve. Accepts all ISO8061 formats",
    ),
    refresh_time: int = typer.Option(
        None,
        "--refresh",
        help="If set, the logs will be streamed with the given refresh time in seconds",
    ),
    event_table: Optional[str] = typer.Option(
        None,
        "--table",
        help="The table to query for logs. If not provided, the default table will be used",
    ),
    log_level: Optional[str] = typer.Option(
        "INFO",
        "--log-level",
        help="The log level to filter by. If not provided, INFO will be used",
    ),
    partial_match: bool = typer.Option(
        False,
        "--partial",
        help="Enable partial, case-insensitive matching for object names",
    ),
    **options,
):
    """
    Retrieves logs for a given object.
    """

    if log_level and not log_level.upper() in LOG_LEVELS:
        raise CliArgumentError(
            f"Invalid log level. Please choose from {', '.join(LOG_LEVELS)}"
        )

    if refresh_time and to:
        raise ClickException(
            "You cannot set both --refresh and --to parameters. Please check the values"
        )

    from_time = get_datetime_from_string(from_, "--from") if from_ else None
    to_time = get_datetime_from_string(to, "--to") if to else None

    if refresh_time:
        logs_stream: Iterable[LogsQueryRow] = LogsManager().stream_logs(
            object_type=object_type,
            object_name=object_name,
            from_time=from_time,
            refresh_time=refresh_time,
            event_table=event_table,
            log_level=log_level,
            partial_match=partial_match,
        )
        logs = itertools.chain(
            (MessageResult(log.log_message) for logs in logs_stream for log in logs)
        )
    else:
        logs_iterable: Iterable[LogsQueryRow] = LogsManager().get_logs(
            object_type=object_type,
            object_name=object_name,
            from_time=from_time,
            to_time=to_time,
            event_table=event_table,
            log_level=log_level,
            partial_match=partial_match,
        )
        logs = (MessageResult(log.log_message) for log in logs_iterable)  # type: ignore

    return StreamResult(cast(Generator[CommandResult, None, None], logs))


def get_datetime_from_string(
    date_str: str,
    name: Optional[str] = None,
) -> datetime:
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        raise ClickException(
            f"Incorrect format for '{name}'. Please use one of approved ISO formats."
        )
