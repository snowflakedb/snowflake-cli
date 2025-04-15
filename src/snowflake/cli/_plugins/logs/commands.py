import itertools
from datetime import datetime
from typing import Generator, Iterable, Optional, cast

import typer
from click import ClickException
from snowflake.cli._plugins.logs.manager import LogsManager
from snowflake.cli._plugins.logs.utils import LOG_LEVELS, LogsQueryRow
from snowflake.cli._plugins.object.commands import NameArgument, ObjectArgument
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.exceptions import CliArgumentError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
    StreamResult,
)

app = SnowTyperFactory()


@app.command(name="logs", requires_connection=True)
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
