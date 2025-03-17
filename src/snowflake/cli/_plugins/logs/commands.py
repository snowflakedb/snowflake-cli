from datetime import datetime
from typing import Generator, Optional, cast

import typer
from click import ClickException
from snowflake.cli._plugins.logs.manager import DATETIME_FORMAT, LogsManager
from snowflake.cli._plugins.object.commands import NameArgument, ObjectArgument
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
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
        help="The start time of the logs to retrieve. The format is 'YYYY-MM-DD HH:MM:SS'.",
    ),
    to: Optional[str] = typer.Option(
        None,
        "--to",
        help="The end time of the logs to retrieve. The format is 'YYYY-MM-DD HH:MM:SS'.",
    ),
    refresh_time: int = typer.Option(
        None,
        "--refresh",
        help="If set, the logs will be streamed with the given refresh time in seconds",
    ),
    **options,
):
    """
    Retrieves logs for a given object.
    """
    if refresh_time and to:
        raise ClickException(
            "You cannot set both --refresh and --to parameters. Please check the values"
        )

    from_time = (
        get_datetime_from_string(from_, "--from", DATETIME_FORMAT) if from_ else None
    )
    to_time = get_datetime_from_string(to, "--to", DATETIME_FORMAT) if to else None

    if refresh_time:
        logs = LogsManager().stream_logs(
            object_type=object_type,
            object_name=object_name,
            from_time=from_time,
            refresh_time=refresh_time,
        )
    else:
        logs = LogsManager().get_logs(
            object_type=object_type,
            object_name=object_name,
            from_time=from_time,
            to_time=to_time,
        )

    messages = [MessageResult(log) for log in logs]

    return StreamResult(cast(Generator[CommandResult, None, None], messages))


def get_datetime_from_string(
    date_str: str,
    name: Optional[str] = None,
    format_: str = DATETIME_FORMAT,
) -> datetime:
    try:
        return datetime.strptime(date_str, format_)
    except ValueError:
        raise ClickException(
            f"Incorrect format for '{name}'. Please use '{format_}' format."
        )
