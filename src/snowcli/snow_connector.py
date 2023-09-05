from __future__ import annotations

import click
import logging

from pathlib import Path
from typing import Optional

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import ForbiddenError, DatabaseError

from snowcli.config import cli_config, get_default_connection
from snowcli.exception import SnowflakeConnectionError, InvalidConnectionConfiguration

log = logging.getLogger(__name__)
TEMPLATES_PATH = Path(__file__).parent / "sql"


def connect_to_snowflake(connection_name: Optional[str] = None, **overrides) -> SnowflakeConnection:  # type: ignore
    connection_name = (
        connection_name if connection_name is not None else get_default_connection()
    )
    try:
        connection_parameters = cli_config.get_connection(connection_name)
        if overrides:
            connection_parameters.update(
                {k: v for k, v in overrides.items() if v is not None}
            )
        return snowflake.connector.connect(
            application=_find_command_path(),
            **connection_parameters,
        )
    except ForbiddenError as err:
        raise SnowflakeConnectionError(err)
    except DatabaseError as err:
        raise InvalidConnectionConfiguration(err.msg)


def _find_command_path():
    ctx = click.get_current_context(silent=True)
    if ctx:
        # Example: SNOWCLI.WAREHOUSE.STATUS
        return ".".join(["SNOWCLI", *ctx.command_path.split(" ")[1:]]).upper()
    return "SNOWCLI"
