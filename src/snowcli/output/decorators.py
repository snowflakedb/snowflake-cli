from __future__ import annotations

import typer

from functools import wraps

from snowcli.exception import CommandReturnTypeError
from snowcli.output.printing import OutputData
from snowflake.connector.cursor import SnowflakeCursor


def with_output(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        output_data = func(*args, **kwargs)

        if not isinstance(output_data, OutputData):
            raise CommandReturnTypeError(type(output_data))

        output_data.print()
        if output_data.exit_code is not None:
            raise typer.Exit(output_data.exit_code)

    return wrapper


def _is_list_of_results(result):
    return (
        isinstance(result, list)
        and len(result) > 0
        and (isinstance(result[0], list) or isinstance(result[0], SnowflakeCursor))
    )
