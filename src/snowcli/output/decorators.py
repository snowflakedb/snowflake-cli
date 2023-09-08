from __future__ import annotations

from functools import wraps

from snowcli.exception import CommandReturnTypeError
from snowcli.output.printing import print_result
from snowflake.connector.cursor import SnowflakeCursor
from snowcli.output.types import CommandResult


def with_output(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        output_data = func(*args, **kwargs)
        if not isinstance(output_data, CommandResult):
            raise CommandReturnTypeError(type(output_data))
        print_result(output_data)

    return wrapper


def _is_list_of_results(result):
    return (
        isinstance(result, list)
        and len(result) > 0
        and (isinstance(result[0], list) or isinstance(result[0], SnowflakeCursor))
    )
