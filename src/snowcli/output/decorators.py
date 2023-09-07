from __future__ import annotations

import typer
from sys import stderr
from typing import Type, Optional
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


def catch_error(
    exception_class: Type[Exception], message: Optional[str] = None, exit_code: int = 1
):
    """
    Catches a specific type of exception and exits fatally, optionally with
    a custom message or process exit code.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exception_class as e:
                print(message if message else str(e), file=stderr)
                raise typer.Exit(code=exit_code)

        return wrapper

    return decorator
