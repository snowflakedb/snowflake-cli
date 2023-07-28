from __future__ import annotations

from functools import wraps

from snowcli.output.printing import print_db_cursor


def with_output(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        print_db_cursor(result)

    return wrapper
