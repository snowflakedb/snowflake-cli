from typing import Callable, Optional

from snowflake.connector.cursor import DictCursor


def find_row(cursor: DictCursor, predicate: Callable[[dict], bool]) -> Optional[dict]:
    """Returns the first row that matches the predicate, or None."""
    return next(
        (row for row in cursor.fetchall() if predicate(row)),
        None,
    )


def needs_confirmation(needs_confirm: bool, auto_yes: bool) -> bool:
    return needs_confirm and not auto_yes
