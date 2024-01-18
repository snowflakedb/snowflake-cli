from sys import stdin, stdout
from typing import Callable, List, Optional

from snowflake.connector.cursor import DictCursor


def _rows_generator(cursor: DictCursor, predicate: Callable[[dict], bool]):
    return (row for row in cursor.fetchall() if predicate(row))


def find_all_rows(cursor: DictCursor, predicate: Callable[[dict], bool]) -> List[dict]:
    return list(_rows_generator(cursor, predicate))


def find_first_row(
    cursor: DictCursor, predicate: Callable[[dict], bool]
) -> Optional[dict]:
    """Returns the first row that matches the predicate, or None."""
    return next(_rows_generator(cursor, predicate), None)


def needs_confirmation(needs_confirm: bool, auto_yes: bool) -> bool:
    return needs_confirm and not auto_yes


def is_tty_interactive():
    return stdin.isatty() and stdout.isatty()
