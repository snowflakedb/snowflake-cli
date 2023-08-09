from typing import List, Dict
from snowflake.connector.cursor import SnowflakeCursor


def row_from_mock(mock_print) -> List[Dict[str, str]]:
    return row_from_cursor(mock_print.call_args.args[0])


def rows_from_mock(mock_print) -> List[List[Dict[str, str]]]:
    return [row_from_cursor(args.args[0]) for args in mock_print.call_args_list]


def row_from_snowflake_session(
    result: List[SnowflakeCursor],
) -> List[Dict[str, str]]:
    return row_from_cursor(result[-1])


def rows_from_snowflake_session(
    result: List[SnowflakeCursor],
) -> List[List[Dict[str, str]]]:
    return [row_from_cursor(cursor) for cursor in result]


def row_from_cursor(cursor: SnowflakeCursor) -> List[Dict[str, str]]:
    column_names = [column.name for column in cursor.description]
    return [dict(zip(column_names, row)) for row in cursor.fetchall()]


def contains_row_with(rows: List[Dict[str, str]], values: Dict[str, str]) -> bool:
    values_items = values.items()
    for row in rows:
        if row.items() >= values_items:
            return True
    return False


def not_contains_row_with(rows: List[Dict[str, str]], values: Dict[str, str]) -> bool:
    values_items = values.items()
    for row in rows:
        if row.items() >= values_items:
            return False
    return True
