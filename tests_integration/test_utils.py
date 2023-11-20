import datetime
from typing import Any, Dict, List

from snowflake.connector.cursor import SnowflakeCursor


def row_from_mock(mock_print) -> List[Dict[str, Any]]:
    return row_from_cursor(mock_print.call_args.args[0])


def rows_from_mock(mock_print) -> List[List[Dict[str, Any]]]:
    return [row_from_cursor(args.args[0]) for args in mock_print.call_args_list]


def row_from_snowflake_session(
    result: List[SnowflakeCursor],
) -> List[Dict[str, Any]]:
    return row_from_cursor(result[-1])


def rows_from_snowflake_session(
    results: List[SnowflakeCursor],
) -> List[List[Dict[str, Any]]]:
    return [row_from_cursor(cursor) for cursor in results]


def row_from_cursor(cursor: SnowflakeCursor) -> List[Dict[str, Any]]:
    column_names = [column.name for column in cursor.description]
    rows = [dict(zip(column_names, row)) for row in cursor.fetchall()]
    for row in rows:
        for column in row:
            if isinstance(row[column], datetime.datetime):
                row[column] = row[column].isoformat()
    return rows


def contains_row_with(rows: List[Dict[str, Any]], values: Dict[str, Any]) -> bool:
    values_items = values.items()
    if isinstance(rows, dict):
        return rows.items() >= values_items

    for row in rows:
        if row.items() >= values_items:
            return True
    return False


def not_contains_row_with(rows: List[Dict[str, Any]], values: Dict[str, Any]) -> bool:
    values_items = values.items()
    for row in rows:
        if row.items() >= values_items:
            return False
    return True
