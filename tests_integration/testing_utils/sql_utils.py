from typing import Any, Dict, List

import pytest
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor


def _get_values_from_row_as_dictionary(
    row: tuple, all_column_names: List[str]
) -> Dict[str, Any]:
    all_column_indexes = [all_column_names.index(name) for name in all_column_names]
    return {all_column_names[i]: row[i] for i in all_column_indexes}


def get_values_from_cursor_as_dictionaries(
    cursor: SnowflakeCursor,
) -> List[Dict[str, Any]]:
    all_column_names = [column.name for column in cursor.description]
    return [
        _get_values_from_row_as_dictionary(row, all_column_names)
        for row in cursor.fetchall()
    ]


class SqlTestHelper:
    def __init__(self, snowflake_session: SnowflakeConnection):
        self._snowflake_session = snowflake_session

    def execute_single_sql(self, sql: str) -> List[Dict[str, Any]]:
        results = self._snowflake_session.execute_string(sql)
        cursor = results.__iter__().__next__()
        return get_values_from_cursor_as_dictionaries(cursor)


@pytest.fixture(scope="session")
def sql_test_helper(snowflake_session):
    yield SqlTestHelper(snowflake_session)
