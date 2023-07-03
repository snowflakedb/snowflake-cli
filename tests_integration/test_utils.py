from typing import List, Dict
from snowflake.connector.cursor import SnowflakeCursor


def extract_data(mock_print) -> List[Dict[str, str]]:
    return extract_data_from_cursor(mock_print.call_args.args[0])


def extract_list_of_data(mock_print) -> List[List[Dict[str, str]]]:
    return [
        extract_data_from_cursor(args.args[0]) for args in mock_print.call_args_list
    ]


def extract_data_from_cursor(cursor: SnowflakeCursor) -> List[Dict[str, str]]:
    column_names = [column.name for column in cursor.description]
    return [dict(zip(column_names, row)) for row in cursor.fetchall()]
