# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Set
from unittest.mock import MagicMock

from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.telemetry import TelemetryField


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


def extract_first_telemetry_message_of_type(
    mock_telemetry: MagicMock, message_type: str
) -> Dict[str, Any]:
    # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
    return next(
        args.args[0].to_dict()["message"]
        for args in mock_telemetry.call_args_list
        if args.args[0].to_dict().get("message").get(TelemetryField.KEY_TYPE.value)
        == message_type
    )


def assert_stage_has_files(runner, stage: str, expected_files: Set[str]) -> None:
    result = runner.invoke_with_connection_json(["stage", "list-files", stage])
    assert result.exit_code == 0, result.output
    assert set(file["name"] for file in result.json) == set(expected_files)


def does_stage_exist(runner, stage: str) -> bool:
    result = runner.invoke_with_connection_json(["stage", "list", "--like", stage])
    assert result.exit_code == 0, result.output
    return len(result.json) == 1
