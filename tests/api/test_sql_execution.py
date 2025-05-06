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

from unittest import mock

import pytest
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.sql_execution import SqlExecutor

EXECUTE_QUERY = f"snowflake.cli.api.sql_execution.BaseSqlExecutor.execute_query"


@pytest.mark.parametrize(
    "new_role, expected_new_role, current_role, expected_current_role",
    [
        ("new_role", "new_role", "current_role", "current_role"),
        ("new-role", '"new-role"', "current-role", '"current-role"'),
    ],
)
@mock.patch(EXECUTE_QUERY)
def test_use_role_different_id(
    mock_execute_query,
    mock_cursor,
    new_role,
    expected_new_role,
    current_role,
    expected_current_role,
):
    mock_execute_query.return_value = mock_cursor([(current_role,)], [])
    with SqlExecutor().use_role(new_role):
        pass
    assert mock_execute_query.mock_calls == [
        mock.call("select current_role()"),
        mock.call(f"use role {expected_new_role}"),
        mock.call(f"use role {expected_current_role}"),
    ]


@pytest.mark.parametrize(
    "new_role, current_role",
    [
        ("test_role", "test_role"),
        ("test role", '"test role"'),
        ("test role", "test role"),
    ],
)
@mock.patch(EXECUTE_QUERY)
def test_use_role_same_id(mock_execute_query, mock_cursor, new_role, current_role):
    mock_execute_query.return_value = mock_cursor([(current_role,)], [])
    with SqlExecutor().use_role(new_role):
        pass
    assert mock_execute_query.mock_calls == [mock.call("select current_role()")]


@pytest.mark.parametrize(
    "new_warehouse, expected_new_warehouse, current_warehouse, expected_current_warehouse",
    [
        ("new_warehouse", "new_warehouse", "current_warehouse", "current_warehouse"),
        (
            "new-warehouse",
            '"new-warehouse"',
            "current-warehouse",
            '"current-warehouse"',
        ),
    ],
)
@mock.patch(EXECUTE_QUERY)
def test_use_warehouse_different_id(
    mock_execute_query,
    mock_cursor,
    new_warehouse,
    expected_new_warehouse,
    current_warehouse,
    expected_current_warehouse,
):
    mock_execute_query.return_value = mock_cursor([(current_warehouse,)], [])
    with SqlExecutor().use_warehouse(new_warehouse):
        pass
    assert mock_execute_query.mock_calls == [
        mock.call("select current_warehouse()"),
        mock.call(f"use warehouse {expected_new_warehouse}"),
        mock.call(f"use warehouse {expected_current_warehouse}"),
    ]


@pytest.mark.parametrize(
    "new_warehouse, current_warehouse",
    [
        ("test_warehouse", "test_warehouse"),
        ("test warehouse", '"test warehouse"'),
        ("test warehouse", "test warehouse"),
    ],
)
@mock.patch(EXECUTE_QUERY)
def test_use_warehouse_same_id(
    mock_execute_query, mock_cursor, new_warehouse, current_warehouse
):
    mock_execute_query.return_value = mock_cursor([(current_warehouse,)], [])
    with SqlExecutor().use_warehouse(new_warehouse):
        pass
    assert mock_execute_query.mock_calls == [mock.call("select current_warehouse()")]


@mock.patch(EXECUTE_QUERY)
def test_use_schema_fqn(mock_execute_query):
    SqlExecutor().use(ObjectType.SCHEMA, "db.schema")
    assert mock_execute_query.mock_calls == [mock.call("use schema db.schema")]
