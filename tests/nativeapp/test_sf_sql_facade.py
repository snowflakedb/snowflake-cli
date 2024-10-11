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

from snowflake.cli._plugins.nativeapp.sf_facade import get_snowflake_facade
from snowflake.cli.api.entities.common import get_sql_executor

from tests.nativeapp.utils import (
    SQL_EXECUTOR_EXECUTE,
    SQL_EXECUTOR_EXECUTE_QUERIES,
    mock_execute_helper,
)

# TODO: Add more test coverage
# TODO: Add test for non-identifier names
sql_facade = get_snowflake_facade(get_sql_executor())


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
def test_execute_with_role_wh_db(mock_execute_queries, mock_execute_query, mock_cursor):
    # Arrange
    mock_script = "-- my comment\nselect 1;\nselect 2;"
    mock_script_name = "test-user-sql-script.sql"
    role = "mock_role"
    wh = "mock_wh"
    database = "mock_db"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role mock_role")),
            (
                mock_cursor([("old_wh",)], []),
                mock.call("select current_warehouse()"),
            ),
            (None, mock.call("use warehouse mock_wh")),
            (
                mock_cursor([("old_db",)], []),
                mock.call("select current_database()"),
            ),
            (None, mock.call("use database mock_db")),
            (None, mock.call("use database old_db")),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute_query.side_effect = side_effects

    mock_parent = mock.Mock()
    mock_parent.attach_mock(mock_execute_query, "mock_execute_query")
    mock_parent.attach_mock(mock_execute_queries, "mock_execute_queries")

    all_execute_calls = [
        mock.call.mock_execute_query("select current_role()"),
        mock.call.mock_execute_query("use role mock_role"),
        mock.call.mock_execute_query("select current_warehouse()"),
        mock.call.mock_execute_query("use warehouse mock_wh"),
        mock.call.mock_execute_query("select current_database()"),
        mock.call.mock_execute_query("use database mock_db"),
        mock.call.mock_execute_queries(mock_script),
        mock.call.mock_execute_query("use database old_db"),
        mock.call.mock_execute_query("use warehouse old_wh"),
        mock.call.mock_execute_query("use role old_role"),
    ]

    # Act
    sql_facade.execute_user_script(mock_script, mock_script_name, role, wh, database)

    # Assert
    assert mock_execute_query.mock_calls == expected
    assert mock_execute_queries.mock_calls == [mock.call(mock_script)]
    # Assert - order of calls
    mock_parent.assert_has_calls(all_execute_calls)
