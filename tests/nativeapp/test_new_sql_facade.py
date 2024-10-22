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
from collections import deque
from textwrap import dedent
from typing import Any, List, Tuple

from snowflake.cli._plugins.nativeapp.sf_sql_facade import SnowflakeSQLFacade
from snowflake.cli.api.sql_execution import SqlExecutor

# Objective:
# - correct calls are made to SF. in the correct order.
# Requirements:

# - can ensure execute_queries is called with expected query (closest method to SF before _execute_string, all queries call this method.)
# - can mock execute_queries to return result we want
# - can assert calls are made in the right order


class MockQueriesExecutor(SqlExecutor):
    def __init__(self, expected_calls: List[Tuple[str, Any]]):
        super().__init__()
        self._mock_results = deque(expected_calls)

    @property
    def _conn(self):
        raise Exception("Connection should not be accessed in tests.")

    def _execute_queries(self, queries: str, **kwargs):
        (query_expected, result) = self._mock_results.popleft()
        if dedent(queries).strip() != dedent(query_expected).strip():
            raise Exception(
                f"Test failed, expected query {query_expected} but received query {queries}"
            )

        return result

    def all_calls_made(self):
        return len(self._mock_results) == 0


def test_execute_with_role_wh_db(mock_cursor):
    mock_script = "-- my comment\nselect 1;\nselect 2;"
    mock_script_name = "test-user-sql-script.sql"
    role = "mock_role"
    wh = "mock_wh"
    database = "mock_db"
    sql_facade = SnowflakeSQLFacade(
        MockQueriesExecutor(
            expected_calls=[
                ("select current_role()", [mock_cursor([("old_role",)], [])]),
                ("use role mock_role", [None]),
                ("select current_warehouse()", [mock_cursor([("old_wh",)], [])]),
                ("use warehouse mock_wh", [None]),
                ("select current_database()", [mock_cursor([("old_db",)], [])]),
                ("use database mock_db", [None]),
                (mock_script, [None]),
                ("use database old_db", [None]),
                ("use warehouse old_wh", [None]),
                ("use role old_role", [None]),
            ]
        )
    )

    sql_facade.execute_user_script(
        queries=mock_script,
        script_name=mock_script_name,
        role=role,
        warehouse=wh,
        database=database,
    )

    assert sql_facade._sql_executor.all_calls_made()  # noqa: SLF001
