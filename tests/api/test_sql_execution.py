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
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import BaseSqlExecutor, SqlExecutor

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


@mock.patch(EXECUTE_QUERY)
def test_use_role_no_current_role(mock_execute_query, mock_cursor):
    mock_execute_query.return_value = mock_cursor([(None,)], [])
    with SqlExecutor().use_role("new_role"):
        pass
    assert mock_execute_query.mock_calls == [
        mock.call("select current_role()"),
        mock.call(f"use role new_role"),
    ]


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


@mock.patch(EXECUTE_QUERY)
def test_use_warehouse_no_current_wh(
    mock_execute_query,
    mock_cursor,
):
    mock_execute_query.return_value = mock_cursor([(None,)], [])
    with SqlExecutor().use_warehouse("new_warehouse"):
        pass
    assert mock_execute_query.mock_calls == [
        mock.call("select current_warehouse()"),
        mock.call(f"use warehouse new_warehouse"),
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


@pytest.mark.parametrize(
    "username, password, expected_username, expected_password",
    [
        ("john_doe", "admin123", "john_doe", "admin123"),
        (
            "ok'); GRANT ROLE ACCOUNTADMIN TO USER attacker;--",
            "admin123",
            "ok''); GRANT ROLE ACCOUNTADMIN TO USER attacker;--",
            "admin123",
        ),
        ("john_doe", "a'b'c", "john_doe", "a''b''c"),
        ("o'brien", "d'arcy", "o''brien", "d''arcy"),
        # Backslash before quote: doubled so neither STANDARD_ESCAPE_SEQUENCES
        # mode lets the literal terminate prematurely.
        ("foo\\'bar", "p\\'wd", "foo\\\\''bar", "p\\\\''wd"),
    ],
)
@mock.patch(EXECUTE_QUERY)
def test_create_password_secret_escapes_single_quotes(
    mock_execute_query, username, password, expected_username, expected_password
):
    SqlExecutor().create_password_secret(
        name=FQN.from_string("my_secret"),
        username=username,
        password=password,
    )
    assert mock_execute_query.call_count == 1
    query = mock_execute_query.mock_calls[0].args[0]
    assert f"username = '{expected_username}'" in query
    assert f"password = '{expected_password}'" in query


@pytest.mark.parametrize(
    "allowed_prefix, expected_prefix",
    [
        ("https://github.com/repo.git", "https://github.com/repo.git"),
        (
            "https://github.com/'); GRANT ROLE ACCOUNTADMIN TO USER attacker;--",
            "https://github.com/''); GRANT ROLE ACCOUNTADMIN TO USER attacker;--",
        ),
        ("a'b'c", "a''b''c"),
        ("https://example.com/x\\'y", "https://example.com/x\\\\''y"),
    ],
)
@mock.patch(EXECUTE_QUERY)
def test_create_api_integration_escapes_allowed_prefix(
    mock_execute_query, allowed_prefix, expected_prefix
):
    SqlExecutor().create_api_integration(
        name=FQN.from_string("my_api"),
        api_provider="git_https_api",
        allowed_prefix=allowed_prefix,
        secret=None,
    )
    assert mock_execute_query.call_count == 1
    query = mock_execute_query.mock_calls[0].args[0]
    assert f"api_allowed_prefixes = ('{expected_prefix}')" in query


def test_execute_query_with_params_forces_qmark_paramstyle():
    """cursor.execute() defaults to pyformat, which client-side `%`-interpolates
    the query and never recognizes a literal `?` as a bind marker. Passing
    _force_qmark_paramstyle=True is what makes the `?` a real server-side bind."""
    mock_connection = mock.MagicMock()
    mock_cursor = mock_connection.cursor.return_value

    executor = BaseSqlExecutor(connection=mock_connection)
    query = "EXECUTE DCM PROJECT IDENTIFIER('p') DEPLOY ENVIRONMENT (?) FROM @stage"
    result = executor.execute_query_with_params(query, params=['{"KEY": "value"}'])

    mock_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with(
        query, ['{"KEY": "value"}'], _force_qmark_paramstyle=True
    )
    assert result is mock_cursor


def test_execute_query_with_params_defaults_params_to_none():
    mock_connection = mock.MagicMock()
    mock_cursor = mock_connection.cursor.return_value

    executor = BaseSqlExecutor(connection=mock_connection)
    executor.execute_query_with_params("select current_role()")

    mock_cursor.execute.assert_called_once_with(
        "select current_role()", None, _force_qmark_paramstyle=True
    )
