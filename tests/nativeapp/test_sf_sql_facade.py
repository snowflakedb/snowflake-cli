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
from snowflake.cli._plugins.nativeapp.sf_facade import get_snowflake_facade
from snowflake.cli._plugins.nativeapp.sf_facade_exceptions import (
    CouldNotUseObjectError,
    UnknownConnectorError,
    UnknownSQLError,
    UserScriptError,
)
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.errno import (
    DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    NO_WAREHOUSE_SELECTED_IN_SESSION,
)
from snowflake.connector import DatabaseError, Error
from snowflake.connector.errors import (
    InternalServerError,
    ProgrammingError,
    ServiceUnavailableError,
)

from tests.nativeapp.utils import (
    SQL_EXECUTOR_EXECUTE,
    SQL_EXECUTOR_EXECUTE_QUERIES,
    mock_execute_helper,
)

sql_facade = get_snowflake_facade()


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


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
def test_execute_no_db(mock_execute_queries, mock_execute_query, mock_cursor):
    # Arrange
    mock_script = "-- my comment\nselect 1;\nselect 2;"
    mock_script_name = "test-user-sql-script.sql"
    role = "mock_role"
    wh = "mock_wh"
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
        mock.call.mock_execute_queries(mock_script),
        mock.call.mock_execute_query("use warehouse old_wh"),
        mock.call.mock_execute_query("use role old_role"),
    ]

    # Act
    sql_facade.execute_user_script(mock_script, mock_script_name, role, wh)

    # Assert
    assert mock_execute_query.mock_calls == expected
    assert mock_execute_queries.mock_calls == [mock.call(mock_script)]
    # Assert - order of calls
    mock_parent.assert_has_calls(all_execute_calls)


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
def test_execute_no_wh(mock_execute_queries, mock_execute_query, mock_cursor):
    # Arrange
    mock_script = "-- my comment\nselect 1;\nselect 2;"
    mock_script_name = "test-user-sql-script.sql"
    role = "mock_role"
    database = "mock_db"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role mock_role")),
            (
                mock_cursor([("old_db",)], []),
                mock.call("select current_database()"),
            ),
            (None, mock.call("use database mock_db")),
            (None, mock.call("use database old_db")),
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
        mock.call.mock_execute_query("select current_database()"),
        mock.call.mock_execute_query("use database mock_db"),
        mock.call.mock_execute_queries(mock_script),
        mock.call.mock_execute_query("use database old_db"),
        mock.call.mock_execute_query("use role old_role"),
    ]

    # Act
    sql_facade.execute_user_script(mock_script, mock_script_name, role, None, database)

    # Assert
    assert mock_execute_query.mock_calls == expected
    assert mock_execute_queries.mock_calls == [mock.call(mock_script)]
    # Assert - order of calls
    mock_parent.assert_has_calls(all_execute_calls)


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
def test_execute_no_role(mock_execute_queries, mock_execute_query, mock_cursor):
    # Arrange
    mock_script = "-- my comment\nselect 1;\nselect 2;"
    mock_script_name = "test-user-sql-script.sql"
    wh = "mock_wh"
    database = "mock_db"
    side_effects, expected = mock_execute_helper(
        [
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
        ]
    )
    mock_execute_query.side_effect = side_effects

    mock_parent = mock.Mock()
    mock_parent.attach_mock(mock_execute_query, "mock_execute_query")
    mock_parent.attach_mock(mock_execute_queries, "mock_execute_queries")

    all_execute_calls = [
        mock.call.mock_execute_query("select current_warehouse()"),
        mock.call.mock_execute_query("use warehouse mock_wh"),
        mock.call.mock_execute_query("select current_database()"),
        mock.call.mock_execute_query("use database mock_db"),
        mock.call.mock_execute_queries(mock_script),
        mock.call.mock_execute_query("use database old_db"),
        mock.call.mock_execute_query("use warehouse old_wh"),
    ]

    # Act
    sql_facade.execute_user_script(
        queries=mock_script,
        script_name=mock_script_name,
        warehouse=wh,
        database=database,
    )

    # Assert
    assert mock_execute_query.mock_calls == expected
    assert mock_execute_queries.mock_calls == [mock.call(mock_script)]
    # Assert - order of calls
    mock_parent.assert_has_calls(all_execute_calls)


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
def test_execute_no_wh_no_db(mock_execute_queries, mock_execute_query, mock_cursor):
    # Arrange
    mock_script = "-- my comment\nselect 1;\nselect 2;"
    mock_script_name = "test-user-sql-script.sql"
    role = "mock_role"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role mock_role")),
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
        mock.call.mock_execute_queries(mock_script),
        mock.call.mock_execute_query("use role old_role"),
    ]

    # Act
    sql_facade.execute_user_script(mock_script, mock_script_name, role)

    # Assert
    assert mock_execute_query.mock_calls == expected
    assert mock_execute_queries.mock_calls == [mock.call(mock_script)]
    # Assert - order of calls
    mock_parent.assert_has_calls(all_execute_calls)


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
def test_execute_no_role_no_wh(mock_execute_queries, mock_execute_query, mock_cursor):
    # Arrange
    mock_script = "-- my comment\nselect 1;\nselect 2;"
    mock_script_name = "test-user-sql-script.sql"
    database = "mock_db"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_db",)], []),
                mock.call("select current_database()"),
            ),
            (None, mock.call("use database mock_db")),
            (None, mock.call("use database old_db")),
        ]
    )
    mock_execute_query.side_effect = side_effects

    mock_parent = mock.Mock()
    mock_parent.attach_mock(mock_execute_query, "mock_execute_query")
    mock_parent.attach_mock(mock_execute_queries, "mock_execute_queries")

    all_execute_calls = [
        mock.call.mock_execute_query("select current_database()"),
        mock.call.mock_execute_query("use database mock_db"),
        mock.call.mock_execute_queries(mock_script),
        mock.call.mock_execute_query("use database old_db"),
    ]

    # Act
    sql_facade.execute_user_script(
        queries=mock_script, script_name=mock_script_name, database=database
    )

    # Assert
    assert mock_execute_query.mock_calls == expected
    assert mock_execute_queries.mock_calls == [mock.call(mock_script)]
    # Assert - order of calls
    mock_parent.assert_has_calls(all_execute_calls)


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
def test_execute_no_role_no_db(mock_execute_queries, mock_execute_query, mock_cursor):
    # Arrange
    mock_script = "-- my comment\nselect 1;\nselect 2;"
    mock_script_name = "test-user-sql-script.sql"
    wh = "mock_wh"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_wh",)], []),
                mock.call("select current_warehouse()"),
            ),
            (None, mock.call("use warehouse mock_wh")),
            (None, mock.call("use warehouse old_wh")),
        ]
    )
    mock_execute_query.side_effect = side_effects

    mock_parent = mock.Mock()
    mock_parent.attach_mock(mock_execute_query, "mock_execute_query")
    mock_parent.attach_mock(mock_execute_queries, "mock_execute_queries")

    all_execute_calls = [
        mock.call.mock_execute_query("select current_warehouse()"),
        mock.call.mock_execute_query("use warehouse mock_wh"),
        mock.call.mock_execute_queries(mock_script),
        mock.call.mock_execute_query("use warehouse old_wh"),
    ]

    # Act
    sql_facade.execute_user_script(
        queries=mock_script, script_name=mock_script_name, warehouse=wh
    )

    # Assert
    assert mock_execute_query.mock_calls == expected
    assert mock_execute_queries.mock_calls == [mock.call(mock_script)]
    # Assert - order of calls
    mock_parent.assert_has_calls(all_execute_calls)


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
def test_execute_no_role_no_wh_no_db(mock_execute_queries, mock_execute_query):
    # Arrange
    mock_script = "-- my comment\nselect 1;\nselect 2;"
    mock_script_name = "test-user-sql-script.sql"

    # Act
    sql_facade.execute_user_script(mock_script, mock_script_name)

    # Assert
    mock_execute_query.assert_not_called()
    assert mock_execute_queries.mock_calls == [mock.call(mock_script)]


@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
def test_execute_catches_no_warehouse_error_raises_user_error(mock_execute_queries):
    # Arrange
    mock_script = "-- my comment\nselect 1;\nselect 2;"
    mock_script_name = "test-user-sql-script.sql"
    mock_execute_queries.side_effect = ProgrammingError(
        errno=NO_WAREHOUSE_SELECTED_IN_SESSION
    )

    # Act
    with pytest.raises(UserScriptError) as err:
        sql_facade.execute_user_script(mock_script, mock_script_name)

    # Assert
    assert "Failed to run script test-user-sql-script.sql" in err.value.message
    assert (
        "Please provide a warehouse in your project definition file, config.toml file, or via command line"
        in err.value.message
    )


@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
def test_execute_raises_other_programming_error_as_user_error(mock_execute_queries):
    # Arrange
    mock_script = "-- my comment\nselect 1;\nselect 2;"
    mock_script_name = "test-user-sql-script.sql"
    mock_execute_queries.side_effect = ProgrammingError()

    # Act
    with pytest.raises(UserScriptError) as err:
        sql_facade.execute_user_script(mock_script, mock_script_name)

    # Assert
    assert "Failed to run script test-user-sql-script.sql" in err.value.message


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            Exception(),
            Exception,
            "Unclassified exception occurred. Failed to run script test-user-sql-script.sql",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to run script test-user-sql-script.sql. some database error",
        ),
        (
            ServiceUnavailableError(),
            UnknownConnectorError,
            "Unknown error occurred. Failed to run script test-user-sql-script.sql. HTTP 503: Service Unavailable",
        ),
    ],
)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
def test_execute_catch_all_exception(
    mock_execute_queries, error_raised, error_caught, error_message
):
    # Arrange
    mock_script = "-- my comment\nselect 1;\nselect 2;"
    mock_script_name = "test-user-sql-script.sql"
    mock_execute_queries.side_effect = error_raised

    # Act
    with pytest.raises(error_caught) as err:
        sql_facade.execute_user_script(mock_script, mock_script_name)

    # Assert
    assert error_message in str(err)


@pytest.mark.parametrize(
    "object_type, object_name",
    [
        (ObjectType.ROLE, "test_role"),
        (ObjectType.DATABASE, "test_db"),
        (ObjectType.WAREHOUSE, "test_wh"),
    ],
)
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_use_object(mock_execute_query, object_type, object_name, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [(None, mock.call(f"use {object_type} {object_name}"))]
    )

    sql_facade._use_object(object_type, object_name)  # noqa: SLF001
    assert mock_execute_query.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_use_object_catches_not_exists_error(mock_execute_query):
    object_type = ObjectType.ROLE
    object_name = "test_err_role"
    side_effects, expected = mock_execute_helper(
        [
            (
                ProgrammingError(errno=DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED),
                mock.call("use role test_err_role"),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects
    with pytest.raises(CouldNotUseObjectError) as err:
        sql_facade._use_object(object_type, object_name)  # noqa: SLF001
    assert (
        err.value.message
        == "Could not use role test_err_role. Object does not exist, or operation cannot be performed."
    )


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_use_object_catches_other_programming_error_raises_unknown_sql_error(
    mock_execute_query,
):
    object_type = ObjectType.WAREHOUSE
    object_name = "test_warehouse"
    side_effects, expected = mock_execute_helper(
        [
            (
                ProgrammingError("Some programming error"),
                mock.call("use warehouse test_warehouse"),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects
    with pytest.raises(UnknownSQLError) as err:
        sql_facade._use_object(object_type, object_name)  # noqa: SLF001
    assert (
        err.value.msg
        == "Unknown SQL error occurred. Failed to use warehouse test_warehouse. Some programming error"
    )


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_use_object_catches_other_sql_error(mock_execute_query):
    object_type = ObjectType.ROLE
    object_name = "test_err_role"
    side_effects, expected = mock_execute_helper(
        [(Exception(), mock.call("use role test_err_role"))]
    )
    mock_execute_query.side_effect = side_effects
    with pytest.raises(Exception) as err:
        sql_facade._use_object(object_type, object_name)  # noqa: SLF001
    assert "Unclassified exception occurred. Failed to use role test_err_role." in str(
        err
    )


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_use_warehouse_single_quoted_id(mock_execute_query, mock_cursor):
    single_quoted_name = "test warehouse"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_wh",)], []),
                mock.call("select current_warehouse()"),
            ),
            (None, mock.call('use warehouse "test warehouse"')),
            (None, mock.call(f"use warehouse old_wh")),
        ]
    )
    mock_execute_query.side_effect = side_effects

    with sql_facade._use_warehouse_optional(single_quoted_name):  # noqa: SLF001
        pass

    assert mock_execute_query.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_use_warehouse_same_id_single_quotes(mock_execute_query, mock_cursor):
    single_quoted_name = "test warehouse"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([('"test warehouse"',)], []),
                mock.call("select current_warehouse()"),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    with sql_facade._use_warehouse_optional(single_quoted_name):  # noqa: SLF001
        pass

    assert mock_execute_query.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_use_role_single_quoted_id(mock_execute_query, mock_cursor):
    single_quoted_name = "test role"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call('use role "test role"')),
            (None, mock.call(f"use role old_role")),
        ]
    )
    mock_execute_query.side_effect = side_effects

    with sql_facade._use_role_optional(single_quoted_name):  # noqa: SLF001
        pass

    assert mock_execute_query.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_use_role_same_id_single_quotes(mock_execute_query, mock_cursor):
    single_quoted_name = "test role"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([('"test role"',)], []),
                mock.call("select current_role()"),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    with sql_facade._use_role_optional(single_quoted_name):  # noqa: SLF001
        pass

    assert mock_execute_query.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_use_db_single_quoted_id(mock_execute_query, mock_cursor):
    single_quoted_name = "test db"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_db",)], []),
                mock.call("select current_database()"),
            ),
            (None, mock.call('use database "test db"')),
            (None, mock.call(f"use database old_db")),
        ]
    )
    mock_execute_query.side_effect = side_effects

    with sql_facade._use_database_optional(single_quoted_name):  # noqa: SLF001
        pass

    assert mock_execute_query.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_use_db_same_id_single_quotes(mock_execute_query, mock_cursor):
    single_quoted_name = "test db"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([('"test db"',)], []),
                mock.call("select current_database()"),
            ),
        ]
    )
    mock_execute_query.side_effect = side_effects

    with sql_facade._use_database_optional(single_quoted_name):  # noqa: SLF001
        pass

    assert mock_execute_query.mock_calls == expected


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED),
            CouldNotUseObjectError,
            "Could not use warehouse test_warehouse. Object does not exist, or operation cannot be performed.",
        ),
        (
            DatabaseError("Database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to use warehouse test_warehouse. Database error",
        ),
        (
            ProgrammingError(),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to use warehouse test_warehouse. Unknown error",
        ),
        (
            Error(),
            UnknownConnectorError,
            "Unknown error occurred. Failed to use warehouse test_warehouse. Unknown error",
        ),
        (
            InternalServerError(),
            UnknownConnectorError,
            "Unknown error occurred. Failed to use warehouse test_warehouse. HTTP 500: Internal Server Error",
        ),
        (
            Exception(),
            Exception,
            "Unclassified exception occurred. Failed to use warehouse test_warehouse.",
        ),
    ],
)
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_use_warehouse_bubbles_errors(
    mock_execute_query, error_raised, error_caught, error_message, mock_cursor
):
    name = "test_warehouse"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_wh",)], []),
                mock.call("select current_warehouse()"),
            ),
            (error_raised, mock.call("use warehouse test_warehouse")),
        ]
    )
    mock_execute_query.side_effect = side_effects

    with pytest.raises(error_caught) as err:
        with sql_facade._use_warehouse_optional(name):  # noqa: SLF001
            pass

    assert error_message in str(err)


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED),
            CouldNotUseObjectError,
            "Could not use role test_role. Object does not exist, or operation cannot be performed.",
        ),
        (
            DatabaseError("Database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to use role test_role. Database error",
        ),
        (
            ProgrammingError(),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to use role test_role. Unknown error",
        ),
        (
            Error(),
            UnknownConnectorError,
            "Unknown error occurred. Failed to use role test_role. Unknown error",
        ),
        (
            InternalServerError(),
            UnknownConnectorError,
            "Unknown error occurred. Failed to use role test_role. HTTP 500: Internal Server Error",
        ),
        (
            Exception(),
            Exception,
            "Unclassified exception occurred. Failed to use role test_role.",
        ),
    ],
)
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_use_role_bubbles_errors(
    mock_execute_query, error_raised, error_caught, error_message, mock_cursor
):
    name = "test_role"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (error_raised, mock.call("use role test_role")),
        ]
    )
    mock_execute_query.side_effect = side_effects

    with pytest.raises(error_caught) as err:
        with sql_facade._use_role_optional(name):  # noqa: SLF001
            pass

    assert error_message in str(err)


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED),
            CouldNotUseObjectError,
            "Could not use database test_db. Object does not exist, or operation cannot be performed.",
        ),
        (
            DatabaseError("Database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to use database test_db. Database error",
        ),
        (
            ProgrammingError(),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to use database test_db. Unknown error",
        ),
        (
            Error(),
            UnknownConnectorError,
            "Unknown error occurred. Failed to use database test_db. Unknown error",
        ),
        (
            InternalServerError(),
            UnknownConnectorError,
            "Unknown error occurred. Failed to use database test_db. HTTP 500: Internal Server Error",
        ),
        (
            Exception(),
            Exception,
            "Unclassified exception occurred. Failed to use database test_db.",
        ),
    ],
)
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_use_db_bubbles_errors(
    mock_execute_query, error_raised, error_caught, error_message, mock_cursor
):
    name = "test_db"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_db",)], []),
                mock.call("select current_warehouse()"),
            ),
            (error_raised, mock.call("use database test_db")),
        ]
    )
    mock_execute_query.side_effect = side_effects

    with pytest.raises(error_caught) as err:
        with sql_facade._use_database_optional(name):  # noqa: SLF001
            pass

    assert error_message in str(err)
