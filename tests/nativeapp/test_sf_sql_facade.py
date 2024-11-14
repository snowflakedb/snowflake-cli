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
from contextlib import contextmanager
from textwrap import dedent
from unittest import mock
from unittest.mock import _Call as Call

import pytest
from snowflake.cli._plugins.nativeapp.sf_facade_constants import UseObjectType
from snowflake.cli._plugins.nativeapp.sf_facade_exceptions import (
    CouldNotUseObjectError,
    InsufficientPrivilegesError,
    InvalidSQLError,
    UnknownConnectorError,
    UnknownSQLError,
    UserScriptError,
)
from snowflake.cli._plugins.nativeapp.sf_sql_facade import (
    SnowflakeSQLFacade,
)
from snowflake.cli.api.errno import (
    DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    INSUFFICIENT_PRIVILEGES,
    NO_WAREHOUSE_SELECTED_IN_SESSION,
)
from snowflake.connector import DatabaseError, DictCursor, Error
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

sql_facade = None


@pytest.fixture(autouse=True)
def reset_sql_facade():
    global sql_facade
    sql_facade = SnowflakeSQLFacade()


@pytest.fixture
def mock_execute_query():
    with mock.patch(SQL_EXECUTOR_EXECUTE) as mock_execute_query:
        yield mock_execute_query


@pytest.fixture
def mock_use_warehouse():
    with mock.patch.object(sql_facade, "_use_warehouse_optional") as mock_use_warehouse:
        yield mock_use_warehouse


@pytest.fixture
def mock_use_role():
    with mock.patch.object(sql_facade, "_use_role_optional") as mock_use_role:
        yield mock_use_role


@pytest.fixture
def mock_use_database():
    with mock.patch.object(sql_facade, "_use_database_optional") as mock_use_database:
        yield mock_use_database


@pytest.fixture
def mock_use_schema():
    with mock.patch.object(sql_facade, "_use_schema_optional") as mock_use_schema:
        yield mock_use_schema


@contextmanager
def assert_in_context(
    mock_cms: list[tuple[mock.Mock, Call]],
    inner_mocks: list[tuple[mock.Mock, Call]],
):
    """Assert that certain calls are made within a series of context managers.

    Use it like so:

    expected_use_objects = [
        (mock_use_role, mock.call("test_role")),
        (mock_use_warehouse, mock.call("test_wh")),
    ]
    expected_execute_query = [
        (mock_execute_query, mock.call("select 1")),
        (mock_execute_query, mock.call("select 2")),
    ]
    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.foo()

    This will assert that sql_facade.foo() calls use_role and use_warehouse with the correct arguments
    in the correct order, and that execute_query was called within the context managers
    returned by use_role and use_warehouse (i.e. in between the __enter__ and __exit__ calls).
    """
    parent_mock = mock.Mock()

    def reparent_mock(mock_instance, expected_call):
        # Attach the mock to a shared parent mock so that we can assert that the calls
        # were made in the correct order
        name = (
            # Either the mock was created with a name, or fallback to the default name behaviour
            mock_instance._mock_name  # noqa: SLF001
            or mock_instance._extract_mock_name()  # noqa: SLF001
        )
        parent_mock.attach_mock(mock_instance, name)

        # Also re-parent the expected call object since the name of a call object
        # is checked when calling assert_has_calls on a mock and the name has to match the
        # name of the child mock when it's attached to the parent
        # Calling getattr on a call object returns a new call object with the name set to the
        # attribute name, so we can use this to set the name to the parent_name
        return getattr(mock.call, name)(*expected_call.args, **expected_call.kwargs)

    pre: list[Call] = []
    inner: list[Call] = []
    post: list[Call] = []
    for mock_instance, expected_call in mock_cms:
        # Add the modified expected_call as well as the __enter__ method of its return value to the list of expected pre-calls
        # and add the return value's __exit__ method to the list of expected post-calls (in reverse order)
        expected_call = reparent_mock(mock_instance, expected_call)
        pre += [expected_call, expected_call.__enter__()]
        post.insert(0, expected_call.__exit__(None, None, None))

    for mock_instance, expected_call in inner_mocks:
        # Just add the modified expected_call to the list of assertions to be made within the context managers
        expected_call = reparent_mock(mock_instance, expected_call)
        inner.append(expected_call)

    # Run the code under test
    yield

    # Assert that the parent mock has all the expected calls in the correct order
    parent_mock.assert_has_calls(pre + inner + post)


def test_assert_in_context():
    cm1 = mock.MagicMock(name="cm1")
    cm2 = mock.MagicMock(name="cm2")

    fn1 = mock.Mock(name="fn1")
    fn2 = mock.Mock(name="fn2")

    def sut():
        with cm1("cm1"), cm2("cm2"):
            fn1(1)
            fn1(2)
            fn2(3)
            fn2(4)

    with assert_in_context(
        [(cm1, mock.call("cm1")), (cm2, mock.call("cm2"))],
        [
            (fn1, mock.call(1)),
            (fn1, mock.call(2)),
            (fn2, mock.call(3)),
            (fn2, mock.call(4)),
        ],
    ):
        sut()


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
    sql_facade.execute_user_script(
        queries=mock_script,
        script_name=mock_script_name,
        role=role,
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
    sql_facade.execute_user_script(
        queries=mock_script, script_name=mock_script_name, role=role, warehouse=wh
    )

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
    sql_facade.execute_user_script(
        queries=mock_script, script_name=mock_script_name, role=role, database=database
    )

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
    sql_facade.execute_user_script(
        queries=mock_script, script_name=mock_script_name, role=role
    )

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
    sql_facade.execute_user_script(queries=mock_script, script_name=mock_script_name)

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
        sql_facade.execute_user_script(
            queries=mock_script, script_name=mock_script_name
        )

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
        sql_facade.execute_user_script(
            queries=mock_script, script_name=mock_script_name
        )

    # Assert
    assert "Failed to run script test-user-sql-script.sql" in err.value.message


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            Exception(),
            Exception,
            "Failed to run script test-user-sql-script.sql",
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
        sql_facade.execute_user_script(
            queries=mock_script, script_name=mock_script_name
        )

    # Assert
    assert error_message in str(err)


@pytest.mark.parametrize(
    "object_type, object_name",
    [
        (UseObjectType.ROLE, "test_role"),
        (UseObjectType.DATABASE, "test_db"),
        (UseObjectType.SCHEMA, "test_schema"),
        (UseObjectType.WAREHOUSE, "test_wh"),
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
    object_type = UseObjectType.ROLE
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
    object_type = UseObjectType.WAREHOUSE
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
    with pytest.raises(InvalidSQLError) as err:
        sql_facade._use_object(object_type, object_name)  # noqa: SLF001
    assert (
        err.value.msg
        == "Invalid SQL error occurred. Failed to use warehouse test_warehouse. Some programming error"
    )


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_use_object_catches_other_sql_error(mock_execute_query):
    object_type = UseObjectType.ROLE
    object_name = "test_err_role"
    side_effects, expected = mock_execute_helper(
        [(Exception(), mock.call("use role test_err_role"))]
    )
    mock_execute_query.side_effect = side_effects
    with pytest.raises(Exception) as err:
        sql_facade._use_object(object_type, object_name)  # noqa: SLF001
    assert "Failed to use role test_err_role." in str(err)


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
            InvalidSQLError,
            "Invalid SQL error occurred. Failed to use warehouse test_warehouse. Unknown error",
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
            "Failed to use warehouse test_warehouse.",
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
            InvalidSQLError,
            "Invalid SQL error occurred. Failed to use role test_role. Unknown error",
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
            "Failed to use role test_role.",
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
            InvalidSQLError,
            "Invalid SQL error occurred. Failed to use database test_db. Unknown error",
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
            "Failed to use database test_db.",
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


@mock.patch(SQL_EXECUTOR_EXECUTE)
@pytest.mark.parametrize(
    "parameter_value,event_table",
    [
        ["db.schema.event_table", "db.schema.event_table"],
        [None, None],
        ["NONE", None],
    ],
)
def test_account_event_table(
    mock_execute_query, mock_cursor, parameter_value, event_table
):
    query_result = (
        [dict(key="EVENT_TABLE", value=parameter_value)]
        if parameter_value is not None
        else []
    )
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor(query_result, []),
                mock.call(
                    "show parameters like 'event_table' in account",
                    cursor_class=DictCursor,
                ),
            ),
        ]
    )
    mock_execute_query.side_effect = side_effects

    assert sql_facade.get_account_event_table() == event_table


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_event_definitions_base_case(mock_execute_query, mock_cursor):
    app_name = "test_app"
    query = "show telemetry event definitions in application test_app"
    events_definitions = [
        {
            "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
            "type": "ERRORS_AND_WARNINGS",
            "sharing": "MANDATORY",
            "status": "ENABLED",
        }
    ]
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor(events_definitions, []),
                mock.call(query, cursor_class=DictCursor),
            ),
        ]
    )
    mock_execute_query.side_effect = side_effects

    result = sql_facade.get_event_definitions(app_name)

    assert mock_execute_query.mock_calls == expected
    assert result == events_definitions


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_event_definitions_with_non_safe_identifier(
    mock_execute_query, mock_cursor
):
    app_name = "test.app"
    query = 'show telemetry event definitions in application "test.app"'
    events_definitions = [
        {
            "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
            "type": "ERRORS_AND_WARNINGS",
            "sharing": "MANDATORY",
            "status": "ENABLED",
        }
    ]
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor(events_definitions, []),
                mock.call(query, cursor_class=DictCursor),
            ),
        ]
    )
    mock_execute_query.side_effect = side_effects

    result = sql_facade.get_event_definitions(app_name)

    assert mock_execute_query.mock_calls == expected
    assert result == events_definitions


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_event_definitions_with_role(mock_execute_query, mock_cursor):
    app_name = "test_app"
    role_name = "my_role"
    query = "show telemetry event definitions in application test_app"
    events_definitions = [
        {
            "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
            "type": "ERRORS_AND_WARNINGS",
            "sharing": "MANDATORY",
            "status": "ENABLED",
        }
    ]
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call(f"use role {role_name}")),
            (
                mock_cursor(events_definitions, []),
                mock.call(query, cursor_class=DictCursor),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute_query.side_effect = side_effects

    result = sql_facade.get_event_definitions(app_name, role_name)

    assert mock_execute_query.mock_calls == expected
    assert result == events_definitions


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_event_definitions_bubbles_errors(mock_execute_query):
    app_name = "test_app"
    query = "show telemetry event definitions in application test_app"
    error_message = "Some programming error"
    side_effects, expected = mock_execute_helper(
        [
            (
                ProgrammingError(error_message),
                mock.call(query, cursor_class=DictCursor),
            ),
        ]
    )
    mock_execute_query.side_effect = side_effects

    with pytest.raises(InvalidSQLError) as err:
        sql_facade.get_event_definitions(app_name)

    assert (
        f"Failed to get event definitions for application {app_name}. {error_message}"
        in str(err)
    )


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_app_properties_base_case(mock_execute_query, mock_cursor):
    app_name = "test_app"
    query = f"desc application {app_name}"
    expected_result = [
        {"property": "some_param", "value": "param_value"},
        {"property": "comment", "value": "this is a test app"},
    ]
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor(expected_result, []),
                mock.call(query, cursor_class=DictCursor),
            ),
        ]
    )
    mock_execute_query.side_effect = side_effects

    result = sql_facade.get_app_properties(app_name)

    assert mock_execute_query.mock_calls == expected
    assert result == {"some_param": "param_value", "comment": "this is a test app"}


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_app_properties_with_non_safe_identifier(mock_execute_query, mock_cursor):
    app_name = "test.app"
    query = f'desc application "test.app"'
    expected_result = [
        {"property": "some_param", "value": "param_value"},
        {"property": "comment", "value": "this is a test app"},
    ]
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor(expected_result, []),
                mock.call(query, cursor_class=DictCursor),
            ),
        ]
    )
    mock_execute_query.side_effect = side_effects

    result = sql_facade.get_app_properties(app_name)

    assert mock_execute_query.mock_calls == expected
    assert result == {"some_param": "param_value", "comment": "this is a test app"}


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_app_properties_with_role(mock_execute_query, mock_cursor):
    app_name = "test_app"
    role_name = "my_role"
    query = f"desc application {app_name}"
    expected_result = [
        {"property": "some_param", "value": "param_value"},
        {"property": "comment", "value": "this is a test app"},
    ]
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call(f"use role {role_name}")),
            (
                mock_cursor(expected_result, []),
                mock.call(query, cursor_class=DictCursor),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute_query.side_effect = side_effects

    result = sql_facade.get_app_properties(app_name, role_name)

    assert mock_execute_query.mock_calls == expected
    assert result == {"some_param": "param_value", "comment": "this is a test app"}


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_app_properties_bubbles_errors(mock_execute_query):
    app_name = "test_app"
    query = f"desc application {app_name}"
    error_message = "Some programming error"
    side_effects, expected = mock_execute_helper(
        [
            (
                ProgrammingError(error_message),
                mock.call(query, cursor_class=DictCursor),
            ),
        ]
    )
    mock_execute_query.side_effect = side_effects

    with pytest.raises(InvalidSQLError) as err:
        sql_facade.get_app_properties(app_name)

    assert f"Failed to describe application {app_name}. {error_message}" in str(err)


expected_ui_params_query = dedent(
    f"""
    select value['value']::string as PARAM_VALUE, value['name']::string as PARAM_NAME from table(flatten(
        input => parse_json(SYSTEM$BOOTSTRAP_DATA_REQUEST()),
        path => 'clientParamsInfo'
    )) where value['name'] in ('ENABLE_EVENT_SHARING_V2_IN_THE_SAME_ACCOUNT', 'ENFORCE_MANDATORY_FILTERS_FOR_SAME_ACCOUNT_INSTALLATION', 'UI_SNOWSIGHT_ENABLE_REGIONLESS_REDIRECT');
    """
)


@mock.patch(SQL_EXECUTOR_EXECUTE)
@pytest.mark.parametrize(
    "events, expected_result",
    [
        ([], "alter application test_app set shared telemetry events ()"),
        (
            ["SNOWFLAKE$EVENT1", "SNOWFLAKE$EVENT2"],
            "alter application test_app set shared telemetry events ('SNOWFLAKE$EVENT1', 'SNOWFLAKE$EVENT2')",
        ),
    ],
)
def test_share_telemetry_events(mock_execute_query, events, expected_result):
    app_name = "test_app"
    mock_execute_query.return_value = None

    sql_facade.share_telemetry_events(app_name, events)

    mock_execute_query.assert_called_once_with(expected_result)


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_share_telemtry_events_with_non_safe_identifier(mock_execute_query):
    app_name = "test.app"
    events = ["SNOWFLAKE$EVENT1", "SNOWFLAKE$EVENT2"]
    mock_execute_query.return_value = None

    sql_facade.share_telemetry_events(app_name, events)

    mock_execute_query.assert_called_once_with(
        """alter application "test.app" set shared telemetry events ('SNOWFLAKE$EVENT1', 'SNOWFLAKE$EVENT2')"""
    )


def test_share_telemetry_events_bubbles_errors():
    app_name = "test_app"
    events = ["SNOWFLAKE$EVENT1", "SNOWFLAKE$EVENT2"]
    error_message = "Some programming error"
    with mock.patch(SQL_EXECUTOR_EXECUTE, side_effect=ProgrammingError(error_message)):
        with pytest.raises(InvalidSQLError) as err:
            sql_facade.share_telemetry_events(app_name, events)

    assert (
        f"Failed to share telemetry events for application {app_name}. {error_message}"
        in str(err)
    )


def test_create_schema(mock_execute_query, mock_use_role, mock_use_database):
    schema = "test_schema"

    expected_use_objects = [
        (mock_use_role, mock.call(None)),
        (mock_use_database, mock.call(None)),
    ]
    expected_execute_query = [
        (mock_execute_query, mock.call(f"create schema if not exists {schema}"))
    ]
    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.create_schema("test_schema")


def test_create_schema_uses_role_and_db(
    mock_execute_query, mock_use_role, mock_use_database
):
    schema = "test_schema"
    database = "test_db"
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_database, mock.call(database)),
    ]
    expected_execute_query = [
        (mock_execute_query, mock.call(f"create schema if not exists {schema}"))
    ]
    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.create_schema("test_schema", role=role, database=database)


def test_create_schema_uses_database_from_fqn(
    mock_execute_query, mock_use_role, mock_use_database
):
    schema = "test_schema"
    database = "test_db"
    schema_fqn = f"{database}.{schema}"
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_database, mock.call(database)),
    ]
    expected_execute_query = [
        (mock_execute_query, mock.call(f"create schema if not exists {schema}"))
    ]
    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.create_schema(schema_fqn, role=role, database="not_database")


def test_create_schema_raises_insufficient_privileges_error(
    mock_execute_query, mock_use_role, mock_use_database
):
    schema = "test_schema"
    database = "test_db"
    role = "test_role"
    side_effects, expected = mock_execute_helper(
        [
            (
                ProgrammingError(errno=INSUFFICIENT_PRIVILEGES),
                mock.call.execute_query(f"create schema if not exists {schema}"),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    with pytest.raises(InsufficientPrivilegesError):
        sql_facade.create_schema(schema, role=role, database=database)

    mock_execute_query.assert_has_calls(expected)


def test_stage_exists(
    mock_execute_query, mock_cursor, mock_use_role, mock_use_database, mock_use_schema
):
    stage = "test_stage"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([(stage,)], ["name"]),
                mock.call(f"show stages like 'TEST\\\\_STAGE'"),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [(mock_use_role, mock.call(None))]
    expected_execute_query = [(mock_execute_query, call) for call in expected]
    with assert_in_context(expected_use_objects, expected_execute_query):
        assert sql_facade.stage_exists(stage)


def test_stage_exists_fqn(
    mock_execute_query, mock_cursor, mock_use_role, mock_use_database, mock_use_schema
):
    stage = "test_db.test_schema.test_stage"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([(stage,)], ["name"]),
                mock.call(
                    f"show stages like 'TEST\\\\_STAGE' in schema test_db.test_schema"
                ),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [(mock_use_role, mock.call(None))]
    expected_execute_query = [(mock_execute_query, call) for call in expected]
    with assert_in_context(expected_use_objects, expected_execute_query):
        assert sql_facade.stage_exists(stage)


def test_stage_exists_database_and_schema_options(
    mock_execute_query, mock_cursor, mock_use_role, mock_use_database, mock_use_schema
):
    stage = "test_stage"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([(stage,)], ["name"]),
                mock.call(
                    f"show stages like 'TEST\\\\_STAGE' in schema test_db.test_schema"
                ),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [(mock_use_role, mock.call(None))]
    expected_execute_query = [(mock_execute_query, call) for call in expected]
    with assert_in_context(expected_use_objects, expected_execute_query):
        assert sql_facade.stage_exists(stage, database="test_db", schema="test_schema")


def test_stage_exists_returns_false_for_empty_result(
    mock_execute_query, mock_cursor, mock_use_role, mock_use_database, mock_use_schema
):
    stage = "test_stage"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([], []),
                mock.call("show stages like 'TEST\\\\_STAGE' in schema"),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    assert not sql_facade.stage_exists(stage)


def test_stage_exists_returns_false_for_does_not_exist_error(
    mock_execute_query, mock_cursor, mock_use_role, mock_use_database, mock_use_schema
):
    stage = "test_stage"
    side_effects, expected = mock_execute_helper(
        [
            (
                ProgrammingError(errno=DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED),
                mock.call("show stages like 'TEST\\\\_STAGE' in schema"),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    assert not sql_facade.stage_exists(stage)


def test_stage_exists_raises_insufficient_privileges_error(
    mock_execute_query, mock_cursor, mock_use_role, mock_use_database, mock_use_schema
):
    stage = "test_stage"
    side_effects, expected = mock_execute_helper(
        [
            (
                ProgrammingError(errno=INSUFFICIENT_PRIVILEGES),
                mock.call("show stages like 'TEST\\\\_STAGE' in schema"),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    with pytest.raises(InsufficientPrivilegesError):
        assert sql_facade.stage_exists(stage)


def test_create_stage(
    mock_execute_query, mock_use_role, mock_use_database, mock_use_schema
):
    stage = "test_stage"

    expected_use_objects = [
        (mock_use_role, mock.call(None)),
        (mock_use_database, mock.call(None)),
        (mock_use_schema, mock.call(None)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                f"create stage if not exists {stage} encryption = (type = 'SNOWFLAKE_SSE') directory = (enable = True)"
            ),
        )
    ]
    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.create_stage("test_stage")


def test_create_stage_with_options(
    mock_execute_query, mock_use_role, mock_use_database, mock_use_schema
):
    stage = "test_stage"

    expected_use_objects = [
        (mock_use_role, mock.call(None)),
        (mock_use_database, mock.call(None)),
        (mock_use_schema, mock.call(None)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                f"create stage if not exists {stage} encryption = (type = 'SNOWFLAKE_FULL')"
            ),
        )
    ]
    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.create_stage(
            "test_stage", encryption_type="SNOWFLAKE_FULL", enable_directory=False
        )


def test_create_stage_uses_role_db_and_schema(
    mock_execute_query, mock_use_role, mock_use_database, mock_use_schema
):
    stage = "test_stage"
    database = "test_db"
    schema = "test_schema"
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_database, mock.call(database)),
        (mock_use_schema, mock.call(schema)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                f"create stage if not exists {stage} encryption = (type = 'SNOWFLAKE_SSE') directory = (enable = True)"
            ),
        )
    ]
    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.create_stage(
            "test_stage", role=role, database=database, schema=schema
        )


def test_create_stage_uses_schema_from_fqn(
    mock_execute_query, mock_use_role, mock_use_database, mock_use_schema
):
    stage = "test_stage"
    database = "test_db"
    schema = "test_schema"
    stage_fqn = f"{schema}.{stage}"
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_database, mock.call(database)),
        (mock_use_schema, mock.call(schema)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                f"create stage if not exists {stage} encryption = (type = 'SNOWFLAKE_SSE') directory = (enable = True)"
            ),
        )
    ]
    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.create_stage(
            stage_fqn, role=role, database=database, schema="not_schema"
        )


def test_create_stage_uses_database_from_fqn(
    mock_execute_query, mock_use_role, mock_use_database, mock_use_schema
):
    stage = "test_stage"
    database = "test_db"
    schema = "test_schema"
    stage_fqn = f"{database}.{schema}.{stage}"
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_database, mock.call(database)),
        (mock_use_schema, mock.call(schema)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                f"create stage if not exists {stage} encryption = (type = 'SNOWFLAKE_SSE') directory = (enable = True)"
            ),
        )
    ]
    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.create_stage(stage_fqn, role=role, database="not_database")


def test_create_stage_raises_insufficient_privileges_error(
    mock_execute_query, mock_use_role, mock_use_database, mock_use_schema
):
    stage = "test_stage"
    database = "test_db"
    role = "test_role"
    side_effects, expected = mock_execute_helper(
        [
            (
                ProgrammingError(errno=INSUFFICIENT_PRIVILEGES),
                mock.call.execute_query(
                    f"create stage if not exists {stage} encryption = (type = 'SNOWFLAKE_SSE') directory = (enable = True)"
                ),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    with pytest.raises(InsufficientPrivilegesError):
        sql_facade.create_stage(stage, role=role, database=database)

    mock_execute_query.assert_has_calls(expected)
