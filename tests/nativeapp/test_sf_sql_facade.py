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
from datetime import datetime
from textwrap import dedent
from unittest import mock
from unittest.mock import _Call as Call

import pytest
from snowflake.cli._plugins.connection.util import UIParameter
from snowflake.cli._plugins.nativeapp.constants import (
    AUTHORIZE_TELEMETRY_COL,
    CHANNEL_COL,
    COMMENT_COL,
    NAME_COL,
    SPECIAL_COMMENT,
)
from snowflake.cli._plugins.nativeapp.same_account_install_method import (
    SameAccountInstallMethod,
)
from snowflake.cli._plugins.nativeapp.sf_facade_constants import UseObjectType
from snowflake.cli._plugins.nativeapp.sf_facade_exceptions import (
    CouldNotUseObjectError,
    InsufficientPrivilegesError,
    InvalidSQLError,
    UnknownConnectorError,
    UnknownSQLError,
    UpgradeApplicationRestrictionError,
    UserInputError,
    UserScriptError,
)
from snowflake.cli._plugins.nativeapp.sf_sql_facade import (
    SnowflakeSQLFacade,
)
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.errno import (
    ACCOUNT_DOES_NOT_EXIST,
    ACCOUNT_HAS_TOO_MANY_QUALIFIERS,
    APPLICATION_INSTANCE_FAILED_TO_RUN_SETUP_SCRIPT,
    APPLICATION_PACKAGE_MAX_VERSIONS_HIT,
    APPLICATION_PACKAGE_PATCH_ALREADY_EXISTS,
    APPLICATION_REQUIRES_TELEMETRY_SHARING,
    CANNOT_ADD_PATCH_WITH_NON_INCREASING_PATCH_NUMBER,
    CANNOT_CREATE_VERSION_WITH_NON_ZERO_PATCH,
    CANNOT_DEREGISTER_VERSION_ASSOCIATED_WITH_CHANNEL,
    CANNOT_DISABLE_MANDATORY_TELEMETRY,
    CANNOT_DISABLE_RELEASE_CHANNELS,
    CANNOT_MODIFY_RELEASE_CHANNEL_ACCOUNTS,
    CANNOT_SET_DEBUG_MODE_WITH_MANIFEST_VERSION,
    DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    DOES_NOT_EXIST_OR_NOT_AUTHORIZED,
    INSUFFICIENT_PRIVILEGES,
    MAX_UNBOUND_VERSIONS_REACHED,
    NO_WAREHOUSE_SELECTED_IN_SESSION,
    RELEASE_DIRECTIVE_DOES_NOT_EXIST,
    RELEASE_DIRECTIVE_UNAPPROVED_VERSION_OR_PATCH,
    RELEASE_DIRECTIVES_VERSION_PATCH_NOT_FOUND,
    SQL_COMPILATION_ERROR,
    TARGET_ACCOUNT_USED_BY_OTHER_RELEASE_DIRECTIVE,
    VERSION_DOES_NOT_EXIST,
    VERSION_NOT_ADDED_TO_RELEASE_CHANNEL,
    VERSION_NOT_IN_RELEASE_CHANNEL,
    VERSION_REFERENCED_BY_RELEASE_DIRECTIVE,
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
    SQL_FACADE_GET_UI_PARAMETER,
    assert_programmingerror_cause_with_errno,
    mock_execute_helper,
)

sql_facade = SnowflakeSQLFacade()


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


@pytest.fixture
def mock_get_app_properties():
    with mock.patch.object(sql_facade, "get_app_properties") as mock_get_app_properties:
        mock_get_app_properties.return_value = {AUTHORIZE_TELEMETRY_COL: "false"}
        yield mock_get_app_properties


@pytest.fixture
def mock_get_existing_app_info():
    with mock.patch.object(
        sql_facade, "get_existing_app_info"
    ) as mock_get_existing_app_info:
        mock_get_existing_app_info.return_value = {COMMENT_COL: SPECIAL_COMMENT}
        yield mock_get_existing_app_info


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
        post.insert(0, expected_call.__exit__(mock.ANY, mock.ANY, mock.ANY))

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
def test_use_object(mock_execute_query, object_type, object_name, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [(None, mock.call(f"use {object_type} {object_name}"))]
    )

    sql_facade._use_object(object_type, object_name)  # noqa: SLF001
    assert mock_execute_query.mock_calls == expected


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


@pytest.mark.parametrize(
    "old_warehouse, expected_old_warehouse",
    [("old_wh", "old_wh"), ("old wh", '"old wh"')],
)
def test_use_warehouse_single_quoted_id(
    mock_execute_query, mock_cursor, old_warehouse, expected_old_warehouse
):
    single_quoted_name = "test warehouse"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([(old_warehouse,)], []),
                mock.call("select current_warehouse()"),
            ),
            (None, mock.call('use warehouse "test warehouse"')),
            (None, mock.call(f"use warehouse {expected_old_warehouse}")),
        ]
    )
    mock_execute_query.side_effect = side_effects

    with sql_facade._use_warehouse_optional(single_quoted_name):  # noqa: SLF001
        pass

    assert mock_execute_query.mock_calls == expected


@pytest.mark.parametrize(
    "new_wh, current_wh",
    [("test_wh", "test_wh"), ("test wh", '"test wh"'), ("test wh", "test wh")],
)
def test_use_warehouse_same_id(mock_execute_query, mock_cursor, new_wh, current_wh):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([(current_wh,)], []),
                mock.call("select current_warehouse()"),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    with sql_facade._use_warehouse_optional(new_wh):  # noqa: SLF001
        pass

    assert mock_execute_query.mock_calls == expected


@pytest.mark.parametrize(
    "old_role, expected_old_role",
    [("old_role", "old_role"), ("old role", '"old role"')],
)
def test_use_role_single_quoted_id(
    mock_execute_query, mock_cursor, old_role, expected_old_role
):
    single_quoted_name = "test role"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([(old_role,)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call('use role "test role"')),
            (None, mock.call(f"use role {expected_old_role}")),
        ]
    )
    mock_execute_query.side_effect = side_effects

    with sql_facade._use_role_optional(single_quoted_name):  # noqa: SLF001
        pass

    assert mock_execute_query.mock_calls == expected


@pytest.mark.parametrize(
    "new_role, current_role",
    [
        ("test_role", "test_role"),
        ("test role", '"test role"'),
        ("test role", "test role"),
    ],
)
def test_use_role_same_id(mock_execute_query, mock_cursor, new_role, current_role):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([(current_role,)], []),
                mock.call("select current_role()"),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    with sql_facade._use_role_optional(new_role):  # noqa: SLF001
        pass

    assert mock_execute_query.mock_calls == expected


@pytest.mark.parametrize(
    "old_db, expected_old_db",
    [("old_db", "old_db"), ("old db", '"old db"')],
)
def test_use_db_single_quoted_id(
    mock_execute_query, mock_cursor, old_db, expected_old_db
):
    single_quoted_name = "test db"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([(old_db,)], []),
                mock.call("select current_database()"),
            ),
            (None, mock.call('use database "test db"')),
            (None, mock.call(f"use database {expected_old_db}")),
        ]
    )
    mock_execute_query.side_effect = side_effects

    with sql_facade._use_database_optional(single_quoted_name):  # noqa: SLF001
        pass

    assert mock_execute_query.mock_calls == expected


@pytest.mark.parametrize(
    "new_db, current_db",
    [("test_db", "test_db"), ("test db", '"test db"'), ("test db", "test db")],
)
def test_use_db_same_id(mock_execute_query, mock_cursor, new_db, current_db):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([(current_db,)], []),
                mock.call("select current_database()"),
            ),
        ]
    )
    mock_execute_query.side_effect = side_effects

    with sql_facade._use_database_optional(new_db):  # noqa: SLF001
        pass

    assert mock_execute_query.mock_calls == expected


@pytest.mark.parametrize(
    "old_schema, expected_old_schema",
    [("old_schema", "old_schema"), ("old schema", '"old schema"')],
)
def test_use_schema_single_quoted_id(
    mock_execute_query, mock_cursor, old_schema, expected_old_schema
):
    single_quoted_name = "test schema"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([(old_schema,)], []),
                mock.call("select current_schema()"),
            ),
            (None, mock.call('use schema "test schema"')),
            (None, mock.call(f"use schema {expected_old_schema}")),
        ]
    )
    mock_execute_query.side_effect = side_effects

    with sql_facade._use_schema_optional(single_quoted_name):  # noqa: SLF001
        pass

    assert mock_execute_query.mock_calls == expected


@pytest.mark.parametrize(
    "new_schema, current_schema",
    [
        ("test_schema", "test_schema"),
        ("test schema", '"test schema"'),
        ("test schema", "test schema"),
    ],
)
def test_use_schema_same_id(
    mock_execute_query, mock_cursor, new_schema, current_schema
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([(current_schema,)], []),
                mock.call("select current_schema()"),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    with sql_facade._use_schema_optional(new_schema):  # noqa: SLF001
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


def test_share_telemetry_events_with_non_safe_identifier(mock_execute_query):
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


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=INSUFFICIENT_PRIVILEGES),
            InsufficientPrivilegesError,
            "Insufficient privileges to create schema test_schema",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Invalid SQL error occurred. Failed to create schema test_schema. Unknown error",
        ),
        (
            DatabaseError("Database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to create schema test_schema. Database error",
        ),
    ],
)
def test_create_schema_with_error(
    mock_execute_query,
    mock_use_role,
    mock_use_database,
    error_raised,
    error_caught,
    error_message,
):
    schema = "test_schema"
    database = "test_db"
    role = "test_role"

    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.create_schema(schema, role=role, database=database)

    assert error_message in str(err)


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


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=INSUFFICIENT_PRIVILEGES),
            InsufficientPrivilegesError,
            "Insufficient privileges to check if stage test_stage exists",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Invalid SQL error occurred. Failed to check if stage test_stage exists. Unknown error",
        ),
        (
            DatabaseError("Database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to check if stage test_stage exists. Database error",
        ),
    ],
)
def test_stage_exists_with_error(
    mock_execute_query,
    mock_use_role,
    mock_use_database,
    mock_use_schema,
    error_raised,
    error_caught,
    error_message,
):
    stage = "test_stage"
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        assert sql_facade.stage_exists(stage)

    assert error_message in str(err)


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


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=INSUFFICIENT_PRIVILEGES),
            InsufficientPrivilegesError,
            "Insufficient privileges to create stage test_stage",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Invalid SQL error occurred. Failed to create stage test_stage. Unknown error",
        ),
        (
            DatabaseError("Database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to create stage test_stage. Database error",
        ),
    ],
)
def test_create_stage_with_error(
    mock_execute_query,
    mock_use_role,
    mock_use_database,
    mock_use_schema,
    error_raised,
    error_caught,
    error_message,
):
    stage = "test_stage"
    database = "test_db"
    role = "test_role"
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.create_stage(stage, role=role, database=database)

    assert error_message in str(err)


@pytest.mark.parametrize(
    "args,expected_query",
    [
        (
            {
                "privileges": ["install", "develop"],
                "object_type": ObjectType.APPLICATION_PACKAGE,
                "object_identifier": "package_name",
                "role_to_grant": "app_role",
                "role_to_use": "package_role",
            },
            "grant install, develop on application package package_name to role app_role",
        ),
        (
            {
                "privileges": ["usage"],
                "object_type": ObjectType.SCHEMA,
                "object_identifier": "package_name.stage_schema",
                "role_to_grant": "app_role",
                "role_to_use": "package_role",
            },
            "grant usage on schema package_name.stage_schema to role app_role",
        ),
        (
            {
                "privileges": ["read"],
                "object_type": ObjectType.STAGE,
                "object_identifier": "stage_fqn",
                "role_to_grant": "app_role",
                "role_to_use": None,
            },
            "grant read on stage stage_fqn to role app_role",
        ),
    ],
)
def test_grant_privileges_to_role(
    mock_use_role,
    mock_execute_query,
    args,
    expected_query,
):
    expected_use_objects = [(mock_use_role, mock.call(args["role_to_use"]))]
    expected_execute_query = [(mock_execute_query, mock.call(expected_query))]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.grant_privileges_to_role(**args)


@pytest.mark.parametrize(
    "args,expected_query",
    [
        (
            {"name": "example_app", "role": "example_role"},
            r"show applications like 'EXAMPLE\\_APP'",
        ),
        (
            {"name": "nounderscores", "role": None},
            r"show applications like 'NOUNDERSCORES'",
        ),
    ],
)
def test_get_existing_app_info(
    mock_use_role, mock_execute_query, args, expected_query, mock_cursor
):
    expected_use_objects = [(mock_use_role, mock.call(args["role"]))]

    mock_cursor_results = [
        {
            NAME_COL: "NOT_NAME",
        },
        {
            NAME_COL: args["name"].upper(),
        },
    ]
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor(mock_cursor_results, []),
                mock.call(expected_query),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects
    expected_execute_query = [
        (mock_execute_query, mock.call(expected_query, cursor_class=DictCursor))
    ]

    with assert_in_context(expected_use_objects, expected_execute_query):
        result = sql_facade.get_existing_app_info(**args)

    assert result == {NAME_COL: args["name"].upper()}


@pytest.mark.parametrize(
    "value_from_col, app_name, expected_to_match",
    [
        # value_from_col is lowercase, implying quoted treatment
        # but app_name is not quoted:
        ("test_app", "test_app", False),
        ("test_app", "TEST_APP", False),
        # value_from_col is uppercase, implying unquoted treatment
        ("TEST_APP", "test_app", True),
        ("TEST_APP", "TEST_APP", True),
        # value_from_col is lowercase, implying quoted treatment
        # app_name implicitly quoted due to special characters
        ("test.app", "test.app", True),
        # app_name is quoted
        ("test_app", '"test_app"', True),
        ("test_app", '"test_APP"', False),
        ("TEST_APP", '"test_app"', False),
        ("TEST_APP", '"TEST_APP"', True),
    ],
)
def test_get_existing_app_info_handling_of_app_names(
    mock_use_role,
    mock_execute_query,
    mock_cursor,
    value_from_col,
    app_name,
    expected_to_match,
):
    mock_execute_query.side_effect = [
        mock_cursor(
            [
                {
                    NAME_COL: value_from_col,
                }
            ],
            [],
        ),
    ]

    result = sql_facade.get_existing_app_info(name=app_name, role=None)

    if expected_to_match:
        assert result[NAME_COL] == value_from_col
    else:
        assert result is None


def test_upgrade_application_unversioned(
    mock_get_existing_app_info,
    mock_use_warehouse,
    mock_use_role,
    mock_get_app_properties,
    mock_execute_query,
    mock_cursor,
):
    app_name = "test_app"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([], []),
                mock.call(f"alter application {app_name} upgrade using @{stage_fqn}"),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_warehouse, mock.call(warehouse)),
    ]
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.upgrade_application(
            name=app_name,
            install_method=SameAccountInstallMethod.unversioned_dev(),
            path_to_version_directory=stage_fqn,
            debug_mode=None,
            should_authorize_event_sharing=None,
            role=role,
            warehouse=warehouse,
        )


def test_upgrade_application_version_and_patch(
    mock_get_existing_app_info,
    mock_use_role,
    mock_use_warehouse,
    mock_get_app_properties,
    mock_execute_query,
    mock_cursor,
):
    app_name = "test_app"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([], []),
                mock.call(
                    # make sure that "3" is quoted since that was a bug we found
                    f'alter application {app_name} upgrade using version "3" patch 2'
                ),
            ),
            (None, mock.call(f"alter application {app_name} set debug_mode = True")),
            (
                None,
                mock.call(
                    f"alter application {app_name} set AUTHORIZE_TELEMETRY_EVENT_SHARING = TRUE"
                ),
            ),
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_warehouse, mock.call(warehouse)),
    ]
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.upgrade_application(
            name=app_name,
            install_method=SameAccountInstallMethod.versioned_dev("3", 2),
            path_to_version_directory=stage_fqn,
            debug_mode=True,
            should_authorize_event_sharing=True,
            role=role,
            warehouse=warehouse,
        )


def test_upgrade_application_from_release_directive(
    mock_get_app_properties,
    mock_get_existing_app_info,
    mock_use_warehouse,
    mock_use_role,
    mock_execute_query,
    mock_cursor,
):
    app_name = "test_app"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"
    mock_get_app_properties.return_value = {
        COMMENT_COL: SPECIAL_COMMENT,
        AUTHORIZE_TELEMETRY_COL: "true",
    }

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([], []),
                mock.call(f"alter application {app_name} upgrade "),
                # not dev mode so no debug mode call
                # authorize telemetry col is the same as arg, so no call
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_warehouse, mock.call(warehouse)),
    ]
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.upgrade_application(
            name=app_name,
            install_method=SameAccountInstallMethod.release_directive(),
            path_to_version_directory=stage_fqn,
            debug_mode=True,
            should_authorize_event_sharing=True,
            role=role,
            warehouse=warehouse,
        )


def test_upgrade_application_converts_expected_programmingerrors_to_user_errors(
    mock_get_existing_app_info,
    mock_use_warehouse,
    mock_use_role,
    mock_get_app_properties,
    mock_execute_query,
):
    app_name = "test_app"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"
    programming_error_message = "programming error message"

    side_effects, expected = mock_execute_helper(
        [
            (
                ProgrammingError(
                    errno=APPLICATION_INSTANCE_FAILED_TO_RUN_SETUP_SCRIPT,
                    msg=programming_error_message,
                ),
                mock.call(f"alter application {app_name} upgrade using @{stage_fqn}"),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_warehouse, mock.call(warehouse)),
    ]
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with (
        assert_in_context(expected_use_objects, expected_execute_query),
        pytest.raises(UserInputError) as err,
    ):
        sql_facade.upgrade_application(
            name=app_name,
            install_method=SameAccountInstallMethod.unversioned_dev(),
            path_to_version_directory=stage_fqn,
            debug_mode=True,
            should_authorize_event_sharing=True,
            role=role,
            warehouse=warehouse,
        )

    assert_programmingerror_cause_with_errno(
        err, APPLICATION_INSTANCE_FAILED_TO_RUN_SETUP_SCRIPT
    )
    assert err.match(
        f"Failed to upgrade application {app_name} with the following error message:\n"
    )
    assert err.match(programming_error_message)


def test_upgrade_application_special_message_for_event_sharing_error(
    mock_get_existing_app_info,
    mock_get_app_properties,
    mock_use_warehouse,
    mock_use_role,
    mock_execute_query,
    mock_cursor,
):
    app_name = "test_app"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"
    mock_get_app_properties.return_value = {
        COMMENT_COL: SPECIAL_COMMENT,
        AUTHORIZE_TELEMETRY_COL: "true",
    }

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([], []),
                mock.call(f"alter application {app_name} upgrade using version v1 "),
            ),
            (None, mock.call(f"alter application {app_name} set debug_mode = False")),
            (
                ProgrammingError(
                    errno=CANNOT_DISABLE_MANDATORY_TELEMETRY,
                ),
                mock.call(
                    f"alter application {app_name} set AUTHORIZE_TELEMETRY_EVENT_SHARING = FALSE"
                ),
            ),
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_warehouse, mock.call(warehouse)),
    ]
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with (
        assert_in_context(expected_use_objects, expected_execute_query),
        pytest.raises(UserInputError) as err,
    ):
        sql_facade.upgrade_application(
            name=app_name,
            install_method=SameAccountInstallMethod.versioned_dev("v1"),
            path_to_version_directory=stage_fqn,
            debug_mode=False,
            should_authorize_event_sharing=False,
            role=role,
            warehouse=warehouse,
        )

    assert_programmingerror_cause_with_errno(err, CANNOT_DISABLE_MANDATORY_TELEMETRY)
    assert err.match(
        "Could not disable telemetry event sharing for the application because it contains mandatory events. Please set 'share_mandatory_events' to true in the application telemetry section of the project definition file."
    )


def test_upgrade_application_converts_unexpected_programmingerrors_to_unclassified_errors(
    mock_get_existing_app_info,
    mock_use_warehouse,
    mock_use_role,
    mock_get_app_properties,
    mock_execute_query,
):
    app_name = "test_app"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"

    side_effects, expected = mock_execute_helper(
        [
            (
                ProgrammingError(
                    errno=SQL_COMPILATION_ERROR,
                ),
                mock.call(f"alter application {app_name} upgrade using @{stage_fqn}"),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_warehouse, mock.call(warehouse)),
    ]
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with (
        assert_in_context(expected_use_objects, expected_execute_query),
        pytest.raises(InvalidSQLError) as err,
    ):
        sql_facade.upgrade_application(
            name=app_name,
            install_method=SameAccountInstallMethod.unversioned_dev(),
            path_to_version_directory=stage_fqn,
            debug_mode=True,
            should_authorize_event_sharing=True,
            role=role,
            warehouse=warehouse,
        )

    assert_programmingerror_cause_with_errno(err, SQL_COMPILATION_ERROR)


def test_upgrade_application_with_release_channel_same_as_app_properties(
    mock_get_app_properties,
    mock_get_existing_app_info,
    mock_use_warehouse,
    mock_use_role,
    mock_execute_query,
    mock_cursor,
):
    app_name = "test_app"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"
    release_channel = "test_channel"
    mock_get_app_properties.return_value = {
        COMMENT_COL: SPECIAL_COMMENT,
        AUTHORIZE_TELEMETRY_COL: "true",
        CHANNEL_COL: release_channel.upper(),
    }

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([], []),
                mock.call(f"alter application {app_name} upgrade "),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_warehouse, mock.call(warehouse)),
    ]
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.upgrade_application(
            name=app_name,
            install_method=SameAccountInstallMethod.release_directive(),
            path_to_version_directory=stage_fqn,
            debug_mode=True,
            should_authorize_event_sharing=True,
            role=role,
            warehouse=warehouse,
            release_channel=release_channel,
        )


def test_upgrade_application_with_release_channel_not_same_as_app_properties_then_upgrade_error(
    mock_get_app_properties,
    mock_get_existing_app_info,
    mock_use_warehouse,
    mock_use_role,
    mock_execute_query,
    mock_cursor,
):
    app_name = "test_app"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"
    release_channel = "test_channel"
    mock_get_app_properties.return_value = {
        COMMENT_COL: SPECIAL_COMMENT,
        AUTHORIZE_TELEMETRY_COL: "true",
        CHANNEL_COL: "different_channel",
    }

    with pytest.raises(UpgradeApplicationRestrictionError) as err:
        sql_facade.upgrade_application(
            name=app_name,
            install_method=SameAccountInstallMethod.release_directive(),
            path_to_version_directory=stage_fqn,
            debug_mode=True,
            should_authorize_event_sharing=True,
            role=role,
            warehouse=warehouse,
            release_channel=release_channel,
        )

    assert (
        str(err.value)
        == f"Application {app_name} is currently on release channel different_channel. Cannot upgrade to release channel {release_channel}."
    )


def test_create_application_with_minimal_clauses(
    mock_use_warehouse,
    mock_use_role,
    mock_execute_query,
    mock_cursor,
):
    app_name = "test_app"
    pkg_name = "test_pkg"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([], []),
                mock.call(
                    dedent(
                        f"""\
                        create application {app_name}
                            from application package {pkg_name}
                            comment = {SPECIAL_COMMENT}
                        """
                    )
                ),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_warehouse, mock.call(warehouse)),
    ]
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.create_application(
            name=app_name,
            package_name=pkg_name,
            install_method=SameAccountInstallMethod.release_directive(),
            path_to_version_directory=stage_fqn,
            debug_mode=None,
            should_authorize_event_sharing=None,
            role=role,
            warehouse=warehouse,
        )


def test_create_application_with_all_clauses(
    mock_use_warehouse,
    mock_use_role,
    mock_execute_query,
    mock_cursor,
):
    app_name = "test_app"
    pkg_name = "test_pkg"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([], []),
                mock.call(
                    dedent(
                        f"""\
                        create application {app_name}
                            from application package {pkg_name}
                            using @{stage_fqn}
                            AUTHORIZE_TELEMETRY_EVENT_SHARING = TRUE
                            comment = {SPECIAL_COMMENT}
                        """
                    )
                ),
            ),
            (
                mock_cursor([], []),
                mock.call(
                    dedent(
                        f"""\
                            alter application {app_name}
                            set debug_mode = True
                        """
                    )
                ),
            ),
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_warehouse, mock.call(warehouse)),
    ]
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.create_application(
            name=app_name,
            package_name=pkg_name,
            install_method=SameAccountInstallMethod.unversioned_dev(),
            path_to_version_directory=stage_fqn,
            debug_mode=True,
            should_authorize_event_sharing=True,
            role=role,
            warehouse=warehouse,
        )


def test_create_application_converts_expected_programmingerrors_to_user_errors(
    mock_use_warehouse, mock_use_role, mock_execute_query
):
    app_name = "test_app"
    pkg_name = "test_pkg"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"
    programming_error_message = "programming error message"

    side_effects, expected = mock_execute_helper(
        [
            (
                ProgrammingError(
                    errno=APPLICATION_INSTANCE_FAILED_TO_RUN_SETUP_SCRIPT,
                    msg=programming_error_message,
                ),
                mock.call(
                    dedent(
                        f"""\
                        create application {app_name}
                            from application package {pkg_name}
                            comment = {SPECIAL_COMMENT}
                        """
                    )
                ),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_warehouse, mock.call(warehouse)),
    ]
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with (
        assert_in_context(expected_use_objects, expected_execute_query),
        pytest.raises(UserInputError) as err,
    ):
        sql_facade.create_application(
            name=app_name,
            package_name=pkg_name,
            install_method=SameAccountInstallMethod.release_directive(),
            path_to_version_directory=stage_fqn,
            debug_mode=None,
            should_authorize_event_sharing=None,
            role=role,
            warehouse=warehouse,
        )

    assert_programmingerror_cause_with_errno(
        err, APPLICATION_INSTANCE_FAILED_TO_RUN_SETUP_SCRIPT
    )
    assert err.match(
        f"Failed to create application {app_name} with the following error message:\n"
    )
    assert err.match(programming_error_message)


def test_create_application_special_message_for_event_sharing_error(
    mock_use_warehouse, mock_use_role, mock_execute_query
):
    app_name = "test_app"
    pkg_name = "test_pkg"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"

    side_effects, expected = mock_execute_helper(
        [
            (
                ProgrammingError(
                    errno=APPLICATION_REQUIRES_TELEMETRY_SHARING,
                ),
                mock.call(
                    dedent(
                        f"""\
                        create application {app_name}
                            from application package {pkg_name}
                            using version "3" patch 1
                            AUTHORIZE_TELEMETRY_EVENT_SHARING = FALSE
                            comment = {SPECIAL_COMMENT}
                        """
                    )
                ),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_warehouse, mock.call(warehouse)),
    ]
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with (
        assert_in_context(expected_use_objects, expected_execute_query),
        pytest.raises(UserInputError) as err,
    ):
        sql_facade.create_application(
            name=app_name,
            package_name=pkg_name,
            install_method=SameAccountInstallMethod.versioned_dev("3", 1),
            path_to_version_directory=stage_fqn,
            debug_mode=False,
            should_authorize_event_sharing=False,
            role=role,
            warehouse=warehouse,
        )

    assert_programmingerror_cause_with_errno(
        err, APPLICATION_REQUIRES_TELEMETRY_SHARING
    )
    assert err.match(
        "The application package requires event sharing to be authorized. Please set 'share_mandatory_events' to true in the application telemetry section of the project definition file."
    )


def test_create_application_returns_warning_with_debug_mode_and_manifest_app_spec(
    mock_use_warehouse, mock_use_role, mock_execute_query, mock_cursor
):
    app_name = "test_app"
    pkg_name = "test_pkg"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([], []),
                mock.call(
                    dedent(
                        f"""\
                        create application {app_name}
                            from application package {pkg_name}
                            using @{stage_fqn}
                            AUTHORIZE_TELEMETRY_EVENT_SHARING = TRUE
                            comment = {SPECIAL_COMMENT}
                        """
                    )
                ),
            ),
            (
                ProgrammingError(
                    errno=CANNOT_SET_DEBUG_MODE_WITH_MANIFEST_VERSION,
                ),
                mock.call(
                    dedent(
                        f"""\
                            alter application {app_name}
                            set debug_mode = True
                        """
                    )
                ),
            ),
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_warehouse, mock.call(warehouse)),
    ]
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with assert_in_context(expected_use_objects, expected_execute_query):
        _, warnings = sql_facade.create_application(
            name=app_name,
            package_name=pkg_name,
            install_method=SameAccountInstallMethod.unversioned_dev(),
            path_to_version_directory=stage_fqn,
            debug_mode=True,
            should_authorize_event_sharing=True,
            role=role,
            warehouse=warehouse,
        )

    assert len(warnings) == 1
    warning = warnings[0]
    assert (
        "Did not apply debug mode to application because the manifest version is set to 2 or higher. Please use session debugging instead."
        == warning
    )


def test_create_application_succeeds_when_debug_mode_couldnt_be_set(
    mock_use_warehouse, mock_use_role, mock_execute_query, mock_cursor
):
    app_name = "test_app"
    pkg_name = "test_pkg"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([], []),
                mock.call(
                    dedent(
                        f"""\
                        create application {app_name}
                            from application package {pkg_name}
                            using @{stage_fqn}
                            AUTHORIZE_TELEMETRY_EVENT_SHARING = TRUE
                            comment = {SPECIAL_COMMENT}
                        """
                    )
                ),
            ),
            (
                ProgrammingError(
                    msg="Failed to set debug mode because of an error",
                    errno=1234,
                ),
                mock.call(
                    dedent(
                        f"""\
                            alter application {app_name}
                            set debug_mode = True
                        """
                    )
                ),
            ),
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_warehouse, mock.call(warehouse)),
    ]
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with assert_in_context(expected_use_objects, expected_execute_query):
        _, warnings = sql_facade.create_application(
            name=app_name,
            package_name=pkg_name,
            install_method=SameAccountInstallMethod.unversioned_dev(),
            path_to_version_directory=stage_fqn,
            debug_mode=True,
            should_authorize_event_sharing=True,
            role=role,
            warehouse=warehouse,
        )

    assert len(warnings) == 1
    warning = warnings[0]
    assert (
        "Failed to set debug mode for application test_app. 001234: 1234: Failed to set debug mode because of an error"
        == warning
    )


def test_create_application_converts_unexpected_programmingerrors_to_unclassified_errors(
    mock_use_warehouse, mock_use_role, mock_execute_query
):
    app_name = "test_app"
    pkg_name = "test_pkg"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"

    side_effects, expected = mock_execute_helper(
        [
            (
                ProgrammingError(
                    errno=SQL_COMPILATION_ERROR,
                ),
                mock.call(
                    dedent(
                        f"""\
                        create application {app_name}
                            from application package {pkg_name}
                            comment = {SPECIAL_COMMENT}
                        """
                    )
                ),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_warehouse, mock.call(warehouse)),
    ]
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with (
        assert_in_context(expected_use_objects, expected_execute_query),
        pytest.raises(InvalidSQLError) as err,
    ):
        sql_facade.create_application(
            name=app_name,
            package_name=pkg_name,
            install_method=SameAccountInstallMethod.release_directive(),
            path_to_version_directory=stage_fqn,
            debug_mode=None,
            should_authorize_event_sharing=None,
            role=role,
            warehouse=warehouse,
        )

    assert_programmingerror_cause_with_errno(err, SQL_COMPILATION_ERROR)


def test_create_application_with_release_channel(
    mock_use_warehouse,
    mock_use_role,
    mock_execute_query,
    mock_cursor,
):
    app_name = "test_app"
    pkg_name = "test_pkg"
    stage_fqn = "app_pkg.app_src.stage"
    role = "test_role"
    warehouse = "test_warehouse"
    release_channel = "test_channel"

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([], []),
                mock.call(
                    dedent(
                        f"""\
                        create application {app_name}
                            from application package {pkg_name}
                            using release channel {release_channel}
                            comment = {SPECIAL_COMMENT}
                        """
                    )
                ),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
        (mock_use_warehouse, mock.call(warehouse)),
    ]
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.create_application(
            name=app_name,
            package_name=pkg_name,
            install_method=SameAccountInstallMethod.release_directive(),
            path_to_version_directory=stage_fqn,
            debug_mode=None,
            should_authorize_event_sharing=None,
            role=role,
            warehouse=warehouse,
            release_channel=release_channel,
        )


@pytest.mark.parametrize(
    "pkg_name, sanitized_pkg_name",
    [("test_pkg", "test_pkg"), ("test.pkg", '"test.pkg"')],
)
def test_given_basic_pkg_when_create_application_package_then_success(
    mock_execute_query, mock_use_role, pkg_name, sanitized_pkg_name
):
    distribution = "INTERNAL"
    role = "test_role"

    expected_use_objects = [(mock_use_role, mock.call(role))]

    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                dedent(
                    f"""\
                    create application package {sanitized_pkg_name}
                        comment = {SPECIAL_COMMENT}
                        distribution = {distribution}
                    """
                )
            ),
        )
    ]
    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.create_application_package(pkg_name, distribution, role=role)


@pytest.mark.parametrize("enable_release_channels", [True, False])
def test_given_release_channels_when_create_application_package_then_success(
    mock_execute_query, mock_use_role, enable_release_channels
):
    package_name = "test_package"
    distribution = "INTERNAL"
    role = "test_role"

    expected_use_objects = [(mock_use_role, mock.call(role))]

    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                dedent(
                    f"""\
                    create application package {package_name}
                        comment = {SPECIAL_COMMENT}
                        distribution = {distribution}
                        enable_release_channels = {str(enable_release_channels).lower()}
                    """
                )
            ),
        )
    ]
    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.create_application_package(
            package_name,
            distribution,
            role=role,
            enable_release_channels=enable_release_channels,
        )


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=INSUFFICIENT_PRIVILEGES),
            InsufficientPrivilegesError,
            "Insufficient privileges to create application package test_stage",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Invalid SQL error occurred. Failed to create application package test_stage. Unknown error",
        ),
        (
            DatabaseError("Database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to create application package test_stage. Database error",
        ),
    ],
)
def test_create_application_package_with_error(
    mock_execute_query,
    mock_use_role,
    error_raised,
    error_caught,
    error_message,
):
    package_name = "test_stage"
    distribution = "INTERNAL"
    role = "test_role"
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.create_application_package(package_name, distribution, role=role)

    assert error_message in str(err)


@pytest.mark.parametrize(
    "pkg_name, sanitized_pkg_name",
    [("test_pkg", "test_pkg"), ("test.pkg", '"test.pkg"')],
)
@pytest.mark.parametrize("enable_release_channels", [True, False])
def test_given_basic_pkg_when_update_application_package_properties_then_success(
    mock_execute_query,
    mock_use_role,
    pkg_name,
    sanitized_pkg_name,
    enable_release_channels,
):
    expected_use_objects = [(mock_use_role, mock.call(None))]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                dedent(
                    f"""\
                    alter application package {sanitized_pkg_name}
                        set enable_release_channels = {str(enable_release_channels).lower()}
                    """
                )
            ),
        )
    ]
    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.alter_application_package_properties(
            pkg_name, enable_release_channels=enable_release_channels
        )


def test_given_no_enable_release_channel_flag_when_update_application_package_then_no_action(
    mock_execute_query,
):
    sql_facade.alter_application_package_properties("test_pkg", role="test_role")

    assert mock_execute_query.call_count == 0


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=INSUFFICIENT_PRIVILEGES),
            InsufficientPrivilegesError,
            "Insufficient privileges to update enable_release_channels for application package test_pkg",
        ),
        (
            ProgrammingError(errno=CANNOT_DISABLE_RELEASE_CHANNELS),
            UserInputError,
            "Cannot disable release channels for application package test_pkg after it is enabled. Try recreating the application package",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Invalid SQL error occurred. Failed to update enable_release_channels for application package test_pkg. Unknown error",
        ),
        (
            DatabaseError("Database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to update enable_release_channels for application package test_pkg. Database error",
        ),
    ],
)
def test_alter_application_pacakge_properties_for_enable_release_channels_with_error(
    mock_execute_query,
    mock_use_role,
    error_raised,
    error_caught,
    error_message,
):
    pkg_name = "test_pkg"
    role = "test_role"
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.alter_application_package_properties(
            pkg_name, enable_release_channels=True, role=role
        )

    assert error_message in str(err)


expected_ui_params_query = "call system$bootstrap_data_request('CLIENT_PARAMS_INFO')"


def test_get_ui_parameter_with_value(mock_cursor):
    with mock.patch.object(sql_facade, "_sql_executor") as mock_sql_executor:
        execute_str_mock = mock_sql_executor._conn.execute_string  # noqa: SLF001
        execute_str_mock.return_value = (
            None,
            mock_cursor(
                [
                    (
                        """\
                        {
                            "clientParamsInfo": [{
                                "name": "FEATURE_RELEASE_CHANNELS",
                                "value": true
                            }]
                        }
                        """,
                    )
                ],
                [],
            ),
        )

        assert (
            sql_facade.get_ui_parameter(UIParameter.NA_FEATURE_RELEASE_CHANNELS, False)
            is True
        )

        execute_str_mock.assert_called_once_with(expected_ui_params_query)


def test_get_ui_parameter_with_empty_value_then_use_empty_value(mock_cursor):
    with mock.patch.object(sql_facade, "_sql_executor") as mock_sql_executor:
        execute_str_mock = mock_sql_executor._conn.execute_string  # noqa: SLF001
        execute_str_mock.return_value = (
            None,
            mock_cursor(
                [
                    (
                        """\
                        {
                            "clientParamsInfo": [{
                                "name": "FEATURE_RELEASE_CHANNELS",
                                "value": ""
                            }]
                        }
                        """,
                    )
                ],
                [],
            ),
        )

        assert (
            sql_facade.get_ui_parameter(UIParameter.NA_FEATURE_RELEASE_CHANNELS, False)
            == ""
        )

        execute_str_mock.assert_called_once_with(expected_ui_params_query)


def test_get_ui_parameter_with_no_value_then_use_default(mock_cursor):
    with mock.patch.object(sql_facade, "_sql_executor") as mock_sql_executor:
        execute_str_mock = mock_sql_executor._conn.execute_string  # noqa: SLF001
        execute_str_mock.return_value = (
            None,
            mock_cursor(
                [
                    (
                        """\
                        {
                            "clientParamsInfo": []
                        }
                        """,
                    )
                ],
                [],
            ),
        )

        assert (
            sql_facade.get_ui_parameter(UIParameter.NA_FEATURE_RELEASE_CHANNELS, "any")
            == "any"
        )

        execute_str_mock.assert_called_once_with(expected_ui_params_query)


def test_show_release_directives_no_release_channel_specified(
    mock_execute_query, mock_cursor
):
    package_name = "test_package"
    expected_query = f"show release directives in application package {package_name}"
    mock_cursor_results = [
        {
            "name": "test_directive",
            "created_on": datetime(2021, 2, 1),
            "modified_on": datetime(2021, 4, 3),
            "version": "v1",
            "patch": 1,
        }
    ]
    mock_execute_query.side_effect = [mock_cursor(mock_cursor_results, [])]

    result = sql_facade.show_release_directives(package_name)

    assert result == mock_cursor_results
    mock_execute_query.assert_called_once_with(expected_query, cursor_class=DictCursor)


def test_show_release_directive_with_release_channel_specified(
    mock_execute_query, mock_cursor
):
    package_name = "test_package"
    release_channel = "test_channel"
    expected_query = f"show release directives in application package {package_name} for release channel {release_channel}"
    mock_cursor_results = [
        {
            "name": "test_directive",
            "created_on": datetime(2021, 2, 1),
            "modified_on": datetime(2021, 4, 3),
            "version": "v1",
            "patch": 1,
        }
    ]
    mock_execute_query.side_effect = [mock_cursor(mock_cursor_results, [])]

    result = sql_facade.show_release_directives(package_name, release_channel)

    assert result == mock_cursor_results
    mock_execute_query.assert_called_once_with(expected_query, cursor_class=DictCursor)


def test_show_release_directive_with_special_characters_in_names(
    mock_execute_query, mock_cursor
):
    package_name = "test.package"
    release_channel = "test.channel"
    expected_query = f'show release directives in application package "{package_name}" for release channel "{release_channel}"'
    mock_cursor_results = [
        {
            "name": "test_directive",
            "created_on": datetime(2021, 2, 1),
            "modified_on": datetime(2021, 4, 3),
            "version": "v1",
            "patch": 1,
        }
    ]
    mock_execute_query.side_effect = [mock_cursor(mock_cursor_results, [])]

    result = sql_facade.show_release_directives(package_name, release_channel)

    assert result == mock_cursor_results
    mock_execute_query.assert_called_once_with(expected_query, cursor_class=DictCursor)


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(),
            InvalidSQLError,
            "Failed to show release directives for application package test_package",
        ),
        (
            ProgrammingError(errno=INSUFFICIENT_PRIVILEGES),
            InsufficientPrivilegesError,
            "Insufficient privileges to show release directives for application package test_package",
        ),
        (
            ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
            UserInputError,
            "Application package test_package does not exist or you are not authorized to access it.",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to show release directives for application package test_package. some database error",
        ),
    ],
)
def test_show_release_directive_with_error(
    mock_execute_query, error_raised, error_caught, error_message
):
    package_name = "test_package"
    release_channel = "test_channel"
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.show_release_directives(package_name, release_channel)

    assert error_message in str(err)


@mock.patch(SQL_FACADE_GET_UI_PARAMETER, return_value=False)
def test_show_release_channels_when_feature_not_enabled(
    mock_get_ui_parameter, mock_execute_query, mock_cursor
):
    package_name = "test_package"

    result = sql_facade.show_release_channels(package_name)

    assert result == []
    mock_get_ui_parameter.assert_called_once_with(
        UIParameter.NA_FEATURE_RELEASE_CHANNELS, True
    )
    mock_execute_query.assert_not_called()


@mock.patch(SQL_FACADE_GET_UI_PARAMETER, return_value=True)
@pytest.mark.parametrize(
    "package_name, expected_used_package_name",
    [("test_package", "test_package"), ("test.package", '"test.package"')],
)
def test_show_release_channels_when_feature_enabled(
    mock_get_ui_parameter,
    mock_execute_query,
    mock_cursor,
    package_name,
    expected_used_package_name,
):

    expected_query = (
        f"show release channels in application package {expected_used_package_name}"
    )
    mock_cursor_results = [
        {
            "name": "test_channel",
            "description": "test_description",
            "versions": '["V1"]',
            "created_on": datetime(2021, 2, 1),
            "updated_on": datetime(2021, 4, 3),
            "targets": '{"accounts": ["org1.acc1", "org2.acc2"]}',
        }
    ]
    mock_execute_query.side_effect = [mock_cursor(mock_cursor_results, [])]

    result = sql_facade.show_release_channels(package_name)

    # assert result is same as mock_cursor_results except for keys "targets" and "versions":
    assert result == [
        {
            "name": "test_channel",
            "description": "test_description",
            "created_on": datetime(2021, 2, 1),
            "updated_on": datetime(2021, 4, 3),
            "targets": {"accounts": ["org1.acc1", "org2.acc2"]},
            "versions": ["V1"],
        }
    ]

    mock_get_ui_parameter.assert_called_once_with(
        UIParameter.NA_FEATURE_RELEASE_CHANNELS, True
    )
    mock_execute_query.assert_called_once_with(expected_query, cursor_class=DictCursor)


@mock.patch(SQL_FACADE_GET_UI_PARAMETER, return_value=True)
def test_show_release_channels_with_no_accounts_and_no_versions(
    mock_get_ui_parameter, mock_execute_query, mock_cursor
):
    package_name = "test_package"

    expected_query = f"show release channels in application package {package_name}"
    mock_cursor_results = [
        {
            "name": "test_channel",
            "description": "test_description",
            "created_on": datetime(2021, 2, 1),
            "updated_on": datetime(2021, 4, 3),
            "targets": "",
            "versions": "[]",
        }
    ]
    mock_execute_query.side_effect = [mock_cursor(mock_cursor_results, [])]

    result = sql_facade.show_release_channels(package_name)

    assert result == [
        {
            "name": "test_channel",
            "description": "test_description",
            "created_on": datetime(2021, 2, 1),
            "updated_on": datetime(2021, 4, 3),
            "targets": {},
            "versions": [],
        }
    ]

    mock_get_ui_parameter.assert_called_once_with(
        UIParameter.NA_FEATURE_RELEASE_CHANNELS, True
    )
    mock_execute_query.assert_called_once_with(expected_query, cursor_class=DictCursor)


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(),
            InvalidSQLError,
            "Failed to show release channels for application package test_package",
        ),
        (
            ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
            UserInputError,
            "Application package test_package does not exist or you are not authorized to access it.",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to show release channels for application package test_package. some database error",
        ),
    ],
)
@mock.patch(SQL_FACADE_GET_UI_PARAMETER, return_value=True)
def test_show_release_channels_when_error(
    mock_get_ui_parameter, mock_execute_query, error_raised, error_caught, error_message
):
    package_name = "test_package"

    expected_query = f"show release channels in application package {package_name}"
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.show_release_channels(package_name)

    assert error_message in str(err)
    mock_get_ui_parameter.assert_called_once_with(
        UIParameter.NA_FEATURE_RELEASE_CHANNELS, True
    )
    mock_execute_query.assert_called_once_with(expected_query, cursor_class=DictCursor)


def test_unset_release_directive_with_release_channel(
    mock_execute_query,
):
    package_name = "test_package"
    release_directive = "test_directive"
    release_channel = "test_channel"
    expected_query = f"alter application package {package_name} modify release channel {release_channel} unset release directive {release_directive}"

    sql_facade.unset_release_directive(package_name, release_directive, release_channel)

    mock_execute_query.assert_called_once_with(expected_query)


def test_unset_release_directive_for_default_channel(mock_execute_query):
    package_name = "test_package"
    release_directive = "test_directive"
    release_channel = "DEFAULT"
    expected_query = f"alter application package {package_name} modify release channel {release_channel} unset release directive {release_directive}"

    sql_facade.unset_release_directive(package_name, release_directive, release_channel)

    mock_execute_query.assert_called_once_with(expected_query)


def test_unset_release_directive_with_special_chars_in_names(mock_execute_query):
    package_name = "test.package"
    release_directive = "test.directive"
    release_channel = "test.channel"
    expected_query = f'alter application package "{package_name}" modify release channel "{release_channel}" unset release directive "{release_directive}"'

    sql_facade.unset_release_directive(package_name, release_directive, release_channel)

    mock_execute_query.assert_called_once_with(expected_query)


def test_unset_release_directive_without_release_channel(
    mock_execute_query,
):
    package_name = "test_package"
    release_directive = "test_directive"
    expected_query = f"alter application package {package_name} unset release directive {release_directive}"

    sql_facade.unset_release_directive(package_name, release_directive, None)

    mock_execute_query.assert_called_once_with(expected_query)


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=RELEASE_DIRECTIVE_DOES_NOT_EXIST),
            UserInputError,
            "Release directive test_directive does not exist in application package test_package.",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Failed to unset release directive test_directive for application package test_package.",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to unset release directive test_directive for application package test_package. some database error",
        ),
    ],
)
def test_unset_release_directive_with_error(
    mock_execute_query, error_raised, error_caught, error_message
):
    package_name = "test_package"
    release_directive = "test_directive"
    release_channel = "test_channel"
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.unset_release_directive(
            package_name, release_directive, release_channel
        )

    assert error_message in str(err)


def test_set_release_directive_with_non_default_directive(
    mock_execute_query,
):
    package_name = "test_package"
    release_directive = "test_directive"
    release_channel = "test_channel"
    version = "1.0.0"
    patch = 1
    target_accounts = ["account1"]
    expected_query = dedent(
        f"""\
            alter application package {package_name}
                modify release channel {release_channel}
                set release directive {release_directive}
                accounts = ({",".join(target_accounts)})
                version = "{version}" patch = {patch}
        """
    )

    sql_facade.set_release_directive(
        package_name,
        release_directive,
        release_channel,
        target_accounts,
        version,
        patch,
    )

    mock_execute_query.assert_called_once_with(expected_query)


def test_set_default_release_directive(
    mock_execute_query,
):
    package_name = "test_package"
    release_directive = "DEFAULT"
    release_channel = "test_channel"
    version = "1.0.0"
    patch = 1
    target_accounts = None
    expected_query = dedent(
        f"""\
            alter application package {package_name}
                modify release channel {release_channel}
                set default release directive
                version = "{version}" patch = {patch}
        """
    )

    sql_facade.set_release_directive(
        package_name,
        release_directive,
        release_channel,
        target_accounts,
        version,
        patch,
    )

    mock_execute_query.assert_called_once_with(expected_query)


def test_set_release_directive_with_special_chars_in_names(
    mock_execute_query,
):
    package_name = "test.package"
    release_directive = "test.directive"
    release_channel = "test.channel"
    version = "1.0.0"
    patch = 1
    target_accounts = ["account1"]
    expected_query = dedent(
        f"""\
            alter application package "{package_name}"
                modify release channel "{release_channel}"
                set release directive "{release_directive}"
                accounts = ({",".join(target_accounts)})
                version = "{version}" patch = {patch}
        """
    )

    sql_facade.set_release_directive(
        package_name,
        release_directive,
        release_channel,
        target_accounts,
        version,
        patch,
    )

    mock_execute_query.assert_called_once_with(expected_query)


def test_set_release_directive_no_release_channel(
    mock_execute_query,
):
    package_name = "test_package"
    release_directive = "test_directive"
    version = "1.0.0"
    patch = 1
    target_accounts = ["account1"]
    expected_query = dedent(
        f"""\
            alter application package {package_name}
                set release directive {release_directive}
                accounts = ({",".join(target_accounts)})
                version = "{version}" patch = {patch}
        """
    )

    sql_facade.set_release_directive(
        package_name,
        release_directive,
        None,
        target_accounts,
        version,
        patch,
    )

    mock_execute_query.assert_called_once_with(expected_query)


def test_set_default_release_directive_no_release_channel(
    mock_execute_query,
):
    package_name = "test_package"
    release_directive = "DEFAULT"
    version = "1.0.0"
    patch = 1
    target_accounts = None
    expected_query = dedent(
        f"""\
            alter application package {package_name}
                set default release directive
                version = "{version}" patch = {patch}
        """
    )

    sql_facade.set_release_directive(
        package_name,
        release_directive,
        None,
        target_accounts,
        version,
        patch,
    )

    mock_execute_query.assert_called_once_with(expected_query)


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=VERSION_NOT_ADDED_TO_RELEASE_CHANNEL),
            UserInputError,
            'Version "1.0.0" is not added to release channel test_channel. Please add it to the release channel first.',
        ),
        (
            ProgrammingError(errno=RELEASE_DIRECTIVES_VERSION_PATCH_NOT_FOUND),
            UserInputError,
            'Patch 1 for version "1.0.0" not found in application package test_package.',
        ),
        (
            ProgrammingError(errno=VERSION_DOES_NOT_EXIST),
            UserInputError,
            'Version "1.0.0" does not exist in application package test_package.',
        ),
        (
            ProgrammingError(errno=ACCOUNT_DOES_NOT_EXIST),
            UserInputError,
            "Invalid account passed in.",
        ),
        (
            ProgrammingError(errno=ACCOUNT_HAS_TOO_MANY_QUALIFIERS),
            UserInputError,
            "Invalid account passed in.",
        ),
        (
            ProgrammingError(errno=RELEASE_DIRECTIVE_DOES_NOT_EXIST),
            UserInputError,
            "Release directive test_directive does not exist in application package test_package.",
        ),
        (
            ProgrammingError(errno=TARGET_ACCOUNT_USED_BY_OTHER_RELEASE_DIRECTIVE),
            UserInputError,
            "Some target accounts are already referenced by other release directives in application package test_package.",
        ),
        (
            ProgrammingError(errno=RELEASE_DIRECTIVE_UNAPPROVED_VERSION_OR_PATCH),
            UserInputError,
            'Version "1.0.0", patch 1 has not yet been approved to release to accounts outside of this organization.',
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Failed to set release directive test_directive for application package test_package.",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to set release directive test_directive for application package test_package. some database error",
        ),
    ],
)
def test_set_release_directive_errors(
    mock_execute_query, error_raised, error_caught, error_message
):
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.set_release_directive(
            "test_package",
            "test_directive",
            "test_channel",
            ["account1"],
            "1.0.0",
            1,
            None,
        )

    assert error_message in str(err)


def test_modify_release_directive_with_non_default_directive(
    mock_execute_query,
):
    package_name = "test_package"
    release_directive = "test_directive"
    release_channel = "test_channel"
    version = "1.0.0"
    patch = 1
    expected_query = dedent(
        f"""\
            alter application package {package_name}
                modify release channel {release_channel}
                modify release directive {release_directive}
                version = "{version}" patch = {patch}
        """
    )

    sql_facade.set_release_directive(
        package_name=package_name,
        release_directive=release_directive,
        release_channel=release_channel,
        version=version,
        patch=patch,
        target_accounts=None,
    )

    mock_execute_query.assert_called_once_with(expected_query)


def test_modify_release_directive_with_special_chars_in_names(
    mock_execute_query,
):
    package_name = "test.package"
    release_directive = "test.directive"
    release_channel = "test.channel"
    version = "1.0.0"
    patch = 1
    expected_query = dedent(
        f"""\
            alter application package "{package_name}"
                modify release channel "{release_channel}"
                modify release directive "{release_directive}"
                version = "{version}" patch = {patch}
        """
    )

    sql_facade.set_release_directive(
        package_name=package_name,
        release_directive=release_directive,
        release_channel=release_channel,
        version=version,
        patch=patch,
        target_accounts=None,
    )

    mock_execute_query.assert_called_once_with(expected_query)


def test_modify_release_directive_no_release_channel(
    mock_execute_query,
):
    package_name = "test_package"
    release_directive = "test_directive"
    version = "1.0.0"
    patch = 1
    expected_query = dedent(
        f"""\
            alter application package {package_name}
                modify release directive {release_directive}
                version = "{version}" patch = {patch}
        """
    )

    sql_facade.set_release_directive(
        package_name=package_name,
        release_directive=release_directive,
        release_channel=None,
        version=version,
        patch=patch,
        target_accounts=None,
    )

    mock_execute_query.assert_called_once_with(expected_query)


def test_add_accounts_to_release_directive_with_non_default_directive(
    mock_execute_query,
):
    package_name = "test_package"
    release_directive = "test_directive"
    release_channel = "test_channel"
    target_accounts = ["org1.account1", "org2.account2"]

    expected_query = dedent(
        f"""\
            alter application package {package_name}
                modify release channel {release_channel}
                modify release directive {release_directive}
                add accounts = ({",".join(target_accounts)})
        """
    )

    sql_facade.add_accounts_to_release_directive(
        package_name=package_name,
        release_directive=release_directive,
        release_channel=release_channel,
        target_accounts=target_accounts,
    )

    mock_execute_query.assert_called_once_with(expected_query)


def test_add_accounts_to_default_release_directive_then_error(
    mock_execute_query,
):
    package_name = "test_package"
    release_directive = "DEFAULT"
    release_channel = "test_channel"
    target_accounts = ["org1.account1", "org2.account2"]

    with pytest.raises(UserInputError) as err:
        sql_facade.add_accounts_to_release_directive(
            package_name=package_name,
            release_directive=release_directive,
            release_channel=release_channel,
            target_accounts=target_accounts,
        )

    assert (
        "Default release directive does not support adding accounts. Please specify a non-default release directive."
        in str(err)
    )


def test_add_accounts_to_release_directive_no_release_channel(
    mock_execute_query,
):
    package_name = "test_package"
    release_directive = "test_directive"
    target_accounts = ["org1.account1", "org2.account2"]

    expected_query = dedent(
        f"""\
            alter application package {package_name}
                modify release directive {release_directive}
                add accounts = ({",".join(target_accounts)})
        """
    )

    sql_facade.add_accounts_to_release_directive(
        package_name=package_name,
        release_directive=release_directive,
        release_channel=None,
        target_accounts=target_accounts,
    )

    mock_execute_query.assert_called_once_with(expected_query)


def test_add_accounts_to_release_directive_with_special_chars_in_names(
    mock_execute_query,
):
    package_name = "test.package"
    release_directive = "test.directive"
    release_channel = "test.channel"
    target_accounts = ["org1.account1", "org2.account2"]

    expected_query = dedent(
        f"""\
            alter application package "{package_name}"
                modify release channel "{release_channel}"
                modify release directive "{release_directive}"
                add accounts = ({",".join(target_accounts)})
        """
    )

    sql_facade.add_accounts_to_release_directive(
        package_name=package_name,
        release_directive=release_directive,
        release_channel=release_channel,
        target_accounts=target_accounts,
    )

    mock_execute_query.assert_called_once_with(expected_query)


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=ACCOUNT_DOES_NOT_EXIST),
            UserInputError,
            "Invalid account passed in.",
        ),
        (
            ProgrammingError(errno=ACCOUNT_HAS_TOO_MANY_QUALIFIERS),
            UserInputError,
            "Invalid account passed in.",
        ),
        (
            ProgrammingError(errno=RELEASE_DIRECTIVE_DOES_NOT_EXIST),
            UserInputError,
            "Release directive test_directive does not exist in application package test_package.",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Failed to add accounts to release directive test_directive for application package test_package.",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to add accounts to release directive test_directive for application package test_package. some database error",
        ),
    ],
)
def test_add_accounts_to_release_directive_errors(
    mock_execute_query, error_raised, error_caught, error_message
):
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.add_accounts_to_release_directive(
            "test_package",
            "test_directive",
            "test_channel",
            ["account1"],
        )

    assert error_message in str(err)


def test_remove_accounts_from_release_directive_with_non_default_directive(
    mock_execute_query,
):
    package_name = "test_package"
    release_directive = "test_directive"
    release_channel = "test_channel"
    target_accounts = ["org1.account1", "org2.account2"]

    expected_query = dedent(
        f"""\
            alter application package {package_name}
                modify release channel {release_channel}
                modify release directive {release_directive}
                remove accounts = ({",".join(target_accounts)})
        """
    )

    sql_facade.remove_accounts_from_release_directive(
        package_name=package_name,
        release_directive=release_directive,
        release_channel=release_channel,
        target_accounts=target_accounts,
    )

    mock_execute_query.assert_called_once_with(expected_query)


def test_remove_accounts_from_default_release_directive_then_error(
    mock_execute_query,
):
    package_name = "test_package"
    release_directive = "DEFAULT"
    release_channel = "test_channel"
    target_accounts = ["org1.account1", "org2.account2"]

    with pytest.raises(UserInputError) as err:
        sql_facade.remove_accounts_from_release_directive(
            package_name=package_name,
            release_directive=release_directive,
            release_channel=release_channel,
            target_accounts=target_accounts,
        )

    assert (
        "Default release directive does not support removing accounts. Please specify a non-default release directive."
        in str(err)
    )


def test_remove_accounts_from_release_directive_no_release_channel(
    mock_execute_query,
):
    package_name = "test_package"
    release_directive = "test_directive"
    target_accounts = ["org1.account1", "org2.account2"]

    expected_query = dedent(
        f"""\
            alter application package {package_name}
                modify release directive {release_directive}
                remove accounts = ({",".join(target_accounts)})
        """
    )

    sql_facade.remove_accounts_from_release_directive(
        package_name=package_name,
        release_directive=release_directive,
        release_channel=None,
        target_accounts=target_accounts,
    )

    mock_execute_query.assert_called_once_with(expected_query)


def test_remove_accounts_from_release_directive_with_special_chars_in_names(
    mock_execute_query,
):
    package_name = "test.package"
    release_directive = "test.directive"
    release_channel = "test.channel"
    target_accounts = ["org1.account1", "org2.account2"]

    expected_query = dedent(
        f"""\
            alter application package "{package_name}"
                modify release channel "{release_channel}"
                modify release directive "{release_directive}"
                remove accounts = ({",".join(target_accounts)})
        """
    )

    sql_facade.remove_accounts_from_release_directive(
        package_name=package_name,
        release_directive=release_directive,
        release_channel=release_channel,
        target_accounts=target_accounts,
    )

    mock_execute_query.assert_called_once_with(expected_query)


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=ACCOUNT_DOES_NOT_EXIST),
            UserInputError,
            "Invalid account passed in.",
        ),
        (
            ProgrammingError(errno=ACCOUNT_HAS_TOO_MANY_QUALIFIERS),
            UserInputError,
            "Invalid account passed in.",
        ),
        (
            ProgrammingError(errno=RELEASE_DIRECTIVE_DOES_NOT_EXIST),
            UserInputError,
            "Release directive test_directive does not exist in application package test_package.",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Failed to remove accounts from release directive test_directive for application package test_package.",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to remove accounts from release directive test_directive for application package test_package. some database error",
        ),
    ],
)
def test_remove_accounts_from_release_directive_errors(
    mock_execute_query, error_raised, error_caught, error_message
):
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.remove_accounts_from_release_directive(
            "test_package",
            "test_directive",
            "test_channel",
            ["account1"],
        )

    assert error_message in str(err)


@contextmanager
def mock_release_channels(facade, enabled):
    with mock.patch.object(
        facade, "show_release_channels"
    ) as mock_show_release_channels:
        mock_show_release_channels.return_value = (
            [{"name": "test_channel"}] if enabled else []
        )
        yield


@pytest.mark.parametrize("release_channels_enabled", [True, False])
def test_create_version_in_package(
    release_channels_enabled, mock_use_role, mock_execute_query
):
    action = "register" if release_channels_enabled else "add"
    package_name = "test_package"
    version = "v1"
    role = "test_role"
    stage_fqn = f"{package_name}.app_src.stage"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                dedent(
                    f"""\
                        alter application package {package_name}
                            {action} version {version}
                            using @{stage_fqn}
                    """
                )
            ),
        ),
    ]

    with mock_release_channels(sql_facade, release_channels_enabled):
        with assert_in_context(expected_use_objects, expected_execute_query):
            sql_facade.create_version_in_package(
                package_name=package_name,
                version=version,
                role=role,
                path_to_version_directory=stage_fqn,
            )


@pytest.mark.parametrize("release_channels_enabled", [True, False])
@pytest.mark.parametrize("label", ["test_label", ""])
def test_create_version_in_package_with_label(
    label, release_channels_enabled, mock_use_role, mock_execute_query
):
    action = "register" if release_channels_enabled else "add"
    package_name = "test_package"
    version = "v1"
    role = "test_role"
    stage_fqn = f"{package_name}.app_src.stage"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                dedent(
                    f"""\
                        alter application package {package_name}
                            {action} version {version}
                            using @{stage_fqn}
                            label='{label}'
                    """
                )
            ),
        ),
    ]

    with mock_release_channels(sql_facade, release_channels_enabled):
        with assert_in_context(expected_use_objects, expected_execute_query):
            sql_facade.create_version_in_package(
                package_name=package_name,
                version=version,
                role=role,
                path_to_version_directory=stage_fqn,
                label=label,
            )


@pytest.mark.parametrize("release_channels_enabled", [True, False])
def test_create_version_with_special_characters(
    release_channels_enabled, mock_use_role, mock_execute_query
):
    action = "register" if release_channels_enabled else "add"
    package_name = "test.package"
    version = "v1.0"
    role = "test_role"
    stage_fqn = f"{package_name}.app_src.stage"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                dedent(
                    f"""\
                        alter application package "{package_name}"
                            {action} version "{version}"
                            using @{stage_fqn}
                    """
                )
            ),
        ),
    ]

    with mock_release_channels(sql_facade, release_channels_enabled):
        with assert_in_context(expected_use_objects, expected_execute_query):
            sql_facade.create_version_in_package(
                package_name=package_name,
                version=version,
                role=role,
                path_to_version_directory=stage_fqn,
            )


@pytest.mark.parametrize("release_channels_enabled", [True, False])
@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=MAX_UNBOUND_VERSIONS_REACHED),
            UserInputError,
            "Maximum unbound versions reached for application package test_package. "
            "Please drop other unbound versions first, or add them to a release channel. "
            "Use `snow app version list` to view all versions.",
        ),
        (
            ProgrammingError(errno=APPLICATION_PACKAGE_MAX_VERSIONS_HIT),
            UserInputError,
            "Maximum versions reached for application package test_package. "
            "Please drop the other versions first.",
        ),
        (
            ProgrammingError(errno=CANNOT_CREATE_VERSION_WITH_NON_ZERO_PATCH),
            UserInputError,
            "Cannot create a new version with a non-zero patch in the manifest file.",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Failed to ACTION_PLACEHOLDER version v1 to application package test_package.",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to ACTION_PLACEHOLDER version v1 to application package test_package. some database error",
        ),
    ],
)
def test_create_version_in_package_with_error(
    release_channels_enabled,
    mock_use_role,
    mock_execute_query,
    error_raised,
    error_caught,
    error_message,
):
    package_name = "test_package"
    version = "v1"
    role = "test_role"
    stage_fqn = f"{package_name}.app_src.stage"

    action_placeholder = "register" if release_channels_enabled else "add"
    error_message = error_message.replace("ACTION_PLACEHOLDER", action_placeholder)

    mock_execute_query.side_effect = error_raised

    with mock_release_channels(sql_facade, release_channels_enabled):
        with pytest.raises(error_caught) as err:
            sql_facade.create_version_in_package(
                package_name=package_name,
                version=version,
                role=role,
                path_to_version_directory=stage_fqn,
            )
        assert error_message in str(err)


@pytest.mark.parametrize("release_channels_enabled", [True, False])
def test_drop_version_from_package(
    release_channels_enabled, mock_use_role, mock_execute_query
):
    action = "deregister" if release_channels_enabled else "drop"
    package_name = "test_package"
    version = "v1"
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                f"alter application package {package_name} {action} version {version}"
            ),
        ),
    ]

    with mock_release_channels(sql_facade, release_channels_enabled):
        with assert_in_context(expected_use_objects, expected_execute_query):
            sql_facade.drop_version_from_package(
                package_name=package_name, version=version, role=role
            )


@pytest.mark.parametrize("release_channels_enabled", [True, False])
def test_drop_version_from_package_with_special_characters(
    release_channels_enabled, mock_use_role, mock_execute_query
):
    action = "deregister" if release_channels_enabled else "drop"
    package_name = "test.package"
    version = "v1.0"
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                f'alter application package "{package_name}" {action} version "{version}"'
            ),
        ),
    ]

    with mock_release_channels(sql_facade, release_channels_enabled):
        with assert_in_context(expected_use_objects, expected_execute_query):
            sql_facade.drop_version_from_package(
                package_name=package_name, version=version, role=role
            )


@pytest.mark.parametrize("release_channels_enabled", [True, False])
@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=VERSION_REFERENCED_BY_RELEASE_DIRECTIVE),
            UserInputError,
            "Cannot drop version v1 from application package test_package because it is in use by one or more release directives.",
        ),
        (
            ProgrammingError(errno=CANNOT_DEREGISTER_VERSION_ASSOCIATED_WITH_CHANNEL),
            UserInputError,
            "Cannot drop version v1 from application package test_package because it is associated with a release channel.",
        ),
        (
            ProgrammingError(errno=VERSION_DOES_NOT_EXIST),
            UserInputError,
            "Version v1 does not exist in application package test_package.",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Failed to ACTION_PLACEHOLDER version v1 from application package test_package.",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to ACTION_PLACEHOLDER version v1 from application package test_package. some database error",
        ),
    ],
)
def test_drop_version_from_package_with_error(
    release_channels_enabled,
    mock_use_role,
    mock_execute_query,
    error_raised,
    error_caught,
    error_message,
):
    package_name = "test_package"
    version = "v1"
    role = "test_role"

    mock_execute_query.side_effect = error_raised
    action_placeholder = "deregister" if release_channels_enabled else "drop"
    error_message = error_message.replace("ACTION_PLACEHOLDER", action_placeholder)

    with mock_release_channels(sql_facade, release_channels_enabled):

        with pytest.raises(error_caught) as err:
            sql_facade.drop_version_from_package(
                package_name=package_name, version=version, role=role
            )
        assert error_message in str(err)


def test_add_patch_to_package_version_valid_input_then_success(
    mock_use_role, mock_execute_query, mock_cursor
):
    package_name = "test_package"
    version = "v1"
    patch = 1
    stage_fqn = "src.stage"

    expected_query = dedent(
        f"""\
            alter application package test_package
                add patch 1 for version v1
                using @src.stage
        """
    )

    mock_execute_query.side_effect = [mock_cursor([{"patch": 1}], [])]
    result = sql_facade.add_patch_to_package_version(
        package_name=package_name,
        path_to_version_directory=stage_fqn,
        version=version,
        patch=patch,
    )

    assert result == patch
    mock_execute_query.assert_called_once_with(expected_query, cursor_class=DictCursor)


# patch 0 shouldn't be treated in a special way (rely on backend saying 0 already exists)
def test_add_patch_to_package_version_valid_input_then_success_patch_0(
    mock_use_role, mock_execute_query, mock_cursor
):
    package_name = "test_package"
    version = "v1"
    patch = 0
    stage_fqn = "src.stage"

    expected_query = dedent(
        """\
            alter application package test_package
                add patch 0 for version v1
                using @src.stage
        """
    )

    mock_execute_query.side_effect = [mock_cursor([{"patch": 0}], [])]
    result = sql_facade.add_patch_to_package_version(
        package_name=package_name,
        path_to_version_directory=stage_fqn,
        version=version,
        patch=patch,
    )

    assert result == patch
    mock_execute_query.assert_called_once_with(expected_query, cursor_class=DictCursor)


def test_add_patch_to_package_version_valid_input_then_success_no_patch_in_input(
    mock_use_role, mock_execute_query, mock_cursor
):
    package_name = "test_package"
    version = "v1"
    stage_fqn = "src.stage"

    expected_query = dedent(
        """\
            alter application package test_package
                add patch for version v1
                using @src.stage
        """
    )

    mock_execute_query.side_effect = [mock_cursor([{"patch": 5}], [])]
    result = sql_facade.add_patch_to_package_version(
        package_name=package_name,
        path_to_version_directory=stage_fqn,
        version=version,
        patch=None,
    )

    assert result == 5
    mock_execute_query.assert_called_once_with(expected_query, cursor_class=DictCursor)


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=APPLICATION_PACKAGE_PATCH_ALREADY_EXISTS),
            UserInputError,
            "Patch 1 already exists for version v1 in application package test_package.",
        ),
        (
            ProgrammingError(errno=CANNOT_ADD_PATCH_WITH_NON_INCREASING_PATCH_NUMBER),
            UserInputError,
            "Cannot add a patch with a non-increasing patch number to version v1 in application package test_package.",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Failed to create patch 1 for version v1 in application package test_package.",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to create patch 1 for version v1 in application package test_package. some database error",
        ),
    ],
)
def test_add_patch_to_package_version_with_error(
    mock_use_role, mock_execute_query, error_raised, error_caught, error_message
):
    package_name = "test_package"
    version = "v1"
    patch = 1
    stage_fqn = "src.stage"

    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.add_patch_to_package_version(
            package_name=package_name,
            path_to_version_directory=stage_fqn,
            version=version,
            patch=patch,
        )
    assert error_message in str(err)


def test_add_patch_to_package_with_patch_already_exist_error_and_null_patch(
    mock_use_role, mock_execute_query
):
    # In the case where the patch is None but we still get patch already exist error,
    # this would be caused by a hard-coded patch value in the manifest
    package_name = "test_package"
    version = "v1"
    patch = None
    stage_fqn = "src.stage"

    mock_execute_query.side_effect = ProgrammingError(
        errno=APPLICATION_PACKAGE_PATCH_ALREADY_EXISTS
    )

    with pytest.raises(UserInputError) as err:
        sql_facade.add_patch_to_package_version(
            package_name=package_name,
            path_to_version_directory=stage_fqn,
            version=version,
            patch=patch,
        )
    assert "Patch already exists for version v1 in application package test_package. Check the manifest file for any harded-coded patch value."


def test_add_accounts_to_release_channel_valid_input_then_success(
    mock_use_role, mock_execute_query
):
    package_name = "test_package"
    release_channel = "test_channel"
    accounts = ["org1.acc1", "org2.acc2"]
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                "alter application package test_package modify release channel test_channel add accounts = (org1.acc1,org2.acc2)"
            ),
        ),
    ]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.add_accounts_to_release_channel(
            package_name, release_channel, accounts, role
        )


def test_add_accounts_to_release_channel_with_special_chars_in_names(
    mock_use_role, mock_execute_query
):
    package_name = "test.package"
    release_channel = "test.channel"
    accounts = ["org1.acc1", "org2.acc2"]
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                'alter application package "test.package" modify release channel "test.channel" add accounts = (org1.acc1,org2.acc2)'
            ),
        ),
    ]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.add_accounts_to_release_channel(
            package_name, release_channel, accounts, role
        )


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=ACCOUNT_DOES_NOT_EXIST),
            UserInputError,
            "Invalid account passed in.",
        ),
        (
            ProgrammingError(errno=ACCOUNT_HAS_TOO_MANY_QUALIFIERS),
            UserInputError,
            "Invalid account passed in.",
        ),
        (
            ProgrammingError(errno=CANNOT_MODIFY_RELEASE_CHANNEL_ACCOUNTS),
            UserInputError,
            "Cannot modify accounts for release channel test_channel in application package test_package.",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Failed to add accounts to release channel test_channel in application package test_package.",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to add accounts to release channel test_channel in application package test_package. some database error",
        ),
    ],
)
def test_add_accounts_to_release_channel_error(
    mock_execute_query, error_raised, error_caught, error_message, mock_use_role
):
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.add_accounts_to_release_channel(
            "test_package", "test_channel", ["org1.acc1"], "test_role"
        )

    assert error_message in str(err)


def test_remove_accounts_from_release_channel_valid_input_then_success(
    mock_use_role, mock_execute_query
):
    package_name = "test_package"
    release_channel = "test_channel"
    accounts = ["org1.acc1", "org2.acc2"]
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                "alter application package test_package modify release channel test_channel remove accounts = (org1.acc1,org2.acc2)"
            ),
        ),
    ]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.remove_accounts_from_release_channel(
            package_name, release_channel, accounts, role
        )


def test_remove_accounts_from_release_channel_with_special_chars_in_names(
    mock_use_role, mock_execute_query
):
    package_name = "test.package"
    release_channel = "test.channel"
    accounts = ["org1.acc1", "org2.acc2"]
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                'alter application package "test.package" modify release channel "test.channel" remove accounts = (org1.acc1,org2.acc2)'
            ),
        ),
    ]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.remove_accounts_from_release_channel(
            package_name, release_channel, accounts, role
        )


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=ACCOUNT_DOES_NOT_EXIST),
            UserInputError,
            "Invalid account passed in.",
        ),
        (
            ProgrammingError(errno=ACCOUNT_HAS_TOO_MANY_QUALIFIERS),
            UserInputError,
            "Invalid account passed in.",
        ),
        (
            ProgrammingError(errno=CANNOT_MODIFY_RELEASE_CHANNEL_ACCOUNTS),
            UserInputError,
            "Cannot modify accounts for release channel test_channel in application package test_package.",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Failed to remove accounts from release channel test_channel in application package test_package.",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to remove accounts from release channel test_channel in application package test_package. some database error",
        ),
    ],
)
def test_remove_accounts_from_release_channel_error(
    mock_execute_query, error_raised, error_caught, error_message, mock_use_role
):
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.remove_accounts_from_release_channel(
            "test_package", "test_channel", ["org1.acc1"], "test_role"
        )

    assert error_message in str(err)


def test_set_accounts_for_release_channel_valid_input_then_success(
    mock_use_role, mock_execute_query
):
    package_name = "test_package"
    release_channel = "test_channel"
    accounts = ["org1.acc1", "org2.acc2"]
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                "alter application package test_package modify release channel test_channel set accounts = (org1.acc1,org2.acc2)"
            ),
        ),
    ]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.set_accounts_for_release_channel(
            package_name, release_channel, accounts, role
        )


def test_set_accounts_for_release_channel_with_special_chars_in_names(
    mock_use_role, mock_execute_query
):
    package_name = "test.package"
    release_channel = "test.channel"
    accounts = ["org1.acc1", "org2.acc2"]
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                'alter application package "test.package" modify release channel "test.channel" set accounts = (org1.acc1,org2.acc2)'
            ),
        ),
    ]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.set_accounts_for_release_channel(
            package_name, release_channel, accounts, role
        )


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=ACCOUNT_DOES_NOT_EXIST),
            UserInputError,
            "Invalid account passed in.",
        ),
        (
            ProgrammingError(errno=ACCOUNT_HAS_TOO_MANY_QUALIFIERS),
            UserInputError,
            "Invalid account passed in.",
        ),
        (
            ProgrammingError(errno=CANNOT_MODIFY_RELEASE_CHANNEL_ACCOUNTS),
            UserInputError,
            "Cannot modify accounts for release channel test_channel in application package test_package.",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Failed to set accounts for release channel test_channel in application package test_package.",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to set accounts for release channel test_channel in application package test_package. some database error",
        ),
    ],
)
def test_set_accounts_for_release_channel_error(
    mock_execute_query, error_raised, error_caught, error_message, mock_use_role
):
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.set_accounts_for_release_channel(
            "test_package", "test_channel", ["org1.acc1"], "test_role"
        )

    assert error_message in str(err)


def test_add_version_to_release_channel_valid_input_then_success(
    mock_use_role, mock_execute_query
):
    package_name = "test_package"
    release_channel = "test_channel"
    version = "v1"
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                f"alter application package {package_name} modify release channel {release_channel} add version {version}"
            ),
        ),
    ]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.add_version_to_release_channel(
            package_name, release_channel, version, role
        )


def test_add_version_to_release_channel_with_special_chars_in_names(
    mock_use_role, mock_execute_query
):
    package_name = "test.package"
    release_channel = "test.channel"
    version = "v1.0"
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                f'alter application package "{package_name}" modify release channel "{release_channel}" add version "{version}"'
            ),
        ),
    ]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.add_version_to_release_channel(
            package_name, release_channel, version, role
        )


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=VERSION_DOES_NOT_EXIST),
            UserInputError,
            "Version v1 does not exist in application package test_package.",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Failed to add version v1 to release channel test_channel in application package test_package.",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to add version v1 to release channel test_channel in application package test_package. some database error",
        ),
    ],
)
def test_add_version_to_release_channel_error(
    mock_execute_query, error_raised, error_caught, error_message, mock_use_role
):
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.add_version_to_release_channel(
            "test_package", "test_channel", "v1", "test_role"
        )

    assert error_message in str(err)


def test_remove_version_from_release_channel_valid_input_then_success(
    mock_use_role, mock_execute_query
):
    package_name = "test_package"
    release_channel = "test_channel"
    version = "v1"
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                f"alter application package {package_name} modify release channel {release_channel} drop version {version}"
            ),
        ),
    ]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.remove_version_from_release_channel(
            package_name, release_channel, version, role
        )


def test_remove_version_from_release_channel_with_special_chars_in_names(
    mock_use_role, mock_execute_query
):
    package_name = "test.package"
    release_channel = "test.channel"
    version = "v1.0"
    role = "test_role"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_execute_query = [
        (
            mock_execute_query,
            mock.call(
                f'alter application package "{package_name}" modify release channel "{release_channel}" drop version "{version}"'
            ),
        ),
    ]

    with assert_in_context(expected_use_objects, expected_execute_query):
        sql_facade.remove_version_from_release_channel(
            package_name, release_channel, version, role
        )


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(errno=VERSION_NOT_IN_RELEASE_CHANNEL),
            UserInputError,
            "Version v1 is not found in release channel test_channel.",
        ),
        (
            ProgrammingError(),
            InvalidSQLError,
            "Failed to remove version v1 from release channel test_channel in application package test_package.",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to remove version v1 from release channel test_channel in application package test_package. some database error",
        ),
    ],
)
def test_remove_version_from_release_channel_error(
    mock_execute_query, error_raised, error_caught, error_message, mock_use_role
):
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.remove_version_from_release_channel(
            "test_package", "test_channel", "v1", "test_role"
        )

    assert error_message in str(err)


def test_get_versions_valid_input_then_success(
    mock_execute_query, mock_use_role, mock_cursor
):
    package_name = "test_package"
    role = "test_role"
    expected_query = f"show versions in application package {package_name}"

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_value = [
        {"version": "v1", "patch": 0, "created_on": datetime(2021, 2, 1)},
        {"version": "v2", "patch": 1, "created_on": datetime(2021, 2, 2)},
    ]

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor(expected_value, []),
                mock.call(expected_query, cursor_class=DictCursor),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with assert_in_context(expected_use_objects, expected_execute_query):
        result = sql_facade.show_versions(package_name, role)

    assert result == expected_value


def test_get_versions_with_special_chars_in_names(
    mock_execute_query, mock_use_role, mock_cursor
):
    package_name = "test.package"
    role = "test_role"
    expected_query = f'show versions in application package "{package_name}"'

    expected_use_objects = [
        (mock_use_role, mock.call(role)),
    ]
    expected_value = [
        {"version": "v1", "patch": 0, "created_on": datetime(2021, 2, 1)},
        {"version": "v2", "patch": 1, "created_on": datetime(2021, 2, 2)},
    ]

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor(expected_value, []),
                mock.call(expected_query, cursor_class=DictCursor),
            )
        ]
    )
    mock_execute_query.side_effect = side_effects
    expected_execute_query = [(mock_execute_query, call) for call in expected]

    with assert_in_context(expected_use_objects, expected_execute_query):
        result = sql_facade.show_versions(package_name, role)

    assert result == expected_value


@pytest.mark.parametrize(
    "error_raised, error_caught, error_message",
    [
        (
            ProgrammingError(),
            InvalidSQLError,
            "Failed to show versions for application package test_package.",
        ),
        (
            DatabaseError("some database error"),
            UnknownSQLError,
            "Unknown SQL error occurred. Failed to show versions for application package test_package. some database error",
        ),
        (
            ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
            UserInputError,
            "Application package test_package does not exist or you are not authorized to access it.",
        ),
    ],
)
def test_get_versions_error(
    mock_execute_query, error_raised, error_caught, error_message, mock_use_role
):
    mock_execute_query.side_effect = error_raised

    with pytest.raises(error_caught) as err:
        sql_facade.show_versions("test_package", "test_role")

    assert error_message in str(err)
