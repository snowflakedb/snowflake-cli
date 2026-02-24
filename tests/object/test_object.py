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
from tempfile import NamedTemporaryFile
from unittest import mock

import pytest
from click import ClickException
from snowflake.cli._plugins.object.commands import _scope_validate
from snowflake.cli.api.constants import OBJECT_TO_NAMES, SUPPORTED_OBJECTS

from tests.testing_utils.result_assertions import assert_that_result_is_usage_error


@mock.patch("snowflake.connector.connect")
@pytest.mark.parametrize(
    "object_type, expected",
    [
        ("compute-pool", "compute pools"),
        ("network-rule", "network rules"),
        ("database", "databases"),
        ("function", "functions"),
        # ("job", "jobs"),
        ("procedure", "procedures"),
        ("role", "roles"),
        ("schema", "schemas"),
        ("service", "services"),
        ("secret", "secrets"),
        ("stage", "stages"),
        ("stream", "streams"),
        ("streamlit", "streamlits"),
        ("table", "tables"),
        ("task", "tasks"),
        ("user", "users"),
        ("warehouse", "warehouses"),
        ("view", "views"),
        ("image-repository", "image repositories"),
        ("git-repository", "git repositories"),
        ("notebook", "notebooks"),
    ],
)
def test_show(
    mock_connector,
    object_type,
    expected,
    mock_cursor,
    runner,
    os_agnostic_snapshot,
    mock_ctx,
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["object", "list", object_type], catch_exceptions=False)
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [f"show {expected} like '%%'"]


DESCRIBE_TEST_OBJECTS = [
    ("compute-pool", "compute_pool_example"),
    ("network-rule", "network_rule_example"),
    ("integration", "integration_example"),
    ("database", "database_example"),
    ("function", "function_example"),
    # ("job", "job_example"),
    ("procedure", "procedure_example"),
    ("role", "role_example"),
    ("schema", "schema_example"),
    ("service", "service_example"),
    ("secret", "secret_example"),
    ("stage", "stage_example"),
    ("stream", "stream_example"),
    ("streamlit", "streamlit_example"),
    ("table", "table_example"),
    ("task", "task_example"),
    ("user", "user_example"),
    ("warehouse", "warehouse_example"),
    ("view", "view_example"),
    ("git-repository", "git_repository_example"),
    ("notebook", "notebook_example"),
]


@mock.patch("snowflake.connector.connect")
@pytest.mark.parametrize(
    "object_type, input_scope, input_name",
    [
        ("schema", "database", "test_db"),
        ("table", "schema", "test_schema"),
        ("service", "compute-pool", "test_pool"),
    ],
)
def test_show_with_scope(
    mock_connector, object_type, input_scope, input_name, runner, mock_ctx
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        ["object", "list", object_type, "--in", input_scope, input_name]
    )
    obj = OBJECT_TO_NAMES[object_type]
    scope_obj = OBJECT_TO_NAMES[input_scope]
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        f"show {obj.sf_plural_name} like '%%' in {scope_obj.sf_name} {input_name}"
    ]


@mock.patch("snowflake.connector.connect")
@pytest.mark.parametrize(
    "object_type, input_scope, input_name, expected",
    [
        (
            "table",
            "invalid_scope",
            "name",
            "Scope type must be one of the following",
        ),  # invalid scope label
        (
            "table",
            "database",
            "invalid name",
            "Scope name must be a valid identifier.",
        ),  # invalid scope identifier
    ],
)
def test_show_with_invalid_scope(
    mock_connector, object_type, input_scope, input_name, expected, runner
):
    result = runner.invoke(
        ["object", "list", object_type, "--in", input_scope, input_name]
    )
    assert expected in result.output


@pytest.mark.parametrize(
    "object_type, input_scope, input_name",
    [
        ("user", None, None),
        ("schema", "database", "test_db"),
        ("table", "schema", "test_schema"),
        ("service", "compute-pool", "test_pool"),
    ],
)
def test_scope_validate(object_type, input_scope, input_name):
    _scope_validate(object_type, (input_scope, input_name))


@pytest.mark.parametrize(
    "object_type, input_scope, input_name, expected_msg",
    [
        (
            "table",
            "database",
            "invalid identifier",
            "Scope name must be a valid identifier.",
        ),
        (
            "table",
            "invalid-scope",
            "identifier",
            "Scope type must be one of the following",
        ),
        (
            "table",
            "compute-pool",
            "test_pool",
            "compute-pool scope is only supported for listing service.",
        ),  # 'compute-pool' scope can only be used with 'service'
    ],
)
def test_invalid_scope_validate(object_type, input_scope, input_name, expected_msg):
    with pytest.raises(ClickException) as exc:
        _scope_validate(object_type, (input_scope, input_name))
    assert expected_msg in exc.value.message


@mock.patch("snowflake.connector")
@pytest.mark.parametrize("object_type, object_name", DESCRIBE_TEST_OBJECTS)
def test_describe(
    mock_connector, object_type, object_name, mock_cursor, runner, os_agnostic_snapshot
):
    mock_connector.connect.return_value.execute_stream.return_value = (
        None,
        mock_cursor(
            rows=[("ID", "NUMBER(38,0", "COLUMN"), ("NAME", "VARCHAR(100", "COLUMN")],
            columns=["name", "type", "kind"],
        ),
    )
    result = runner.invoke(["object", "describe", object_type, object_name])
    assert result.exit_code == 0, result.output
    assert result.output == os_agnostic_snapshot


@mock.patch("snowflake.connector")
def test_describe_fails_image_repository(mock_cursor, runner, os_agnostic_snapshot):
    result = runner.invoke(["object", "describe", "image-repository", "test_repo"])
    assert result.exit_code == 1, result.output
    assert result.output == os_agnostic_snapshot


DROP_TEST_OBJECTS = [
    *DESCRIBE_TEST_OBJECTS,
    ("image-repository", "image_repository_example"),
]


@mock.patch("snowflake.connector")
@pytest.mark.parametrize(
    "object_type, object_name",
    DROP_TEST_OBJECTS,
)
def test_drop(
    mock_connector, object_type, object_name, mock_cursor, runner, os_agnostic_snapshot
):
    mock_connector.connect.return_value.execute_stream.return_value = (
        None,
        mock_cursor(
            rows=[(f"{object_name} successfully dropped.",)],
            columns=["status"],
        ),
    )

    result = runner.invoke(["object", "drop", object_type, object_name])
    assert result.exit_code == 0, result.output
    assert result.output == os_agnostic_snapshot


@pytest.mark.parametrize("command", ["list", "drop", "describe"])
def test_that_objects_list_is_in_help(command, runner):
    result = runner.invoke(["object", command, "--help"])
    for obj in SUPPORTED_OBJECTS:
        if command == "describe" and obj == "image-repository":
            assert obj not in result.output, f"{obj} should not be in help message"
        else:
            assert obj in result.output, f"{obj} in help message"


@pytest.mark.parametrize(
    "command,expect_argument_exception",
    [
        (["object", "drop"], "OBJECT_TYPE"),
        (["object", "drop", "function"], "OBJECT_NAME"),
        (["object", "list"], "OBJECT_TYPE"),
        (["object", "describe"], "OBJECT_TYPE"),
        (["object", "describe", "function"], "OBJECT_NAME"),
    ],
)
def test_throw_exception_because_of_missing_arguments(
    runner, command, expect_argument_exception
):
    result = runner.invoke(command)
    assert result.exit_code == 2, result.output
    assert result.output.__contains__(
        f"Missing argument '{expect_argument_exception}'."
    )


def test_object_create_with_multiple_json_sources(runner):
    with NamedTemporaryFile("r") as tmp_file:
        result = runner.invoke(
            ["object", "create", "schema", "name=schema_name", "--json", "json_data"]
        )
        assert_that_result_is_usage_error(
            result,
            f"Parameters 'object_attributes' and '--json' are incompatible and cannot be used simultaneously.",
        )


def test_replace_and_not_exists_cannot_be_used_together(runner, os_agnostic_snapshot):
    result = runner.invoke(
        [
            "object",
            "create",
            "schema",
            "name=schema_name",
            "--replace",
            "--if-not-exists",
        ]
    )
    assert result.exit_code == 2, result.output
    assert result.output == os_agnostic_snapshot


@mock.patch("snowflake.cli._plugins.object.manager.ObjectManager.execute_query")
@pytest.mark.parametrize(
    "terse, limit, expected_query",
    [
        (True, None, "show terse tables like '%%'"),
        (False, 10, "show tables like '%%' limit 10"),
        (False, 5, "show tables like '%%' limit 5"),
        (True, 10, "show terse tables like '%%' limit 10"),
    ],
)
def test_show_with_terse_and_limit_options(
    mock_execute_query, terse, limit, expected_query, mock_cursor
):
    """Test TERSE and LIMIT functionality in ObjectManager.show method."""
    from snowflake.cli._plugins.object.manager import ObjectManager

    mock_execute_query.return_value = mock_cursor(["row"], [])

    # Test ObjectManager.show method directly
    manager = ObjectManager()
    manager.show(
        object_type="table",
        like="%%",
        terse=terse,
        limit=limit,
    )

    mock_execute_query.assert_called_once_with(expected_query)


@mock.patch("snowflake.cli._plugins.object.manager.ObjectManager.execute_query")
def test_show_with_all_options_combined(mock_execute_query, mock_cursor):
    """Test ObjectManager.show with all options (like, scope, terse, limit) combined."""
    from snowflake.cli._plugins.object.manager import ObjectManager

    mock_execute_query.return_value = mock_cursor(["row"], [])

    manager = ObjectManager()
    manager.show(
        object_type="table",
        like="test%",
        scope=("database", "my_db"),
        terse=True,
        limit=25,
    )

    expected_query = "show terse tables like 'test%' in database my_db limit 25"
    mock_execute_query.assert_called_once_with(expected_query)


@mock.patch("snowflake.connector")
@pytest.mark.parametrize(
    "object_type, object_name",
    DROP_TEST_OBJECTS,
)
def test_drop_with_if_exists(
    mock_connector, object_type, object_name, mock_cursor, runner, os_agnostic_snapshot
):
    mock_connector.connect.return_value.execute_stream.return_value = (
        None,
        mock_cursor(
            rows=[(f"{object_name} successfully dropped.",)],
            columns=["status"],
        ),
    )

    result = runner.invoke(["object", "drop", object_type, object_name, "--if-exists"])
    assert result.exit_code == 0, result.output
    assert result.output == os_agnostic_snapshot


@mock.patch("snowflake.cli._plugins.object.manager.ObjectManager.execute_query")
@pytest.mark.parametrize(
    "if_exists, expected_query",
    [
        (False, "drop table IDENTIFIER('test_table')"),
        (True, "drop table if exists IDENTIFIER('test_table')"),
    ],
)
def test_drop_manager_if_exists(
    mock_execute_query, if_exists, expected_query, mock_cursor
):
    """Test ObjectManager.drop method with if_exists parameter."""
    from snowflake.cli._plugins.object.manager import ObjectManager
    from snowflake.cli.api.identifiers import FQN

    mock_execute_query.return_value = mock_cursor(["row"], [])

    manager = ObjectManager()
    manager.drop(
        object_type="table", fqn=FQN.from_string("test_table"), if_exists=if_exists
    )

    mock_execute_query.assert_called_once_with(expected_query)


@mock.patch("snowflake.connector.connect")
def test_show_with_in_account_flag(mock_connector, runner, mock_ctx):
    """Test --in-account flag lists objects at account scope."""
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["object", "list", "table", "--in-account"])
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == ["show tables like '%%' in account"]


@mock.patch("snowflake.connector.connect")
def test_in_account_and_in_are_mutually_exclusive(mock_connector, runner, mock_ctx):
    """Test that --in-account and --in cannot be used together."""
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        ["object", "list", "table", "--in-account", "--in", "database", "my_db"]
    )
    assert result.exit_code == 2, result.output
    assert "incompatible" in result.output.lower()


@mock.patch("snowflake.connector.connect")
@pytest.mark.parametrize(
    "object_type, expected_query",
    [
        ("external-access-integration", "show external access integrations like '%%'"),
        ("integration", "show integrations like '%%'"),
    ],
)
def test_show_integration_with_in_account_flag(
    mock_connector, runner, mock_ctx, object_type, expected_query
):
    """Test that --in-account flag is effectively ignored for integration objects"""
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["object", "list", object_type, "--in-account"])
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [expected_query]
