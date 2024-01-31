from unittest import mock

import pytest
from snowflake.cli.api.constants import SUPPORTED_OBJECTS


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
    ],
)
def test_show(
    mock_connector, object_type, expected, mock_cursor, runner, snapshot, mock_ctx
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["object", "list", object_type], catch_exceptions=False)
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [f"show {expected} like '%%'"]


DESCRIBE_TEST_OBJECTS = [
    ("compute-pool", "compute-pool-example"),
    ("network-rule", "network-rule-example"),
    ("integration", "integration"),
    ("network-rule", "network rule"),
    ("database", "database-example"),
    ("function", "function-example"),
    # ("job", "job-example"),
    ("procedure", "procedure-example"),
    ("role", "role-example"),
    ("schema", "schema-example"),
    ("service", "service-example"),
    ("secret", "secret-example"),
    ("stage", "stage-example"),
    ("stream", "stream-example"),
    ("streamlit", "streamlit-example"),
    ("table", "table-example"),
    ("task", "task-example"),
    ("user", "user-example"),
    ("warehouse", "warehouse-example"),
    ("view", "view-example"),
]


@mock.patch("snowflake.connector")
@pytest.mark.parametrize("object_type, object_name", DESCRIBE_TEST_OBJECTS)
def test_describe(
    mock_connector, object_type, object_name, mock_cursor, runner, snapshot
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
    assert result.output == snapshot


@mock.patch("snowflake.connector")
def test_describe_fails_image_repository(mock_cursor, runner, snapshot):
    result = runner.invoke(["object", "describe", "image-repository", "test_repo"])
    assert result.exit_code == 1, result.output
    assert result.output == snapshot


DROP_TEST_OBJECTS = [
    *DESCRIBE_TEST_OBJECTS,
    ("image-repository", "image-repository-example"),
]


@mock.patch("snowflake.connector")
@pytest.mark.parametrize(
    "object_type, object_name",
    DROP_TEST_OBJECTS,
)
def test_drop(mock_connector, object_type, object_name, mock_cursor, runner, snapshot):
    mock_connector.connect.return_value.execute_stream.return_value = (
        None,
        mock_cursor(rows=[f"{object_name} successfully dropped."], columns=["status"]),
    )

    result = runner.invoke(["object", "drop", object_type, object_name])
    assert result.exit_code == 0, result.output
    assert result.output == snapshot


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
