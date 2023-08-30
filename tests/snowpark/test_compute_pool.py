from unittest import mock

from tests.testing_utils.fixtures import *


@mock.patch("snowflake.connector.connect")
def test_create_cp(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        [
            "snowpark",
            "cp",
            "create",
            "--name",
            "cpName",
            "--num",
            "42",
            "--family",
            "familyValue",
        ]
    )

    assert result.exit_code == 0
    assert (
        ctx.get_query()
        == """\
CREATE COMPUTE POOL cpName
MIN_NODES = 42
MAX_NODES = 42
INSTANCE_FAMILY = familyValue;
"""
    )


@mock.patch("snowflake.connector.connect")
def test_list_cp(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "cp", "list"])

    assert result.exit_code == 0
    assert ctx.get_query() == "show compute pools;"


@mock.patch("snowflake.connector.connect")
def test_drop_cp(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "cp", "drop", "cpNameToDrop"])

    assert result.exit_code == 0
    assert ctx.get_query() == "drop compute pool cpNameToDrop;"


@mock.patch("snowflake.connector.connect")
def test_stop_cp(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "cp", "stop", "cpNameToStop"])

    assert result.exit_code == 0
    assert ctx.get_query() == "alter compute pool cpNameToStop stop all;"
