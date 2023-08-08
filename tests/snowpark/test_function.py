from tempfile import NamedTemporaryFile
from unittest import mock


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.function.snowpark_package")
def test_create_function(
    mock_package_create, mock_connector, runner, mock_ctx, snapshot
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    with NamedTemporaryFile() as fh:
        result = runner.invoke(
            [
                "snowpark",
                "function",
                "create",
                "--name",
                "functionName",
                "--file",
                fh.name,
                "--handler",
                "main.py:app",
                "--return-type",
                "table(variant)",
                "--input-parameters",
                "(a string, b number)" "--overwrite",
            ]
        )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot
    mock_package_create.assert_called_once_with("ask", True, "ask")


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.function.snowpark_package")
def test_update_function(
    mock_package_create, mock_connector, runner, mock_ctx, snapshot
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    with NamedTemporaryFile() as fh:
        result = runner.invoke(
            [
                "snowpark",
                "function",
                "create",
                "--name",
                "functionName",
                "--file",
                fh.name,
                "--handler",
                "main.py:app",
                "--return-type",
                "table(variant)",
                "--input-parameters",
                "(a string, b number)" "--replace-always",
            ]
        )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot
    mock_package_create.assert_called_once_with("ask", True, "ask")


@mock.patch("snowflake.connector.connect")
def test_execute_function(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "function",
            "execute",
            "--function",
            "functionName(42, 'string')",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_describe_function_from_signature(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "function",
            "describe",
            "--function",
            "functionName(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_describe_function_from_name(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "function",
            "describe",
            "--name",
            "functionName",
            "--input-parameters",
            "(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_list_function(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "function",
            "list",
            "--like",
            "foo_bar%",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_drop_function_from_signature(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "function",
            "drop",
            "--function",
            "functionName(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_drop_function_from_name(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "function",
            "drop",
            "--name",
            "functionName",
            "--input-parameters",
            "(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot
