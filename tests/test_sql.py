from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest import mock

import pytest

from tests.testing_utils.result_assertions import assert_that_result_is_usage_error


@mock.patch("snowflake.cli.plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_sql_execute_query(mock_execute, runner, mock_cursor):
    mock_execute.return_value = (mock_cursor(["row"], []) for _ in range(1))

    result = runner.invoke(["sql", "-q", "query"])

    assert result.exit_code == 0
    mock_execute.assert_called_once_with("query")


@mock.patch("snowflake.cli.plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_sql_execute_file(mock_execute, runner, mock_cursor):
    mock_execute.return_value = (mock_cursor(["row"], []) for _ in range(1))
    query = "query from file"

    with NamedTemporaryFile("r") as tmp_file:
        Path(tmp_file.name).write_text(query)
        result = runner.invoke(["sql", "-f", tmp_file.name])

    assert result.exit_code == 0
    mock_execute.assert_called_once_with(query)


@mock.patch("snowflake.cli.plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_sql_execute_from_stdin(mock_execute, runner, mock_cursor):
    mock_execute.return_value = (mock_cursor(["row"], []) for _ in range(1))
    query = "query from input"

    result = runner.invoke(["sql", "-i"], input=query)

    assert result.exit_code == 0
    mock_execute.assert_called_once_with(query)


def test_sql_fails_if_no_query_file_or_stdin(runner):
    result = runner.invoke(["sql"])

    assert_that_result_is_usage_error(
        result, "Use either query, filename or input option."
    )


@pytest.mark.parametrize("inputs", [("-i", "-q", "foo"), ("-i",), ("-q", "foo")])
def test_sql_fails_if_other_inputs_and_file_provided(runner, inputs):
    with NamedTemporaryFile("r") as tmp_file:
        result = runner.invoke(["sql", *inputs, "-f", tmp_file.name])
        assert_that_result_is_usage_error(
            result, "Multiple input sources specified. Please specify only one. "
        )


def test_sql_fails_if_query_and_stdin_provided(runner):
    result = runner.invoke(["sql", "-q", "fooo", "-i"])
    assert_that_result_is_usage_error(
        result, "Multiple input sources specified. Please specify only one. "
    )


@mock.patch("snowflake.cli.app.snow_connector.connect_to_snowflake")
def test_sql_overrides_connection_configuration(mock_conn, runner, mock_cursor):
    mock_conn.return_value.execute_string.return_value = [mock_cursor(["row"], [])]

    result = runner.invoke(
        [
            "sql",
            "-q",
            "select 1",
            "--connection",
            "connectionName",
            "--accountname",
            "accountnameValue",
            "--username",
            "usernameValue",
            "--dbname",
            "dbnameValue",
            "--schemaname",
            "schemanameValue",
            "--rolename",
            "rolenameValue",
            "--warehouse",
            "warehouseValue",
            "--password",
            "passFromTest",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    mock_conn.assert_called_once_with(
        temporary_connection=False,
        connection_name="connectionName",
        account="accountnameValue",
        user="usernameValue",
        warehouse="warehouseValue",
        database="dbnameValue",
        schema="schemanameValue",
        role="rolenameValue",
        password="passFromTest",
    )
