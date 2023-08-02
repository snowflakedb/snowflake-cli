from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest import mock

from tests.testing_utils.result_assertions import assert_that_result_is_usage_error

MOCK_CONNECTION = "snowcli.cli.sql.snow_cli_global_context_manager.get_connection"


@mock.patch(MOCK_CONNECTION)
def test_sql_execute_query(mock_conn, runner):
    result = runner.invoke(["sql", "-q", "query"])

    assert result.exit_code == 0
    mock_conn.return_value.ctx.execute_string.assert_called_once_with(
        sql_text="query", remove_comments=True
    )


@mock.patch(MOCK_CONNECTION)
def test_sql_execute_file(mock_conn, runner):
    with NamedTemporaryFile("r") as tmp_file:
        Path(tmp_file.name).write_text("query from file")
        result = runner.invoke(["sql", "-f", tmp_file.name])

    assert result.exit_code == 0
    mock_conn.return_value.ctx.execute_string.assert_called_once_with(
        sql_text="query from file", remove_comments=True
    )


@mock.patch(MOCK_CONNECTION)
def test_sql_execute_from_stdin(mock_conn, runner):
    result = runner.invoke(["sql"], input="query from input")

    assert result.exit_code == 0
    mock_conn.return_value.ctx.execute_string.assert_called_once_with(
        sql_text="query from input", remove_comments=True
    )


def test_sql_fails_if_no_query_file_or_stdin(runner):
    result = runner.invoke(["sql"])

    assert_that_result_is_usage_error(
        result, "Provide either query or filename argument"
    )


def test_sql_fails_for_both_stdin_and_other_query_source(runner):
    with NamedTemporaryFile("r") as tmp_file:
        result = runner.invoke(["sql", "-f", tmp_file.name], input="query from input")

    assert_that_result_is_usage_error(
        result, "Can't use stdin input together with query or filename"
    )


def test_sql_fails_for_both_query_and_file(runner):
    with NamedTemporaryFile("r") as tmp_file:
        result = runner.invoke(["sql", "-f", tmp_file.name, "-q", "query"])

    assert_that_result_is_usage_error(result, "Both query and file provided")


@mock.patch("snowcli.cli.common.snow_cli_global_context.connect_to_snowflake")
def test_sql_overrides_connection_configuration(mock_conn, runner):
    result = runner.invoke_with_config(
        [
            "sql",
            "-q",
            "select 1",
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
        connection_name="dev",
        account="accountnameValue",
        user="usernameValue",
        warehouse="warehouseValue",
        database="dbnameValue",
        schema="schemanameValue",
        role="rolenameValue",
        password="passFromTest",
    )
