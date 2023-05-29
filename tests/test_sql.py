from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest import mock

from testing_utils.result_assertions import assert_that_result_is_usage_error

CONFIG_MOCK = "snowcli.cli.sql.config"


@mock.patch(CONFIG_MOCK)
def test_sql_execute_query(mock_config, runner):
    result = runner.invoke(["sql", "-q", "query"])

    assert result.exit_code == 0
    mock_config.snowflake_connection.ctx.execute_string.assert_called_once_with(
        sql_text="query", remove_comments=True, cursor_class=mock.ANY
    )


@mock.patch(CONFIG_MOCK)
def test_sql_execute_file(mock_config, runner):
    with NamedTemporaryFile("r") as tmp_file:
        Path(tmp_file.name).write_text("query from file")
        result = runner.invoke(["sql", "-f", tmp_file.name])

    assert result.exit_code == 0
    mock_config.snowflake_connection.ctx.execute_string.assert_called_once_with(
        sql_text="query from file", remove_comments=True, cursor_class=mock.ANY
    )


@mock.patch(CONFIG_MOCK)
def test_sql_execute_from_stdin(mock_config, runner):
    result = runner.invoke(["sql"], input="query from input")

    assert result.exit_code == 0
    mock_config.snowflake_connection.ctx.execute_string.assert_called_once_with(
        sql_text="query from input", remove_comments=True, cursor_class=mock.ANY
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


@mock.patch("snowcli.cli.sql.config.is_auth")
def test_sql_fails_if_user_not_authenticated(mock_is_auth, runner):
    mock_is_auth.return_value = False
    result = runner.invoke(["sql", "-q", "select 1"])

    assert_that_result_is_usage_error(result, "Not authenticated")


@mock.patch("snowcli.config.AppConfig")
@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.sql.config.is_auth")
def test_sql_overrides_connection_configuration(_, mock_conn, mock_app_config, runner):
    with NamedTemporaryFile() as tmp:
        Path(tmp.name).write_text("[connections.fooConn]")

        mock_app_config.return_value.config = {"snowsql_config_path": tmp.name}
        result = runner.invoke(
            [
                "sql",
                "-q",
                "select 1",
                "--connection",
                "fooConn",
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
            ]
        )

    assert result.exit_code == 0
    mock_conn.assert_called_once_with(
        account="accountnameValue",
        user="usernameValue",
        warehouse="warehouseValue",
        role="rolenameValue",
        database="dbnameValue",
        schema="schemanameValue",
        application="SNOWCLI",
    )
