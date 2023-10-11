import json
import pytest

from tempfile import NamedTemporaryFile
from textwrap import dedent
from unittest import mock

from snowcli.exception import SnowflakeConnectionError
from tests.testing_utils.fixtures import *


def test_new_connection_can_be_added(runner, snapshot):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke(
            [
                "--config-file",
                tmp_file.name,
                "connection",
                "add",
                "--connection-name",
                "conn1",
                "--username",
                "user1",
                "--password",
                "password1",
                "--account",
                "account1",
                "--port",
                "8080",
            ]
        )
        content = tmp_file.read()
    assert result.exit_code == 0, result.output
    assert content == snapshot


def test_port_has_cannot_be_string(runner):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke(
            [
                "--config-file",
                tmp_file.name,
                "connection",
                "add",
                "--connection-name",
                "conn1",
                "--username",
                "user1",
                "--account",
                "account1",
                "--port",
                "portValue",
            ]
        )
    assert result.exit_code == 1, result.output
    assert "Value of port must be integer" in result.output


def test_port_has_cannot_be_float(runner):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke(
            [
                "--config-file",
                tmp_file.name,
                "connection",
                "add",
                "--connection-name",
                "conn1",
                "--username",
                "user1",
                "--account",
                "account1",
                "--port",
                "123.45",
            ]
        )
    assert result.exit_code == 1, result.output
    assert "Value of port must be integer" in result.output


def test_new_connection_add_prompt_handles_default_values(runner, snapshot):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke(
            [
                "--config-file",
                tmp_file.name,
                "connection",
                "add",
            ],
            input="connName\naccName\nuserName",
        )
        content = tmp_file.read()
    assert result.exit_code == 0, result.output
    assert content == snapshot


def test_new_connection_add_prompt_handles_prompt_override(runner, snapshot):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke(
            [
                "--config-file",
                tmp_file.name,
                "connection",
                "add",
            ],
            input="connName\naccName\nuserName\ndbName",
        )
        content = tmp_file.read()
    assert result.exit_code == 0, result.output
    assert content == snapshot


def test_fails_if_existing_connection(runner):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        tmp_file.write(
            dedent(
                """\
        [connections]
        [connections.conn2]
        username = "foo"
        """
            )
        )
        tmp_file.flush()
        result = runner.invoke(
            [
                "--config-file",
                tmp_file.name,
                "connection",
                "add",
                "--connection-name",
                "conn2",
                "--username",
                "user1",
                "--password",
                "password1",
                "--account",
                "account1",
            ]
        )
    assert result.exit_code == 1, result.output
    assert "Connection conn2 already exists  " in result.output


def test_lists_connection_information(runner):
    result = runner.invoke_with_config(["connection", "list", "--format", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == [
        {
            "connection_name": "full",
            "parameters": {
                "account": "dev_account",
                "database": "dev_database",
                "host": "dev_host",
                "port": 8000,
                "protocol": "dev_protocol",
                "role": "dev_role",
                "schema": "dev_schema",
                "user": "dev_user",
                "warehouse": "dev_warehouse",
            },
        },
        {
            "connection_name": "dev",
            "parameters": {
                "database": "db_for_test",
                "password": "****",  # masked
                "role": "test_role",
                "schema": "test_public",
                "warehouse": "xs",
            },
        },
        {"connection_name": "empty", "parameters": {}},
    ]


def test_second_connection_not_update_default_connection(runner, snapshot):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        tmp_file.write(
            dedent(
                """\
        [connections]
        [connections.conn]
        username = "foo"
        
        [options]
        default_connection = "conn"
        """
            )
        )
        tmp_file.flush()
        result = runner.invoke(
            [
                "--config-file",
                tmp_file.name,
                "connection",
                "add",
                "--connection-name",
                "conn2",
                "--username",
                "user1",
                "--password",
                "password1",
                "--account",
                "account1",
            ]
        )
        tmp_file.seek(0)
        content = tmp_file.read()

        assert result.exit_code == 0, result.output
        assert content == snapshot


@mock.patch("snowcli.cli.connection.commands.connect_to_snowflake")
def test_connection_test(mock_connect, runner):
    result = runner.invoke_with_config(["connection", "test", "-c", "full"])
    assert result.exit_code == 0, result.output
    mock_connect.assert_called_once_with(
        connection="full",
        account=None,
        user=None,
        password=None,
        database=None,
        schema=None,
        role=None,
        warehouse=None,
        temporary_connection=False,
        format=None,
        verbose=None,
        debug=None,
    )


@mock.patch("snowcli.cli.connection.commands.connect_to_snowflake")
def test_connection_temporary_connection_test(mock_connect, runner):
    result = runner.invoke_with_config(
        [
            "connection",
            "test",
            "-x",
            "--account",
            "foo",
            "--user",
            "bar",
            "--password",
            "baz",
        ]
    )
    assert result.exit_code == 0, result.output
    mock_connect.assert_called_once_with(
        connection=None,
        account="foo",
        user="bar",
        password="baz",
        database=None,
        schema=None,
        role=None,
        warehouse=None,
        temporary_connection=True,
        format=None,
        verbose=None,
        debug=None,
    )


@mock.patch("snowflake.connector.connect")
@pytest.mark.parametrize("option", ["--temporary-connection", "-x"])
def test_temporary_connection(mock_conn, option, runner):

    mock_conn.side_effect = SnowflakeConnectionError("HTTP 403: Forbidden")
    result = runner.invoke(
        [
            "warehouse",
            "status",
            option,
            "--account",
            "test_account",
            "--user",
            "snowcli_test",
            "--password",
            "top_secret",
            "--warehouse",
            "xsmall",
            "--database",
            "test_dv",
            "--schema",
            "PUBLIC",
        ]
    )

    assert result.exit_code == 1
    mock_conn.assert_called_once_with(
        application="SNOWCLI.WAREHOUSE.STATUS",
        account="test_account",
        user="snowcli_test",
        password="top_secret",
        database="test_dv",
        schema="PUBLIC",
        warehouse="xsmall",
    )
