import os
from tempfile import NamedTemporaryFile
from textwrap import dedent
from unittest import mock

from snowcli.config import CliConfigManager


def test_new_connection_can_be_added(runner, snapshot):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke(
            [
                "--config-file",
                tmp_file.name,
                "connection",
                "add",
                "--connection",
                "conn1",
                "--username",
                "user1",
                "--password",
                "password1",
                "--account",
                "account1",
            ]
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
                "--connection",
                "conn2",
                "--username",
                "user1",
                "--password",
                "password1",
                "--account",
                "account1",
            ]
        )
    assert result.exit_code == 1
    assert "Connection conn2 already exists  " in result.output


def test_environment_variables_override_configuration_value(test_snowcli_config):
    cm = CliConfigManager(file_path=test_snowcli_config)
    cm.read_config()

    assert cm.get("connections", "default", key="warehouse") == "xs"
    with mock.patch.dict(
        os.environ, {"SNOWFLAKE_CONNECTIONS_DEFAULT_WAREHOUSE": "foo42"}
    ):
        assert cm.get("connections", "default", key="warehouse") == "foo42"
