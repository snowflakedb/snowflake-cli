import os
import pytest

from snowcli.connection_config import ConnectionConfigs
from snowcli.exception import EnvironmentVariableNotFoundError
from unittest import mock

CONFIG_PATH = "tests/config/env_variables_config"


@mock.patch.dict(
    os.environ,
    {
        "SNOWCLI_ACCOUNT_NAME": "account_name",
        "SNOWCLI_USER": "username",
        "SNOWCLI_PASS": "password",
        "SNOWCLI_HOST": "localhost",
        "SNOWCLI_PORT": "8080",
        "SNOWCLI_PROTOCOL": "http",
    },
)
def test_replace_with_env_variables():
    result = ConnectionConfigs(CONFIG_PATH)

    assert result.get_connection("test") == {
        "account": "account_name",
        "user": "username",
        "password": "password",
        "host": "localhost",
        "port": "8080",
        "protocol": "http",
    }


@mock.patch.dict(
    os.environ,
    {
        "SNOWCLI_USER": "username",
        "SNOWCLI_PASS": "password",
        "SNOWCLI_HOST": "localhost",
        "SNOWCLI_PORT": "8080",
        "SNOWCLI_PROTOCOL": "http",
    },
)
def test_missing_account_in_environment_variables():
    result = ConnectionConfigs(CONFIG_PATH)

    with pytest.raises(EnvironmentVariableNotFoundError):
        result.get_connection("test")


@mock.patch.dict(os.environ, {"SNOWCLI_USER": "username"})
def test_add_connection_should_not_write_values_from_env_variables(tmp_path):
    config = tmp_path / "config"
    config.write_text("[connections.connection]\nusername = $SNOWCLI_USER")

    connection_configs = ConnectionConfigs(config.absolute())
    connection_configs.add_connection("new_connection", {"username": "$SNOWCLI_USER"})

    assert (
        config.read_text()
        == """[connections.connection]
username = $SNOWCLI_USER

[connections.new_connection]
username = $SNOWCLI_USER

"""
    )
