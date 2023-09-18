import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from snowcli.config import CliConfigManager, get_default_connection, config_init
from tests.testing_utils.fixtures import *


def test_empty_config_file_is_created_if_not_present():
    with TemporaryDirectory() as tmp_dir:
        config_file = Path(tmp_dir) / "sub" / "config.toml"
        assert config_file.exists() is False

        cm = CliConfigManager(file_path=config_file)
        cm.from_context(config_path_override=None)
        assert config_file.exists() is True
        assert config_file.read_text() == """[connections]\n"""


@mock.patch.dict(os.environ, {}, clear=True)
def test_get_connection_from_file(test_snowcli_config):
    cm = CliConfigManager(file_path=test_snowcli_config)
    cm.read_config()

    assert cm.get_connection("full") == {
        "account": "dev_account",
        "user": "dev_user",
        "host": "dev_host",
        "port": 8000,
        "protocol": "dev_protocol",
        "role": "dev_role",
        "schema": "dev_schema",
        "database": "dev_database",
        "warehouse": "dev_warehouse",
    }


@mock.patch.dict(
    os.environ,
    {
        "SNOWFLAKE_CONNECTIONS_DEV_DATABASE": "database_foo",
        "SNOWFLAKE_CONNECTIONS_DEV_WAREHOUSE": "large",
        "SNOWFLAKE_CONNECTIONS_DEV_ACCOUNT": "my_account_123",
        "SNOWFLAKE_CONNECTIONS_DEV_PASSWORD": "my_pass",
    },
    clear=True,
)
def test_environment_variables_override_configuration_value(test_snowcli_config):
    cm = CliConfigManager(file_path=test_snowcli_config)
    cm.read_config()

    assert cm.get_connection("dev") == {
        "database": "database_foo",
        "schema": "test_public",
        "role": "test_role",
        "warehouse": "large",
        "account": "my_account_123",
        "password": "my_pass",
    }


@mock.patch.dict(
    os.environ,
    {
        "SNOWFLAKE_CONNECTIONS_EMPTY_ACCOUNT": "some_account",
        "SNOWFLAKE_CONNECTIONS_EMPTY_DATABASE": "test_database",
        "SNOWFLAKE_CONNECTIONS_EMPTY_WAREHOUSE": "large",
    },
    clear=True,
)
def test_environment_variables_works_if_config_value_not_present(test_snowcli_config):
    cm = CliConfigManager(file_path=test_snowcli_config)
    cm.read_config()

    assert cm.get_connection("empty") == {
        "account": "some_account",
        "database": "test_database",
        "warehouse": "large",
    }


@mock.patch.dict(
    os.environ,
    {
        "SNOWFLAKE_CONNECTIONS_DEV_WAREHOUSE": "large",
        "SNOWFLAKE_CONNECTIONS_EMPTY_ACCOUNT": "some_account",
        "SNOWFLAKE_CONNECTIONS_FULL_DATABASE": "test_database",
    },
    clear=True,
)
def test_get_all_connections(test_snowcli_config):
    cm = CliConfigManager(file_path=test_snowcli_config)
    cm.read_config()

    assert cm.get_section("connections") == {
        "dev": {
            "database": "db_for_test",
            "role": "test_role",
            "schema": "test_public",
            "warehouse": "large",
            "password": "dummy_password",
        },
        "empty": {"account": "some_account"},
        "full": {
            "account": "dev_account",
            "database": "test_database",
            "host": "dev_host",
            "port": 8000,
            "protocol": "dev_protocol",
            "role": "dev_role",
            "schema": "dev_schema",
            "user": "dev_user",
            "warehouse": "dev_warehouse",
        },
    }


@mock.patch.dict(
    os.environ,
    {
        "SNOWFLAKE_OPTIONS_DEFAULT_CONNECTION": "fooBarConn",
    },
    clear=True,
)
def test_get_default_connection_from_env():
    result = get_default_connection()
    assert result == "fooBarConn"


def test_get_default_connection_if_not_present_in_config(test_snowcli_config):
    config_init(test_snowcli_config)
    result = get_default_connection()
    assert result == "dev"


@pytest.mark.parametrize(
    "config_content, expect",
    [
        ("""[connections]\n[options]\ndefault_connection = "conn" """, "conn"),
        (
            """[connections]\n[connections.other_conn]\n[options]\ndefault_connection = "conn" """,
            "conn",
        ),
        ("""[connections]\n[connections.conn]\n[options]\n""", "conn"),
        ("""[connections]\n[connections.conn]\n""", "conn"),
        ("""[connections]\n""", "dev"),
        (
            """[connections]\n[connections.conn1]\n[connections.conn2]\n[options]\n """,
            "dev",
        ),
    ],
)
def test_get_default_connection_from_config(config_content, expect):
    with TemporaryDirectory() as tmp_dir:
        config_path = Path(tmp_dir) / "config.toml"
        with open(config_path, "w+") as config:
            config.write(config_content)
            config.flush()

            config_init(config_path)
            result = get_default_connection()
            assert result == expect
