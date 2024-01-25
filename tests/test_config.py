from tempfile import TemporaryDirectory

from snowflake.cli.api.config import (
    config_init,
    get_config_section,
    get_connection,
    get_default_connection,
)
from snowflake.cli.api.exceptions import MissingConfiguration

from tests.testing_utils.fixtures import *


def test_empty_config_file_is_created_if_not_present():
    with TemporaryDirectory() as tmp_dir:
        config_file = Path(tmp_dir) / "sub" / "config.toml"
        assert config_file.exists() is False

        config_init(config_file)
        assert config_file.exists() is True


@mock.patch.dict(os.environ, {}, clear=True)
def test_get_connection_from_file(test_snowcli_config):
    config_init(test_snowcli_config)

    assert get_connection("full") == {
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
        "SNOWFLAKE_CONNECTIONS_DEFAULT_DATABASE": "database_foo",
        "SNOWFLAKE_CONNECTIONS_DEFAULT_WAREHOUSE": "large",
        "SNOWFLAKE_CONNECTIONS_DEFAULT_ACCOUNT": "my_account_123",
        "SNOWFLAKE_CONNECTIONS_DEFAULT_PASSWORD": "my_pass",
    },
    clear=True,
)
def test_environment_variables_override_configuration_value(test_snowcli_config):
    config_init(test_snowcli_config)

    assert get_connection("default") == {
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
    config_init(test_snowcli_config)

    assert get_connection("empty") == {
        "account": "some_account",
        "database": "test_database",
        "warehouse": "large",
    }


@mock.patch.dict(
    os.environ,
    {
        "SNOWFLAKE_CONNECTIONS_DEFAULT_WAREHOUSE": "large",
        "SNOWFLAKE_CONNECTIONS_EMPTY_ACCOUNT": "some_account",
        "SNOWFLAKE_CONNECTIONS_FULL_DATABASE": "test_database",
    },
    clear=True,
)
def test_get_all_connections(test_snowcli_config):
    config_init(test_snowcli_config)

    assert get_config_section("connections") == {
        "default": {
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


@mock.patch("snowflake.cli.api.config.CONFIG_MANAGER")
def test_create_default_config_if_not_exists(mock_config_manager):

    with TemporaryDirectory() as tmp_dir:
        config_path = Path(f"{tmp_dir}/snowflake/config.toml")
        mock_config_manager.file_path = config_path
        mock_config_manager.conf_file_cache = {}

        config_init(None)

        assert config_path.exists()


@mock.patch.dict(
    os.environ,
    {
        "SNOWFLAKE_CONNECTIONS_DEFAULT_ACCOUNT": "default_account",
    },
    clear=True,
)
def test_default_connection_with_overwritten_values(test_snowcli_config):
    config_init(test_snowcli_config)

    assert get_default_connection() == {
        "database": "db_for_test",
        "role": "test_role",
        "schema": "test_public",
        "warehouse": "xs",
        "password": "dummy_password",
        "account": "default_account",
    }


def test_not_found_default_connection(test_root_path):
    config_init(Path(test_root_path / "empty_config.toml"))
    with pytest.raises(MissingConfiguration) as ex:
        get_default_connection()

    assert ex.value.message == "Connection default is not configured"


@mock.patch.dict(
    os.environ,
    {
        "SNOWFLAKE_DEFAULT_CONNECTION_NAME": "not_existed_connection",
    },
    clear=True,
)
def test_not_found_default_connection_from_evn_variable(test_root_path):
    config_init(Path(test_root_path / "empty_config.toml"))
    with pytest.raises(MissingConfiguration) as ex:
        get_default_connection()

    assert ex.value.message == "Connection not_existed_connection is not configured"
