# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from unittest import mock

import pytest
from snowflake.cli.api.secret import SecretType


# Used as a solution to syrupy having some problems with comparing multilines string
class CustomStr(str):
    def __repr__(self):
        return str(self)


MOCK_CONNECTION = {
    "database": "databaseValue",
    "schema": "schemaValue",
    "role": "roleValue",
    "show": "warehouseValue",
}


@pytest.mark.parametrize(
    "cmd,expected",
    [
        ("snow sql", "SNOWCLI.SQL"),
        ("snow show warehouses", "SNOWCLI.SHOW.WAREHOUSES"),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._app.snow_connector.command_info")
def test_command_context_is_passed_to_snowflake_connection(
    mock_command_info, mock_connect, cmd, expected, test_snowcli_config
):
    from snowflake.cli._app.snow_connector import connect_to_snowflake
    from snowflake.cli.api.config import config_init

    config_init(test_snowcli_config)

    mock_ctx = mock.Mock()
    mock_ctx.command_path = cmd
    mock_command_info.return_value = expected

    connect_to_snowflake(connection_name="default")

    mock_connect.assert_called_once_with(
        application=expected,
        database="db_for_test",
        schema="test_public",
        role="test_role",
        warehouse="xs",
        password="dummy_password",
        application_name="snowcli",
        using_session_keep_alive=True,
    )


@pytest.mark.parametrize(
    "connection_name, user_input, should_override",
    [
        ("private_key_file", None, False),
        ("private_key_file", "SNOWFLAKE_PRIVATE_KEY_FILE", False),
        ("private_key_file", "SNOWFLAKE_PRIVATE_KEY_PATH", False),
        ("private_key_file", "private_key_file", True),
        ("private_key_file", "private_key_path", True),
        ("private_key_path", None, False),
        ("private_key_path", "SNOWFLAKE_PRIVATE_KEY_FILE", False),
        ("private_key_path", "SNOWFLAKE_PRIVATE_KEY_PATH", False),
        ("private_key_path", "private_key_file", True),
        ("private_key_path", "private_key_path", True),
        ("no_private_key", None, False),
        ("no_private_key", "SNOWFLAKE_PRIVATE_KEY_FILE", True),
        ("no_private_key", "SNOWFLAKE_PRIVATE_KEY_PATH", True),
        ("no_private_key", "private_key_file", True),
        ("no_private_key", "private_key_path", True),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._app.snow_connector.command_info")
@mock.patch("snowflake.cli._app.snow_connector._load_pem_to_der")
@mock.patch("snowflake.cli._app.snow_connector._load_pem_from_file")
def test_private_key_loading_and_aliases(
    mock_load_pem_from_file,
    mock_load_pem_to_der,
    mock_command_info,
    mock_connect,
    test_snowcli_config,
    connection_name,
    user_input,
    should_override,
):
    """
    Ensures that the interaction between private_key_file and private_key_path is sound.
    """
    from snowflake.cli._app.snow_connector import connect_to_snowflake
    from snowflake.cli.api.config import config_init, get_connection_dict

    config_init(test_snowcli_config)

    # set up an override for private key path, either via env or override
    override_value = "/override/value"
    env = {}
    overrides = {}
    if user_input is not None:
        if user_input.startswith("SNOWFLAKE_"):
            env[user_input] = override_value
        else:
            overrides[user_input] = override_value

    key = SecretType(b"bytes")
    mock_command_info.return_value = "SNOWCLI.SQL"
    mock_load_pem_from_file.return_value = key
    mock_load_pem_to_der.return_value = key

    conn_dict = get_connection_dict(connection_name)
    default_value = conn_dict.get("private_key_file", None) or conn_dict.get(
        "private_key_path", None
    )
    expected_private_key_file_value = (
        override_value if should_override else default_value
    )

    with mock.patch.dict(os.environ, env, clear=True):
        connect_to_snowflake(connection_name=connection_name, **overrides)
        expected_private_key_args = (
            {}
            if expected_private_key_file_value is None
            else dict(private_key=b"bytes")
        )
        mock_connect.assert_called_once_with(
            application=mock_command_info.return_value,
            authenticator="SNOWFLAKE_JWT",
            application_name="snowcli",
            using_session_keep_alive=True,
            **expected_private_key_args,
        )
        if expected_private_key_file_value is not None:
            mock_load_pem_from_file.assert_called_with(expected_private_key_file_value)
            mock_load_pem_to_der.assert_called_with(key)


@mock.patch.dict(os.environ, {}, clear=True)
def test_returns_nice_error_in_case_of_connectivity_error(runner):
    result = runner.invoke(["sql", "-q", "select 1"])

    assert result.exit_code == 1, result.output
    assert "Invalid connection configuration" in result.output
    assert "User is empty" in result.output


@mock.patch("snowflake.connector")
def test_no_output_from_connection(mock_connect, runner):
    funny_text = "what's taters, my precious?"

    def _mock(*args, **kwargs):
        print(funny_text)
        return mock.MagicMock()

    mock_connect.connect = _mock

    result = runner.invoke(["sql", "-q", "select 1"])
    assert funny_text not in result.output


@mock.patch.dict(os.environ, {}, clear=True)
def test_returns_nice_error_in_case_of_master_token_without_temporary_connection(
    runner,
):
    result = runner.invoke(
        ["sql", "-q", "select 1", "--master-token", "dummy-master-token"]
    )
    assert result.exit_code == 1
    assert (
        "When using a session or master token, you must use a temporary connection"
        in result.output
    )


@mock.patch.dict(os.environ, {}, clear=True)
def test_returns_nice_error_in_case_of_session_token_without_temporary_connection(
    runner,
):
    result = runner.invoke(
        ["sql", "-q", "select 1", "--session-token", "dummy-session-token"]
    )
    assert result.exit_code == 1
    assert (
        "When using a session or master token, you must use a temporary connection"
        in result.output
    )


@mock.patch.dict(os.environ, {}, clear=True)
def test_returns_nice_error_in_case_of_missing_session_token(runner):
    result = runner.invoke(
        ["sql", "-q", "select 1", "--master-token", "dummy-master-token", "-x"]
    )
    assert result.exit_code == 1
    assert (
        "When using a master token, you must provide the corresponding session token"
        in result.output
    )


@mock.patch.dict(os.environ, {}, clear=True)
def test_returns_nice_error_in_case_of_missing_master_token(runner):
    result = runner.invoke(
        ["sql", "-q", "select 1", "--session-token", "dummy-session-token", "-x"]
    )
    assert result.exit_code == 1
    assert (
        "When using a session token, you must provide the corresponding master token"
        in result.output
    )


@mock.patch("snowflake.connector.connect")
@pytest.mark.parametrize("feature_flag", [None, True, False])
def test_internal_application_data_is_sent_if_feature_flag_is_set(
    mock_connect, runner, feature_flag
):
    expected_kwargs = {
        "application": "SNOWCLI.SQL",
        "database": "db_for_test",
        "schema": "test_public",
        "role": "test_role",
        "warehouse": "xs",
        "password": "dummy_password",
        "application_name": "snowcli",
        "using_session_keep_alive": True,
    }
    env = {}
    if feature_flag is not None:
        env["SNOWFLAKE_CLI_FEATURES_ENABLE_SEPARATE_AUTHENTICATION_POLICY_ID"] = str(
            feature_flag
        )
    if feature_flag:
        # internal app data should be disabled by default
        expected_kwargs["internal_application_name"] = "SNOWFLAKE_CLI"
        expected_kwargs["internal_application_version"] = "0.0.0-test_patched"
    with mock.patch.dict(os.environ, env):
        result = runner.invoke(["sql", "-q", "select 1"])
    assert result.exit_code == 0
    mock_connect.assert_called_once_with(**expected_kwargs)
