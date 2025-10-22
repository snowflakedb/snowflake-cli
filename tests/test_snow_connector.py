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
from contextlib import nullcontext
from unittest import mock

import pytest
from snowflake.cli._app.constants import AUTHENTICATOR_WORKLOAD_IDENTITY
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.secret import SecretType
from snowflake.connector.auth.workload_identity import ApiFederatedAuthenticationType

from tests_common.feature_flag_utils import with_feature_flags


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
    if feature_flag:
        # internal app data should be disabled by default
        expected_kwargs["internal_application_name"] = "SNOWFLAKE_CLI"
        expected_kwargs["internal_application_version"] = "0.0.0-test_patched"
    with with_feature_flags(
        {FeatureFlag.ENABLE_SEPARATE_AUTHENTICATION_POLICY_ID: feature_flag}
    ) if feature_flag is not None else nullcontext():
        result = runner.invoke(["sql", "-q", "select 1"])
    assert result.exit_code == 0
    mock_connect.assert_called_once_with(**expected_kwargs)


@mock.patch("snowflake.cli._app.snow_connector.OidcManager")
@mock.patch("snowflake.connector.connect")
def test_maybe_update_oidc_token_sets_token_when_token_available(
    mock_connect, mock_oidc_manager_class, test_snowcli_config
):
    """Test that _maybe_update_oidc_token properly sets token value."""
    from snowflake.cli._app.snow_connector import _maybe_update_oidc_token

    # Setup mock
    mock_manager = mock.Mock()
    mock_oidc_manager_class.return_value = mock_manager
    mock_token = "test-oidc-token-123"
    mock_manager.read_token.return_value = mock_token

    # Test connection parameters
    connection_parameters = {
        "authenticator": AUTHENTICATOR_WORKLOAD_IDENTITY,
        "account": "test_account",
        "user": "test_user",
    }

    # Call the function
    result = _maybe_update_oidc_token(connection_parameters)

    # Verify the token is set correctly
    assert connection_parameters["token"] == mock_token
    assert result == connection_parameters


@mock.patch("snowflake.cli._app.snow_connector.OidcManager")
@mock.patch("snowflake.connector.connect")
def test_maybe_update_oidc_token_handles_exception_gracefully(
    mock_connect, mock_oidc_manager_class, test_snowcli_config
):
    """Test that _maybe_update_oidc_token handles exceptions without failing."""
    from snowflake.cli._app.snow_connector import _maybe_update_oidc_token

    # Setup mock to raise exception
    mock_manager = mock.Mock()
    mock_oidc_manager_class.return_value = mock_manager
    mock_manager.read_token.side_effect = Exception("Token fetch failed")

    # Test connection parameters
    connection_parameters = {
        "authenticator": AUTHENTICATOR_WORKLOAD_IDENTITY,
        "account": "test_account",
        "user": "test_user",
    }
    original_params = connection_parameters.copy()

    # Call the function
    result = _maybe_update_oidc_token(connection_parameters)

    # Verify parameters are unchanged when exception occurs
    assert connection_parameters == original_params
    assert result == connection_parameters
    assert "token" not in connection_parameters


@mock.patch("snowflake.cli._app.snow_connector.OidcManager")
@mock.patch("snowflake.connector.connect")
def test_maybe_update_oidc_token_no_update_when_no_token(
    mock_connect, mock_oidc_manager_class, test_snowcli_config
):
    """Test that _maybe_update_oidc_token doesn't update when no token is returned."""
    from snowflake.cli._app.snow_connector import _maybe_update_oidc_token

    # Setup mock to return None (no token)
    mock_manager = mock.Mock()
    mock_oidc_manager_class.return_value = mock_manager
    mock_manager.read_token.return_value = None

    # Test connection parameters
    connection_parameters = {
        "authenticator": AUTHENTICATOR_WORKLOAD_IDENTITY,
        "account": "test_account",
        "user": "test_user",
    }
    original_params = connection_parameters.copy()

    # Call the function
    result = _maybe_update_oidc_token(connection_parameters)

    # Verify parameters are unchanged when no token is returned
    assert connection_parameters == original_params
    assert result == connection_parameters
    assert "token" not in connection_parameters


@mock.patch("snowflake.cli._app.snow_connector.command_info")
@mock.patch("snowflake.cli._app.snow_connector.OidcManager")
@mock.patch("snowflake.connector.connect")
def test_oidc_token_integration_in_connect_to_snowflake(
    mock_connect, mock_oidc_manager_class, mock_command_info, test_snowcli_config
):
    """Test that OIDC token is properly integrated in the full connect_to_snowflake flow."""
    from snowflake.cli._app.snow_connector import connect_to_snowflake
    from snowflake.cli.api.config import config_init

    config_init(test_snowcli_config)

    # Setup mocks
    mock_command_info.return_value = "SNOWCLI.TEST"
    mock_manager = mock.Mock()
    mock_oidc_manager_class.return_value = mock_manager
    mock_token = "integration-test-token-456"
    mock_manager.read_token.return_value = mock_token

    # Use temporary connection with WORKLOAD_IDENTITY authenticator and OIDC provider
    connect_to_snowflake(
        temporary_connection=True,
        authenticator=AUTHENTICATOR_WORKLOAD_IDENTITY,
        workload_identity_provider=ApiFederatedAuthenticationType.OIDC.value,
        account="test_account",
        user="test_user",
    )

    # Verify snowflake.connector.connect was called with the token
    mock_connect.assert_called_once()
    call_kwargs = mock_connect.call_args[1]

    assert call_kwargs["token"] == mock_token
    assert call_kwargs["authenticator"] == AUTHENTICATOR_WORKLOAD_IDENTITY


@mock.patch("snowflake.cli._app.snow_connector.command_info")
@mock.patch("snowflake.cli._app.snow_connector.OidcManager")
@mock.patch("snowflake.connector.connect")
def test_oidc_token_not_set_when_workload_identity_provider_not_oidc(
    mock_connect, mock_oidc_manager_class, mock_command_info, test_snowcli_config
):
    """Test that OIDC token is NOT set when workload_identity_provider is not 'OIDC'."""
    from snowflake.cli._app.snow_connector import connect_to_snowflake
    from snowflake.cli.api.config import config_init

    config_init(test_snowcli_config)

    # Setup mocks
    mock_command_info.return_value = "SNOWCLI.TEST"
    mock_manager = mock.Mock()
    mock_oidc_manager_class.return_value = mock_manager
    mock_token = "should-not-be-set-token"
    mock_manager.read_token.return_value = mock_token

    # Use temporary connection with WORKLOAD_IDENTITY authenticator but different provider
    connect_to_snowflake(
        temporary_connection=True,
        authenticator=AUTHENTICATOR_WORKLOAD_IDENTITY,
        workload_identity_provider=ApiFederatedAuthenticationType.AWS.value,  # Not OIDC
        account="test_account",
        user="test_user",
    )

    # Verify snowflake.connector.connect was called without the token
    mock_connect.assert_called_once()
    call_kwargs = mock_connect.call_args[1]

    assert "token" not in call_kwargs
    assert call_kwargs["authenticator"] == AUTHENTICATOR_WORKLOAD_IDENTITY
    assert (
        call_kwargs["workload_identity_provider"]
        == ApiFederatedAuthenticationType.AWS.value
    )
    # Verify that OidcManager was not called at all
    mock_oidc_manager_class.assert_not_called()


@mock.patch("snowflake.cli._app.snow_connector.command_info")
@mock.patch("snowflake.cli._app.snow_connector.OidcManager")
@mock.patch("snowflake.connector.connect")
def test_oidc_token_not_set_when_workload_identity_provider_missing(
    mock_connect, mock_oidc_manager_class, mock_command_info, test_snowcli_config
):
    """Test that OIDC token is NOT set when workload_identity_provider is not specified."""
    from snowflake.cli._app.snow_connector import connect_to_snowflake
    from snowflake.cli.api.config import config_init

    config_init(test_snowcli_config)

    # Setup mocks
    mock_command_info.return_value = "SNOWCLI.TEST"
    mock_manager = mock.Mock()
    mock_oidc_manager_class.return_value = mock_manager
    mock_token = "should-not-be-set-token"
    mock_manager.read_token.return_value = mock_token

    # Use temporary connection with WORKLOAD_IDENTITY authenticator but no provider
    connect_to_snowflake(
        temporary_connection=True,
        authenticator=AUTHENTICATOR_WORKLOAD_IDENTITY,
        # workload_identity_provider not specified
        account="test_account",
        user="test_user",
    )

    # Verify snowflake.connector.connect was called without the token
    mock_connect.assert_called_once()
    call_kwargs = mock_connect.call_args[1]

    assert "token" not in call_kwargs
    assert call_kwargs["authenticator"] == AUTHENTICATOR_WORKLOAD_IDENTITY
    # workload_identity_provider should not be in call_kwargs since it wasn't provided
    assert "workload_identity_provider" not in call_kwargs
    # Verify that OidcManager was not called at all
    mock_oidc_manager_class.assert_not_called()
