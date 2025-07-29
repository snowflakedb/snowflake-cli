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

from unittest.mock import Mock, patch

import pytest
from snowflake.cli._app.auth.oidc_providers import OidcProviderType
from snowflake.cli._plugins.auth.oidc.manager import (
    OidcManager,
)
from snowflake.cli.api.exceptions import CliError


class TestOidcManager:
    """Test cases for OidcManager."""

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_oidc_provider")
    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_setup_creates_federated_user(self, mock_execute_query, mock_get_provider):
        """Test that setup method creates a federated user with WORKLOAD_IDENTITY syntax."""
        # Mock the provider
        mock_provider = Mock()
        mock_provider.issuer = "https://token.actions.githubusercontent.com"
        mock_get_provider.return_value = mock_provider

        manager = OidcManager()

        result = manager.setup(
            user="test_user",
            subject="repo:owner/repo:environment:prod",
            default_role="test_role",
            provider_type="github",
        )

        # Verify the correct provider was requested
        mock_get_provider.assert_called_once_with("github")

        # Verify the SQL command was executed once (CREATE USER only)
        mock_execute_query.assert_called_once()

        # Check CREATE USER call
        create_user_call = mock_execute_query.call_args[0][0]

        # Verify the SQL contains expected elements with WORKLOAD_IDENTITY
        assert "CREATE USER test_user" in create_user_call
        assert "WORKLOAD_IDENTITY = (" in create_user_call
        assert "FEDERATED_AUTHENTICATION" not in create_user_call
        assert "TYPE = 'OIDC'" in create_user_call
        assert (
            "ISSUER = 'https://token.actions.githubusercontent.com'" in create_user_call
        )
        assert "SUBJECT = 'repo:owner/repo:environment:prod'" in create_user_call
        assert "DEFAULT_ROLE = test_role" in create_user_call

        # Verify return message
        assert "Successfully created federated user 'test_user'" in result

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_oidc_provider")
    def test_setup_provider_fails(self, mock_get_provider):
        """Test setup when provider fails."""
        manager = OidcManager()
        mock_get_provider.side_effect = CliError("Provider 'github' not available")

        with pytest.raises(CliError, match="Failed to get OIDC provider 'github'"):
            manager.setup(
                "test_user", "repo:owner/repo:environment:prod", "test_role", "github"
            )

    def test_setup_parameter_validation(self):
        """Test parameter validation in setup method."""
        manager = OidcManager()

        with patch(
            "snowflake.cli._plugins.auth.oidc.manager.get_oidc_provider"
        ) as mock_get_provider:
            # Mock the provider
            mock_provider = Mock()
            mock_provider.issuer = "https://token.actions.githubusercontent.com"
            mock_get_provider.return_value = mock_provider

            # Test empty subject
            with pytest.raises(CliError, match="Subject cannot be empty"):
                manager.setup("test_user", "", "test_role", "github")

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_oidc_provider")
    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_setup_sql_exception_handling(self, mock_execute_query, mock_get_provider):
        """Test that setup method handles SQL execution exceptions."""
        # Mock the provider
        mock_provider = Mock()
        mock_provider.issuer = "https://token.actions.githubusercontent.com"
        mock_get_provider.return_value = mock_provider

        # Mock CREATE USER to fail
        mock_execute_query.side_effect = Exception("SQL execution failed")

        manager = OidcManager()

        with pytest.raises(
            CliError,
            match="Failed to create federated user 'test_user': SQL execution failed",
        ):
            manager.setup(
                "test_user", "repo:owner/repo:environment:prod", "test_role", "github"
            )

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_oidc_provider")
    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_setup_with_custom_subject(self, mock_execute_query, mock_get_provider):
        """Test that setup method works with a custom subject."""
        # Mock the provider
        mock_provider = Mock()
        mock_provider.issuer = "https://token.actions.githubusercontent.com"
        mock_get_provider.return_value = mock_provider

        manager = OidcManager()

        custom_subject = "repo:custom/repo:environment:test"
        result = manager.setup(
            user="test_user",
            subject=custom_subject,
            default_role="test_role",
            provider_type="github",
        )

        # Verify the SQL command was executed once
        mock_execute_query.assert_called_once()

        # Check CREATE USER call
        create_user_call = mock_execute_query.call_args[0][0]

        # Verify the SQL contains expected elements with custom subject
        assert "CREATE USER test_user" in create_user_call
        assert "WORKLOAD_IDENTITY = (" in create_user_call
        assert "TYPE = 'OIDC'" in create_user_call
        assert (
            "ISSUER = 'https://token.actions.githubusercontent.com'" in create_user_call
        )
        assert f"SUBJECT = '{custom_subject}'" in create_user_call
        assert "DEFAULT_ROLE = test_role" in create_user_call

        # Verify return message
        assert "Successfully created federated user 'test_user'" in result

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_oidc_provider")
    def test_setup_provider_not_found(self, mock_get_provider):
        """Test setup with non-existent provider."""
        manager = OidcManager()
        mock_get_provider.side_effect = CliError("Unknown provider 'github'")

        with pytest.raises(CliError, match="Failed to get OIDC provider 'github'"):
            manager.setup(
                "test_user", "repo:owner/repo:environment:prod", "test_role", "github"
            )

    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_delete_drops_federated_user(self, mock_execute_query):
        """Test that delete method drops a federated user."""
        manager = OidcManager()

        result = manager.delete(user="test_user")

        # Verify the SQL command was executed
        mock_execute_query.assert_called_once_with("DROP USER test_user")

        # Verify return message
        assert "Successfully deleted federated user 'test_user'" in result

    def test_delete_parameter_validation(self):
        """Test parameter validation in delete method."""
        manager = OidcManager()

        # Test empty user name
        with pytest.raises(CliError, match="Federated user name cannot be empty"):
            manager.delete("")

        # Test invalid user name
        with pytest.raises(CliError, match="Invalid federated user name"):
            manager.delete("123invalid")

    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_delete_sql_exception_handling(self, mock_execute_query):
        """Test that delete method handles SQL execution exceptions."""
        manager = OidcManager()
        mock_execute_query.side_effect = Exception("SQL execution failed")

        with pytest.raises(
            CliError,
            match="Failed to delete federated user 'test_user': SQL execution failed",
        ):
            manager.delete("test_user")

    def test_read_with_auto_type(self):
        """Test read method with auto type delegates to auto_detect_oidc_provider."""
        manager = OidcManager()

        with patch(
            "snowflake.cli._plugins.auth.oidc.manager.auto_detect_oidc_provider"
        ) as mock_auto_detect:
            mock_provider = Mock()
            mock_provider.provider_name = "github"
            mock_provider.get_token.return_value = "auto detect result"
            mock_auto_detect.return_value = mock_provider

            result = manager.read("auto")

            mock_auto_detect.assert_called_once()
            mock_provider.get_token.assert_called_once()
            assert result == "auto detect result"

    def test_read_with_specific_type(self):
        """Test read method with specific provider type delegates to get_active_oidc_provider."""
        manager = OidcManager()

        with patch(
            "snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider"
        ) as mock_get_provider:
            mock_provider = Mock()
            mock_provider.provider_name = "github"
            mock_provider.get_token.return_value = "specific result"
            mock_get_provider.return_value = mock_provider

            result = manager.read(OidcProviderType.GITHUB.value)

            mock_get_provider.assert_called_once_with(OidcProviderType.GITHUB.value)
            mock_provider.get_token.assert_called_once()
            assert result == "specific result"

    @patch("snowflake.cli._plugins.auth.oidc.manager.auto_detect_oidc_provider")
    def test_read_auto_detect_token_success(self, mock_auto_detect):
        """Test read with auto when provider is available and working."""
        # Create mock provider
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.get_token.return_value = "mock_token"

        mock_auto_detect.return_value = mock_provider

        manager = OidcManager()
        result = manager.read("auto")

        mock_auto_detect.assert_called_once()
        mock_provider.get_token.assert_called_once()
        assert result == "mock_token"

    @patch("snowflake.cli._plugins.auth.oidc.manager.auto_detect_oidc_provider")
    def test_read_auto_detect_token_success_no_info(self, mock_auto_detect):
        """Test read with auto when provider works but has no token info."""
        # Create mock provider with no token info
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.get_token.return_value = "mock_token"
        mock_provider.get_token_info.return_value = {}

        mock_auto_detect.return_value = mock_provider

        manager = OidcManager()
        result = manager.read("auto")

        mock_auto_detect.assert_called_once()
        mock_provider.get_token.assert_called_once()
        assert result == "mock_token"

    @patch("snowflake.cli._plugins.auth.oidc.manager.auto_detect_oidc_provider")
    def test_read_auto_detect_token_no_provider_with_available_providers(
        self, mock_auto_detect
    ):
        """Test read with auto when no provider detected but providers are registered."""
        mock_auto_detect.side_effect = CliError(
            "No OIDC provider detected in current environment. Available providers: github, other_provider. Use --type <provider> to specify a provider explicitly."
        )

        manager = OidcManager()

        with pytest.raises(
            CliError, match="No OIDC provider detected in current environment"
        ):
            manager.read("auto")

    @patch("snowflake.cli._plugins.auth.oidc.manager.auto_detect_oidc_provider")
    def test_read_auto_detect_token_provider_fails(self, mock_auto_detect):
        """Test read with auto when provider fails to get token."""
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.get_token.side_effect = Exception("Token retrieval failed")

        mock_auto_detect.return_value = mock_provider

        manager = OidcManager()

        with pytest.raises(
            CliError,
            match="Failed to read OIDC token: Token retrieval failed",
        ):
            manager.read("auto")

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider")
    def test_read_specific_token_success(self, mock_get_provider):
        """Test read with specific provider when provider exists and works."""
        # Create mock provider
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.is_available = True
        mock_provider.get_token.return_value = "mock_token"

        mock_get_provider.return_value = mock_provider

        manager = OidcManager()
        result = manager.read(OidcProviderType.GITHUB.value)

        mock_get_provider.assert_called_once_with(OidcProviderType.GITHUB.value)
        mock_provider.get_token.assert_called_once()
        assert result == "mock_token"

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider")
    def test_read_specific_token_provider_not_found(self, mock_get_provider):
        """Test read with specific provider when provider doesn't exist."""
        mock_get_provider.side_effect = CliError(
            "Unknown provider 'unknown_provider'. Available providers: github, other_provider"
        )

        manager = OidcManager()

        with pytest.raises(CliError, match="Unknown provider 'unknown_provider'"):
            manager.read("unknown_provider")

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider")
    def test_read_specific_token_provider_not_available(self, mock_get_provider):
        """Test read with specific provider when provider exists but is not available."""
        mock_get_provider.side_effect = CliError(
            f"Provider '{OidcProviderType.GITHUB.value}' is not available in the current environment."
        )

        manager = OidcManager()

        with pytest.raises(
            CliError,
            match=f"Provider '{OidcProviderType.GITHUB.value}' is not available in the current environment",
        ):
            manager.read(OidcProviderType.GITHUB.value)

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider")
    def test_read_specific_token_provider_fails(self, mock_get_provider):
        """Test read with specific provider when provider exists but fails to get token."""
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.is_available = True
        mock_provider.get_token.side_effect = Exception("Token retrieval failed")

        mock_get_provider.return_value = mock_provider

        manager = OidcManager()

        with pytest.raises(
            CliError,
            match="Failed to read OIDC token: Token retrieval failed",
        ):
            manager.read(OidcProviderType.GITHUB.value)

    def test_manager_inherits_from_sql_execution_mixin(self):
        """Test that OidcManager inherits from SqlExecutionMixin."""
        from snowflake.cli.api.sql_execution import SqlExecutionMixin

        manager = OidcManager()
        assert isinstance(manager, SqlExecutionMixin)

    def test_read_method_parameter_validation(self):
        """Test read method with different parameter types."""
        manager = OidcManager()

        # Test with empty string (should be treated as specific provider)
        with patch(
            "snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider"
        ) as mock_get_provider:
            mock_provider = Mock()
            mock_provider.provider_name = "empty"
            mock_provider.get_token.return_value = "empty result"
            mock_get_provider.return_value = mock_provider

            result = manager.read("")
            mock_get_provider.assert_called_once_with("")
            mock_provider.get_token.assert_called_once()
            assert result == "empty result"

    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_list_with_oidc_enabled(self, mock_execute_query):
        """Test list method uses has_workload_identity column."""
        # Mock parameter check response (first call)
        mock_parameter_cursor = Mock()
        mock_parameter_cursor.fetchone.return_value = {
            "key": "ENABLE_USERS_HAS_WORKLOAD_IDENTITY",
            "value": "true",
        }

        # Mock users query response with a cursor-like object (second call)
        mock_cursor = Mock()
        mock_cursor.rowcount = 2
        users_response = [
            {"name": "user1", "has_workload_identity": True},
            {"name": "user2", "has_workload_identity": True},
        ]
        # Make the cursor behave like the actual cursor for iteration/result access
        mock_cursor.__iter__ = Mock(return_value=iter(users_response))

        # Set up side effects for the two calls
        mock_execute_query.side_effect = [mock_parameter_cursor, mock_cursor]

        manager = OidcManager()
        result = manager.get_users_list()

        # Verify the correct queries were executed
        assert mock_execute_query.call_count == 2

        # Check parameter query (first call)
        parameter_call = mock_execute_query.call_args_list[0]
        assert "ENABLE_USERS_HAS_WORKLOAD_IDENTITY" in parameter_call[0][0]

        # Check users query uses has_workload_identity column (second call)
        users_call = mock_execute_query.call_args_list[1]
        users_query = users_call[0][0]
        assert "has_workload_identity" in users_query
        assert "has_federated_workload_authentication" not in users_query

        # Verify result
        assert result == mock_cursor

    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_list_sql_exception_handling(self, mock_execute_query):
        """Test that list method handles SQL execution exceptions."""
        manager = OidcManager()
        mock_execute_query.side_effect = Exception("SQL execution failed")

        with pytest.raises(
            CliError,
            match="Failed to list users with workload identity: SQL execution failed",
        ):
            manager.get_users_list()

    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_list_empty_results(self, mock_execute_query):
        """Test list method when no users have workload identity enabled."""
        # Mock parameter check response (first call)
        mock_parameter_cursor = Mock()
        mock_parameter_cursor.fetchone.return_value = {
            "key": "ENABLE_USERS_HAS_WORKLOAD_IDENTITY",
            "value": "false",
        }

        # Mock empty cursor response (second call)
        mock_cursor = Mock()
        mock_cursor.rowcount = 0
        mock_cursor.__iter__ = Mock(return_value=iter([]))

        # Set up side effects for the two calls
        mock_execute_query.side_effect = [mock_parameter_cursor, mock_cursor]

        manager = OidcManager()
        result = manager.get_users_list()

        # Verify result is the cursor
        assert result == mock_cursor
        assert mock_execute_query.call_count == 2

    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_list_uses_legacy_column_when_parameter_disabled(self, mock_execute_query):
        """Test list method uses legacy column when ENABLE_USERS_HAS_WORKLOAD_IDENTITY is false."""
        # Mock parameter check response (first call) - parameter disabled
        mock_parameter_cursor = Mock()
        mock_parameter_cursor.fetchone.return_value = {
            "key": "ENABLE_USERS_HAS_WORKLOAD_IDENTITY",
            "value": "false",
        }

        # Mock users query response with a cursor-like object (second call)
        mock_cursor = Mock()
        mock_cursor.rowcount = 1
        users_response = [
            {"name": "legacy_user", "has_federated_workload_authentication": True},
        ]
        mock_cursor.__iter__ = Mock(return_value=iter(users_response))

        # Set up side effects for the two calls
        mock_execute_query.side_effect = [mock_parameter_cursor, mock_cursor]

        manager = OidcManager()
        result = manager.get_users_list()

        # Verify the correct queries were executed
        assert mock_execute_query.call_count == 2

        # Check parameter query (first call)
        parameter_call = mock_execute_query.call_args_list[0]
        assert "ENABLE_USERS_HAS_WORKLOAD_IDENTITY" in parameter_call[0][0]

        # Check users query uses legacy column (second call)
        users_call = mock_execute_query.call_args_list[1]
        users_query = users_call[0][0]
        assert "has_federated_workload_authentication" in users_query
        assert "has_workload_identity" not in users_query

        # Verify result
        assert result == mock_cursor
