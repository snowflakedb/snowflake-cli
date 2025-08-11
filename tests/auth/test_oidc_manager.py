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

from enum import Enum
from unittest.mock import Mock, patch

import pytest
from snowflake.cli._app.auth.errors import (
    OidcProviderAutoDetectionError,
    OidcProviderNotFoundError,
    OidcProviderUnavailableError,
    OidcTokenRetrievalError,
)
from snowflake.cli._app.auth.oidc_providers import (
    OidcProviderType,
    OidcProviderTypeWithAuto,
)
from snowflake.cli._plugins.auth.oidc.manager import OidcManager
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
            provider_type=OidcProviderType.GITHUB,
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
        error_message = f"Provider '{OidcProviderType.GITHUB}' not available"
        mock_get_provider.side_effect = OidcProviderUnavailableError(error_message)

        with pytest.raises(CliError, match=error_message):
            manager.setup(
                "test_user",
                "repo:owner/repo:environment:prod",
                "test_role",
                OidcProviderType.GITHUB,
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
                manager.setup(
                    "test_user",
                    "",
                    "test_role",
                    OidcProviderType.GITHUB,
                )

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
                "test_user",
                "repo:owner/repo:environment:prod",
                "test_role",
                OidcProviderType.GITHUB,
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
            provider_type=OidcProviderType.GITHUB,
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
        error_message = f"Unknown provider '{OidcProviderType.GITHUB}'"
        mock_get_provider.side_effect = OidcProviderNotFoundError(error_message)

        with pytest.raises(CliError, match=error_message):
            manager.setup(
                "test_user",
                "repo:owner/repo:environment:prod",
                "test_role",
                OidcProviderType.GITHUB,
            )

    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_delete_drops_federated_user(self, mock_execute_query):
        """Test that delete method drops a federated user."""
        # Mock search results - return one user found
        mock_search_cursor = Mock()
        mock_search_cursor.fetchall.return_value = [("test_user", True)]

        # Mock second call for DROP USER
        mock_drop_cursor = Mock()

        # Set up mock to return different results for each call
        mock_execute_query.side_effect = [mock_search_cursor, mock_drop_cursor]

        manager = OidcManager()
        result = manager.delete(user="test_user")

        # Verify the SQL commands were executed
        assert mock_execute_query.call_count == 2

        # Check search query
        search_call = mock_execute_query.call_args_list[0][0][0]
        assert "show terse users" in search_call
        assert "has_workload_identity" in search_call
        assert "test_user" in search_call

        # Check drop query
        drop_call = mock_execute_query.call_args_list[1][0][0]
        assert 'DROP USER "test_user"' in drop_call

        # Verify return message
        assert "Successfully deleted federated user 'test_user'" in result

    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_delete_parameter_validation(self, mock_execute_query):
        """Test parameter validation in delete method."""
        manager = OidcManager()

        # Test empty user name
        with pytest.raises(CliError, match="Federated user name cannot be empty"):
            manager.delete("")

        # Test whitespace only user name
        with pytest.raises(CliError, match="Federated user name cannot be empty"):
            manager.delete("   ")

    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_delete_sql_exception_handling(self, mock_execute_query):
        """Test that delete method handles SQL execution exceptions."""
        manager = OidcManager()
        mock_execute_query.side_effect = Exception("SQL execution failed")

        # The new implementation should let the exception bubble up from execute_query
        with pytest.raises(Exception, match="SQL execution failed"):
            manager.delete("test_user")

    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_delete_user_not_found(self, mock_execute_query):
        """Test delete when user is not found."""
        # Mock search results - return no users found
        mock_search_cursor = Mock()
        mock_search_cursor.fetchall.return_value = []

        mock_execute_query.return_value = mock_search_cursor

        manager = OidcManager()

        with pytest.raises(CliError, match="Federated 'test_user' user not found"):
            manager.delete("test_user")

        # Verify only search query was executed
        assert mock_execute_query.call_count == 1
        search_call = mock_execute_query.call_args_list[0][0][0]
        assert "show terse users" in search_call

    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_delete_multiple_users_found(self, mock_execute_query):
        """Test delete when multiple users are found."""
        # Mock search results - return multiple users found
        mock_search_cursor = Mock()
        mock_search_cursor.fetchall.return_value = [
            ("test_user_1", True),
            ("test_user_2", True),
        ]

        mock_execute_query.return_value = mock_search_cursor

        manager = OidcManager()

        with pytest.raises(
            CliError, match="Error searching for federated user 'test_user'"
        ):
            manager.delete("test_user")

        # Verify only search query was executed
        assert mock_execute_query.call_count == 1
        search_call = mock_execute_query.call_args_list[0][0][0]
        assert "show terse users" in search_call

    def test_read_token_with_auto_type(self):
        """Test read_token method with auto type delegates to auto_detect_oidc_provider."""
        manager = OidcManager()

        with patch(
            "snowflake.cli._plugins.auth.oidc.manager.auto_detect_oidc_provider"
        ) as mock_auto_detect:
            mock_provider = Mock()
            mock_provider.provider_name = "github"
            mock_provider.get_token.return_value = "auto detect result"
            mock_auto_detect.return_value = mock_provider

            result = manager.read_token(OidcProviderTypeWithAuto.AUTO)

            mock_auto_detect.assert_called_once()
            mock_provider.get_token.assert_called_once()
            assert result == "auto detect result"

    def test_read_token_with_specific_type(self):
        """Test read_token method with specific provider type delegates to get_active_oidc_provider."""
        manager = OidcManager()

        with patch(
            "snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider"
        ) as mock_get_provider:
            mock_provider = Mock()
            mock_provider.provider_name = "github"
            mock_provider.get_token.return_value = "specific result"
            mock_get_provider.return_value = mock_provider

            result = manager.read_token(OidcProviderType.GITHUB)

            mock_get_provider.assert_called_once_with(OidcProviderType.GITHUB.value)
            mock_provider.get_token.assert_called_once()
            assert result == "specific result"

    @patch("snowflake.cli._plugins.auth.oidc.manager.auto_detect_oidc_provider")
    def test_read_token_auto_detect_token_success(self, mock_auto_detect):
        """Test read_token with auto when provider is available and working."""
        # Create mock provider
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB
        mock_provider.get_token.return_value = "mock_token"

        mock_auto_detect.return_value = mock_provider

        manager = OidcManager()
        result = manager.read_token(OidcProviderTypeWithAuto.AUTO)

        mock_auto_detect.assert_called_once()
        mock_provider.get_token.assert_called_once()
        assert result == "mock_token"

    @patch("snowflake.cli._plugins.auth.oidc.manager.auto_detect_oidc_provider")
    def test_read_token_auto_detect_token_success_no_info(self, mock_auto_detect):
        """Test read_token with auto when provider works but has no token info."""
        # Create mock provider with no token info
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.get_token.return_value = "mock_token"
        mock_provider.get_token_info.return_value = {}

        mock_auto_detect.return_value = mock_provider

        manager = OidcManager()
        result = manager.read_token(OidcProviderTypeWithAuto.AUTO)

        mock_auto_detect.assert_called_once()
        mock_provider.get_token.assert_called_once()
        assert result == "mock_token"

    @patch("snowflake.cli._plugins.auth.oidc.manager.auto_detect_oidc_provider")
    def test_read_token_auto_detect_token_no_provider_with_available_providers(
        self, mock_auto_detect
    ):
        """Test read_token with auto when no provider detected but providers are registered."""

        error_message = "No OIDC provider detected in current environment. Available providers: github, other_provider. Use --type <provider> to specify a provider explicitly."
        mock_auto_detect.side_effect = OidcProviderAutoDetectionError(error_message)

        manager = OidcManager()

        with pytest.raises(
            CliError, match="No OIDC provider detected in current environment"
        ):
            manager.read_token(OidcProviderTypeWithAuto.AUTO)

    @patch("snowflake.cli._plugins.auth.oidc.manager.auto_detect_oidc_provider")
    def test_read_token_auto_detect_token_provider_fails(self, mock_auto_detect):
        """Test read_token with auto when provider fails to get token."""
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        error_message = "Token retrieval failed"
        mock_provider.get_token.side_effect = OidcTokenRetrievalError(error_message)

        mock_auto_detect.return_value = mock_provider

        manager = OidcManager()

        with pytest.raises(CliError, match=error_message):
            manager.read_token(OidcProviderTypeWithAuto.AUTO)

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider")
    def test_read_token_specific_token_success(self, mock_get_provider):
        """Test read_token with specific provider when provider exists and works."""
        # Create mock provider
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.is_available = True
        mock_provider.get_token.return_value = "mock_token"

        mock_get_provider.return_value = mock_provider

        manager = OidcManager()
        result = manager.read_token(OidcProviderType.GITHUB)

        mock_get_provider.assert_called_once_with(OidcProviderType.GITHUB.value)
        mock_provider.get_token.assert_called_once()
        assert result == "mock_token"

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider")
    def test_read_token_specific_token_provider_not_found(self, mock_get_provider):
        """Test read_token with specific provider when provider doesn't exist."""

        class InvalidProvider(Enum):
            UNKNOWN_PROVIDER = "unknown_provider"

        error_message = f"Unknown provider {InvalidProvider.UNKNOWN_PROVIDER}. Available providers: github, other_provider"
        mock_get_provider.side_effect = OidcProviderNotFoundError(error_message)

        manager = OidcManager()

        with pytest.raises(
            CliError, match=f"Unknown provider {InvalidProvider.UNKNOWN_PROVIDER}"
        ):
            manager.read_token(InvalidProvider.UNKNOWN_PROVIDER)  # type: ignore

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider")
    def test_read_token_specific_token_provider_not_available(self, mock_get_provider):
        """Test read_token with specific provider when provider exists but is not available."""
        error_message = f"Provider '{OidcProviderType.GITHUB.value}' is not available in the current environment."
        mock_get_provider.side_effect = OidcProviderUnavailableError(error_message)

        manager = OidcManager()

        with pytest.raises(
            CliError,
            match=f"Provider '{OidcProviderType.GITHUB.value}' is not available in the current environment",
        ):
            manager.read_token(OidcProviderType.GITHUB)

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider")
    def test_read_token_specific_token_provider_fails(self, mock_get_provider):
        """Test read_token with specific provider when provider exists but fails to get token."""
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.is_available = True
        error_message = "Token retrieval failed"
        mock_provider.get_token.side_effect = OidcTokenRetrievalError(error_message)

        mock_get_provider.return_value = mock_provider

        manager = OidcManager()

        with pytest.raises(CliError, match=error_message):
            manager.read_token(OidcProviderType.GITHUB)

    def test_manager_inherits_from_sql_execution_mixin(self):
        """Test that OidcManager inherits from SqlExecutionMixin."""
        from snowflake.cli.api.sql_execution import SqlExecutionMixin

        manager = OidcManager()
        assert isinstance(manager, SqlExecutionMixin)

    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_list_sql_exception_handling(self, mock_execute_query):
        """Test that list method handles SQL execution exceptions."""
        manager = OidcManager()
        mock_execute_query.side_effect = Exception("SQL execution failed")

        with pytest.raises(
            CliError,
            match="Failed to list users with OIDC federated authentication: SQL execution failed",
        ):
            manager.get_users_list()

    @patch("snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query")
    def test_list_empty_results(self, mock_execute_query):
        """Test list method when no users have workload identity enabled."""
        # Mock empty cursor response
        mock_cursor = Mock()
        mock_cursor.rowcount = 0
        mock_cursor.__iter__ = Mock(return_value=iter([]))

        # Set up mock for the single call
        mock_execute_query.return_value = mock_cursor

        manager = OidcManager()
        result = manager.get_users_list()

        # Verify result is the cursor
        assert result == mock_cursor
        assert mock_execute_query.call_count == 1
