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

from unittest.mock import Mock, PropertyMock, patch

import pytest
from snowflake.cli._plugins.remote.constants import SERVICE_NAME_PREFIX
from snowflake.cli._plugins.remote.manager import RemoteManager


class TestRemoteManager:
    """Test general RemoteManager functionality."""

    def test_get_current_snowflake_user(self):
        """Test getting current Snowflake user from connection."""
        manager = RemoteManager()

        with patch.object(manager, "execute_query") as mock_execute:
            from unittest.mock import Mock

            mock_cursor = Mock()
            mock_cursor.fetchone.return_value = ["JOHN_DOE"]
            mock_execute.return_value = mock_cursor

            result = manager._get_current_snowflake_user()  # noqa: SLF001
            assert result == "JOHN_DOE"
            mock_execute.assert_called_once_with("SELECT CURRENT_USER()")

    def test_get_current_snowflake_user_fallback(self):
        """Test fallback when getting current Snowflake user fails."""
        manager = RemoteManager()

        with patch.object(manager, "execute_query") as mock_execute:
            from unittest.mock import Mock

            mock_cursor = Mock()
            mock_cursor.fetchone.return_value = None
            mock_execute.return_value = mock_cursor

            result = manager._get_current_snowflake_user()  # noqa: SLF001
            assert result == "unknown"
            mock_execute.assert_called_once_with("SELECT CURRENT_USER()")

    def test_resolve_service_name_with_customer_name(self):
        """Test resolving customer input name to full service name."""
        manager = RemoteManager()

        with patch.object(
            manager, "_get_current_snowflake_user", return_value="SNOWFLAKE_USER"
        ):
            result = manager._resolve_service_name("myproject")  # noqa: SLF001
        assert result == f"{SERVICE_NAME_PREFIX}_SNOWFLAKE_USER_MYPROJECT"

    def test_resolve_service_name_with_full_service_name(self):
        """Test that full service names are passed through unchanged."""
        manager = RemoteManager()

        result = manager._resolve_service_name(  # noqa: SLF001
            f"{SERVICE_NAME_PREFIX}_admin_myproject"
        )
        assert result == f"{SERVICE_NAME_PREFIX}_admin_myproject"

    def test_create_service_with_spec_sql_generation(self):
        """Test that _create_service_with_spec generates correct SQL."""
        manager = RemoteManager()

        with patch.object(manager, "execute_query") as mock_execute:
            manager._create_service_with_spec(  # noqa: SLF001
                service_name="test_service",
                compute_pool="test_pool",
                spec_content="spec:\n  containers:\n  - name: main",
                external_access_integrations=["eai1", "eai2"],
            )

            # Verify the SQL query was called
            mock_execute.assert_called_once()
            call_args = mock_execute.call_args[0][0]

            # Check key components of the generated SQL
            assert "CREATE SERVICE test_service" in call_args
            assert "IN COMPUTE POOL test_pool" in call_args
            assert "FROM SPECIFICATION $$" in call_args
            assert "MIN_INSTANCES = 1" in call_args
            assert "MAX_INSTANCES = 1" in call_args
            assert "AUTO_RESUME = true" in call_args
            assert "EXTERNAL_ACCESS_INTEGRATIONS = (eai1,eai2)" in call_args
            assert (
                "COMMENT = 'Remote development environment created by Snowflake CLI'"
                in call_args
            )

    def test_create_service_with_spec_no_external_access(self):
        """Test _create_service_with_spec without external access integrations."""
        manager = RemoteManager()

        with patch.object(manager, "execute_query") as mock_execute:
            manager._create_service_with_spec(  # noqa: SLF001
                service_name="test_service",
                compute_pool="test_pool",
                spec_content="spec:\n  containers:\n  - name: main",
            )

            call_args = mock_execute.call_args[0][0]
            assert "EXTERNAL_ACCESS_INTEGRATIONS" not in call_args

    def test_list_services_query_format(self):
        """Test that list_services generates correct SQL query."""
        from snowflake.cli._plugins.remote.constants import SERVICE_NAME_PREFIX

        manager = RemoteManager()

        with patch.object(manager, "execute_query") as mock_execute:
            # Mock the first query (SHOW SERVICES)
            mock_cursor1 = Mock()
            mock_cursor1.sfqid = "query_id_123"
            mock_execute.return_value = mock_cursor1

            # Mock the second query (SELECT from RESULT_SCAN)
            mock_cursor2 = Mock()
            mock_cursor1.execute.return_value = mock_cursor2

            result = manager.list_services()

            # Verify the SHOW SERVICES query
            first_call = mock_execute.call_args_list[0][0][0]
            assert f"LIKE '{SERVICE_NAME_PREFIX}_%'" in first_call
            assert "SHOW SERVICES EXCLUDE JOBS" in first_call

            # Verify the RESULT_SCAN query
            second_call = mock_cursor1.execute.call_args[0][0]
            assert "RESULT_SCAN('query_id_123')" in second_call
            assert result == mock_cursor2

    def test_start_uses_name_resolution(self):
        """Test that start method uses _resolve_service_name for name resolution."""
        manager = RemoteManager()

        # Test that _resolve_service_name is called with the provided name
        with patch.object(manager, "_resolve_service_name") as mock_resolve:
            mock_resolve.return_value = "SNOW_REMOTE_user_myproject"

            # Mock the service status check to avoid complex mocking
            with patch(
                "snowflake.cli._plugins.spcs.services.manager.ServiceManager"
            ) as mock_service_manager_class:
                mock_service_manager = Mock()
                mock_service_manager_class.return_value = mock_service_manager
                mock_service_manager.status.side_effect = Exception("Service not found")

                # Should raise the compute pool error, but we verify name resolution was called
                with pytest.raises(
                    ValueError,
                    match="compute_pool is required for creating a new service",
                ):
                    manager.start(name="myproject", compute_pool=None)

                # Verify name resolution was called with the provided name
                mock_resolve.assert_called_once_with("myproject")

    def test_start_compute_pool_validation_for_new_service(self):
        """Test that compute pool is required only for new service creation."""
        manager = RemoteManager()

        with patch.object(
            manager, "_resolve_service_name", return_value="SNOW_REMOTE_user_newservice"
        ):
            with patch(
                "snowflake.cli._plugins.spcs.services.manager.ServiceManager"
            ) as mock_service_manager_class:
                mock_service_manager = Mock()
                mock_service_manager_class.return_value = mock_service_manager

                # Mock service doesn't exist (will try to create new service)
                mock_service_manager.status.side_effect = Exception("Service not found")

                # Should raise error when compute_pool is None for new service
                with pytest.raises(
                    ValueError,
                    match="compute_pool is required for creating a new service",
                ):
                    manager.start(name="newservice", compute_pool=None)

    def test_start_validation_requires_name_or_compute_pool(self):
        """Test that either name or compute_pool must be provided."""
        manager = RemoteManager()

        # Should raise error when both name and compute_pool are None
        with pytest.raises(
            ValueError,
            match="Either 'name' \\(for service resumption\\) or 'compute_pool' \\(for service creation\\) must be provided",
        ):
            manager.start(name=None, compute_pool=None)


class TestRemoteManagerSSH:
    """Test SSH-related functionality in RemoteManager."""

    @patch("snowflake.cli._plugins.remote.manager.cleanup_ssh_config")
    @patch(
        "snowflake.cli._plugins.remote.manager.RemoteManager._ssh_token_refresh_loop"
    )
    @patch("snowflake.cli._plugins.remote.manager.RemoteManager.get_endpoint_url")
    @patch("snowflake.cli._plugins.remote.manager.get_ssh_key_paths")
    def test_setup_ssh_connection_with_existing_key(
        self, mock_get_paths, mock_get_endpoint, mock_refresh_loop, mock_cleanup
    ):
        """Test SSH connection setup when private key exists."""
        manager = RemoteManager()

        # Mock SSH key exists
        mock_private_path = Mock()
        mock_private_path.exists.return_value = True
        mock_get_paths.return_value = (mock_private_path, Mock())
        mock_get_endpoint.return_value = "example.com"

        # Mock session setup - using PropertyMock to avoid click context issues
        mock_session = Mock()
        mock_session.sql.return_value.collect.return_value = []

        with patch.object(
            type(manager), "snowpark_session", new_callable=PropertyMock
        ) as mock_session_prop:
            mock_session_prop.return_value = mock_session

            manager.setup_ssh_connection("test_service")

            # Verify key detection
            mock_get_paths.assert_called_once_with("test_service")
            mock_private_path.exists.assert_called_once()

            # Verify refresh loop called with private key path
            mock_refresh_loop.assert_called_once()
            args = mock_refresh_loop.call_args[0]
            assert args[2] == str(mock_private_path)  # private_key_path argument

            # Verify cleanup was called in finally block
            mock_cleanup.assert_called_once_with("test_service")

    @patch("snowflake.cli._plugins.remote.manager.cleanup_ssh_config")
    @patch(
        "snowflake.cli._plugins.remote.manager.RemoteManager._ssh_token_refresh_loop"
    )
    @patch("snowflake.cli._plugins.remote.manager.RemoteManager.get_endpoint_url")
    @patch("snowflake.cli._plugins.remote.manager.get_ssh_key_paths")
    def test_setup_ssh_connection_without_key(
        self, mock_get_paths, mock_get_endpoint, mock_refresh_loop, mock_cleanup
    ):
        """Test SSH connection setup when no private key exists."""
        manager = RemoteManager()

        # Mock SSH key doesn't exist
        mock_private_path = Mock()
        mock_private_path.exists.return_value = False
        mock_get_paths.return_value = (mock_private_path, Mock())
        mock_get_endpoint.return_value = "example.com"

        # Mock session setup - using PropertyMock to avoid click context issues
        mock_session = Mock()
        mock_session.sql.return_value.collect.return_value = []

        with patch.object(
            type(manager), "snowpark_session", new_callable=PropertyMock
        ) as mock_session_prop:
            mock_session_prop.return_value = mock_session

            manager.setup_ssh_connection("test_service")

            # Verify key detection
            mock_get_paths.assert_called_once_with("test_service")
            mock_private_path.exists.assert_called_once()

            # Verify refresh loop called with None for private key path
            mock_refresh_loop.assert_called_once()
            args = mock_refresh_loop.call_args[0]
            assert args[2] is None  # private_key_path argument

            # Verify cleanup was called in finally block
            mock_cleanup.assert_called_once_with("test_service")

    @patch("snowflake.cli._plugins.remote.manager.ServiceManager")
    def test_get_public_endpoint_urls(self, mock_service_manager_class):
        """Test getting public endpoint URLs with column-based parsing."""
        manager = RemoteManager()

        # Mock cursor with column descriptions and data
        mock_cursor = Mock()
        # Create proper column description mocks with .name attribute
        name_col = Mock()
        name_col.name = "NAME"
        url_col = Mock()
        url_col.name = "INGRESS_URL"
        public_col = Mock()
        public_col.name = "IS_PUBLIC"
        mock_cursor.description = [name_col, url_col, public_col]
        mock_cursor.fetchall.return_value = [
            ("server-ui", "https://example.com", True),
            ("websocket-ssh", "https://ssh.example.com", True),
            ("internal-api", "https://internal.example.com", False),
            ("empty-url", None, True),
        ]

        mock_service_manager = Mock()
        mock_service_manager.list_endpoints.return_value = mock_cursor
        mock_service_manager_class.return_value = mock_service_manager

        result = manager.get_public_endpoint_urls("test_service")

        expected = {
            "server-ui": "https://example.com",
            "websocket-ssh": "https://ssh.example.com",
        }
        assert result == expected
        mock_service_manager.list_endpoints.assert_called_once_with("test_service")

    @patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
    @patch("snowflake.cli.api.cli_global_context.get_cli_context")
    def test_get_fresh_token_success(self, mock_get_context, mock_connect):
        """Test successful fresh token retrieval."""
        manager = RemoteManager()

        # Mock context and connection
        mock_context = Mock()
        mock_context.connection_context.connection_name = "test_conn"
        mock_context.connection_context.temporary_connection = False
        mock_get_context.return_value = mock_context

        mock_connection = Mock()
        mock_connection.rest.token = "fresh_token_123"
        mock_connection.is_closed.return_value = False
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        result = manager._get_fresh_token()  # noqa: SLF001

        assert result == "fresh_token_123"
        mock_connect.assert_called_once_with(
            connection_name="test_conn",
            temporary_connection=False,
            using_session_keep_alive=False,
        )
        mock_cursor.execute.assert_called_once_with(
            "ALTER SESSION SET python_connector_query_result_format = 'JSON'"
        )
        # Connection is designed to expire naturally, no explicit close

    @patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
    @patch("snowflake.cli.api.cli_global_context.get_cli_context")
    def test_get_fresh_token_no_token(self, mock_get_context, mock_connect):
        """Test fresh token retrieval when no token available."""
        manager = RemoteManager()

        # Mock context and connection
        mock_context = Mock()
        mock_context.connection_context.connection_name = "test_conn"
        mock_context.connection_context.temporary_connection = False
        mock_get_context.return_value = mock_context

        mock_connection = Mock()
        mock_connection.rest.token = None  # No token
        mock_connection.is_closed.return_value = False
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        result = manager._get_fresh_token()  # noqa: SLF001

        assert result is None
        # Connection is designed to expire naturally, no explicit close

    @patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
    @patch("snowflake.cli.api.cli_global_context.get_cli_context")
    def test_get_fresh_token_connection_error(self, mock_get_context, mock_connect):
        """Test fresh token retrieval when connection fails."""
        manager = RemoteManager()

        # Mock context
        mock_context = Mock()
        mock_get_context.return_value = mock_context

        mock_connect.side_effect = Exception("Connection failed")

        result = manager._get_fresh_token()  # noqa: SLF001

        assert result is None
