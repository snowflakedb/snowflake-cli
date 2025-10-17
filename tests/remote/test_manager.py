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
from snowflake.cli.api.exceptions import CliError
from snowflake.connector.cursor import DictCursor


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

    def test_resolve_service_name_sanitizes_username_special_chars(self):
        """Username with invalid chars should be sanitized in service name."""
        manager = RemoteManager()

        with patch.object(
            manager, "_get_current_snowflake_user", return_value="john.doe+eng@acme-co"
        ):
            result = manager._resolve_service_name("proj1")  # noqa: SLF001

        # Expected: non-alphanumeric replaced with underscores and collapsed
        # "john.doe+eng@acme-co" -> "john_doe_eng_acme_co" then full name uppercased
        assert result == f"{SERVICE_NAME_PREFIX}_JOHN_DOE_ENG_ACME_CO_PROJ1"

    def test_resolve_service_name_username_empty_uses_default(self):
        """Empty username falls back to DEFAULT_USER in service name."""
        manager = RemoteManager()

        with patch.object(manager, "_get_current_snowflake_user", return_value=""):
            result = manager._resolve_service_name("proj")  # noqa: SLF001

        assert result == f"{SERVICE_NAME_PREFIX}_DEFAULT_USER_PROJ"

    def test_resolve_service_name_keeps_project_name_unchanged_except_case(self):
        """Project name is not sanitized, only uppercased in final name."""
        manager = RemoteManager()

        with patch.object(manager, "_get_current_snowflake_user", return_value="alice"):
            # name contains dashes and dots; per current behavior, it is used as-is and then uppercased
            result = manager._resolve_service_name("my-project.v1")  # noqa: SLF001

        assert result == f"{SERVICE_NAME_PREFIX}_ALICE_MY-PROJECT.V1"

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

    def test_get_service_info_success(self):
        """Test get_service_info with valid service name."""
        manager = RemoteManager()

        with patch.object(manager, "execute_query") as mock_execute, patch.object(
            manager, "_resolve_service_name"
        ) as mock_resolve, patch.object(
            manager, "get_public_endpoint_urls"
        ) as mock_endpoints:

            mock_resolve.return_value = "SNOW_REMOTE_admin_test"

            # Mock DESC SERVICE response - returns single row with all service details
            mock_cursor = Mock()
            mock_cursor.fetchone.return_value = {
                "name": "SNOW_REMOTE_admin_test",
                "status": "RUNNING",
                "database_name": "TEST_DB",
                "schema_name": "TEST_SCHEMA",
                "compute_pool": "test_pool",
                "external_access_integrations": "eai1,eai2",
                "created_on": "2024-01-01 12:00:00",
                "updated_on": "2024-01-01 13:00:00",
                "resumed_on": "2024-01-01 13:00:00",
                "suspended_on": None,
                "comment": "Test service",
            }
            mock_execute.return_value = mock_cursor

            # Mock endpoints
            mock_endpoints.return_value = {
                "VS Code Server": "https://test.url:8080",
                "Jupyter": "https://test.url:8888",
            }

            result = manager.get_service_info("test")

            # Verify name resolution
            mock_resolve.assert_called_once_with("test")

            # Verify DESC SERVICE query with DictCursor
            mock_execute.assert_called_once_with(
                "DESC SERVICE SNOW_REMOTE_admin_test", cursor_class=DictCursor
            )

            # Verify endpoints were fetched
            mock_endpoints.assert_called_once_with("SNOW_REMOTE_admin_test")

            # Verify result structure
            assert isinstance(result, dict)
            assert "Service Information" in result
            assert "Timestamps" in result
            assert "Public Endpoints" in result

            # Verify service information
            service_info = result["Service Information"]
            assert service_info["Name"] == "SNOW_REMOTE_admin_test"
            assert service_info["Status"] == "RUNNING"
            assert service_info["Database"] == "TEST_DB"
            assert service_info["Compute Pool"] == "test_pool"

            # Verify endpoints
            endpoints = result["Public Endpoints"]
            assert endpoints["VS Code Server"] == "https://test.url:8080"
            assert endpoints["Jupyter"] == "https://test.url:8888"

    def test_get_service_info_not_found(self):
        """Test get_service_info when service is not found."""
        manager = RemoteManager()

        with patch.object(manager, "execute_query") as mock_execute, patch.object(
            manager, "_resolve_service_name"
        ) as mock_resolve:

            mock_resolve.return_value = "SNOW_REMOTE_admin_nonexistent"

            # Mock DESC SERVICE response - empty result (service not found)
            mock_cursor = Mock()
            mock_cursor.fetchone.return_value = None  # No service found
            mock_execute.return_value = mock_cursor

            with pytest.raises(
                CliError, match="Remote service 'nonexistent' not found"
            ):
                manager.get_service_info("nonexistent")

    def test_get_service_info_does_not_exist_error(self):
        """Test get_service_info when service does not exist (SQL error)."""
        manager = RemoteManager()

        with patch.object(manager, "execute_query") as mock_execute, patch.object(
            manager, "_resolve_service_name"
        ) as mock_resolve:

            mock_resolve.return_value = "SNOW_REMOTE_admin_nonexistent"

            # Mock DESC SERVICE raising "does not exist" error
            mock_execute.side_effect = Exception(
                "Service 'SNOW_REMOTE_admin_nonexistent' does not exist"
            )

            with pytest.raises(
                CliError, match="Remote service 'nonexistent' not found"
            ):
                manager.get_service_info("nonexistent")

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
                    CliError,
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
                    CliError,
                    match="compute_pool is required for creating a new service",
                ):
                    manager.start(name="newservice", compute_pool=None)

    def test_start_validation_requires_name_or_compute_pool(self):
        """Test that either name or compute_pool must be provided."""
        manager = RemoteManager()

        # Should raise error when both name and compute_pool are None
        with pytest.raises(
            CliError,
            match="Either 'name' \\(for service resumption\\) or 'compute_pool' \\(for service creation\\) must be provided",
        ):
            manager.start(name=None, compute_pool=None)


class TestRemoteManagerSSH:
    """Test SSH-related functionality in RemoteManager."""

    @patch("snowflake.cli._plugins.remote.manager.cleanup_ssh_config")
    @patch("snowflake.cli._plugins.remote.manager.setup_ssh_config_with_token")
    @patch("snowflake.cli._plugins.remote.manager.RemoteManager._get_fresh_token")
    @patch(
        "snowflake.cli._plugins.remote.manager.RemoteManager._ssh_token_refresh_loop"
    )
    @patch("snowflake.cli._plugins.remote.manager.RemoteManager.get_endpoint_url")
    @patch("snowflake.cli._plugins.remote.manager.get_ssh_key_paths")
    def test_setup_ssh_connection_with_existing_key(
        self,
        mock_get_paths,
        mock_get_endpoint,
        mock_refresh_loop,
        mock_get_token,
        mock_setup_config,
        mock_cleanup,
    ):
        """Test SSH connection setup when private key exists."""
        manager = RemoteManager()

        # Mock SSH key exists
        mock_private_path = Mock()
        mock_private_path.exists.return_value = True
        mock_get_paths.return_value = (mock_private_path, Mock())
        mock_get_endpoint.return_value = "example.com"
        mock_get_token.return_value = "test_token"

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

            # Verify token fetch and SSH config setup
            mock_get_token.assert_called_once()
            mock_setup_config.assert_called_once_with(
                "test_service", "example.com", "test_token", str(mock_private_path)
            )

            # Verify refresh loop called with private key path and delay
            mock_refresh_loop.assert_called_once()
            args, kwargs = mock_refresh_loop.call_args
            assert args[2] == str(mock_private_path)  # private_key_path argument
            assert (
                kwargs.get("delay_first_refresh") is True
            )  # delay_first_refresh argument

            # Verify cleanup was called in finally block
            mock_cleanup.assert_called_once_with("test_service")

    @patch("snowflake.cli._plugins.remote.manager.cleanup_ssh_config")
    @patch("snowflake.cli._plugins.remote.manager.setup_ssh_config_with_token")
    @patch("snowflake.cli._plugins.remote.manager.RemoteManager._get_fresh_token")
    @patch(
        "snowflake.cli._plugins.remote.manager.RemoteManager._ssh_token_refresh_loop"
    )
    @patch("snowflake.cli._plugins.remote.manager.RemoteManager.get_endpoint_url")
    @patch("snowflake.cli._plugins.remote.manager.get_ssh_key_paths")
    def test_setup_ssh_connection_without_key(
        self,
        mock_get_paths,
        mock_get_endpoint,
        mock_refresh_loop,
        mock_get_token,
        mock_setup_config,
        mock_cleanup,
    ):
        """Test SSH connection setup when no private key exists."""
        manager = RemoteManager()

        # Mock SSH key doesn't exist
        mock_private_path = Mock()
        mock_private_path.exists.return_value = False
        mock_get_paths.return_value = (mock_private_path, Mock())
        mock_get_endpoint.return_value = "example.com"
        mock_get_token.return_value = "test_token"

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

            # Verify token fetch and SSH config setup
            mock_get_token.assert_called_once()
            mock_setup_config.assert_called_once_with(
                "test_service", "example.com", "test_token", None
            )

            # Verify refresh loop called with None for private key path and delay
            mock_refresh_loop.assert_called_once()
            args, kwargs = mock_refresh_loop.call_args
            assert args[2] is None  # private_key_path argument
            assert (
                kwargs.get("delay_first_refresh") is True
            )  # delay_first_refresh argument

            # Verify cleanup was called in finally block
            mock_cleanup.assert_called_once_with("test_service")

    @patch("snowflake.cli._plugins.remote.manager.cleanup_ssh_config")
    @patch("snowflake.cli._plugins.remote.manager.launch_ide")
    @patch("snowflake.cli._plugins.remote.manager.setup_ssh_config_with_token")
    @patch("snowflake.cli._plugins.remote.manager.RemoteManager._get_fresh_token")
    @patch(
        "snowflake.cli._plugins.remote.manager.RemoteManager._ssh_token_refresh_loop"
    )
    @patch("snowflake.cli._plugins.remote.manager.RemoteManager.get_endpoint_url")
    @patch("snowflake.cli._plugins.remote.manager.get_ssh_key_paths")
    def test_setup_ssh_connection_with_ide(
        self,
        mock_get_paths,
        mock_get_endpoint,
        mock_refresh_loop,
        mock_get_token,
        mock_setup_config,
        mock_launch_ide,
        mock_cleanup,
    ):
        """Test SSH connection setup with IDE launch."""
        manager = RemoteManager()

        # Mock SSH key exists
        mock_private_path = Mock()
        mock_private_path.exists.return_value = True
        mock_get_paths.return_value = (mock_private_path, Mock())
        mock_get_endpoint.return_value = "example.com"
        mock_get_token.return_value = "test_token"

        # Mock session setup - using PropertyMock to avoid click context issues
        mock_session = Mock()
        mock_session.sql.return_value.collect.return_value = []

        with patch.object(
            type(manager), "snowpark_session", new_callable=PropertyMock
        ) as mock_session_prop:
            mock_session_prop.return_value = mock_session

            manager.setup_ssh_connection("test_service", ide="code")

            # Verify key detection
            mock_get_paths.assert_called_once_with("test_service")
            mock_private_path.exists.assert_called_once()

            # Verify token fetch and SSH config setup
            mock_get_token.assert_called_once()
            mock_setup_config.assert_called_once_with(
                "test_service", "example.com", "test_token", str(mock_private_path)
            )

            # Verify IDE launch
            mock_launch_ide.assert_called_once_with(
                "code", "test_service", "/root/user-default"
            )

            # Verify refresh loop called with private key path and delay
            mock_refresh_loop.assert_called_once()
            args, kwargs = mock_refresh_loop.call_args
            assert args[2] == str(mock_private_path)  # private_key_path argument
            assert (
                kwargs.get("delay_first_refresh") is True
            )  # delay_first_refresh argument

            # Verify cleanup was called in finally block
            mock_cleanup.assert_called_once_with("test_service")

    def test_validate_ide_requirements_new_service_with_eai(self):
        """Test IDE validation when creating new service with EAI."""
        manager = RemoteManager()

        # Should not raise for new service with EAI
        creating = manager.validate_ide_requirements(None, ["ALLOW_ALL"])
        assert creating is True

    def test_validate_ide_requirements_new_service_without_eai(self):
        """Test IDE validation when creating new service without EAI."""
        manager = RemoteManager()

        # Should raise for new service without EAI
        with pytest.raises(
            CliError, match="External access integration is required for IDE launch"
        ):
            manager.validate_ide_requirements(None, None)

    @patch.object(RemoteManager, "execute_query")
    @patch.object(RemoteManager, "_resolve_service_name")
    @patch.object(RemoteManager, "_get_service_status")
    def test_validate_ide_requirements_existing_service(
        self, mock_get_status, mock_resolve, mock_execute_query
    ):
        """Test IDE validation when using existing service with EAI."""
        manager = RemoteManager()

        mock_resolve.return_value = "resolved_service_name"
        mock_get_status.return_value = (True, "RUNNING")  # Service exists

        # Mock the DESC SERVICE query to return service with EAI
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {
            "external_access_integrations": "my_eai,another_eai"
        }
        mock_execute_query.return_value = mock_cursor

        # Should not raise for existing service with EAI
        creating = manager.validate_ide_requirements("my_service", None)
        assert creating is False

    @patch.object(RemoteManager, "execute_query")
    @patch.object(RemoteManager, "_resolve_service_name")
    @patch.object(RemoteManager, "_get_service_status")
    def test_validate_ide_requirements_existing_service_no_eai(
        self, mock_get_status, mock_resolve, mock_execute_query
    ):
        """Test IDE validation when using existing service without EAI."""
        manager = RemoteManager()

        mock_resolve.return_value = "resolved_service_name"
        mock_get_status.return_value = (True, "RUNNING")  # Service exists

        # Mock the DESC SERVICE query to return service without EAI
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {"external_access_integrations": None}
        mock_execute_query.return_value = mock_cursor

        # Should raise for existing service without EAI
        with pytest.raises(
            CliError, match="does not have external access integration configured"
        ):
            manager.validate_ide_requirements("my_service", None)

    @patch.object(RemoteManager, "execute_query")
    def test_get_public_endpoint_urls(self, mock_execute_query):
        """Test getting public endpoint URLs with DictCursor."""
        manager = RemoteManager()

        # Mock DictCursor data - each row is a dictionary
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(
            return_value=iter(
                [
                    {
                        "name": "server-ui",
                        "ingress_url": "https://example.com",
                        "is_public": True,
                    },
                    {
                        "name": "websocket-ssh",
                        "ingress_url": "https://ssh.example.com",
                        "is_public": True,
                    },
                    {
                        "name": "internal-api",
                        "ingress_url": "https://internal.example.com",
                        "is_public": False,
                    },
                    {"name": "empty-url", "ingress_url": None, "is_public": True},
                ]
            )
        )
        mock_execute_query.return_value = mock_cursor

        result = manager.get_public_endpoint_urls("test_service")

        expected = {
            "server-ui": "https://example.com",
            "websocket-ssh": "https://ssh.example.com",
        }
        assert result == expected
        mock_execute_query.assert_called_once_with(
            "show endpoints in service test_service", cursor_class=DictCursor
        )

    @patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
    @patch("snowflake.cli._plugins.remote.manager.get_cli_context")
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
            using_session_keep_alive=True,
        )
        mock_cursor.execute.assert_called_once_with(
            "ALTER SESSION SET python_connector_query_result_format = 'JSON'"
        )

    @patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
    @patch("snowflake.cli._plugins.remote.manager.get_cli_context")
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

        # Should raise RuntimeError when no token is available
        with pytest.raises(
            RuntimeError, match="No token available from fresh connection"
        ):
            manager._get_fresh_token()  # noqa: SLF001

    @patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
    @patch("snowflake.cli._plugins.remote.manager.get_cli_context")
    def test_get_fresh_token_connection_error(self, mock_get_context, mock_connect):
        """Test fresh token retrieval when connection fails."""
        manager = RemoteManager()

        # Mock context
        mock_context = Mock()
        mock_get_context.return_value = mock_context

        mock_connect.side_effect = Exception("Connection failed")

        # Should raise the connection exception
        with pytest.raises(Exception, match="Connection failed"):
            manager._get_fresh_token()  # noqa: SLF001

    @patch("snowflake.cli.api.console.cli_console.warning")
    def test_warn_about_config_changes_no_options(self, mock_warning):
        """Test that no warning is displayed when no config options are provided."""
        manager = RemoteManager()

        manager._warn_about_config_changes(service_name="test_service")  # noqa: SLF001

        mock_warning.assert_not_called()

    @patch(
        "snowflake.cli._plugins.remote.manager.RemoteManager.get_validated_server_ui_url"
    )
    @patch("snowflake.cli.api.console.cli_console.warning")
    def test_handle_existing_service_running_with_config_warning(
        self, mock_warning, mock_get_validated_url
    ):
        """Test that _handle_existing_service warns about config changes for running services."""
        manager = RemoteManager()
        mock_get_validated_url.return_value = "https://example.com"

        result = manager._handle_existing_service(  # noqa: SLF001
            service_name="test_service",
            current_status="RUNNING",
            stage="@my_stage",
            image="custom:latest",
        )

        # Should return service result
        assert result is not None
        assert result.service_name == "test_service"
        assert result.url == "https://example.com"
        assert result.status == "running"

        # Should have called warning
        mock_warning.assert_called_once()
        call_args = mock_warning.call_args[0][0]
        assert (
            "Configuration changes provided to 'snow remote start' will be ignored"
            in call_args
        )
        assert "snow remote delete test_service" in call_args
