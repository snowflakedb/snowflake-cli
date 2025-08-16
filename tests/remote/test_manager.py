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
