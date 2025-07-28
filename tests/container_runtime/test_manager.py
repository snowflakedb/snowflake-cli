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
from snowflake.cli._plugins.container_runtime.manager import ContainerRuntimeManager
from snowflake.connector.cursor import SnowflakeCursor

from .test_helpers import (
    MockContainerRuntimeManager,
    mock_cli_context_and_sql_execution,
)

EXECUTE_QUERY = "snowflake.cli._plugins.container_runtime.manager.ContainerRuntimeManager.execute_query"


@patch("snowflake.cli._app.telemetry.command_info")
@patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
@patch("snowflake.cli.api.cli_global_context.get_cli_context")
@patch("snowflake.cli._plugins.container_runtime.container_spec.generate_service_spec")
@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager")
@patch(EXECUTE_QUERY)
@patch("tempfile.NamedTemporaryFile")
@patch("yaml.dump")
def test_create_container_runtime_minimal(
    mock_yaml_dump,
    mock_tempfile,
    mock_execute_query,
    mock_service_manager,
    mock_generate_spec,
    mock_get_context,
    mock_connect,
    mock_command_info,
):
    """Test creating container runtime with minimal parameters."""
    # Setup mocks
    mock_cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = mock_cursor

    mock_context = Mock()
    mock_context.connection.user = "testuser"
    mock_context.connection.warehouse = "test_warehouse"
    mock_get_context.return_value = mock_context

    mock_service_instance = Mock()
    mock_service_manager.return_value = mock_service_instance

    mock_spec = {"spec": {"containers": [{"name": "vscode"}]}}
    mock_generate_spec.return_value = mock_spec

    # Mock tempfile
    mock_temp_file = Mock()
    mock_temp_file.name = "/tmp/test_spec.yaml"
    mock_tempfile.return_value.__enter__.return_value = mock_temp_file

    # Mock telemetry function that requires click context
    mock_command_info.return_value = {"command": "test"}

    # Mock connection creation to avoid snowflake connection issues
    mock_connect.return_value = Mock()

    # Create manager and call create
    manager = ContainerRuntimeManager()

    # Mock the snowpark session to avoid session creation issues
    with MockContainerRuntimeManager.patch_snowpark_session(manager):
        # Mock wait_for_service_ready and get_service_endpoint_url
        with patch.object(manager, "wait_for_service_ready"):
            with patch.object(manager, "get_service_endpoint_url") as mock_get_url:
                mock_get_url.return_value = "https://example.com/vscode"

                result = manager.create(compute_pool="test_pool")

        # Verify the service name was generated
        expected_service_prefix = "SNOW_CR_testuser_"
        assert result == "https://example.com/vscode"

        # Verify ServiceManager was called with correct parameters
        mock_service_instance.create.assert_called_once()
        call_args = mock_service_instance.create.call_args

        # Check that the service name has the right prefix
        service_name = call_args.kwargs["service_name"]
        assert service_name.startswith(expected_service_prefix)

        # Check other parameters
        assert call_args.kwargs["compute_pool"] == "test_pool"
        assert call_args.kwargs["query_warehouse"] == "test_warehouse"
        assert call_args.kwargs["min_instances"] == 1
        assert call_args.kwargs["max_instances"] == 1
        assert call_args.kwargs["auto_resume"] is True


@patch("snowflake.cli._app.telemetry.command_info")
@patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
@patch("snowflake.cli.api.cli_global_context.get_cli_context")
def test_create_container_runtime_invalid_stage(
    mock_get_context, mock_connect, mock_command_info
):
    """Test creating container runtime with invalid stage path."""
    mock_context = Mock()
    mock_context.connection.user = "testuser"
    mock_context.connection.warehouse = "test_warehouse"
    mock_get_context.return_value = mock_context

    # Mock telemetry function that requires click context
    mock_command_info.return_value = {"command": "test"}

    # Mock connection creation to avoid snowflake connection issues
    mock_connect.return_value = Mock()

    manager = ContainerRuntimeManager()

    with pytest.raises(
        ValueError, match=r"Stage name must start with '@'.*or 'snow://'"
    ):
        manager.create(compute_pool="test_pool", stage="invalid_stage_path")


@mock_cli_context_and_sql_execution
@patch(EXECUTE_QUERY)
def test_list_services(mock_execute_query):
    """Test listing container runtime services."""
    mock_cursor = Mock(spec=SnowflakeCursor)
    mock_cursor.sfqid = "query_id_123"

    # Mock multiple calls to execute_query - first for SHOW, second for SELECT
    mock_execute_query.side_effect = [mock_cursor, mock_cursor]

    manager = ContainerRuntimeManager()
    result = manager.list_services()

    # Verify the correct SQL queries were called
    assert mock_execute_query.call_count == 2
    # Check the SHOW SERVICES query
    assert "SHOW SERVICES EXCLUDE JOBS" in mock_execute_query.call_args_list[0][0][0]
    assert (
        f"LIKE '{manager.DEFAULT_SERVICE_PREFIX}%'"
        in mock_execute_query.call_args_list[0][0][0]
    )


@mock_cli_context_and_sql_execution
@patch("snowflake.cli._plugins.container_runtime.manager.generate_service_spec")
def test_generate_service_spec_method(mock_generate_spec):
    """Test the generate_service_spec method of ContainerRuntimeManager."""
    # Mock the spec generation
    mock_spec = {"spec": {"containers": [{"name": "test"}]}}
    mock_generate_spec.return_value = mock_spec

    manager = ContainerRuntimeManager()

    # Mock the snowpark session to avoid session creation issues
    with MockContainerRuntimeManager.patch_snowpark_session(manager):
        # Test the generate_service_spec method directly
        result = manager.generate_service_spec(
            compute_pool="test_pool", stage="@my_stage", image_tag="custom:latest"
        )

        # Verify generate_service_spec was called with correct parameters
        mock_generate_spec.assert_called_once()
        call_args = mock_generate_spec.call_args[1]

        assert call_args["compute_pool"] == "test_pool"
        assert call_args["stage"] == "@my_stage"
        assert call_args["image_tag"] == "custom:latest"
        assert call_args["enable_metrics"] is True

        # Check environment variables
        env_vars = call_args["environment_vars"]
        assert env_vars["TZ"] == "Etc/UTC"
        assert env_vars["VSCODE_PORT"] == "12020"

        assert result == mock_spec


@mock_cli_context_and_sql_execution
def test_service_name_generation():
    """Test service name generation logic."""
    manager = ContainerRuntimeManager()

    with patch(
        "snowflake.cli._plugins.container_runtime.manager.datetime"
    ) as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "20240101120000"

        # Test auto-generated name
        with patch.object(manager, "generate_service_spec") as mock_generate:
            with patch.object(manager, "wait_for_service_ready"):
                with patch.object(manager, "get_service_endpoint_url") as mock_get_url:
                    with patch(
                        "snowflake.cli._plugins.spcs.services.manager.ServiceManager"
                    ):
                        with patch("tempfile.NamedTemporaryFile") as mock_tempfile:
                            with patch("yaml.dump"):
                                with patch(
                                    "pathlib.Path.read_text",
                                    return_value="spec content",
                                ):
                                    # Mock the temporary file properly
                                    mock_temp_file = Mock()
                                    mock_temp_file.name = "/tmp/test_spec.yaml"
                                    mock_tempfile.return_value.__enter__.return_value = (
                                        mock_temp_file
                                    )

                                    mock_generate.return_value = {"spec": {}}
                                    mock_get_url.return_value = "test_url"

                                    manager.create(compute_pool="test_pool")

                                    # Check generated service name includes correct prefix and username
                                    assert mock_generate.called

        # Test custom name
        with patch.object(manager, "generate_service_spec") as mock_generate:
            with patch.object(manager, "wait_for_service_ready"):
                with patch.object(manager, "get_service_endpoint_url") as mock_get_url:
                    with patch(
                        "snowflake.cli._plugins.spcs.services.manager.ServiceManager"
                    ):
                        with patch("tempfile.NamedTemporaryFile") as mock_tempfile:
                            with patch("yaml.dump"):
                                with patch(
                                    "pathlib.Path.read_text",
                                    return_value="spec content",
                                ):
                                    # Mock the temporary file properly
                                    mock_temp_file = Mock()
                                    mock_temp_file.name = "/tmp/test_spec.yaml"
                                    mock_tempfile.return_value.__enter__.return_value = (
                                        mock_temp_file
                                    )

                                    mock_generate.return_value = {"spec": {}}
                                    mock_get_url.return_value = "test_url"

                                    manager.create(
                                        compute_pool="test_pool", name="custom"
                                    )

                                    # Check generated service name includes custom name
                                    assert mock_generate.called


@mock_cli_context_and_sql_execution
@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager")
def test_get_service_endpoint_url(mock_service_manager):
    """Test getting VS Code endpoint URL."""
    mock_service_instance = Mock()
    mock_service_manager.return_value = mock_service_instance

    # Mock endpoint data: [name, port, protocol, ingress_enabled, ingress_url, url]
    mock_service_instance.list_endpoints.return_value = [
        (
            "server-ui",
            12020,
            "HTTP",
            True,
            "https://example.com/ingress",
            "https://example.com/vscode",
        ),
        (
            "websocket-ssh",
            12021,
            "TCP",
            True,
            "wss://example.com/ssh",
            "wss://example.com/ssh",
        ),
    ]

    manager = ContainerRuntimeManager()

    # Since the manager creates its own ServiceManager instance, patch the import in the manager module
    with patch(
        "snowflake.cli._plugins.container_runtime.manager.ServiceManager",
        mock_service_manager,
    ):
        result = manager.get_service_endpoint_url("test_service")

        assert result == "https://example.com/vscode"
        mock_service_instance.list_endpoints.assert_called_once_with("test_service")


@mock_cli_context_and_sql_execution
@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager")
def test_get_service_endpoint_url_not_found(mock_service_manager):
    """Test getting VS Code endpoint URL when endpoint not found."""
    mock_service_instance = Mock()
    mock_service_manager.return_value = mock_service_instance
    mock_service_instance.list_endpoints.return_value = []

    manager = ContainerRuntimeManager()

    with patch(
        "snowflake.cli._plugins.container_runtime.manager.ServiceManager",
        mock_service_manager,
    ):
        with pytest.raises(
            RuntimeError, match="No VS Code endpoint found for service test_service"
        ):
            manager.get_service_endpoint_url("test_service")


@mock_cli_context_and_sql_execution
@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager")
def test_get_public_endpoint_urls(mock_service_manager):
    """Test getting all public endpoint URLs."""
    mock_service_instance = Mock()
    mock_service_manager.return_value = mock_service_instance

    # Mock endpoints response
    mock_service_instance.list_endpoints.return_value = [
        [
            "server-ui",
            "port",
            "public",
            "protocol",
            "host",
            "https://example.com/vscode",
        ],
        ["websocket-ssh", "port", "public", "protocol", "host", "example.com/ssh"],
        [
            "internal-endpoint",
            "port",
            "private",
            "protocol",
            "host",
            None,
        ],  # No URL for private endpoint
    ]

    manager = ContainerRuntimeManager()

    with patch(
        "snowflake.cli._plugins.container_runtime.manager.ServiceManager",
        mock_service_manager,
    ):
        result = manager.get_public_endpoint_urls("test_service")

        expected = {
            "server-ui": "https://example.com/vscode",
            "websocket-ssh": "example.com/ssh",
        }
        assert result == expected


@mock_cli_context_and_sql_execution
@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager")
def test_stop_service(mock_service_manager):
    """Test stopping a container runtime service."""
    mock_service_instance = Mock()
    mock_service_manager.return_value = mock_service_instance
    mock_cursor = Mock(spec=SnowflakeCursor)
    mock_service_instance.suspend.return_value = mock_cursor

    manager = ContainerRuntimeManager()

    with patch(
        "snowflake.cli._plugins.container_runtime.manager.ServiceManager",
        mock_service_manager,
    ):
        result = manager.stop("test_service")

        assert result == mock_cursor
        mock_service_instance.suspend.assert_called_once_with("test_service")


@mock_cli_context_and_sql_execution
@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager")
def test_start_service(mock_service_manager):
    """Test starting a container runtime service."""
    mock_service_instance = Mock()
    mock_service_manager.return_value = mock_service_instance
    mock_cursor = Mock(spec=SnowflakeCursor)
    mock_service_instance.resume.return_value = mock_cursor

    manager = ContainerRuntimeManager()

    with patch(
        "snowflake.cli._plugins.container_runtime.manager.ServiceManager",
        mock_service_manager,
    ):
        result = manager.start("test_service")

        assert result == mock_cursor
        mock_service_instance.resume.assert_called_once_with("test_service")


@mock_cli_context_and_sql_execution
@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager")
def test_wait_for_service_ready_success(mock_service_manager):
    """Test waiting for service to be ready - success case."""
    mock_service_instance = Mock()
    mock_service_manager.return_value = mock_service_instance

    # Mock status response - service becomes ready
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = ['[{"status": "READY"}]']
    mock_service_instance.status.return_value = mock_cursor

    manager = ContainerRuntimeManager()

    with patch(
        "snowflake.cli._plugins.container_runtime.manager.ServiceManager",
        mock_service_manager,
    ):
        with patch("time.sleep"):  # Speed up test
            result = manager.wait_for_service_ready("test_service")

        assert result is True
        mock_service_instance.status.assert_called_with("test_service")


@mock_cli_context_and_sql_execution
@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager")
def test_wait_for_service_ready_failure(mock_service_manager):
    """Test waiting for service to be ready - failure case."""
    mock_service_instance = Mock()
    mock_service_manager.return_value = mock_service_instance

    # Mock status response - service fails
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = ['[{"status": "FAILED"}]']
    mock_service_instance.status.return_value = mock_cursor

    manager = ContainerRuntimeManager()

    with patch(
        "snowflake.cli._plugins.container_runtime.manager.ServiceManager",
        mock_service_manager,
    ):
        with pytest.raises(
            RuntimeError,
            match="Service test_service failed to start with status: FAILED",
        ):
            manager.wait_for_service_ready("test_service")


@mock_cli_context_and_sql_execution
@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager")
def test_wait_for_service_ready_timeout(mock_service_manager):
    """Test waiting for service to be ready - timeout case."""
    mock_service_instance = Mock()
    mock_service_manager.return_value = mock_service_instance

    # Mock status response - service stays pending
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = ['[{"status": "PENDING"}]']
    mock_service_instance.status.return_value = mock_cursor

    manager = ContainerRuntimeManager()

    with patch(
        "snowflake.cli._plugins.container_runtime.manager.ServiceManager",
        mock_service_manager,
    ):
        with patch("time.sleep"):  # Speed up test
            with patch("time.time") as mock_time:
                # Simulate timeout
                mock_time.side_effect = [0, 301]  # Start time, then past timeout

                with pytest.raises(
                    Exception,
                    match="Service 'test_service' did not become ready within 300 seconds",
                ):
                    manager.wait_for_service_ready("test_service")


@mock_cli_context_and_sql_execution
@patch(EXECUTE_QUERY)
def test_delete_service(mock_execute_query):
    """Test deleting a container runtime service."""
    mock_cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = mock_cursor

    manager = ContainerRuntimeManager()
    result = manager.delete("test_service")

    assert result == mock_cursor
    mock_execute_query.assert_called_once_with("DROP SERVICE IF EXISTS test_service")
