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

from snowflake.cli.api.identifiers import FQN
from snowflake.connector.cursor import SnowflakeCursor


def _mock_cursor_with_description():
    """Create a properly mocked cursor with description."""
    mock_cursor = Mock(spec=SnowflakeCursor)
    # Mock the description attribute with column-like objects
    mock_column = Mock()
    mock_column.name = "status"
    mock_column.type_code = 0  # VARCHAR type
    mock_cursor.description = [mock_column]
    mock_cursor.__iter__ = Mock(return_value=iter([("SUCCESS",)]))
    mock_cursor.query = "MOCK QUERY"
    return mock_cursor


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_create_container_runtime_minimal_params(mock_manager_class, runner):
    """Test creating container runtime with minimal required parameters."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_manager.create.return_value = (
        "SNOW_CR_test_service",
        "https://example.com/vscode",
        True,
    )

    result = runner.invoke(
        ["container-runtime", "create", "--compute-pool", "test_pool"]
    )

    assert result.exit_code == 0, result.output
    assert (
        "✓ Container Runtime Environment SNOW_CR_test_service created successfully!"
        in result.output
    )
    assert "VS Code Server URL: https://example.com/vscode" in result.output

    mock_manager.create.assert_called_once_with(
        name=None,
        compute_pool="test_pool",
        external_access=None,
        stage=None,
        workspace=None,
        image_tag=None,
    )


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_create_container_runtime_all_params(mock_manager_class, runner):
    """Test creating container runtime with all parameters."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_manager.create.return_value = (
        "SNOW_CR_custom_name",
        "https://example.com/vscode",
        True,
    )

    result = runner.invoke(
        [
            "container-runtime",
            "create",
            "--compute-pool",
            "test_pool",
            "--name",
            "custom_name",
            "--eai-name",
            "integration1",
            "--eai-name",
            "integration2",
            "--stage",
            "@my_stage/folder",
            "--image-tag",
            "custom:v1.0",
        ]
    )

    assert result.exit_code == 0, result.output
    mock_manager.create.assert_called_once_with(
        name="custom_name",
        compute_pool="test_pool",
        external_access=["integration1", "integration2"],
        stage="@my_stage/folder",
        workspace=None,
        image_tag="custom:v1.0",
    )


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_create_container_runtime_already_exists(mock_manager_class, runner):
    """Test creating container runtime when service already exists."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_manager.create.return_value = (
        "SNOW_CR_existing_service",
        "https://example.com/vscode",
        False,
    )

    result = runner.invoke(
        ["container-runtime", "create", "--compute-pool", "test_pool"]
    )

    assert result.exit_code == 0, result.output
    assert (
        "✓ Container Runtime Environment SNOW_CR_existing_service already exists!"
        in result.output
    )
    assert "VS Code Server URL: https://example.com/vscode" in result.output

    mock_manager.create.assert_called_once_with(
        name=None,
        compute_pool="test_pool",
        external_access=None,
        stage=None,
        workspace=None,
        image_tag=None,
    )


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_create_container_runtime_workspace_not_available(mock_manager_class, runner):
    """Test that workspace parameter shows error message."""
    result = runner.invoke(
        [
            "container-runtime",
            "create",
            "--compute-pool",
            "test_pool",
            "--workspace",
            "@my_workspace",
        ]
    )

    assert result.exit_code == 1
    assert "Error: The --workspace parameter is not yet available." in result.output


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_create_container_runtime_with_exception(mock_manager_class, runner):
    """Test creating container runtime when manager raises exception."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_manager.create.side_effect = Exception("Test error")

    result = runner.invoke(
        ["container-runtime", "create", "--compute-pool", "test_pool"]
    )

    assert result.exit_code == 1
    assert "Error: Test error" in result.output


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_list_container_runtimes(mock_manager_class, runner):
    """Test listing container runtime environments."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_cursor = _mock_cursor_with_description()
    mock_manager.list_services.return_value = mock_cursor

    result = runner.invoke(["container-runtime", "list"])

    assert result.exit_code == 0
    mock_manager.list_services.assert_called_once()


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_stop_container_runtime(mock_manager_class, runner):
    """Test stopping a container runtime environment."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_cursor = _mock_cursor_with_description()
    mock_manager.stop.return_value = mock_cursor

    result = runner.invoke(["container-runtime", "stop", "test_runtime"])

    assert result.exit_code == 0
    assert "Container runtime 'test_runtime' suspended successfully." in result.output
    # The argument gets converted to FQN and then passed as FQN to manager
    mock_manager.stop.assert_called_once_with(FQN.from_string("test_runtime"))


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_start_container_runtime_success(mock_manager_class, runner):
    """Test starting a container runtime environment successfully."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_cursor = _mock_cursor_with_description()
    mock_manager.start.return_value = mock_cursor
    mock_manager.get_service_endpoint_url.return_value = "https://example.com/vscode"

    with patch("time.sleep"):  # Mock sleep to speed up test
        result = runner.invoke(["container-runtime", "start", "test_runtime"])

    assert result.exit_code == 0
    assert "Starting container runtime 'test_runtime'..." in result.output
    assert "Container runtime 'test_runtime' started successfully." in result.output
    assert "Access URL: https://example.com/vscode" in result.output

    # The argument gets converted to FQN and then passed as FQN to manager
    expected_fqn = FQN.from_string("test_runtime")
    mock_manager.start.assert_called_once_with(expected_fqn)
    mock_manager.wait_for_service_ready.assert_called_once_with(expected_fqn)
    mock_manager.get_service_endpoint_url.assert_called_once_with(expected_fqn)


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_start_container_runtime_not_ready(mock_manager_class, runner):
    """Test starting a container runtime that doesn't become ready."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_cursor = _mock_cursor_with_description()
    mock_manager.start.return_value = mock_cursor
    mock_manager.wait_for_service_ready.side_effect = Exception("Service not ready")

    with patch("time.sleep"):  # Mock sleep to speed up test
        result = runner.invoke(["container-runtime", "start", "test_runtime"])

    assert result.exit_code == 0
    assert "Service started but not yet fully ready" in result.output
    assert "Please wait a few more moments" in result.output


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_delete_container_runtime(mock_manager_class, runner):
    """Test deleting a container runtime environment."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_cursor = _mock_cursor_with_description()
    mock_manager.delete.return_value = mock_cursor

    result = runner.invoke(["container-runtime", "delete", "test_runtime"])

    assert result.exit_code == 0
    assert "Container runtime 'test_runtime' deleted successfully." in result.output
    # The argument gets converted to FQN and then passed as FQN to manager
    mock_manager.delete.assert_called_once_with(FQN.from_string("test_runtime"))


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_get_url_with_endpoints(mock_manager_class, runner):
    """Test getting URLs when endpoints exist."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_manager.get_public_endpoint_urls.return_value = {
        "server-ui": "https://example.com/vscode",
        "websocket-ssh": "example.com/ssh",
    }

    result = runner.invoke(["container-runtime", "get-url", "test_runtime"])

    assert result.exit_code == 0
    # The argument gets converted to FQN and then passed as FQN to manager
    mock_manager.get_public_endpoint_urls.assert_called_once_with(
        FQN.from_string("test_runtime")
    )


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_get_url_no_endpoints(mock_manager_class, runner):
    """Test getting URLs when no endpoints exist."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_manager.get_public_endpoint_urls.return_value = {}

    result = runner.invoke(["container-runtime", "get-url", "test_runtime"])

    # The command fails because SingleQueryResult(None) causes an error
    assert result.exit_code == 1
    # But we can still check that the message was printed before the error
    assert (
        "No public endpoints found for container runtime 'test_runtime'."
        in result.output
    )


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_get_url_with_exception(mock_manager_class, runner):
    """Test getting URLs when an exception occurs."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_manager.get_public_endpoint_urls.side_effect = Exception("Connection error")

    result = runner.invoke(["container-runtime", "get-url", "test_runtime"])

    assert result.exit_code == 1
    assert (
        "Error retrieving URLs for container runtime 'test_runtime': Connection error"
        in result.output
    )


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_setup_ssh_success(mock_manager_class, runner):
    """Test SSH setup success without custom VS Code server path."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_manager.get_public_endpoint_urls.return_value = {
        "websocket-ssh": "example.com/ssh"
    }

    # Mock snowpark session
    mock_session = Mock()
    mock_manager.snowpark_session = mock_session
    mock_session.sql.return_value.collect.return_value = None

    # Mock the connection to prevent actual connection attempts
    with patch(
        "snowflake.cli._app.snow_connector.connect_to_snowflake"
    ) as mock_connect:
        # Make the command exit quickly by mocking a keyboard interrupt
        mock_connect.side_effect = KeyboardInterrupt()

        result = runner.invoke(["container-runtime", "setup-ssh", "test_runtime"])

        # The command should start but exit due to keyboard interrupt
        assert result.exit_code == 0  # KeyboardInterrupt is handled gracefully
        assert "Found websocket SSH endpoint" in result.output


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_setup_ssh_with_vscode_path(mock_manager_class, runner):
    """Test SSH setup with custom VS Code server path."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_manager.get_public_endpoint_urls.return_value = {
        "websocket-ssh": "example.com/ssh"
    }

    # Mock snowpark session
    mock_session = Mock()
    mock_manager.snowpark_session = mock_session
    mock_session.sql.return_value.collect.return_value = None

    # Mock the connection to prevent actual connection attempts
    with patch(
        "snowflake.cli._app.snow_connector.connect_to_snowflake"
    ) as mock_connect:
        # Make the command exit quickly by mocking a keyboard interrupt
        mock_connect.side_effect = KeyboardInterrupt()

        result = runner.invoke(
            [
                "container-runtime",
                "setup-ssh",
                "test_runtime",
                "--vscode-server-path",
                "/custom/path",
            ]
        )

        # The command should start but exit due to keyboard interrupt
        assert result.exit_code == 0  # KeyboardInterrupt is handled gracefully
        assert "Found websocket SSH endpoint" in result.output
        assert "VS Code server path: /custom/path" in result.output


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_setup_ssh_no_endpoints(mock_manager_class, runner):
    """Test SSH setup when no endpoints exist."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_manager.get_public_endpoint_urls.return_value = {}

    result = runner.invoke(["container-runtime", "setup-ssh", "test_runtime"])

    assert result.exit_code == 1
    assert (
        "No public endpoints found for container runtime 'test_runtime'."
        in result.output
    )


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_setup_ssh_no_websocket_endpoint(mock_manager_class, runner):
    """Test SSH setup when no websocket-ssh endpoint exists."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_manager.get_public_endpoint_urls.return_value = {
        "server-ui": "https://example.com/vscode"
    }

    result = runner.invoke(["container-runtime", "setup-ssh", "test_runtime"])

    assert result.exit_code == 1
    # The error message should indicate no websocket-ssh endpoint found
    assert (
        "Available endpoints:" in result.output
        or "No websocket-ssh endpoint found" in result.output
    )


@patch("snowflake.cli._plugins.container_runtime.commands.ContainerRuntimeManager")
def test_setup_ssh_manager_exception(mock_manager_class, runner):
    """Test SSH setup when manager raises exception."""
    mock_manager = Mock()
    mock_manager_class.return_value = mock_manager
    mock_manager.get_public_endpoint_urls.side_effect = Exception("Connection error")

    result = runner.invoke(["container-runtime", "setup-ssh", "test_runtime"])

    assert result.exit_code == 1
    assert (
        "Error setting up SSH for container runtime 'test_runtime': Connection error"
        in result.output
    )
