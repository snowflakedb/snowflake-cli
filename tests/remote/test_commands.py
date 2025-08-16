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

from snowflake.cli._plugins.remote.manager import RemoteManager
from snowflake.connector.cursor import SnowflakeCursor


def create_mock_cursor(columns=None, rows=None):
    """Helper function to create a properly mocked cursor."""
    if columns is None:
        columns = ["status"]
    if rows is None:
        rows = [("Success",)]

    mock_cursor = Mock(spec=SnowflakeCursor)
    mock_cursor.description = [Mock(name=col) for col in columns]
    mock_cursor.fetchall.return_value = rows
    mock_cursor.__iter__ = Mock(return_value=iter(rows))
    mock_cursor.query = "MOCK QUERY"  # Add query attribute
    return mock_cursor


class TestRemoteCommands:
    @patch.object(RemoteManager, "start")
    def test_start_command_success(self, mock_start, runner):
        """Test successful start command."""
        mock_start.return_value = ("test_service", "https://test.url", "created")

        result = runner.invoke(
            ["remote", "start", "test_service", "--compute-pool", "test_pool"]
        )

        assert result.exit_code == 0
        assert (
            "✓ Remote Development Environment test_service created successfully!"
            in result.output
        )
        assert "VS Code Server URL: https://test.url" in result.output

        mock_start.assert_called_once_with(
            name="test_service",
            compute_pool="test_pool",
            external_access=None,
            stage=None,
            image_tag=None,
        )

    @patch.object(RemoteManager, "start")
    def test_start_command_with_all_parameters(self, mock_start, runner):
        """Test start command with all possible parameters."""
        mock_start.return_value = (
            "SNOW_REMOTE_user_test",
            "https://example.com/ui",
            "created",
        )

        result = runner.invoke(
            [
                "remote",
                "start",
                "test_service",
                "--compute-pool",
                "test_pool",
                "--stage",
                "@my_stage",
                "--image-tag",
                "custom_tag",
                "--eai-name",
                "eai1",
                "--eai-name",
                "eai2",
                "--database",
                "test_db",
                "--schema",
                "test_schema",
            ]
        )

        assert result.exit_code == 0
        mock_start.assert_called_once()
        call_kwargs = mock_start.call_args.kwargs
        assert call_kwargs["name"] == "test_service"
        assert call_kwargs["compute_pool"] == "test_pool"
        assert call_kwargs["stage"] == "@my_stage"
        assert call_kwargs["image_tag"] == "custom_tag"
        assert call_kwargs["external_access"] == ["eai1", "eai2"]

    @patch.object(RemoteManager, "start")
    def test_start_command_idempotent_behavior(self, mock_start, runner):
        """Test that start command handles idempotent scenarios."""
        test_cases = [
            ("created", "✓ Remote Development Environment"),
            ("running", "already running"),
            ("resumed", "resumed successfully"),
        ]

        for status, expected_output in test_cases:
            mock_start.return_value = (
                "SNOW_REMOTE_user_test",
                "https://example.com/ui",
                status,
            )

            result = runner.invoke(
                [
                    "remote",
                    "start",
                    "test_service",
                    "--compute-pool",
                    "test_pool",
                ]
            )

            assert result.exit_code == 0
            # Check for success indicators
            assert "VS Code Server URL:" in result.output

    @patch.object(RemoteManager, "list_services")
    def test_list_command(self, mock_list, runner):
        """Test list command."""
        mock_cursor = create_mock_cursor(columns=["name", "status"], rows=[])
        mock_list.return_value = mock_cursor

        result = runner.invoke(["remote", "list"])

        assert result.exit_code == 0
        mock_list.assert_called_once()

    @patch.object(RemoteManager, "stop")
    def test_stop_command(self, mock_stop, runner):
        """Test stop command."""
        mock_cursor = create_mock_cursor(rows=[("Service suspended successfully",)])
        mock_stop.return_value = mock_cursor

        result = runner.invoke(["remote", "stop", "test_service"])

        assert result.exit_code == 0
        assert (
            "Remote environment 'test_service' suspended successfully." in result.output
        )
        mock_stop.assert_called_once_with("test_service")

    @patch.object(RemoteManager, "delete")
    def test_delete_command(self, mock_delete, runner):
        """Test delete command."""
        mock_cursor = create_mock_cursor(rows=[("Service deleted successfully",)])
        mock_delete.return_value = mock_cursor

        result = runner.invoke(["remote", "delete", "test_service"])

        assert result.exit_code == 0
        assert (
            "Remote environment 'test_service' deleted successfully." in result.output
        )
        mock_delete.assert_called_once_with("test_service")
