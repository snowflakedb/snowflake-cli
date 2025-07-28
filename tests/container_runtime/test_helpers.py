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

"""
Test helpers for container runtime tests.
"""

from functools import wraps
from typing import Callable
from unittest.mock import Mock, patch


def mock_cli_context_and_sql_execution(test_func: Callable) -> Callable:
    """
    Decorator that mocks CLI context, telemetry, and SQL execution for container runtime manager tests.

    This comprehensive decorator handles all the common mocking needed for manager tests
    that would otherwise fail due to CLI context and database connection issues.
    """

    @wraps(test_func)
    def wrapper(*args, **kwargs):
        with patch("snowflake.cli._app.telemetry.command_info") as mock_command_info:
            with patch(
                "snowflake.cli._app.snow_connector.connect_to_snowflake"
            ) as mock_connect:
                with patch(
                    "snowflake.cli.api.cli_global_context.get_cli_context"
                ) as mock_get_context:
                    with patch(
                        "snowflake.cli.api.sql_execution.SqlExecutionMixin.execute_string"
                    ) as mock_execute_string:
                        # Setup standard mock context
                        mock_context = Mock()
                        mock_context.connection.user = "testuser"
                        mock_context.connection.warehouse = "test_warehouse"
                        mock_get_context.return_value = mock_context

                        # Mock telemetry function that requires click context
                        mock_command_info.return_value = {"command": "test"}

                        # Mock connection creation to avoid snowflake connection issues
                        mock_connect.return_value = Mock()

                        # Mock SQL execution to return mock cursors
                        mock_cursor = create_mock_cursor_with_description()
                        mock_execute_string.return_value = [mock_cursor]

                        return test_func(*args, **kwargs)

    return wrapper


def create_mock_cursor_with_description(data=None, description=None):
    """
    Create a properly configured mock cursor for SQL execution tests.

    Args:
        data: Data to return from fetchone/fetchall calls
        description: Column descriptions for the cursor

    Returns:
        Mock cursor object with proper attributes
    """
    mock_cursor = Mock()
    mock_cursor.description = description or [("column1",), ("column2",)]
    mock_cursor.query = "SELECT * FROM test"
    mock_cursor.__iter__ = Mock(return_value=iter(data or []))

    if data:
        if isinstance(data, list) and len(data) == 1:
            mock_cursor.fetchone.return_value = data[0]
        else:
            mock_cursor.fetchone.return_value = data[0] if data else None
        mock_cursor.fetchall.return_value = data
    else:
        mock_cursor.fetchone.return_value = None
        mock_cursor.fetchall.return_value = []

    return mock_cursor


def create_mock_service_manager(status_data=None, endpoints_data=None):
    """
    Create a properly configured mock ServiceManager for tests.

    Args:
        status_data: Data to return from status calls
        endpoints_data: Data to return from list_endpoints calls

    Returns:
        Mock ServiceManager instance
    """
    mock_service_manager = Mock()
    mock_service_instance = Mock()
    mock_service_manager.return_value = mock_service_instance

    # Mock status method
    if status_data:
        mock_cursor = create_mock_cursor_with_description([status_data])
        mock_service_instance.status.return_value = mock_cursor

    # Mock endpoints method
    if endpoints_data:
        mock_service_instance.list_endpoints.return_value = endpoints_data

    # Mock other service operations
    mock_service_instance.suspend.return_value = create_mock_cursor_with_description()
    mock_service_instance.resume.return_value = create_mock_cursor_with_description()

    return mock_service_manager


class MockContainerRuntimeManager:
    """
    Helper class to create ContainerRuntimeManager instances with proper mocking.
    """

    @staticmethod
    def create_with_mocked_dependencies():
        """
        Create a ContainerRuntimeManager with all dependencies mocked.
        """
        from snowflake.cli._plugins.container_runtime.manager import (
            ContainerRuntimeManager,
        )

        manager = ContainerRuntimeManager()

        # Mock the snowpark session property to avoid session creation
        with patch.object(
            type(manager), "snowpark_session", new_callable=lambda: Mock()
        ):
            return manager

    @staticmethod
    def patch_snowpark_session(manager):
        """
        Patch the snowpark_session property for a manager instance.
        """
        return patch.object(
            type(manager), "snowpark_session", new_callable=lambda: Mock()
        )


def mock_file_operations():
    """
    Context manager that mocks common file operations for utils tests.
    """
    return patch.multiple(
        "os.path",
        exists=Mock(return_value=True),
        expanduser=Mock(return_value="/mocked/path"),
        dirname=Mock(return_value="/mocked/dir"),
    )


def mock_vscode_paths():
    """
    Create side effects for mocking VS Code path operations.
    """

    def expanduser_side_effect(path):
        if "Code - Insiders" in path:
            return "/home/user/Library/Application Support/Code - Insiders/User/settings.json"
        else:
            return "/home/user/Library/Application Support/Code/User/settings.json"

    def dirname_side_effect(path):
        if "settings.json" in path:
            if "Code - Insiders" in path:
                return "/home/user/Library/Application Support/Code - Insiders/User"
            else:
                return "/home/user/Library/Application Support/Code/User"
        else:
            if "Code - Insiders" in path:
                return "/home/user/Library/Application Support/Code - Insiders"
            else:
                return "/home/user/Library/Application Support/Code"

    def exists_side_effect(path):
        if "settings.json" in path:
            return False  # Settings file doesn't exist
        return True  # Directories exist

    return expanduser_side_effect, dirname_side_effect, exists_side_effect
