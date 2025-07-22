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
Integration tests for workload identity commands.

These tests require a properly configured Snowflake environment with:
- Valid integration test credentials
- Permissions to create and drop users
- An account with workload identity capabilities

To run these tests, ensure your environment has the necessary 
SNOWFLAKE_CONNECTIONS_INTEGRATION_* environment variables set.

Example:
    pytest tests_integration/test_workload_identity.py -m integration
"""

import pytest
from uuid import uuid4
from contextlib import contextmanager


@pytest.fixture
def test_federated_user(runner, snowflake_session, resource_suffix):
    """
    Fixture that creates a test federated user for workload identity and cleans up after the test.

    This fixture sets up a federated user using the workload identity setup command
    and ensures cleanup by dropping the user after the test completes.
    """
    # Generate unique user name with resource suffix to avoid conflicts
    user_name = f"test_federated_user{resource_suffix}"
    subject = f"test-subject-{uuid4().hex}"
    default_role = "PUBLIC"  # Use PUBLIC role as it should exist in test environments
    provider_type = "github"  # Use github as the test provider type

    @contextmanager
    def _setup_and_cleanup_user():
        # Setup: Create the federated user using the workload identity setup command
        try:
            setup_result = runner.invoke_with_connection(
                [
                    "auth",
                    "workload-identity",
                    "setup",
                    "--federated-user",
                    user_name,
                    "--subject",
                    subject,
                    "--default-role",
                    default_role,
                    "--type",
                    provider_type,
                    "--role",
                    "accountadmin",
                ]
            )

            # Verify setup was successful
            if setup_result.exit_code != 0:
                pytest.skip(
                    f"Could not set up test federated user: {setup_result.output}"
                )

            yield user_name

        finally:
            # Cleanup: Always attempt to delete the user
            try:
                # First try using the CLI delete command
                delete_result = runner.invoke_with_connection(
                    [
                        "auth",
                        "workload-identity",
                        "delete",
                        "--federated-user",
                        user_name,
                    ],
                    catch_exceptions=True,
                )

                # If CLI delete fails, try direct SQL as fallback
                if delete_result.exit_code != 0:
                    snowflake_session.execute_string(f"DROP USER IF EXISTS {user_name}")

            except Exception:
                # Final fallback cleanup attempt via direct SQL
                try:
                    snowflake_session.execute_string(f"DROP USER IF EXISTS {user_name}")
                except Exception:
                    # If all cleanup attempts fail, log but don't fail the test
                    pass

    with _setup_and_cleanup_user() as created_user:
        yield created_user


@pytest.mark.integration
def test_workload_identity_list_users(runner, test_federated_user):
    """
    Test the workload identity list command.

    This test verifies that:
    1. The list command executes successfully
    2. The test federated user appears in the results
    3. The output format is correct (JSON with user information)
    """
    # Execute the list command
    result = runner.invoke_with_connection_json(["auth", "workload-identity", "list"])

    # Verify the command executed successfully
    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    # Verify the result is a list (JSON format)
    assert isinstance(result.json, list), "Expected list output from command"

    # Verify our test federated user appears in the results
    user_names = [user.get("name", "").upper() for user in result.json]
    assert test_federated_user.upper() in user_names, (
        f"Test federated user '{test_federated_user}' not found in list. "
        f"Found users: {user_names}"
    )

    # Verify the user entries have expected structure
    test_user_entry = next(
        (
            user
            for user in result.json
            if user.get("name", "").upper() == test_federated_user.upper()
        ),
        None,
    )
    assert test_user_entry is not None, "Test user entry not found"

    # Verify essential fields are present
    assert "name" in test_user_entry, "User entry missing 'name' field"


@pytest.mark.integration
def test_workload_identity_list_users_empty_when_no_federated_users(runner):
    """
    Test the workload identity list command when no federated users exist.

    This test verifies that the list command works correctly even when
    there are no users with workload identity enabled.
    """
    # Execute the list command
    result = runner.invoke_with_connection_json(["auth", "workload-identity", "list"])

    # Verify the command executed successfully
    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    # Verify the result is a list (even if empty)
    assert isinstance(result.json, list), "Expected list output from command"

    # The list may be empty or contain existing federated users
    # This test just ensures the command structure works correctly
    for user in result.json:
        assert isinstance(user, dict), "Each user entry should be a dictionary"
        assert "name" in user, "Each user entry should have a 'name' field"
