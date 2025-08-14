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
    pytest tests_integration/test_oidc.py -m integration
"""

import json
import pytest
from uuid import uuid4
from contextlib import contextmanager

GITHUB_ISSUER = "https://token.actions.githubusercontent.com"


@pytest.fixture
def test_user_creation(runner, snowflake_session, resource_suffix):
    """
    Fixture that creates a test user for workload identity and cleans up after the test.

    This fixture sets up a user using the workload identity setup command
    and ensures cleanup by dropping the user after the test completes.
    """
    # Generate unique user name with resource suffix to avoid conflicts
    user_name = f"test_user{resource_suffix}"
    issuer = GITHUB_ISSUER
    subject = f"test-subject-{uuid4().hex}"
    default_role = "PUBLIC"  # Use PUBLIC role as it should exist in test environments

    @contextmanager
    def _setup_and_cleanup_user():
        # Setup: Create the user using the workload identity setup command
        try:
            setup_result = runner.invoke_with_connection(
                [
                    "auth",
                    "oidc",
                    "create-user",
                    "--user-name",
                    user_name,
                    "--issuer",
                    issuer,
                    "--subject",
                    subject,
                    "--default-role",
                    default_role,
                    "--role",
                    "accountadmin",
                ]
            )

            assert setup_result.exit_code == 0, setup_result

            yield user_name

        finally:
            # Cleanup: Always attempt to delete the user
            try:
                # First try using the CLI delete command
                delete_result = runner.invoke_with_connection(
                    [
                        "auth",
                        "oidc",
                        "delete",
                        "--user-name",
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
def test_oidc_user_creation(runner, test_user_creation):
    query = f"""show user workload identity authentication methods for user {test_user_creation} ->>
   select "name", "type", "additional_info", PARSE_JSON("additional_info"):issuer as issuer from $1
   ;
    """
    result = runner.invoke_with_connection_json(["sql", "-q", query])
    assert result.exit_code == 0, result
    output = result.json[0]
    assert output["name"] == "DEFAULT", output
    assert output["type"] == "OIDC", output

    additional_info = json.loads(output["additional_info"])
    assert additional_info["issuer"] == GITHUB_ISSUER
