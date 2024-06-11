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

import pytest

from tests_integration.test_utils import row_from_snowflake_session
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_is_successful_and_output_json_equals,
)
from tests_integration.testing_utils.naming_utils import ObjectNameProvider


@pytest.mark.integration
def test_token(runner):
    result = runner.invoke_with_connection_json(["spcs", "image-registry", "token"])

    assert result.exit_code == 0
    assert result.json
    assert "token" in result.json
    assert result.json["token"]
    assert "expires_in" in result.json
    assert result.json["expires_in"]


@pytest.mark.integration
def test_get_registry_url(test_database, test_role, runner, snowflake_session):
    # newly created test_role should have no access to image repositories and should not be able to get registry URL
    test_repo = ObjectNameProvider("test_repo").create_and_get_next_object_name()
    snowflake_session.execute_string(f"create image repository {test_repo}")

    fail_result = runner.invoke_with_connection(
        ["spcs", "image-registry", "url", "--role", test_role]
    )
    assert fail_result.exit_code == 1, fail_result.output
    assert "No image repository found." in fail_result.output

    # role should be able to get registry URL once granted read access to an image repository
    repo_list_cursor = snowflake_session.execute_string("show image repositories")
    expected_repo_url = row_from_snowflake_session(repo_list_cursor)[0][
        "repository_url"
    ]
    expected_registry_url = "/".join(expected_repo_url.split("/")[:-3])
    snowflake_session.execute_string(
        f"grant usage on database {snowflake_session.database} to role {test_role};"
        f"grant usage on schema {snowflake_session.schema} to role {test_role};"
        f"grant read on image repository {test_repo} to role {test_role};"
    )
    success_result = runner.invoke_with_connection(
        ["spcs", "image-registry", "url", "--role", test_role]
    )
    assert success_result.exit_code == 0, success_result.output
    assert success_result.output.strip() == expected_registry_url


@pytest.mark.integration
def test_registry_login(runner):
    result = runner.invoke_with_connection_json(["spcs", "image-registry", "login"])
    assert_that_result_is_successful_and_output_json_equals(
        result, {"message": "Login Succeeded"}
    )
