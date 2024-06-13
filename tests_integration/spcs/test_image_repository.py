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
from snowflake.cli.api.project.util import escape_like_pattern

from tests_integration.test_utils import contains_row_with, row_from_snowflake_session
from tests_integration.testing_utils import ObjectNameProvider

INTEGRATION_DATABASE = "SNOWCLI_DB"
INTEGRATION_SCHEMA = "PUBLIC"
INTEGRATION_REPOSITORY = "snowcli_repository"


@pytest.mark.integration
def test_list_images_tags(runner):
    # test assumes the testing environment has been set up with /SNOWCLI_DB/PUBLIC/snowcli_repository/snowpark_test_echo:1
    _list_images(runner)
    _list_tags(runner)


def _list_images(runner):
    result = runner.invoke_with_connection_json(
        [
            "spcs",
            "image-repository",
            "list-images",
            "snowcli_repository",
            "--database",
            INTEGRATION_DATABASE,
            "--schema",
            INTEGRATION_SCHEMA,
        ]
    )
    assert isinstance(result.json, list), result.output
    assert contains_row_with(
        result.json,
        {
            "image": f"/{INTEGRATION_DATABASE}/{INTEGRATION_SCHEMA}/{INTEGRATION_REPOSITORY}/snowpark_test_echo"
        },
    )


def _list_tags(runner):
    result = runner.invoke_with_connection_json(
        [
            "spcs",
            "image-repository",
            "list-tags",
            "snowcli_repository",
            "--image-name",
            f"/{INTEGRATION_DATABASE}/{INTEGRATION_SCHEMA}/{INTEGRATION_REPOSITORY}/snowpark_test_echo",
            "--database",
            INTEGRATION_DATABASE,
            "--schema",
            INTEGRATION_SCHEMA,
        ]
    )
    assert isinstance(result.json, list), result.output
    assert contains_row_with(
        result.json,
        {
            "tag": f"/{INTEGRATION_DATABASE}/{INTEGRATION_SCHEMA}/{INTEGRATION_REPOSITORY}/snowpark_test_echo:1"
        },
    )


@pytest.mark.integration
def test_get_repo_url(runner, snowflake_session, test_database):
    repo_name = ObjectNameProvider("TEST_REPO").create_and_get_next_object_name()
    snowflake_session.execute_string(f"create image repository {repo_name}")

    created_repo = snowflake_session.execute_string(
        f"show image repositories like '{escape_like_pattern(repo_name)}'"
    )
    created_row = row_from_snowflake_session(created_repo)[0]
    created_name = created_row["name"]
    assert created_name == repo_name.upper()

    expect_url = created_row["repository_url"]
    result = runner.invoke_with_connection(
        ["spcs", "image-repository", "url", created_name]
    )
    assert isinstance(result.output, str), result.output
    assert result.output.strip() == expect_url


@pytest.mark.integration
def test_create_image_repository(runner, test_database):
    repo_name = ObjectNameProvider("test_repo").create_and_get_next_object_name()
    result = runner.invoke_with_connection_json(
        ["spcs", "image-repository", "create", repo_name]
    )
    assert isinstance(result.json, dict), result.output
    assert result.json == {
        "status": f"Image Repository {repo_name.upper()} successfully created."
    }
