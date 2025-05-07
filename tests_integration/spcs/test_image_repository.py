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
import os
import uuid
from typing import Tuple

import pytest
from snowflake.cli.api.project.util import escape_like_pattern
from tests_integration.spcs.testing_utils.image_repository_utils import (
    ImageRepositoryTestSetup,
    ImageRepositoryTestSteps,
)

from tests_integration.test_utils import contains_row_with, row_from_snowflake_session
from tests_integration.testing_utils import ObjectNameProvider
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_is_successful,
)

INTEGRATION_DATABASE = os.environ.get(
    "SNOWFLAKE_CONNECTIONS_INTEGRATION_DATABASE", "SNOWCLI_DB"
).upper()
INTEGRATION_SCHEMA = "PUBLIC"
INTEGRATION_REPOSITORY = "snowcli_repository"


@pytest.mark.integration
def test_list_images_tags(runner):
    # test assumes the testing environment has been set up with /<DATABASE>/PUBLIC/snowcli_repository/snowpark_test_echo:1
    _list_images(runner)
    _list_images_with_like_positive_case(runner)
    _list_images_with_like_empty_list(runner)
    _list_tags(runner)


def _list_images(runner):
    result = runner.invoke_with_connection_json(
        [
            "spcs",
            "image-repository",
            "list-images",
            INTEGRATION_REPOSITORY,
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
            "image_name": "snowpark_test_echo",
            "tags": "1",
            "image_path": f"{INTEGRATION_DATABASE}/{INTEGRATION_SCHEMA}/{INTEGRATION_REPOSITORY}/snowpark_test_echo:1".lower(),
        },
    )


def _list_images_with_like_empty_list(runner):
    result = runner.invoke_with_connection_json(
        [
            "spcs",
            "image-repository",
            "list-images",
            INTEGRATION_REPOSITORY,
            "--database",
            INTEGRATION_DATABASE,
            "--schema",
            INTEGRATION_SCHEMA,
            "--like",
            "openflow%",
        ]
    )
    assert isinstance(result.json, list), result.output
    assert len(result.json) == 0, result.json


def _list_images_with_like_positive_case(runner):
    result = runner.invoke_with_connection_json(
        [
            "spcs",
            "image-repository",
            "list-images",
            INTEGRATION_REPOSITORY,
            "--database",
            INTEGRATION_DATABASE,
            "--schema",
            INTEGRATION_SCHEMA,
            "--like",
            "test_counter%",
        ]
    )
    assert isinstance(result.json, list), result.output
    assert len(result.json) == 1, result.json
    assert contains_row_with(
        result.json,
        {
            "image_name": "test_counter",
            "tags": "latest",
            "image_path": f"{INTEGRATION_DATABASE}/{INTEGRATION_SCHEMA}/{INTEGRATION_REPOSITORY}/test_counter:latest".lower(),
        },
    )
    # get all the images this time to verify the like filter is not applied
    result2 = runner.invoke_with_connection_json(
        [
            "spcs",
            "image-repository",
            "list-images",
            INTEGRATION_REPOSITORY,
            "--database",
            INTEGRATION_DATABASE,
            "--schema",
            INTEGRATION_SCHEMA,
            "--like",
            "%",
        ]
    )
    assert isinstance(result2.json, list), result2.output
    assert len(result2.json) == 3, result2.json
    assert contains_row_with(
        result.json,
        {
            "image_name": "test_counter",
            "tags": "latest",
            "image_path": f"{INTEGRATION_DATABASE}/{INTEGRATION_SCHEMA}/{INTEGRATION_REPOSITORY}/test_counter:latest".lower(),
        },
    )


def _list_tags(runner):
    result = runner.invoke_with_connection_json(
        [
            "spcs",
            "image-repository",
            "list-tags",
            INTEGRATION_REPOSITORY,
            "--image-name",
            f"/{INTEGRATION_DATABASE}/{INTEGRATION_SCHEMA}/{INTEGRATION_REPOSITORY}/snowpark_test_echo",
            "--database",
            INTEGRATION_DATABASE,
            "--schema",
            INTEGRATION_SCHEMA,
        ]
    )
    assert_that_result_is_successful(result)
    assert "DeprecationWarning: The command 'list-tags' is deprecated." in result.output
    assert (
        f'"tag": "/{INTEGRATION_DATABASE}/{INTEGRATION_SCHEMA}/{INTEGRATION_REPOSITORY}/snowpark_test_echo:1"'
        in result.output
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


@pytest.mark.integration
def test_create_from_project_definition(
    _test_steps: Tuple[ImageRepositoryTestSteps, str],
    project_directory,
    alter_snowflake_yml,
):
    test_steps, image_repository_name = _test_steps

    with project_directory("spcs_image_repository"):
        alter_snowflake_yml(
            "snowflake.yml",
            "entities",
            {
                image_repository_name: {
                    "type": "image-repository",
                    "identifier": {
                        "name": image_repository_name,
                    },
                }
            },
        )
        test_steps.create_from_project_definition(image_repository_name)
        test_steps.create_from_project_definition(
            image_repository_name, additional_flags=["--replace"]
        )


@pytest.fixture
def _test_steps(runner, snowflake_session, test_database):
    image_repository_name = f"image_repository_{uuid.uuid4().hex}"
    compute_pool_test_setup = ImageRepositoryTestSetup(
        runner=runner, snowflake_session=snowflake_session
    )
    test_steps = ImageRepositoryTestSteps(compute_pool_test_setup)

    yield test_steps, image_repository_name

    snowflake_session.execute_string(
        f"drop image repository if exists {image_repository_name}"
    )
