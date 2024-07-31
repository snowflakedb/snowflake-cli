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

import uuid

from snowflake.cli.api.project.util import generate_user_env
from tests.project.fixtures import *
from tests_integration.test_utils import enable_definition_v2_feature_flag

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


# Tests that snow app events --first N --last M exits with an error
@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize("test_project", ["napp_init_v1", "napp_init_v2"])
@pytest.mark.parametrize(
    ["flag_names", "command"],
    [
        [
            ["--first", "--last"],
            ["--first", "10", "--last", "20"],
        ],
        [
            ["--follow", "--first"],
            ["--first", "10", "--follow"],
        ],
        [
            ["--follow", "--until"],
            ["--until", "5 minutes", "--follow"],
        ],
    ],
)
def test_app_events_mutually_exclusive_options(
    test_project, runner, project_directory, flag_names, command
):
    with project_directory(test_project):
        # The integration test account doesn't have an event table set up
        # but this test is still useful to validate the negative case
        result = runner.invoke_with_connection(
            ["app", "events", *command],
            env=TEST_ENV,
        )
        assert result.exit_code == 1, result.output
        assert (
            f"{flag_names[0]} and {flag_names[1]} cannot be used together."
            in result.output
        )


@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize("test_project", ["napp_init_v1", "napp_init_v2"])
def test_app_events_reject_invalid_type(test_project, runner, project_directory):
    with project_directory(test_project):
        # The integration test account doesn't have an event table set up
        # but this test is still useful to validate the negative case
        result = runner.invoke_with_connection(
            ["app", "events", "--type", "foo"],
            env=TEST_ENV,
        )
        assert result.exit_code == 2, result.output
        assert "Invalid value for '--type'" in result.output
