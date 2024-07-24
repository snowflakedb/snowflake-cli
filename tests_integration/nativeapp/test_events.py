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

import pytest

from snowflake.cli.api.project.util import generate_user_env
from tests_integration.test_utils import enable_definition_v2_feature_flag


USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize("definition_version", ["v1", "v2"])
def test_app_events(
    runner,
    definition_version,
    project_directory,
):
    with project_directory(f"napp_init_{definition_version}"):
        # The integration test account doesn't have an event table set up
        # but this test is still useful to validate the negative case
        result = runner.invoke_with_connection(
            ["app", "events"],
            env=TEST_ENV,
        )
        assert result.exit_code == 1, result.output
        assert "No event table was found for this Snowflake account." in result.output
