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
import os.path
import uuid

from snowflake.cli.api.project.util import generate_user_env
from tests.project.fixtures import *
from tests_integration.test_utils import pushd

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


# Tests that snow app events --first N --last M exits with an error
@pytest.mark.integration
def test_app_events_cant_specify_first_and_last(runner, temporary_working_directory):
    project_name = "myapp"
    result = runner.invoke_json(
        ["app", "init", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0, result.output

    with pushd(Path(os.getcwd(), project_name)):
        # The integration test account doesn't have an event table set up
        # but this test is still useful to validate the negative case
        result = runner.invoke_with_connection(
            ["app", "events", "--first", "10", "--last", "20"],
            env=TEST_ENV,
        )
        assert result.exit_code == 1, result.output
        assert "--first and --last cannot be used together." in result.output
