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
from unittest import mock
import re

from snowflake.cli.api.project.util import generate_user_env
from tests_integration.test_utils import enable_definition_v2_feature_flag

from tests.project.fixtures import *

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


@pytest.mark.integration
@enable_definition_v2_feature_flag
@mock.patch("typer.launch")
@pytest.mark.parametrize("test_project", ["napp_init_v1", "napp_init_v2"])
def test_nativeapp_open(
    mock_typer_launch,
    runner,
    test_project,
    project_directory,
):
    project_name = "myapp"
    app_name = f"{project_name}_{USER_NAME}"

    with project_directory(test_project):
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0
        try:
            result = runner.invoke_with_connection_json(
                ["app", "open"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
            assert "Snowflake Native App opened in browser." in result.output

            mock_call = mock_typer_launch.call_args_list[0].args[0]
            assert re.match(
                rf"https://app.snowflake.com/.*#/apps/application/{app_name}",
                mock_call,
                re.IGNORECASE,
            )

        finally:
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force", "--cascade"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
