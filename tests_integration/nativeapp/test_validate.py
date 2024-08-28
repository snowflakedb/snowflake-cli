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

from tests.project.fixtures import *


@pytest.mark.integration
@pytest.mark.parametrize("test_project", ["napp_init_v1", "napp_init_v2"])
def test_nativeapp_validate(test_project, nativeapp_project_directory, runner):
    with nativeapp_project_directory(test_project):
        # validate the app's setup script
        result = runner.invoke_with_connection(["app", "validate"])
        assert result.exit_code == 0, result.output
        assert "Native App validation succeeded." in result.output


@pytest.mark.integration
@pytest.mark.parametrize("test_project", ["napp_init_v1", "napp_init_v2"])
def test_nativeapp_validate_failing(test_project, nativeapp_project_directory, runner):
    with nativeapp_project_directory(test_project):
        # Create invalid SQL file
        Path("app/setup_script.sql").write_text("Lorem ipsum dolor sit amet")

        # validate the app's setup script, this will fail
        # because we include an empty file
        result = runner.invoke_with_connection(["app", "validate"])
        assert result.exit_code == 1, result.output
        assert "Snowflake Native App setup script failed validation." in result.output
        assert "syntax error" in result.output
