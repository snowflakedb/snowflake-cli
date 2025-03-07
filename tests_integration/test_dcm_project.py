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


@pytest.mark.integration
@pytest.mark.qa_only
def test_project_deploy(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    with project_directory("dcm_project"):
        result = runner.invoke_with_connection_json(["project", "create-version"])
        assert result.exit_code == 0, result.output

        # Unsupported command
        # result = runner.invoke_with_connection(["project", "dry-run", "my_project", "--version", "last"])
        # assert result.exit_code == 0

        result = runner.invoke_with_connection(
            [
                "project",
                "execute",
                "my_project",
                "--version",
                "last",
                "-D",
                f"table_name='{test_database}.PUBLIC.MyTable'",
            ]
        )
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(
            [
                "project",
                "list",
                "--like",
                "MY_PROJECT",
            ]
        )
        assert result.exit_code == 0, result.output
        assert len(result.json) == 1
        project = result.json[0]
        assert project["name"].lower() == "my_project".lower()
