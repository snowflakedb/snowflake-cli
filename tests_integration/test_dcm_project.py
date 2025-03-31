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

from snowflake.cli.api.secure_path import SecurePath


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


@pytest.mark.integration
@pytest.mark.qa_only
def test_project_add_version(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    with project_directory("dcm_project") as root:
        # Create a new project
        result = runner.invoke_with_connection_json(["project", "create-version"])
        assert result.exit_code == 0, result.output
        if (root / "output").exists():
            SecurePath(root / "output").rmdir(recursive=True)

        # Modify sql file and upload it to a new stage
        with open(root / "file_a.sql", mode="w") as fp:
            fp.write(
                "define table identifier('{{ table_name }}') (fooBar string, baz string);"
            )

        stage_name = "dcm_project_stage"
        result = runner.invoke_with_connection_json(["stage", "create", stage_name])
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(
            ["stage", "copy", ".", f"@{stage_name}"]
        )
        assert result.exit_code == 0, result.output

        # create a new version of the project
        result = runner.invoke_with_connection_json(
            [
                "project",
                "add-version",
                "my_project",
                "--from",
                f"@{stage_name}",
                "--alias",
                "v2",
            ]
        )
        assert result.exit_code == 0, result.output

        # list project versions
        result = runner.invoke_with_connection_json(
            [
                "project",
                "list-versions",
                "MY_PROJECT",
            ]
        )
        assert result.exit_code == 0, result.output
        assert len(result.json) == 2
        assert result.json[0]["name"].lower() == "VERSION$2".lower()
        assert result.json[0]["alias"].lower() == "v2".lower()
        assert result.json[1]["name"].lower() == "VERSION$1".lower()
