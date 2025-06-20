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

from typing import Set, Optional, Tuple
from tests_integration.test_utils import assert_stage_has_files, does_stage_exist


def _assert_project_has_versions(
    runner, project_name: str, expected_versions: Set[Tuple[str, Optional[str]]]
) -> None:
    """Check whether the project versions (in [name,alias] format) are present in Snowflake."""
    result = runner.invoke_with_connection_json(
        ["project", "list-versions", project_name]
    )
    assert result.exit_code == 0, result.output
    versions = {(version["name"], version["alias"]) for version in result.json}
    assert versions == expected_versions


@pytest.mark.integration
def test_project_deploy(
    runner,
    test_database,
    project_directory,
):
    project_name = "project_descriptive_name"
    entity_id = "my_project"
    with project_directory("dcm_project"):
        result = runner.invoke_with_connection(["project", "create", entity_id])
        assert result.exit_code == 0, result.output
        assert (
            f"Project '{project_name}' successfully created and initial version is added."
            in result.output
        )
        # project should be initialized with a version
        _assert_project_has_versions(
            runner, project_name, expected_versions={("VERSION$1", None)}
        )

        result = runner.invoke_with_connection(
            [
                "project",
                "dry-run",
                project_name,
                "--version",
                "last",
                "-D",
                f"table_name='{test_database}.PUBLIC.MyTable'",
            ]
        )
        assert result.exit_code == 0

        result = runner.invoke_with_connection(
            [
                "project",
                "execute",
                project_name,
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
                project_name,
            ]
        )
        assert result.exit_code == 0, result.output
        assert len(result.json) == 1
        project = result.json[0]
        assert project["name"].lower() == project_name.lower()


@pytest.mark.integration
def test_execute_multiple_configurations(
    runner,
    test_database,
    project_directory,
):
    project_name = "project_descriptive_name"
    entity_id = "my_project"
    with project_directory("dcm_project_multiple_configurations"):
        result = runner.invoke_with_connection(["project", "create", entity_id])
        assert result.exit_code == 0, result.output

        for configuration in ["test", "dev", "prod"]:
            for command in ["dry-run", "execute"]:
                result = runner.invoke_with_connection_json(
                    [
                        "project",
                        command,
                        project_name,
                        "--configuration",
                        configuration,
                        "-D",
                        f"db='{test_database}'",
                    ]
                )
                assert result.exit_code == 0, result.output

                assert (
                    result.json[0]["objectName"]
                    == f"{test_database}.PUBLIC.SNOWCLI_TEST_TABLE_{configuration}".upper()
                )


@pytest.mark.integration
def test_create_corner_cases(
    runner,
    test_database,
    project_directory,
):
    project_name = "project_descriptive_name"
    stage_name = "my_project_stage"
    with project_directory("dcm_project"):
        # case 1: stage already exists
        result = runner.invoke_with_connection(["stage", "create", stage_name])
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection(["project", "create"])
        assert result.exit_code == 1, result.output
        assert f"Stage '{stage_name}' already exists." in result.output

        result = runner.invoke_with_connection(["stage", "drop", stage_name])
        assert result.exit_code == 0, result.output

        # case 2: project already exists
        result = runner.invoke_with_connection(["project", "create"])
        assert result.exit_code == 0, result.output
        _assert_project_has_versions(
            runner, project_name, expected_versions={("VERSION$1", None)}
        )
        result = runner.invoke_with_connection(["project", "create"])
        assert result.exit_code == 1, result.output
        assert f"Project '{project_name}' already exists." in result.output
        _assert_project_has_versions(
            runner, project_name, expected_versions={("VERSION$1", None)}
        )
        result = runner.invoke_with_connection(["project", "create", "--if-not-exists"])
        assert result.exit_code == 0, result.output
        assert f"Project '{project_name}' already exists." in result.output
        _assert_project_has_versions(
            runner, project_name, expected_versions={("VERSION$1", None)}
        )


@pytest.mark.integration
def test_project_add_version(
    runner,
    test_database,
    project_directory,
):
    project_name = "project_descriptive_name"
    entity_id = "my_project"
    default_stage_name = "my_project_stage"
    other_stage_name = "other_project_stage"

    with project_directory("dcm_project") as root:
        # Create a new project
        result = runner.invoke_with_connection_json(
            ["project", "create", "--no-version"]
        )
        assert result.exit_code == 0, result.output
        assert f"Project '{project_name}' successfully created." in result.output
        # project should not be initialized with a new version due to --no-version flag
        _assert_project_has_versions(runner, project_name, expected_versions=set())

        # add version from local files
        result = runner.invoke_with_connection(["project", "add-version"])
        assert result.exit_code == 0, result.output
        assert f"New project version added to project '{project_name}'" in result.output
        _assert_project_has_versions(
            runner, project_name, expected_versions={("VERSION$1", None)}
        )
        assert_stage_has_files(
            runner,
            default_stage_name,
            {
                f"{default_stage_name}/manifest.yml",
                f"{default_stage_name}/file_a.sql",
            },
        )

        # upload files on another stage
        if (root / "output").exists():
            SecurePath(root / "output").rmdir(recursive=True)
        result = runner.invoke_with_connection(["stage", "create", other_stage_name])
        assert result.exit_code == 0, result.output
        result = runner.invoke_with_connection(
            ["stage", "copy", ".", f"@{other_stage_name}"]
        )
        assert result.exit_code == 0, result.output

        # create a new version of the project
        result = runner.invoke_with_connection(
            [
                "project",
                "add-version",
                entity_id,
                "--from",
                f"@{other_stage_name}",
                "--alias",
                "v2",
            ]
        )
        assert result.exit_code == 0, result.output
        assert (
            f"New project version 'v2' added to project '{project_name}'"
            in result.output
        )
        _assert_project_has_versions(
            runner, project_name, {("VERSION$1", None), ("VERSION$2", "v2")}
        )

        # --prune flag should remove unexpected file from the default stage
        unexpected_file = root / "unexpected.txt"
        unexpected_file.write_text("This is unexpected.")
        result = runner.invoke_with_connection(
            ["stage", "copy", str(unexpected_file), f"@{default_stage_name}"]
        )
        assert result.exit_code == 0, result.output

        # --no-prune - unexpected file remains
        result = runner.invoke_with_connection(
            ["project", "add-version", "--alias", "v3.1", "--no-prune"]
        )
        assert result.exit_code == 0, result.output
        _assert_project_has_versions(
            runner,
            project_name,
            {("VERSION$1", None), ("VERSION$2", "v2"), ("VERSION$3", "v3.1")},
        )
        assert_stage_has_files(
            runner,
            default_stage_name,
            {
                f"{default_stage_name}/manifest.yml",
                f"{default_stage_name}/file_a.sql",
                f"{default_stage_name}/unexpected.txt",
            },
        )

        # prune flag - unexpected file should be removed
        result = runner.invoke_with_connection(
            ["project", "add-version", "--alias", "v3.2"]
        )
        assert result.exit_code == 0, result.output
        _assert_project_has_versions(
            runner,
            project_name,
            {
                ("VERSION$1", None),
                ("VERSION$2", "v2"),
                ("VERSION$3", "v3.1"),
                ("VERSION$4", "v3.2"),
            },
        )
        assert_stage_has_files(
            runner,
            default_stage_name,
            {
                f"{default_stage_name}/manifest.yml",
                f"{default_stage_name}/file_a.sql",
            },
        )


@pytest.mark.integration
def test_project_add_version_without_create_fails(
    runner,
    test_database,
    project_directory,
):
    project_name = "project_descriptive_name"
    default_stage_name = "my_project_stage"

    with project_directory("dcm_project"):
        # call add-version first (by mistake)
        result = runner.invoke_with_connection(["project", "add-version"])
        assert result.exit_code == 1, result.output
        assert f"Project '{project_name}' does not exist." in result.output

        assert does_stage_exist(runner, default_stage_name) is False

        # make sure that user can still create a project and stage
        result = runner.invoke_with_connection_json(["project", "create"])
        assert result.exit_code == 0, result.output
        assert does_stage_exist(runner, default_stage_name) is True


@pytest.mark.qa_only
@pytest.mark.integration
def test_project_execute_from_stage(
    runner,
    test_database,
    project_directory,
):
    project_name = "project_descriptive_name"
    entity_id = "my_project"
    other_stage_name = "other_project_stage"

    with project_directory("dcm_project") as project_root:
        # Create a new project
        result = runner.invoke_with_connection(["project", "create", entity_id])
        assert result.exit_code == 0, result.output
        _assert_project_has_versions(
            runner, project_name, expected_versions={("VERSION$1", None)}
        )

        # Edit file_a.sql to add a second table definition
        file_a_path = project_root / "file_a.sql"
        original_content = file_a_path.read_text()
        modified_content = (
            original_content
            + "\ndefine table identifier('{{ table_name }}_SECOND') (id int, name string);\n"
        )
        file_a_path.write_text(modified_content)

        # Create another stage and upload files there
        result = runner.invoke_with_connection(["stage", "create", other_stage_name])
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection(
            ["stage", "copy", ".", f"@{other_stage_name}"]
        )
        assert result.exit_code == 0, result.output

        # Test dry-run from stage
        result = runner.invoke_with_connection_json(
            [
                "project",
                "dry-run",
                project_name,
                "--from",
                f"@{other_stage_name}",
                "-D",
                f"table_name='{test_database}.PUBLIC.MyTable'",
            ]
        )
        assert result.exit_code == 0, result.output
        # Assert that both tables are mentioned in the output
        output_str = str(result.json)
        assert f"{test_database}.PUBLIC.MYTABLE".upper() in output_str.upper()
        assert f"{test_database}.PUBLIC.MYTABLE_SECOND".upper() in output_str.upper()

        # Verify that the second table does not exist after dry-run
        table_check_result = runner.invoke_with_connection_json(
            [
                "object",
                "list",
                "table",
                "--like",
                "MYTABLE_SECOND",
                "--in",
                "database",
                test_database,
            ]
        )
        assert table_check_result.exit_code == 0
        assert len(table_check_result.json) == 0, "Table should not exist after dry-run"

        # Test execute from stage
        result = runner.invoke_with_connection_json(
            [
                "project",
                "execute",
                project_name,
                "--from",
                f"@{other_stage_name}",
                "-D",
                f"table_name='{test_database}.PUBLIC.MyTable'",
            ]
        )
        assert result.exit_code == 0, result.output
        # Assert that both tables are mentioned in the output
        output_str = str(result.json)
        assert f"{test_database}.PUBLIC.MYTABLE".upper() in output_str.upper()
        assert f"{test_database}.PUBLIC.MYTABLE_SECOND".upper() in output_str.upper()

        # Verify that the second table actually exists after execute
        table_check_result = runner.invoke_with_connection_json(
            [
                "object",
                "list",
                "table",
                "--like",
                "MYTABLE_SECOND",
                "--in",
                "database",
                test_database,
            ]
        )
        assert table_check_result.exit_code == 0
        assert (
            "MYTABLE_SECOND" == table_check_result.json[0]["name"]
        ), "Table should exist after execute"
