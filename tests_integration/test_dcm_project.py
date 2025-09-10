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
import pytest

from typing import Set, Optional, Tuple


def _assert_project_has_deployments(
    runner, project_name: str, expected_deployments: Set[Tuple[str, Optional[str]]]
) -> None:
    """Check whether the project deployments (in [name,alias] format) are present in Snowflake."""
    result = runner.invoke_with_connection_json(
        ["dcm", "list-deployments", project_name]
    )
    assert result.exit_code == 0, result.output
    deployments = {
        (deployment["name"], deployment["alias"]) for deployment in result.json
    }
    assert deployments == expected_deployments


@pytest.mark.qa_only
@pytest.mark.integration
def test_project_deploy(
    runner,
    test_database,
    project_directory,
):
    project_name = "project_descriptive_name"
    with project_directory("dcm_project"):
        result = runner.invoke_with_connection(["dcm", "create", project_name])
        assert result.exit_code == 0, result.output
        assert f"DCM Project '{project_name}' successfully created." in result.output

        result = runner.invoke_with_connection_json(["dcm", "describe", project_name])
        assert result.exit_code == 0, result.output
        assert result.json[0]["name"].lower() == project_name.lower()

        # project should have no initial deployments
        _assert_project_has_deployments(
            runner, project_name, expected_deployments=set()
        )

        # Add deployment
        result = runner.invoke_with_connection(
            [
                "dcm",
                "deploy",
                project_name,
                "-D",
                f"table_name='{test_database}.PUBLIC.MyTable'",
            ]
        )
        assert result.exit_code == 0, result.output
        _assert_project_has_deployments(
            runner,
            project_name,
            {("DEPLOYMENT$1", None)},
        )

        # remove project
        result = runner.invoke_with_connection(["dcm", "drop", project_name])
        assert result.exit_code == 0, result.output


@pytest.mark.qa_only
@pytest.mark.integration
def test_deploy_multiple_configurations(
    runner,
    test_database,
    project_directory,
):
    project_name = "project_descriptive_name"
    with project_directory("dcm_project_multiple_configurations"):
        result = runner.invoke_with_connection(["dcm", "create", project_name])
        assert result.exit_code == 0, result.output
        assert f"DCM Project '{project_name}' successfully created." in result.output

        # Verify project was created
        result = runner.invoke_with_connection_json(["dcm", "describe", project_name])
        assert result.exit_code == 0, result.output
        assert result.json[0]["name"].lower() == project_name.lower()

        # Clean up
        result = runner.invoke_with_connection(["dcm", "drop", project_name])
        assert result.exit_code == 0, result.output


@pytest.mark.qa_only
@pytest.mark.integration
def test_create_corner_cases(
    runner,
    test_database,
    project_directory,
):
    project_name = "project_descriptive_name"
    with project_directory("dcm_project"):
        # case 1: project already exists
        result = runner.invoke_with_connection(["dcm", "create", project_name])
        assert result.exit_code == 0, result.output
        _assert_project_has_deployments(
            runner, project_name, expected_deployments=set()
        )
        result = runner.invoke_with_connection(["dcm", "create", project_name])
        assert result.exit_code == 1, result.output
        assert f"DCM Project '{project_name}' already exists." in result.output
        _assert_project_has_deployments(
            runner, project_name, expected_deployments=set()
        )
        result = runner.invoke_with_connection(
            ["dcm", "create", project_name, "--if-not-exists"]
        )
        assert result.exit_code == 0, result.output
        assert f"DCM Project '{project_name}' already exists." in result.output
        _assert_project_has_deployments(
            runner, project_name, expected_deployments=set()
        )

        # Clean up
        result = runner.invoke_with_connection(["dcm", "drop", project_name])
        assert result.exit_code == 0, result.output


@pytest.mark.qa_only
@pytest.mark.integration
def test_project_drop_deployment(
    runner,
    test_database,
    project_directory,
):
    project_name = "project_descriptive_name"

    with project_directory("dcm_project"):
        # Create project
        result = runner.invoke_with_connection(["dcm", "create", project_name])
        assert result.exit_code == 0, result.output
        assert f"DCM Project '{project_name}' successfully created." in result.output
        _assert_project_has_deployments(
            runner, project_name, expected_deployments=set()
        )

        # Drop the non-existent deployment (should fail without --if-exists)
        result = runner.invoke_with_connection(
            ["dcm", "drop-deployment", project_name, "DEPLOYMENT$1"]
        )
        assert result.exit_code == 1, result.output
        assert "Deployment does not exist" in result.output

        # Add deployment
        result = runner.invoke_with_connection(
            [
                "dcm",
                "deploy",
                project_name,
                "-D",
                f"table_name='{test_database}.PUBLIC.MyTable'",
            ]
        )
        assert result.exit_code == 0, result.output
        _assert_project_has_deployments(
            runner,
            project_name,
            {("DEPLOYMENT$1", None)},
        )

        # Add another deployment with alias
        result = runner.invoke_with_connection(
            [
                "dcm",
                "deploy",
                project_name,
                "--alias",
                "test-1",
                "-D",
                f"table_name='{test_database}.PUBLIC.MyTable'",
            ]
        )
        assert result.exit_code == 0, result.output
        result = runner.invoke_with_connection(
            [
                "dcm",
                "deploy",
                project_name,
                "--alias",
                "theDefault",
                "-D",
                f"table_name='{test_database}.PUBLIC.MyTable'",
            ]
        )
        assert result.exit_code == 0, result.output
        _assert_project_has_deployments(
            runner,
            project_name,
            {
                ("DEPLOYMENT$1", None),
                ("DEPLOYMENT$2", "test-1"),
                ("DEPLOYMENT$3", "theDefault"),
            },
        )
        # Drop the deployment by name
        result = runner.invoke_with_connection(
            ["dcm", "drop-deployment", project_name, "DEPLOYMENT$1"]
        )
        assert result.exit_code == 0, result.output
        assert (
            f"Deployment 'DEPLOYMENT$1' dropped from DCM Project '{project_name}'"
            in result.output
        )

        # Drop the deployment by alias
        result = runner.invoke_with_connection(
            ["dcm", "drop-deployment", project_name, "test-1"]
        )
        assert result.exit_code == 0, result.output
        assert (
            f"Deployment 'test-1' dropped from DCM Project '{project_name}'"
            in result.output
        )

        _assert_project_has_deployments(
            runner, project_name, expected_deployments={("DEPLOYMENT$3", "theDefault")}
        )

        # Try to drop the default deployment
        result = runner.invoke_with_connection(
            ["dcm", "drop-deployment", project_name, "DEPLOYMENT$3"]
        )
        assert result.exit_code == 0, result.output
        assert (
            f"Deployment 'DEPLOYMENT$3' dropped from DCM Project '{project_name}'"
            in result.output
        )

        # Try to drop non-existent deployment without --if-exists (should fail)
        result = runner.invoke_with_connection(
            ["dcm", "drop-deployment", project_name, "non_existent"]
        )
        assert result.exit_code == 1, result.output
        assert "Deployment does not exist" in result.output

        # Try to drop non-existent deployment with --if-exists (should succeed)
        result = runner.invoke_with_connection(
            ["dcm", "drop-deployment", project_name, "non_existent", "--if-exists"]
        )
        assert result.exit_code == 0, result.output
        assert (
            f"Deployment 'non_existent' dropped from DCM Project '{project_name}'"
            in result.output
        )


@pytest.mark.qa_only
@pytest.mark.integration
def test_project_deploy_from_stage(
    runner,
    test_database,
    project_directory,
):
    project_name = "project_descriptive_name"
    other_stage_name = "other_project_stage"

    with project_directory("dcm_project") as project_root:
        # Create a new project
        result = runner.invoke_with_connection(["dcm", "create", project_name])
        assert result.exit_code == 0, result.output
        _assert_project_has_deployments(
            runner, project_name, expected_deployments=set()
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
            ["stage", "copy", ".", f"@{other_stage_name}/project"]
        )
        assert result.exit_code == 0, result.output

        # Test plan from stage
        result = runner.invoke_with_connection_json(
            [
                "dcm",
                "plan",
                project_name,
                "--from",
                f"@{other_stage_name}/project",
                "-D",
                f"table_name='{test_database}.PUBLIC.MyTable'",
            ]
        )
        assert result.exit_code == 0, result.output
        # Assert that both tables are mentioned in the output
        output_str = str(result.json)
        assert f"{test_database}.PUBLIC.MYTABLE".upper() in output_str.upper()
        assert f"{test_database}.PUBLIC.MYTABLE_SECOND".upper() in output_str.upper()

        # Verify that the second table does not exist after plan
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
        assert len(table_check_result.json) == 0, "Table should not exist after plan"

        # Test deploy from stage
        result = runner.invoke_with_connection_json(
            [
                "dcm",
                "deploy",
                project_name,
                "--from",
                f"@{other_stage_name}/project",
                "-D",
                f"table_name='{test_database}.PUBLIC.MyTable'",
            ]
        )
        assert result.exit_code == 0, result.output
        # Assert that both tables are mentioned in the output
        output_str = str(result.json)
        assert f"{test_database}.PUBLIC.MYTABLE".upper() in output_str.upper()
        assert f"{test_database}.PUBLIC.MYTABLE_SECOND".upper() in output_str.upper()

        # Verify that the second table actually exists after deploy
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
        ), "Table should exist after deploy"

        # Clean up
        result = runner.invoke_with_connection(["dcm", "drop", project_name])
        assert result.exit_code == 0, result.output


@pytest.mark.qa_only
@pytest.mark.integration
def test_project_plan_with_output_path(
    runner,
    test_database,
    project_directory,
):
    """Test that DCM plan command with --output-path option writes output to the specified stage."""
    project_name = "project_descriptive_name"
    source_stage_name = "source_project_stage"
    output_stage_name = "output_results_stage"
    output_path = f"@{output_stage_name}/plan_results"

    with project_directory("dcm_project") as project_root:
        # Create a new project
        result = runner.invoke_with_connection(["dcm", "create", project_name])
        assert result.exit_code == 0, result.output
        _assert_project_has_deployments(
            runner, project_name, expected_deployments=set()
        )

        # Edit file_a.sql to add a table definition for testing
        file_a_path = project_root / "file_a.sql"
        original_content = file_a_path.read_text()
        modified_content = (
            original_content
            + "\ndefine table identifier('{{ table_name }}_OUTPUT_TEST') (id int, name string);\n"
        )
        file_a_path.write_text(modified_content)

        # Create source stage and upload files there
        result = runner.invoke_with_connection(["stage", "create", source_stage_name])
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection(
            ["stage", "copy", ".", f"@{source_stage_name}/project"]
        )
        assert result.exit_code == 0, result.output

        # Create output stage for plan results
        result = runner.invoke_with_connection(["stage", "create", output_stage_name])
        assert result.exit_code == 0, result.output

        # Test plan with stage output-path option
        result = runner.invoke_with_connection_json(
            [
                "dcm",
                "plan",
                project_name,
                "--from",
                f"@{source_stage_name}/project",
                "--output-path",
                output_path,
                "-D",
                f"table_name='{test_database}.PUBLIC.OutputTestTable'",
            ]
        )
        assert result.exit_code == 0, result.output

        # Verify that the output was written to the specified stage path
        # Check if there are files in the output stage
        stage_list_result = runner.invoke_with_connection_json(
            ["stage", "list-files", output_path]
        )
        assert stage_list_result.exit_code == 0, stage_list_result.output

        # There should be at least one file in the output location
        assert (
            len(stage_list_result.json) > 0
        ), "Plan output should be written to the specified stage path"

        # Verify that one of the files contains plan-related content by checking file names
        file_names = [file["name"] for file in stage_list_result.json]
        assert any(
            "plan" in name.lower()
            or "result" in name.lower()
            or name.endswith((".json", ".txt", ".sql"))
            for name in file_names
        ), f"Expected plan output files, but found: {file_names}"

        # Test plan with local output-path option
        local_output_dir = "./dcm_output"
        result = runner.invoke_with_connection_json(
            [
                "dcm",
                "plan",
                project_name,
                "--from",
                f"@{source_stage_name}/project",
                "--output-path",
                local_output_dir,
                "-D",
                f"table_name='{test_database}.PUBLIC.OutputTestTable'",
            ]
        )
        assert result.exit_code == 0, result.output
        assert os.path.exists(
            local_output_dir
        ), f"Local output directory {local_output_dir} does not exist"

        local_files = set()
        for root, dirs, files in os.walk(local_output_dir):
            for file in files:
                relative_path = os.path.relpath(
                    os.path.join(root, file), local_output_dir
                )
                local_files.add(relative_path)

        stage_files = set()
        for name in file_names:
            normalized_name = name.removeprefix(f"{output_stage_name}/plan_results/")
            stage_files.add(normalized_name)

        diff = stage_files.symmetric_difference(local_files)
        assert (
            not diff
        ), f"Files present in stage but missing in local output directory: {stage_files - local_files}. Local files found but missing on stage: {local_files - stage_files}"

        # Clean up stages
        result = runner.invoke_with_connection(["stage", "drop", source_stage_name])
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection(["stage", "drop", output_stage_name])
        assert result.exit_code == 0, result.output

        # Clean up project
        result = runner.invoke_with_connection(["dcm", "drop", project_name])
        assert result.exit_code == 0, result.output


@pytest.mark.qa_only
@pytest.mark.integration
def test_dcm_plan_and_deploy_from_another_directory(
    runner,
    test_database,
    project_directory,
    tmp_path,
):
    project_name = "project_descriptive_name"

    with project_directory("dcm_project") as project_root:
        project_source_path = project_root

    original_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)

        result = runner.invoke_with_connection(["dcm", "create", project_name])
        assert result.exit_code == 0, result.output
        assert f"DCM Project '{project_name}' successfully created." in result.output

        result = runner.invoke_with_connection(
            [
                "dcm",
                "plan",
                project_name,
                "-D",
                f"table_name='{test_database}.PUBLIC.MyTable'",
                "--from",
                f"{project_source_path}",
            ]
        )
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection(
            [
                "dcm",
                "deploy",
                project_name,
                "-D",
                f"table_name='{test_database}.PUBLIC.MyTable'",
                "--from",
                f"{project_source_path}",
            ]
        )
        assert result.exit_code == 0, result.output
    finally:
        os.chdir(original_cwd)

    # Clean up
    result = runner.invoke_with_connection(["dcm", "drop", project_name])
    assert result.exit_code == 0, result.output
