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

from typing import Set, Optional, Tuple
from tests_integration.test_utils import assert_stage_has_files


def _assert_project_has_versions(
    runner, project_name: str, expected_versions: Set[Tuple[str, Optional[str]]]
) -> None:
    """Check whether the project versions (in [name,alias] format) are present in Snowflake."""
    result = runner.invoke_with_connection_json(
        ["dcm", "list-deployments", project_name]
    )
    assert result.exit_code == 0, result.output
    versions = {(version["name"], version["alias"]) for version in result.json}
    assert versions == expected_versions


@pytest.mark.qa_only
@pytest.mark.integration
def test_project_deploy(
    runner,
    test_database,
    project_directory,
):
    project_name = "project_descriptive_name"
    entity_id = "my_project"
    with project_directory("dcm_project"):
        result = runner.invoke_with_connection(["dcm", "create", entity_id])
        assert result.exit_code == 0, result.output
        assert f"DCM Project '{project_name}' successfully created." in result.output

        result = runner.invoke_with_connection_json(["dcm", "describe", project_name])
        assert result.exit_code == 0, result.output
        assert result.json[0]["name"].lower() == project_name.lower()

        # project should have no initial versions
        _assert_project_has_versions(runner, project_name, expected_versions=set())

        # Add version
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
        _assert_project_has_versions(
            runner,
            project_name,
            {("VERSION$1", None)},
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
    entity_id = "my_project"
    with project_directory("dcm_project_multiple_configurations"):
        result = runner.invoke_with_connection(["dcm", "create", entity_id])
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
    stage_name = "my_project_stage"
    with project_directory("dcm_project"):
        # case 1: stage already exists
        result = runner.invoke_with_connection(["stage", "create", stage_name])
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection(["dcm", "create"])
        assert result.exit_code == 1, result.output
        assert f"Stage '{stage_name}' already exists." in result.output

        result = runner.invoke_with_connection(["stage", "drop", stage_name])
        assert result.exit_code == 0, result.output

        # case 2: project already exists
        result = runner.invoke_with_connection(["dcm", "create"])
        assert result.exit_code == 0, result.output
        _assert_project_has_versions(runner, project_name, expected_versions=set())
        result = runner.invoke_with_connection(["dcm", "create"])
        assert result.exit_code == 1, result.output
        assert f"DCM Project '{project_name}' already exists." in result.output
        _assert_project_has_versions(runner, project_name, expected_versions=set())
        result = runner.invoke_with_connection(["dcm", "create", "--if-not-exists"])
        assert result.exit_code == 0, result.output
        assert f"DCM Project '{project_name}' already exists." in result.output
        _assert_project_has_versions(runner, project_name, expected_versions=set())

        # Clean up
        result = runner.invoke_with_connection(["dcm", "drop", project_name])
        assert result.exit_code == 0, result.output


@pytest.mark.qa_only
@pytest.mark.integration
def test_project_drop_version(
    runner,
    test_database,
    project_directory,
):
    project_name = "project_descriptive_name"
    entity_id = "my_project"

    with project_directory("dcm_project"):
        # Create project with initial version
        result = runner.invoke_with_connection(["dcm", "create", entity_id])
        assert result.exit_code == 0, result.output
        assert f"DCM Project '{project_name}' successfully created." in result.output
        _assert_project_has_versions(runner, project_name, expected_versions=set())

        # Drop the non-existent version (should fail without --if-exists)
        result = runner.invoke_with_connection(
            ["dcm", "drop-deployment", project_name, "VERSION$1"]
        )
        assert result.exit_code == 1, result.output
        assert "Version does not exist" in result.output

        # Add version
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
        _assert_project_has_versions(
            runner,
            project_name,
            {("VERSION$1", None)},
        )

        # Add another version with alias
        result = runner.invoke_with_connection(
            [
                "dcm",
                "deploy",
                project_name,
                "--alias",
                "v2",
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
        _assert_project_has_versions(
            runner,
            project_name,
            {("VERSION$1", None), ("VERSION$2", "V2"), ("VERSION$3", "THEDEFAULT")},
        )

        # Drop the version by name
        result = runner.invoke_with_connection(
            ["dcm", "drop-deployment", project_name, "VERSION$1"]
        )
        assert result.exit_code == 0, result.output
        assert (
            f"Version 'VERSION$1' dropped from DCM Project '{project_name}'"
            in result.output
        )

        # Drop the version by alias
        result = runner.invoke_with_connection(
            ["dcm", "drop-deployment", project_name, "v2"]
        )
        assert result.exit_code == 0, result.output
        assert (
            f"Version 'v2' dropped from DCM Project '{project_name}'" in result.output
        )

        _assert_project_has_versions(
            runner, project_name, expected_versions={("VERSION$3", "THEDEFAULT")}
        )

        # Try to drop the default version
        result = runner.invoke_with_connection(
            ["dcm", "drop-deployment", project_name, "VERSION$3"]
        )
        assert result.exit_code == 0, result.output
        assert (
            f"Version 'VERSION$3' dropped from DCM Project '{project_name}'"
            in result.output
        )

        # Try to drop non-existent version without --if-exists (should fail)
        result = runner.invoke_with_connection(
            ["dcm", "drop-deployment", project_name, "non_existent"]
        )
        assert result.exit_code == 1, result.output
        assert "Version does not exist" in result.output

        # Try to drop non-existent version with --if-exists (should succeed)
        result = runner.invoke_with_connection(
            ["dcm", "drop-deployment", project_name, "non_existent", "--if-exists"]
        )
        assert result.exit_code == 0, result.output
        assert (
            f"Version 'non_existent' dropped from DCM Project '{project_name}'"
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
    entity_id = "my_project"
    other_stage_name = "other_project_stage"

    with project_directory("dcm_project") as project_root:
        # Create a new project
        result = runner.invoke_with_connection(["dcm", "create", entity_id])
        assert result.exit_code == 0, result.output
        _assert_project_has_versions(runner, project_name, expected_versions=set())

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

        # Test plan from stage
        result = runner.invoke_with_connection_json(
            [
                "dcm",
                "plan",
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
def test_project_deploy_with_prune(
    runner,
    test_database,
    project_directory,
):
    """Test that --prune flag removes unused artifacts from the stage and prevents creation of database objects from pruned files."""
    project_name = "project_descriptive_name"
    entity_id = "my_project"
    stage_name = "my_project_stage"

    with project_directory("dcm_project") as project_root:
        # Create a new project
        result = runner.invoke_with_connection(["dcm", "create", entity_id])
        assert result.exit_code == 0, result.output
        assert (
            "DCM Project" in result.output
            and project_name in result.output
            and "successfully created" in result.output
        )
        _assert_project_has_versions(runner, project_name, expected_versions=set())

        # Create an additional file that we'll remove later to test prune
        file_b_path = project_root / "file_b.sql"
        file_b_path.write_text(
            "define table identifier('{{ table_name }}_B') (id int, data string);\n"
        )

        # Update snowflake.yml to include the new file
        config_path = project_root / "snowflake.yml"
        config_content = config_path.read_text()
        config_content = config_content.replace(
            "artifacts:\n      - file_a.sql",
            "artifacts:\n      - file_a.sql\n      - file_b.sql",
        )
        config_path.write_text(config_content)

        # Update manifest.yml to include the new file in definitions
        manifest_path = project_root / "manifest.yml"
        manifest_content = manifest_path.read_text()
        manifest_content = manifest_content.replace(
            "include_definitions:\n  - file_a.sql",
            "include_definitions:\n  - file_a.sql\n  - file_b.sql",
        )
        manifest_path.write_text(manifest_content)

        # Initial deploy with both files
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
        _assert_project_has_versions(
            runner,
            project_name,
            {("VERSION$1", None)},
        )

        # Verify both files are in the stage
        assert_stage_has_files(
            runner,
            f"@{stage_name}",
            [
                f"{stage_name}/file_a.sql",
                f"{stage_name}/file_b.sql",
                f"{stage_name}/manifest.yml",
            ],
        )

        # Verify both tables exist after first deploy
        table_a_check = runner.invoke_with_connection_json(
            [
                "object",
                "list",
                "table",
                "--like",
                "MYTABLE",
                "--in",
                "database",
                test_database,
            ]
        )
        assert table_a_check.exit_code == 0
        assert len(table_a_check.json) == 1, "MyTable should exist after first deploy"
        assert "MYTABLE" == table_a_check.json[0]["name"]

        table_b_check = runner.invoke_with_connection_json(
            [
                "object",
                "list",
                "table",
                "--like",
                "MYTABLE_B",
                "--in",
                "database",
                test_database,
            ]
        )
        assert table_b_check.exit_code == 0
        assert len(table_b_check.json) == 1, "MyTable_B should exist after first deploy"
        assert "MYTABLE_B" == table_b_check.json[0]["name"]

        # Now remove file_b.sql from artifacts to test prune functionality
        config_content = config_path.read_text()
        config_content = config_content.replace(
            "artifacts:\n      - file_a.sql\n      - file_b.sql",
            "artifacts:\n      - file_a.sql",
        )
        config_path.write_text(config_content)

        # Also remove file_b.sql from manifest include_definitions
        manifest_content = manifest_path.read_text()
        manifest_content = manifest_content.replace(
            "include_definitions:\n  - file_a.sql\n  - file_b.sql",
            "include_definitions:\n  - file_a.sql",
        )
        manifest_path.write_text(manifest_content)

        # Deploy again with --prune flag to remove unused file_b.sql
        result = runner.invoke_with_connection(
            [
                "dcm",
                "deploy",
                project_name,
                "--prune",
                "-D",
                f"table_name='{test_database}.PUBLIC.MyTable'",
            ]
        )
        assert result.exit_code == 0, result.output
        _assert_project_has_versions(
            runner,
            project_name,
            {("VERSION$1", None), ("VERSION$2", None)},
        )

        # Verify that file_b.sql was removed from the stage but file_a.sql and manifest.yml remain
        stage_files_result = runner.invoke_with_connection_json(
            ["stage", "list-files", f"@{stage_name}"]
        )
        assert stage_files_result.exit_code == 0
        stage_files = [file["name"] for file in stage_files_result.json]

        # file_a.sql and manifest.yml should still be present
        assert (
            f"{stage_name}/file_a.sql" in stage_files
        ), f"file_a.sql should be present in stage. Files: {stage_files}"
        assert (
            f"{stage_name}/manifest.yml" in stage_files
        ), f"manifest.yml should be present in stage. Files: {stage_files}"

        # file_b.sql should be removed
        assert (
            f"{stage_name}/file_b.sql" not in stage_files
        ), f"file_b.sql should be removed from stage. Files: {stage_files}"

        # Verify that MyTable still exists but MyTable_B does not exist after prune deploy
        table_a_check_after_prune = runner.invoke_with_connection_json(
            [
                "object",
                "list",
                "table",
                "--like",
                "MYTABLE",
                "--in",
                "database",
                test_database,
            ]
        )
        assert table_a_check_after_prune.exit_code == 0
        # Should find only one table (MYTABLE), not MYTABLE_B
        mytable_found = False
        for table in table_a_check_after_prune.json:
            if table["name"] == "MYTABLE":
                mytable_found = True
            # Ensure MYTABLE_B is not in the results
            assert (
                table["name"] != "MYTABLE_B"
            ), "MyTable_B should not exist after prune deploy"
        assert mytable_found, "MyTable should still exist after prune deploy"

        # Double-check: specifically query for MYTABLE_B to ensure it doesn't exist
        table_b_check_after_prune = runner.invoke_with_connection_json(
            [
                "object",
                "list",
                "table",
                "--like",
                "MYTABLE_B",
                "--in",
                "database",
                test_database,
            ]
        )
        assert table_b_check_after_prune.exit_code == 0
        assert (
            len(table_b_check_after_prune.json) == 0
        ), "MyTable_B should not exist after prune deploy"

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
    entity_id = "my_project"
    source_stage_name = "source_project_stage"
    output_stage_name = "output_results_stage"
    output_path = f"@{output_stage_name}/plan_results"

    with project_directory("dcm_project") as project_root:
        # Create a new project
        result = runner.invoke_with_connection(["dcm", "create", entity_id])
        assert result.exit_code == 0, result.output
        _assert_project_has_versions(runner, project_name, expected_versions=set())

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
            ["stage", "copy", ".", f"@{source_stage_name}"]
        )
        assert result.exit_code == 0, result.output

        # Create output stage for plan results
        result = runner.invoke_with_connection(["stage", "create", output_stage_name])
        assert result.exit_code == 0, result.output

        # Test plan with output-path option
        result = runner.invoke_with_connection_json(
            [
                "dcm",
                "plan",
                project_name,
                "--from",
                f"@{source_stage_name}",
                "--output-path",
                output_path,
                "-D",
                f"table_name='{test_database}.PUBLIC.OutputTestTable'",
            ]
        )
        assert result.exit_code == 0, result.output

        # Verify that the plan was executed successfully
        output_str = str(result.json)
        assert (
            f"{test_database}.PUBLIC.OUTPUTTESTTABLE_OUTPUT_TEST".upper()
            in output_str.upper()
        )

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

        # Verify that the table does not exist after plan (plan should not create actual objects)
        table_check_result = runner.invoke_with_connection_json(
            [
                "object",
                "list",
                "table",
                "--like",
                "OUTPUTTESTTABLE_OUTPUT_TEST",
                "--in",
                "database",
                test_database,
            ]
        )
        assert table_check_result.exit_code == 0
        assert (
            len(table_check_result.json) == 0
        ), "Table should not exist after plan operation"

        # Clean up stages
        result = runner.invoke_with_connection(["stage", "drop", source_stage_name])
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection(["stage", "drop", output_stage_name])
        assert result.exit_code == 0, result.output

        # Clean up project
        result = runner.invoke_with_connection(["dcm", "drop", project_name])
        assert result.exit_code == 0, result.output
