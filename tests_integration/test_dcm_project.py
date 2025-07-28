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
    result = runner.invoke_with_connection_json(["dcm", "list-versions", project_name])
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

        # remove project
        result = runner.invoke_with_connection(["dcm", "drop"])
        assert result.exit_code == 0, result.output
        assert f"DCM Project '{project_name}' successfully dropped." in result.output


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
            ["dcm", "drop-version", project_name, "VERSION$1"]
        )
        assert result.exit_code == 1, result.output
        assert "Version does not exist" in result.output

        # Verify still no versions exist
        _assert_project_has_versions(runner, project_name, expected_versions=set())


@pytest.mark.qa_only
@pytest.mark.integration
def test_project_deploy_from_stage(
    runner,
    test_database,
    project_directory,
):
    project_name = "project_descriptive_name"
    entity_id = "my_project"

    with project_directory("dcm_project") as project_root:
        # Create a new project
        result = runner.invoke_with_connection(["dcm", "create", entity_id])
        assert result.exit_code == 0, result.output
        _assert_project_has_versions(runner, project_name, expected_versions=set())

        # Verify project was created successfully
        result = runner.invoke_with_connection_json(["dcm", "describe", project_name])
        assert result.exit_code == 0, result.output
        assert result.json[0]["name"].lower() == project_name.lower()

        # Clean up
        result = runner.invoke_with_connection(["dcm", "drop", project_name])
        assert result.exit_code == 0, result.output
