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

import json
import os
import pytest

from typing import Set, Optional, Tuple

from tests_integration.conftest import CommandResult


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


def _extract_and_validate_raw_analyze_json(output: str):
    lines = output.strip().split("\n")
    json_line = next(
        (line for line in reversed(lines) if line.strip().startswith(("{", "["))), None
    )
    assert json_line, "No JSON output found"
    output_json = json.loads(json_line)
    assert isinstance(output_json, (list, dict)), "Expected JSON response"
    return output_json


def assert_last_stdout_line_equals(expected, result: CommandResult):
    assert result.output is not None
    assert expected in result.output.strip().split("\n")[-1], result.output


@pytest.mark.qa_only
@pytest.mark.integration
def test_dcm_deploy(
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
def test_dcm_drop_deployment(
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
            ["dcm", "drop-deployment", project_name, "--deployment", "DEPLOYMENT$1"]
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
            ["dcm", "drop-deployment", project_name, "--deployment", "DEPLOYMENT$1"]
        )
        assert result.exit_code == 0, result.output
        assert (
            f"Deployment 'DEPLOYMENT$1' dropped from DCM Project '{project_name}'"
            in result.output
        )

        # Drop the deployment by alias
        result = runner.invoke_with_connection(
            ["dcm", "drop-deployment", project_name, "--deployment", "test-1"]
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
            ["dcm", "drop-deployment", project_name, "--deployment", "DEPLOYMENT$3"]
        )
        assert result.exit_code == 0, result.output
        assert (
            f"Deployment 'DEPLOYMENT$3' dropped from DCM Project '{project_name}'"
            in result.output
        )

        # Try to drop non-existent deployment without --if-exists (should fail)
        result = runner.invoke_with_connection(
            ["dcm", "drop-deployment", project_name, "--deployment", "non_existent"]
        )
        assert result.exit_code == 1, result.output
        assert "Deployment does not exist" in result.output

        # Try to drop non-existent deployment with --if-exists (should succeed)
        result = runner.invoke_with_connection(
            [
                "dcm",
                "drop-deployment",
                project_name,
                "--deployment",
                "non_existent",
                "--if-exists",
            ]
        )
        assert result.exit_code == 0, result.output
        assert (
            f"Deployment 'non_existent' dropped from DCM Project '{project_name}'"
            in result.output
        )


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


@pytest.mark.qa_only
@pytest.mark.integration
def test_dcm_plan_with_save_output(
    runner,
    test_database,
    project_directory,
):
    project_name = "project_descriptive_name"
    output_dir = "out"

    with project_directory("dcm_project") as project_root:
        result = runner.invoke_with_connection(["dcm", "create", project_name])
        assert result.exit_code == 0, result.output
        assert f"DCM Project '{project_name}' successfully created." in result.output

        result = runner.invoke_with_connection(
            [
                "dcm",
                "plan",
                project_name,
                "--save-output",
                "-D",
                f"table_name='{test_database}.PUBLIC.OutputTestTable'",
            ]
        )
        assert result.exit_code == 0, result.output
        assert_last_stdout_line_equals(
            "Planned 1 entity (1 to create, 0 to alter, 0 to drop).", result
        )

        output_path = project_root / output_dir
        assert output_path.exists(), f"Output directory {output_dir} was not created."

        local_files = []
        for root, dirs, files in os.walk(output_path):
            for file in files:
                relative_path = os.path.relpath(os.path.join(root, file), output_path)
                local_files.append(relative_path)

        assert len(local_files) > 0, "No output files were downloaded to ./out/"


@pytest.mark.qa_only
@pytest.mark.integration
def test_dcm_preview_command(
    runner,
    test_database,
    project_directory,
    object_name_provider,
    sql_test_helper,
):
    project_name = object_name_provider.create_and_get_next_object_name()
    view_name = f"{test_database}.PUBLIC.PreviewTestView"
    base_table_name = f"{test_database}.PUBLIC.OutputTestTable"

    with project_directory("dcm_project") as project_root:
        result = runner.invoke_with_connection(["dcm", "create", project_name])
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(
            [
                "dcm",
                "deploy",
                project_name,
                "-D",
                f"table_name='{base_table_name}'",
            ]
        )
        assert result.exit_code == 0, result.output

        # Define a view that selects from OutputTestTable. Preview can work on views that are not yet deployed
        view_definition = f"""
define view identifier('{view_name}') as
  select UPPER(fooBar) as upperFooBar from {{{{ table_name }}}};
"""
        file_a_path = project_root / "sources" / "definitions" / "file_a.sql"
        original_content = file_a_path.read_text()
        file_a_path.write_text(original_content + view_definition)

        # Insert sample data into the base table.
        insert_data_sql = f"""
INSERT INTO {base_table_name} (fooBar) VALUES
    ('foo'),
    ('bar'),
    ('baz'),
    ('foobar');
"""
        sql_test_helper.execute_single_sql(insert_data_sql)

        # 1) Preview without limit - should return all rows (or system default).
        result = runner.invoke_with_connection_json(
            [
                "dcm",
                "preview",
                project_name,
                "--object",
                view_name,
                "-D",
                f"table_name='{base_table_name}'",
            ]
        )
        assert result.exit_code == 0, result.output
        assert isinstance(result.json, list)
        assert len(result.json) == 4

        # 2) Preview with limit - should return limited rows.
        result = runner.invoke_with_connection_json(
            [
                "dcm",
                "preview",
                project_name,
                "--object",
                view_name,
                "--limit",
                "2",
                "-D",
                f"table_name='{base_table_name}'",
            ]
        )
        assert result.exit_code == 0, result.output
        assert isinstance(result.json, list)
        assert len(result.json) == 2


@pytest.mark.qa_only
@pytest.mark.integration
def test_dcm_refresh_command(
    runner,
    test_database,
    project_directory,
    object_name_provider,
    sql_test_helper,
):
    project_name = object_name_provider.create_and_get_next_object_name()
    base_table_name = f"{test_database}.PUBLIC.RefreshBaseTable"
    dynamic_table_name = f"{test_database}.PUBLIC.RefreshDynamicTable"

    with project_directory("dcm_project") as project_root:
        result = runner.invoke_with_connection(["dcm", "create", project_name])
        assert result.exit_code == 0, result.output

        # Deploy the project.
        result = runner.invoke_with_connection_json(
            [
                "dcm",
                "deploy",
                project_name,
                "-D",
                f"table_name='{test_database}.PUBLIC.OutputTestTable'",
            ]
        )
        assert result.exit_code == 0, result.output

        # 1) Without any dynamic tables, run refresh command - should report no dynamic tables.
        result = runner.invoke_with_connection(["dcm", "refresh", project_name])
        assert result.exit_code == 0, result.output
        assert_last_stdout_line_equals(
            "No dynamic tables found in the project.", result
        )

        # 2) Define base table and dynamic table with long refresh time.
        table_definitions = f"""
define table identifier('{base_table_name}') (
  id int, name varchar, email varchar
);

define dynamic table identifier('{dynamic_table_name}')
target_lag = '1000 minutes'
WAREHOUSE = xsmall
as select * from {base_table_name};
"""
        file_a_path = project_root / "sources" / "definitions" / "file_a.sql"
        original_content = file_a_path.read_text()
        file_a_path.write_text(original_content + table_definitions)

        # Deploy the project.
        result = runner.invoke_with_connection_json(
            [
                "dcm",
                "deploy",
                project_name,
                "-D",
                f"table_name='{test_database}.PUBLIC.OutputTestTable'",
            ]
        )
        assert result.exit_code == 0, result.output

        # 3) Insert data into the base table.
        insert_data_sql = f"""
INSERT INTO {base_table_name} (id, name, email) VALUES
    (1, 'Alice Johnson', 'alice.j@example.com'),
    (2, 'Bob Williams', 'bob.w@example.com'),
    (3, 'Charlie Brown', 'charlie.b@example.com');
"""
        sql_test_helper.execute_single_sql(insert_data_sql)

        # 4) Run dcm refresh command.
        result = runner.invoke_with_connection(["dcm", "refresh", project_name])
        assert result.exit_code == 0, result.output
        # Should show at least 1 table was refreshed
        assert_last_stdout_line_equals("1 refreshed.", result)

        # 5) Run dcm refresh command again. Response should be different because there's nothing to update
        result = runner.invoke_with_connection(["dcm", "refresh", project_name])
        assert result.exit_code == 0, result.output
        # Should show at least 1 table was refreshed
        assert_last_stdout_line_equals("1 up-to-date.", result)


@pytest.mark.qa_only
@pytest.mark.integration
def test_dcm_test_command(
    runner,
    test_database,
    project_directory,
    object_name_provider,
    sql_test_helper,
):
    project_name = object_name_provider.create_and_get_next_object_name()
    table_name = f"{test_database}.PUBLIC.TestedTable"
    dmf_name = "test_dmf"

    with project_directory("dcm_project") as project_root:
        result = runner.invoke_with_connection(["dcm", "create", project_name])
        assert result.exit_code == 0, result.output

        # 1) Without any data metric functions, run test command to assert that exitcode is 0 and message is returned.
        result = runner.invoke_with_connection(["dcm", "test", project_name])
        assert result.exit_code == 0, result.output
        assert_last_stdout_line_equals("No expectations found in the project.", result)

        # Define table and deploy
        table_definition = f"""
define table identifier('{table_name}') (
  id int, name varchar, email varchar, level int
) data_metric_schedule = '5 minute';
"""
        file_a_path = project_root / "sources" / "definitions" / "file_a.sql"
        original_content = file_a_path.read_text()
        file_a_path.write_text(original_content + table_definition)

        result = runner.invoke_with_connection_json(
            [
                "dcm",
                "deploy",
                project_name,
                "-D",
                f"table_name='{test_database}.PUBLIC.OutputTestTable'",
            ]
        )
        assert result.exit_code == 0, result.output

        # Add some data
        insert_data_sql = f"""
INSERT INTO {table_name} (id, name, email, level) VALUES
    (1, 'Alice Johnson', 'alice.j@example.com', 5),
    (2, 'Bob Williams', 'bob.w@example.com', 3),
    (3, 'Charlie Brown', 'charlie.b@example.com', 3),
    (4, 'Diana Miller', 'diana.m@example.com', 4),
    (5, 'Evan Davis', 'evan.d@example.com', 2);
"""
        sql_test_helper.execute_single_sql(insert_data_sql)

        # 2) Set a DMF that'll fail and run test command - should return exit code 1 with error message
        dmf_sql = f"""
create or alter data metric function {dmf_name}(
   arg_t table(arg_c int)
)
returns int
as $$
select count(*)
from arg_t
where arg_c < 5
$$;

alter table {table_name} add data metric function {dmf_name} on (level)
expectation levels_must_be_higher_than_zero (value = 0);
"""
        sql_test_helper.execute_single_sql(dmf_sql)

        result = runner.invoke_with_connection(["dcm", "test", project_name])
        assert result.exit_code == 1, result.output
        assert "0 passed, 1 failed out of 1 total." in result.output

        # 3) Fix the data and run test command again
        fix_data_sql = f"""
UPDATE {table_name} SET level = 5 WHERE level < 5;
"""
        sql_test_helper.execute_single_sql(fix_data_sql)

        result = runner.invoke_with_connection(["dcm", "test", project_name])
        assert result.exit_code == 0, result.output
        assert_last_stdout_line_equals("1 passed, 0 failed out of 1 total.", result)


@pytest.mark.qa_only
@pytest.mark.integration
@pytest.mark.parametrize(
    "target_args,expected_config",
    [
        pytest.param([], "dev", id="default_target"),
        pytest.param(["--target", "test"], "test", id="explicit_target"),
    ],
)
def test_dcm_end_to_end_workflow(
    runner,
    test_database,
    project_directory,
    target_args,
    expected_config,
):
    target_args = list(target_args)

    with project_directory("dcm_project_multiple_configurations") as project_root:
        result = runner.invoke_with_connection(["dcm", "create"] + target_args)
        assert result.exit_code == 0, result.output
        assert f"successfully created." in result.output

        result = runner.invoke_with_connection_json(["dcm", "describe"] + target_args)
        assert result.exit_code == 0, result.output
        assert (
            result.json[0]["name"]
            == f"project_descriptive_name_{expected_config}".upper()
        )

        # Run raw-analyze
        result = runner.invoke_with_connection(
            ["dcm", "raw-analyze", "-D", f"db='{test_database}'"] + target_args
        )
        assert result.exit_code == 0, result.output
        _extract_and_validate_raw_analyze_json(result.output)
        assert "Analysis completed successfully." in result.output

        result = runner.invoke_with_connection_json(
            ["dcm", "plan", "-D", f"db='{test_database}'"] + target_args
        )
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(
            ["dcm", "deploy", "-D", f"db='{test_database}'"] + target_args
        )
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection(["dcm", "refresh"] + target_args)
        assert result.exit_code == 0, result.output
        assert "No dynamic tables found in the project." in result.output

        result = runner.invoke_with_connection(["dcm", "test"] + target_args)
        assert result.exit_code == 0, result.output
        assert "No expectations found in the project." in result.output

        result = runner.invoke_with_connection(["dcm", "drop"] + target_args)
        assert result.exit_code == 0, result.output


@pytest.mark.qa_only
@pytest.mark.integration
def test_dcm_raw_analyze_with_errors(
    runner,
    test_database,
    project_directory,
    object_name_provider,
):
    project_name = object_name_provider.create_and_get_next_object_name()
    correct_table_fqn = f"{test_database}.PUBLIC.CORRECT_TABLE"
    incorrect_table_fqn = f"{test_database}.PUBLIC.INCORRECT_TABLE"

    with project_directory("dcm_project") as project_root:
        # Create the project
        result = runner.invoke_with_connection(["dcm", "create", project_name])
        assert result.exit_code == 0, result.output

        # Define one correct and one incorrect table
        file_a_path = project_root / "sources" / "definitions" / "file_a.sql"
        file_a_path.write_text(
            f"define table identifier('{correct_table_fqn}') (id int, name varchar);\n"
            f"define table identifier('{incorrect_table_fqn}') (id int, name unknown_type);\n"
        )

        # raw-analyze should detect the error and fail with exit code 1
        result = runner.invoke_with_connection(["dcm", "raw-analyze", project_name])
        assert result.exit_code == 1, result.output
        assert "Analysis found 1 error(s)." in result.output
