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
from pathlib import Path
from typing import List

import pytest

from tests_common import change_directory
from tests_integration.test_utils import (
    contains_row_with,
    row_from_snowflake_session,
)
from tests_integration.testing_utils.working_directory_utils import (
    WorkingDirectoryChanger,
)


# Tests a simple flow of native app with template reading env variables from OS
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files",
    ["integration_templated", "integration_templated_v2"],
    indirect=True,
)
def test_nativeapp_project_templating_use_env_from_os(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_teardown,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent

    test_ci_env = "prod"
    local_test_env = {"INTERMEDIATE_CI_ENV": test_ci_env, "APP_DIR": "app"}

    with change_directory(project_dir):
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=local_test_env,
        )
        assert result.exit_code == 0

        with nativeapp_teardown(env=local_test_env):
            # app + package exist
            package_name = f"{project_name}_{test_ci_env}_pkg_{default_username}{resource_suffix}".upper()
            app_name = f"{project_name}_{test_ci_env}_{default_username}{resource_suffix}".upper()
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show application packages like '{package_name}'",
                    )
                ),
                dict(name=package_name),
            )
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show applications like '{app_name}'"
                    )
                ),
                dict(name=app_name),
            )

            # sanity checks
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"select count(*) from {app_name}.core.shared_view"
                    )
                ),
                {"COUNT(*)": 1},
            )
            test_string = "TEST STRING"
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"call {app_name}.core.echo('{test_string}')"
                    )
                ),
                {"ECHO": test_string},
            )


# Tests a simple flow of native app with template reading env variables from OS through an intermediate var
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files",
    ["integration_templated_v2"],
    indirect=True,
)
def test_nativeapp_project_templating_use_env_from_os_through_intermediate_var(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_teardown,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent

    test_ci_env = "prod"
    local_test_env = {"CI_ENV": test_ci_env, "APP_DIR": "app"}

    with change_directory(project_dir):
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=local_test_env,
        )
        assert result.exit_code == 0

        with nativeapp_teardown(env=local_test_env):
            # app + package exist
            package_name = f"{project_name}_{test_ci_env}_pkg_{default_username}{resource_suffix}".upper()
            app_name = f"{project_name}_{test_ci_env}_{default_username}{resource_suffix}".upper()
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show application packages like '{package_name}'",
                    )
                ),
                dict(name=package_name),
            )
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show applications like '{app_name}'"
                    )
                ),
                dict(name=app_name),
            )

            # sanity checks
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"select count(*) from {app_name}.core.shared_view"
                    )
                ),
                {"COUNT(*)": 1},
            )
            test_string = "TEST STRING"
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"call {app_name}.core.echo('{test_string}')"
                    )
                ),
                {"ECHO": test_string},
            )


# Tests a simple flow of native app with template reading default env values from project definition file
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files",
    ["integration_templated_v2"],
    indirect=True,
)
def test_nativeapp_project_templating_use_default_env_from_project(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_teardown,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent

    default_ci_env = "dev"
    local_test_env = {"APP_DIR": "app"}

    with change_directory(project_dir):
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=local_test_env,
        )
        assert result.exit_code == 0

        with nativeapp_teardown(env=local_test_env):
            # app + package exist
            package_name = f"{project_name}_{default_ci_env}_pkg_{default_username}{resource_suffix}".upper()
            app_name = f"{project_name}_{default_ci_env}_{default_username}{resource_suffix}".upper()
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show application packages like '{package_name}'",
                    )
                ),
                dict(name=package_name),
            )
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show applications like '{app_name}'"
                    )
                ),
                dict(name=app_name),
            )

            # sanity checks
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"select count(*) from {app_name}.core.shared_view"
                    )
                ),
                {"COUNT(*)": 1},
            )
            test_string = "TEST STRING"
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"call {app_name}.core.echo('{test_string}')"
                    )
                ),
                {"ECHO": test_string},
            )


# Tests a native app with --env parameter through command line overwriting values from os env and project definition filetemplate reading env var
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files",
    ["integration_templated_v2"],
    indirect=True,
)
def test_nativeapp_project_templating_use_env_from_cli_as_highest_priority(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_teardown,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent

    local_test_env = {}
    expected_value = "value_from_cli"
    local_test_env["CI_ENV"] = "value_from_os_env"
    local_test_env["APP_DIR"] = "app"

    with change_directory(project_dir):
        result = runner.invoke_with_connection_json(
            ["app", "run", "--env", f"CI_ENV={expected_value}"],
            env=local_test_env,
        )
        assert result.exit_code == 0

        with nativeapp_teardown(env=local_test_env):
            # app + package exist
            package_name = f"{project_name}_{expected_value}_pkg_{default_username}{resource_suffix}".upper()
            app_name = f"{project_name}_{expected_value}_{default_username}{resource_suffix}".upper()
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show application packages like '{package_name}'",
                    )
                ),
                dict(name=package_name),
            )
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show applications like '{app_name}'"
                    )
                ),
                dict(name=app_name),
            )

            # sanity checks
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"select count(*) from {app_name}.core.shared_view"
                    )
                ),
                {"COUNT(*)": 1},
            )
            test_string = "TEST STRING"
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"call {app_name}.core.echo('{test_string}')"
                    )
                ),
                {"ECHO": test_string},
            )

            # make sure we always delete the app
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--env", f"CI_ENV={expected_value}"],
                env=local_test_env,
            )
            assert result.exit_code == 0


# Tests that other native app commands still succeed with templating
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files",
    ["integration_templated_v2"],
    indirect=True,
)
def test_nativeapp_project_templating_bundle_deploy_successful(
    runner,
    nativeapp_teardown,
    project_definition_files: List[Path],
):
    project_dir = project_definition_files[0].parent

    test_ci_env = "prod"
    local_test_env = {"CI_ENV": test_ci_env, "APP_DIR": "app"}

    with change_directory(project_dir):
        with nativeapp_teardown(env=local_test_env):
            result = runner.invoke_json(
                ["app", "bundle"],
                env=local_test_env,
            )
            assert result.exit_code == 0

            result = runner.invoke_with_connection_json(
                ["app", "deploy"],
                env=local_test_env,
            )
            assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.parametrize("test_project", ["napp_templates_processors_v2"])
@pytest.mark.parametrize("with_project_flag", [True, False])
def test_nativeapp_templates_processor_with_run(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_project_directory,
    test_project,
    with_project_flag,
):
    project_name = "myapp"
    app_name = f"{project_name}_{default_username}{resource_suffix}"

    with nativeapp_project_directory(test_project) as tmp_dir:
        project_args = ["--project", f"{tmp_dir}"] if with_project_flag else []

        if with_project_flag:
            working_directory_changer = WorkingDirectoryChanger()
            working_directory_changer.change_working_directory_to("app")
        try:
            result = runner.invoke_with_connection_json(
                ["app", "run"] + project_args,
                env={
                    "schema_name": "test_schema",
                    "table_name": "test_table",
                    "value": "test_value",
                },
            )
            assert result.exit_code == 0

            result = row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"select * from {app_name}.test_schema.test_table",
                )
            )
            assert result == [{"NAME": "test_value"}]

        finally:
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"] + project_args
            )
            assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_project", ["napp_templates_processors_v1", "napp_templates_processors_v2"]
)
@pytest.mark.parametrize("with_project_flag", [True, False])
def test_nativeapp_templates_processor_with_deploy(
    runner,
    nativeapp_project_directory,
    test_project,
    with_project_flag,
):

    with nativeapp_project_directory(test_project) as tmp_dir:
        project_args = ["--project", f"{tmp_dir}"] if with_project_flag else []

        if with_project_flag:
            working_directory_changer = WorkingDirectoryChanger()
            working_directory_changer.change_working_directory_to("app")

        result = runner.invoke_with_connection_json(
            ["app", "deploy"] + project_args,
            env={
                "schema_name": "test_schema",
                "table_name": "test_table",
                "value": "test_value",
            },
        )
        assert result.exit_code == 0

        with open(
            tmp_dir / "output" / "deploy" / "another_script.sql", "r", encoding="utf-8"
        ) as f:
            assert "test_value" in f.read()
