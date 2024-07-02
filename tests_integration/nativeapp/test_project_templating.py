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

from snowflake.cli.api.project.util import generate_user_env

from tests.project.fixtures import *
from tests_integration.test_utils import (
    pushd,
    contains_row_with,
    row_from_snowflake_session,
)

USER_NAME = f"user_{uuid.uuid4().hex}"
DEFAULT_TEST_ENV = generate_user_env(USER_NAME)

# Tests a simple flow of native app with template reading env variables from OS
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files", ["integration_templated"], indirect=True
)
def test_nativeapp_project_templating_use_env_from_os(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent

    test_ci_env = "prod"
    local_test_env = dict(DEFAULT_TEST_ENV)
    local_test_env["INTERMEDIATE_CI_ENV"] = test_ci_env
    local_test_env["APP_DIR"] = "app"

    with pushd(project_dir):
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=local_test_env,
        )
        assert result.exit_code == 0

        try:
            # app + package exist
            package_name = f"{project_name}_{test_ci_env}_pkg_{USER_NAME}".upper()
            app_name = f"{project_name}_{test_ci_env}_{USER_NAME}".upper()
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
                ["app", "teardown"],
                env=local_test_env,
            )
            assert result.exit_code == 0

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=local_test_env,
            )
            assert result.exit_code == 0


# Tests a simple flow of native app with template reading env variables from OS through an intermediate var
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files", ["integration_templated"], indirect=True
)
def test_nativeapp_project_templating_use_env_from_os_through_intermediate_var(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent

    test_ci_env = "prod"
    local_test_env = dict(DEFAULT_TEST_ENV)
    local_test_env["CI_ENV"] = test_ci_env
    local_test_env["APP_DIR"] = "app"

    with pushd(project_dir):
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=local_test_env,
        )
        assert result.exit_code == 0

        try:
            # app + package exist
            package_name = f"{project_name}_{test_ci_env}_pkg_{USER_NAME}".upper()
            app_name = f"{project_name}_{test_ci_env}_{USER_NAME}".upper()
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
                ["app", "teardown"],
                env=local_test_env,
            )
            assert result.exit_code == 0

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=local_test_env,
            )
            assert result.exit_code == 0


# Tests a simple flow of native app with template reading default env values from project definition file
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files", ["integration_templated"], indirect=True
)
def test_nativeapp_project_templating_use_default_env_from_project(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent

    default_ci_env = "dev"
    local_test_env = dict(DEFAULT_TEST_ENV)
    local_test_env["APP_DIR"] = "app"

    with pushd(project_dir):
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=local_test_env,
        )
        assert result.exit_code == 0

        try:
            # app + package exist
            package_name = f"{project_name}_{default_ci_env}_pkg_{USER_NAME}".upper()
            app_name = f"{project_name}_{default_ci_env}_{USER_NAME}".upper()
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
                ["app", "teardown"],
                env=local_test_env,
            )
            assert result.exit_code == 0

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=local_test_env,
            )
            assert result.exit_code == 0


# Tests a native app with --env parameter through command line overwriting values from os env and project definition filetemplate reading env var
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files", ["integration_templated"], indirect=True
)
def test_nativeapp_project_templating_use_env_from_cli_as_highest_priority(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent

    local_test_env = dict(DEFAULT_TEST_ENV)
    expected_value = "value_from_cli"
    local_test_env["CI_ENV"] = "value_from_os_env"
    local_test_env["APP_DIR"] = "app"

    with pushd(project_dir):
        result = runner.invoke_with_connection_json(
            ["app", "run", "--env", f"CI_ENV={expected_value}"],
            env=local_test_env,
        )
        assert result.exit_code == 0

        try:
            # app + package exist
            package_name = f"{project_name}_{expected_value}_pkg_{USER_NAME}".upper()
            app_name = f"{project_name}_{expected_value}_{USER_NAME}".upper()
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

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--env", f"CI_ENV={expected_value}", "--force"],
                env=local_test_env,
            )
            assert result.exit_code == 0


# Tests that other native app commands still succeed with templating
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files", ["integration_templated"], indirect=True
)
def test_nativeapp_project_templating_bundle_deploy_successful(
    runner,
    project_definition_files: List[Path],
):
    project_dir = project_definition_files[0].parent

    test_ci_env = "prod"
    local_test_env = dict(DEFAULT_TEST_ENV)
    local_test_env["CI_ENV"] = test_ci_env
    local_test_env["APP_DIR"] = "app"

    with pushd(project_dir):
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
