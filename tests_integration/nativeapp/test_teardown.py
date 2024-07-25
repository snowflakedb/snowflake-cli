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
    contains_row_with,
    not_contains_row_with,
    row_from_snowflake_session,
    enable_definition_v2_feature_flag,
)

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize(
    "command,expected_error",
    [
        # "snow app teardown --cascade" should drop both application and application objects
        ["app teardown --cascade", None],
        # "snow app teardown --force --no-cascade" should attempt to drop the application and fail
        [
            "app teardown --force --no-cascade",
            "Could not successfully execute the Snowflake SQL statements",
        ],
        # "snow app teardown" with owned application objects should abort the teardown
        ["app teardown", "Aborted"],
    ],
)
@pytest.mark.parametrize("orphan_app", [True, False])
@pytest.mark.parametrize("definition_version", ["v1", "v2"])
def test_nativeapp_teardown_cascade(
    command,
    expected_error,
    orphan_app,
    definition_version,
    project_directory,
    runner,
    snowflake_session,
):
    project_name = "myapp"
    app_name = f"{project_name}_{USER_NAME}".upper()
    db_name = f"{project_name}_db_{USER_NAME}".upper()

    # TODO Use the main project_directory block once "snow app run" supports definition v2
    with project_directory(f"napp_create_db_v1"):
        # Replacing the static DB name with a unique one to avoid collisions between tests
        with open("app/setup_script.sql", "r") as file:
            setup_script_content = file.read()
        setup_script_content = setup_script_content.replace(
            "DB_NAME_PLACEHOLDER", db_name
        )
        with open("app/setup_script.sql", "w") as file:
            file.write(setup_script_content)

        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0

    with project_directory(f"napp_create_db_{definition_version}"):
        try:
            # Grant permission to create databases
            snowflake_session.execute_string(
                f"grant create database on account to application {app_name}",
            )

            # Create the database
            snowflake_session.execute_string("use warehouse xsmall")
            snowflake_session.execute_string(
                f"call {app_name}.core.create_db()",
            )

            # Verify the database is owned by the app
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(f"show databases like '{db_name}'")
                ),
                dict(name=db_name, owner=app_name),
            )

            if orphan_app:
                # orphan the app by dropping the application package,
                # this causes future `show objects owned by application` queries to fail
                # and `snow app teardown` needs to be resilient against this
                package_name = f"{project_name}_pkg_{USER_NAME}".upper()
                snowflake_session.execute_string(
                    f"drop application package {package_name}"
                )
                assert not_contains_row_with(
                    row_from_snowflake_session(
                        snowflake_session.execute_string(
                            f"show application packages like '{package_name}'",
                        )
                    ),
                    dict(name=package_name),
                )

            # Run the teardown command
            result = runner.invoke_with_connection_json(
                command.split(),
                env=TEST_ENV,
            )
            if expected_error is not None:
                assert result.exit_code == 1
                assert expected_error in result.output
                return

            assert result.exit_code == 0

            # Verify the database is dropped
            assert not_contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(f"show databases like '{db_name}'")
                ),
                dict(name=db_name, owner=app_name),
            )

            # Verify the app is dropped
            assert not_contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show applications like '{app_name}'",
                    )
                ),
                dict(name=app_name),
            )

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force", "--cascade"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize("force", [True, False])
@pytest.mark.parametrize("definition_version", ["v1", "v2"])
def test_nativeapp_teardown_unowned_app(
    runner,
    force,
    definition_version,
    project_directory,
):
    project_name = "myapp"
    app_name = f"{project_name}_{USER_NAME}"

    # TODO Use the main project_directory block once "snow app run" supports definition v2
    with project_directory("napp_init_v1"):
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0

    with project_directory(f"napp_init_{definition_version}"):
        try:
            result = runner.invoke_with_connection_json(
                ["sql", "-q", f"alter application {app_name} set comment = 'foo'"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0

            if force:
                result = runner.invoke_with_connection_json(
                    ["app", "teardown", "--force"],
                    env=TEST_ENV,
                )
                assert result.exit_code == 0
            else:
                result = runner.invoke_with_connection_json(
                    ["app", "teardown"],
                    env=TEST_ENV,
                )
                assert result.exit_code == 1

        finally:
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize("default_release_directive", [True, False])
@pytest.mark.parametrize("definition_version", ["v1", "v2"])
def test_nativeapp_teardown_pkg_versions(
    runner,
    default_release_directive,
    definition_version,
    project_directory,
):
    project_name = "myapp"
    pkg_name = f"{project_name}_pkg_{USER_NAME}"

    with project_directory(f"napp_init_{definition_version}"):
        # TODO Use the main project_directory block once "snow app version" supports definition v2
        with project_directory("napp_init_v1"):
            result = runner.invoke_with_connection(
                ["app", "version", "create", "v1"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0

        try:
            # when setting a release directive, we will not have the ability to drop the version later
            if default_release_directive:
                result = runner.invoke_with_connection(
                    [
                        "sql",
                        "-q",
                        f"alter application package {pkg_name} set default release directive version = v1 patch = 0",
                    ],
                    env=TEST_ENV,
                )
                assert result.exit_code == 0

            # try to teardown; fail because we have a version
            result = runner.invoke_with_connection(
                ["app", "teardown"],
                env=TEST_ENV,
            )
            assert result.exit_code == 1
            assert f"Drop versions first, or use --force to override." in result.output

            teardown_args = []
            if not default_release_directive:
                # TODO Use the main project_directory block once "snow app version" supports definition v2
                with project_directory("napp_init_v1"):
                    # if we didn't set a release directive, we can drop the version and try again
                    result = runner.invoke_with_connection(
                        ["app", "version", "drop", "v1", "--force"],
                        env=TEST_ENV,
                    )
                    assert result.exit_code == 0
            else:
                # if we did set a release directive, we need --force for teardown to work
                teardown_args = ["--force"]

            # either way, we can now tear down the application package
            result = runner.invoke_with_connection(
                ["app", "teardown"] + teardown_args,
                env=TEST_ENV,
            )
            assert result.exit_code == 0

        finally:
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
