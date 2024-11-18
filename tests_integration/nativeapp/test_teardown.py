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
from shlex import split

import yaml

from tests.project.fixtures import *
from tests_integration.test_utils import (
    contains_row_with,
    not_contains_row_with,
    row_from_snowflake_session,
)


@pytest.mark.integration
@pytest.mark.parametrize("orphan_app", [True, False])
@pytest.mark.parametrize(
    "test_project,command,expected_error",
    [
        # "--cascade" should drop both application and application objects
        [
            "napp_create_db_v1",
            "app teardown --cascade",
            None,
        ],
        [
            "napp_create_db_v2",
            "app teardown --cascade",
            None,
        ],
        [
            "napp_create_db_v2",
            "ws drop --entity-id=app --cascade",
            None,
        ],
        # "--force --no-cascade" should attempt to drop the application and fail
        [
            "napp_create_db_v1",
            "app teardown --force --no-cascade",
            "Could not successfully execute the Snowflake SQL statements",
        ],
        [
            "napp_create_db_v2",
            "app teardown --force --no-cascade",
            "Could not successfully execute the Snowflake SQL statements",
        ],
        [
            "napp_create_db_v2",
            "ws drop --entity-id=app --force --no-cascade",
            "Could not successfully execute the Snowflake SQL statements",
        ],
        # teardown/drop with owned application objects should abort the teardown
        [
            "napp_create_db_v1",
            "app teardown",
            "Aborted",
        ],
        [
            "napp_create_db_v2",
            "app teardown",
            "Aborted",
        ],
        [
            "napp_create_db_v2",
            "ws drop --entity-id=app",
            "Aborted",
        ],
    ],
)
def test_nativeapp_teardown_cascade(
    orphan_app,
    test_project,
    command,
    expected_error,
    nativeapp_project_directory,
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
):
    project_name = "myapp"
    app_name = f"{project_name}_{default_username}{resource_suffix}".upper()
    db_name = f"{project_name}_db_{default_username}{resource_suffix}".upper()

    with nativeapp_project_directory(test_project):
        # Replacing the static DB name with a unique one to avoid collisions between tests
        with open("app/setup_script.sql", "r") as file:
            setup_script_content = file.read()
        setup_script_content = setup_script_content.replace(
            "DB_NAME_PLACEHOLDER", db_name
        )
        with open("app/setup_script.sql", "w") as file:
            file.write(setup_script_content)

        result = runner.invoke_with_connection_json(["app", "run"])
        assert result.exit_code == 0
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
            package_name = (
                f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
            )
            snowflake_session.execute_string(f"drop application package {package_name}")
            assert not_contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show application packages like '{package_name}'",
                    )
                ),
                dict(name=package_name),
            )

        # Run the teardown command
        result = runner.invoke_with_connection_json(command.split())
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


@pytest.mark.integration
@pytest.mark.parametrize("force", [True, False])
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app teardown", "napp_init_v2"],
    ],
)
def test_nativeapp_teardown_unowned_app(
    runner,
    default_username,
    resource_suffix,
    force,
    command,
    test_project,
    nativeapp_project_directory,
):
    project_name = "myapp"
    app_name = f"{project_name}_{default_username}{resource_suffix}"

    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection_json(["app", "run"])
        assert result.exit_code == 0
        result = runner.invoke_with_connection_json(
            ["sql", "-q", f"alter application {app_name} set comment = 'foo'"]
        )
        assert result.exit_code == 0

        if force:
            result = runner.invoke_with_connection_json([*split(command), "--force"])
            assert result.exit_code == 0
        else:
            result = runner.invoke_with_connection_json(split(command))
            assert result.exit_code == 1


@pytest.mark.integration
@pytest.mark.parametrize("default_release_directive", [True, False])
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app teardown", "napp_init_v2"],
    ],
)
def test_nativeapp_teardown_pkg_versions(
    runner,
    default_username,
    resource_suffix,
    default_release_directive,
    command,
    test_project,
    nativeapp_project_directory,
):
    project_name = "myapp"
    pkg_name = f"{project_name}_pkg_{default_username}{resource_suffix}"

    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection(["app", "version", "create", "v1"])
        assert result.exit_code == 0

        # when setting a release directive, we will not have the ability to drop the version later
        if default_release_directive:
            result = runner.invoke_with_connection(
                [
                    "sql",
                    "-q",
                    f"alter application package {pkg_name} set default release directive version = v1 patch = 0",
                ]
            )
            assert result.exit_code == 0

        # try to teardown; fail because we have a version
        result = runner.invoke_with_connection(split(command))
        assert result.exit_code == 1
        assert f"Drop versions first, or use --force to override." in result.output

        teardown_args = []
        if not default_release_directive:
            # if we didn't set a release directive, we can drop the version and try again
            result = runner.invoke_with_connection(
                ["app", "version", "drop", "v1", "--force"]
            )
            assert result.exit_code == 0
        else:
            # if we did set a release directive, we need --force for teardown to work
            teardown_args = ["--force"]

        # either way, we can now tear down the application package
        result = runner.invoke_with_connection(split(command) + teardown_args)
        assert result.exit_code == 0


@pytest.mark.integration
def test_nativeapp_teardown_multiple_apps_using_snow_app(
    runner,
    nativeapp_project_directory,
    snowflake_session,
    default_username,
    resource_suffix,
):
    test_project = "napp_init_v2"
    project_name = "myapp"
    pkg_name = f"{project_name}_pkg_{default_username}{resource_suffix}"
    app_name_1 = f"{project_name}_{default_username}{resource_suffix}".upper()
    app_name_2 = f"{project_name}2_{default_username}{resource_suffix}".upper()

    with nativeapp_project_directory(test_project) as project_dir:
        # Add a second app to the project
        snowflake_yml = project_dir / "snowflake.yml"
        with open(snowflake_yml, "r") as file:
            project_yml = yaml.safe_load(file)
        project_yml["entities"]["app2"] = project_yml["entities"]["app"] | dict(
            identifier="myapp2_<% ctx.env.USER %>"
        )
        with open(snowflake_yml, "w") as file:
            yaml.dump(project_yml, file)

        # Create the package and both apps
        result = runner.invoke_with_connection_json(
            ["app", "run", "--app-entity-id", "app"]
        )
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(
            ["app", "run", "--app-entity-id", "app2"]
        )
        assert result.exit_code == 0, result.output

        # Run the teardown command
        result = runner.invoke_with_connection_json(["app", "teardown"])
        assert result.exit_code == 0, result.output

        # Verify the package is dropped
        assert not_contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"show application packages like '{pkg_name}'",
                )
            ),
            dict(name=pkg_name),
        )

        # Verify the apps are dropped
        for app_name in [app_name_1, app_name_2]:
            assert not_contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show applications like '{app_name}'",
                    )
                ),
                dict(name=app_name),
            )


@pytest.mark.integration
def test_nativeapp_teardown_multiple_packages_using_snow_app_must_choose(
    runner,
    nativeapp_project_directory,
    snowflake_session,
    default_username,
    resource_suffix,
):
    test_project = "napp_init_v2"
    project_name = "myapp"
    pkgs = {
        "pkg": f"{project_name}_pkg_{default_username}{resource_suffix}",
        "pkg2": f"{project_name}_pkg2_{default_username}{resource_suffix}",
    }

    with nativeapp_project_directory(test_project) as project_dir:
        # Add a second package to the project
        snowflake_yml = project_dir / "snowflake.yml"
        with open(snowflake_yml, "r") as file:
            project_yml = yaml.safe_load(file)
        project_yml["entities"]["pkg2"] = project_yml["entities"]["pkg"] | dict(
            identifier="myapp_pkg2_<% ctx.env.USER %>"
        )
        with open(snowflake_yml, "w") as file:
            yaml.dump(project_yml, file)

        # Create both packages
        for entity_id in pkgs:
            result = runner.invoke_with_connection_json(
                ["app", "deploy", "--package-entity-id", entity_id]
            )
            assert result.exit_code == 0, result.output

        # Run the teardown command without specifying a package, it should fail
        result = runner.invoke_with_connection_json(["app", "teardown"])
        assert result.exit_code == 1, result.output
        assert (
            "More than one application package entity exists in the project definition"
            in result.output
        )

        # Run the teardown command on each package
        for entity_id, pkg_name in pkgs.items():
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--package-entity-id", entity_id]
            )
            assert result.exit_code == 0, result.output

            # Verify the package is dropped
            assert not_contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show application packages like '{pkg_name}'",
                    )
                ),
                dict(name=pkg_name),
            )
