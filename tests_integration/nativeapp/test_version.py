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

from tests.project.fixtures import *
from tests_integration.test_utils import (
    pushd,
    contains_row_with,
    not_contains_row_with,
    row_from_snowflake_session,
)


# Tests a simple flow of an existing project, executing snow app version create, drop and teardown, all with distribution=internal
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files", ["integration", "integration_v2"], indirect=True
)
def test_nativeapp_version_create_and_drop(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_teardown,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent
    with pushd(project_dir):
        result_create = runner.invoke_with_connection_json(
            ["app", "version", "create", "v1", "--force", "--skip-git-check"]
        )
        assert result_create.exit_code == 0

        with nativeapp_teardown():
            # package exist
            package_name = (
                f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
            )
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show application packages like '{package_name}'",
                    )
                ),
                dict(name=package_name),
            )

            # app package contains version v1
            expect = snowflake_session.execute_string(
                f"show versions in application package {package_name}"
            )
            actual = runner.invoke_with_connection_json(["app", "version", "list"])
            assert actual.json == row_from_snowflake_session(expect)

            result_drop = runner.invoke_with_connection_json(
                ["app", "version", "drop", "v1", "--force"]
            )
            assert result_drop.exit_code == 0
            actual = runner.invoke_with_connection_json(["app", "version", "list"])
            assert len(actual.json) == 0


# Tests upgrading an app from an existing loose files installation to versioned installation.
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files", ["integration", "integration_v2"], indirect=True
)
def test_nativeapp_upgrade(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_teardown,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent
    with pushd(project_dir):
        runner.invoke_with_connection_json(["app", "run"])
        runner.invoke_with_connection_json(
            ["app", "version", "create", "v1", "--force", "--skip-git-check"]
        )

        with nativeapp_teardown():
            # package exist
            package_name = (
                f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
            )
            app_name = f"{project_name}_{default_username}{resource_suffix}".upper()
            # app package contains version v1
            expect = snowflake_session.execute_string(
                f"show versions in application package {package_name}"
            )
            actual = runner.invoke_with_connection_json(["app", "version", "list"])
            assert actual.json == row_from_snowflake_session(expect)

            runner.invoke_with_connection_json(
                ["app", "run", "--version", "v1", "--force"]
            )

            expect = row_from_snowflake_session(
                snowflake_session.execute_string(f"desc application {app_name}")
            )
            assert contains_row_with(expect, {"property": "name", "value": app_name})
            assert contains_row_with(expect, {"property": "version", "value": "V1"})
            assert contains_row_with(expect, {"property": "patch", "value": "0"})

            runner.invoke_with_connection_json(
                ["app", "version", "drop", "v1", "--force"]
            )


# Make sure we can create 3+ patches on the same version
@pytest.mark.integration
@pytest.mark.parametrize("project_definition_files", ["integration"], indirect=True)
def test_nativeapp_version_create_3_patches(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_teardown,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent
    with pushd(project_dir):
        with nativeapp_teardown():
            package_name = (
                f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
            )

            # create three patches (deploys too)
            for _ in range(3):
                result = runner.invoke_with_connection_json(
                    ["app", "version", "create", "v1", "--force", "--skip-git-check"]
                )
                assert result.exit_code == 0

            # app package contains 3 patches for version v1
            expect = row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"show versions in application package {package_name}"
                )
            )
            assert contains_row_with(expect, {"version": "V1", "patch": 0})
            assert contains_row_with(expect, {"version": "V1", "patch": 1})
            assert contains_row_with(expect, {"version": "V1", "patch": 2})

            # drop the version
            result_drop = runner.invoke_with_connection_json(
                ["app", "version", "drop", "v1", "--force"]
            )
            assert result_drop.exit_code == 0

            # ensure there are no versions now
            actual = runner.invoke_with_connection_json(["app", "version", "list"])
            assert len(actual.json) == 0


@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files", ["integration", "integration_v2"], indirect=True
)
def test_nativeapp_version_create_patch_is_integer(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_teardown,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent
    with pushd(project_dir):
        with nativeapp_teardown():
            package_name = (
                f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
            )

            # create initial version
            result = runner.invoke_with_connection_json(
                ["app", "version", "create", "v1", "--force", "--skip-git-check"]
            )
            assert result.exit_code == 0

            # create non-integer patch
            result = runner.invoke_with_connection_json(
                [
                    "app",
                    "version",
                    "create",
                    "v1",
                    "--force",
                    "--skip-git-check",
                    "--patch",
                    "foo",
                ],
            )
            assert result.exit_code == 2
            assert (
                "Invalid value for '--patch': 'foo' is not a valid integer."
                in result.output
            )

            # create integer patch
            result = runner.invoke_with_connection_json(
                [
                    "app",
                    "version",
                    "create",
                    "v1",
                    "--force",
                    "--skip-git-check",
                    "--patch",
                    "1",
                ],
            )
            assert result.exit_code == 0

            # drop the version
            result_drop = runner.invoke_with_connection_json(
                ["app", "version", "drop", "v1", "--force"]
            )
            assert result_drop.exit_code == 0

            # ensure there are no versions now
            actual = runner.invoke_with_connection_json(["app", "version", "list"])
            assert len(actual.json) == 0


# Tests creating a version for a package that was not created by the CLI
# (doesn't have the magic CLI comment)
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files", ["integration", "integration_v2"], indirect=True
)
def test_nativeapp_version_create_package_no_magic_comment(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_teardown,
    snapshot,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent
    with pushd(project_dir):
        result_create_abort = runner.invoke_with_connection_json(["app", "deploy"])
        assert result_create_abort.exit_code == 0

        with nativeapp_teardown():
            # package exist
            package_name = (
                f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
            )
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
                        f"alter application package {package_name} set comment = 'not the magic comment'"
                    )
                ),
                dict(status="Statement executed successfully."),
            )

            # say no
            result_create_abort = runner.invoke_with_connection(
                ["app", "version", "create", "v1", "--skip-git-check", "--interactive"],
                input="n\n",
            )
            assert result_create_abort.exit_code == 1
            assert (
                f"An Application Package {package_name} already exists in account "
                "that may have been created without Snowflake CLI.".upper()
                in result_create_abort.output.upper()
            )
            assert "Aborted." in result_create_abort.output

            # say yes
            result_create_yes = runner.invoke_with_connection(
                ["app", "version", "create", "v1", "--skip-git-check", "--interactive"],
                input="y\n",
            )
            assert result_create_yes.exit_code == 0
            assert (
                f"An Application Package {package_name} already exists in account "
                "that may have been created without Snowflake CLI.".upper()
                in result_create_yes.output.upper()
            )

            # force
            result_create_force = runner.invoke_with_connection(
                ["app", "version", "create", "v1", "--force", "--skip-git-check"]
            )
            assert result_create_force.exit_code == 0
            assert (
                f"An Application Package {package_name} already exists in account "
                "that may have been created without Snowflake CLI.".upper()
                in result_create_force.output.upper()
            )

            # app package contains version v1 with 2 patches
            actual = runner.invoke_with_connection_json(["app", "version", "list"])
            for row in actual.json:
                # Remove date field
                row.pop("created_on", None)
            assert actual.json == snapshot
