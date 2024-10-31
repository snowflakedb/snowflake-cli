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
from typing import Any, Union

from yaml import safe_dump, safe_load

from snowflake.cli.api.project.util import (
    is_valid_unquoted_identifier,
    to_identifier,
    unquote_identifier,
)
from tests.project.fixtures import *
from tests_integration.test_utils import contains_row_with, row_from_snowflake_session


def set_version_in_app_manifest(manifest_path: Path, version: Any, patch: Any = None):
    with open(manifest_path, "r") as f:
        manifest = safe_load(f)

    version_info = manifest.setdefault("version", {})
    version_info["name"] = version
    if patch is not None:
        version_info["patch"] = patch
    else:
        version_info.pop("patch", None)

    with open(manifest_path, "w") as f:
        f.write(safe_dump(manifest))


def normalize_identifier(identifier: Union[str, int]) -> str:
    id_str = str(identifier)
    if is_valid_unquoted_identifier(str(id_str)):
        return id_str.upper()
    else:
        return unquote_identifier(to_identifier(id_str))


# Tests a simple flow of an existing project, executing snow app version create, drop and teardown, all with distribution=internal
@pytest.mark.integration
@pytest.mark.parametrize(
    "create_command,list_command,drop_command,test_project",
    [
        ["app version create", "app version list", "app version drop", "napp_init_v1"],
        ["app version create", "app version list", "app version drop", "napp_init_v2"],
        [
            "ws version create --entity-id=pkg",
            "ws version list --entity-id=pkg",
            "ws version drop --entity-id=pkg",
            "napp_init_v2",
        ],
    ],
)
def test_nativeapp_version_create_and_drop(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_project_directory,
    create_command,
    list_command,
    drop_command,
    test_project,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        result_create = runner.invoke_with_connection_json(
            [*split(create_command), "v1", "--force", "--skip-git-check"]
        )
        assert result_create.exit_code == 0

        # package exist
        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
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
        actual = runner.invoke_with_connection_json(split(list_command))
        assert actual.json == row_from_snowflake_session(expect)

        result_drop = runner.invoke_with_connection_json(
            [*split(drop_command), "v1", "--force"]
        )
        assert result_drop.exit_code == 0
        actual = runner.invoke_with_connection_json(split(list_command))
        assert len(actual.json) == 0


# Tests upgrading an app from an existing loose files installation to versioned installation.
@pytest.mark.integration
@pytest.mark.parametrize(
    "create_command,list_command,drop_command,test_project",
    [["app version create", "app version list", "app version drop", "napp_init_v2"]],
)
def test_nativeapp_upgrade(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_project_directory,
    create_command,
    list_command,
    drop_command,
    test_project,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        runner.invoke_with_connection_json(["app", "run"])
        runner.invoke_with_connection_json(
            ["app", "version", "create", "v1", "--force", "--skip-git-check"]
        )

        # package exist
        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
        app_name = f"{project_name}_{default_username}{resource_suffix}".upper()
        # app package contains version v1
        expect = snowflake_session.execute_string(
            f"show versions in application package {package_name}"
        )
        actual = runner.invoke_with_connection_json(split(list_command))
        assert actual.json == row_from_snowflake_session(expect)

        runner.invoke_with_connection_json(["app", "run", "--version", "v1", "--force"])

        expect = row_from_snowflake_session(
            snowflake_session.execute_string(f"desc application {app_name}")
        )
        assert contains_row_with(expect, {"property": "name", "value": app_name})
        assert contains_row_with(expect, {"property": "version", "value": "V1"})
        assert contains_row_with(expect, {"property": "patch", "value": "0"})

        runner.invoke_with_connection_json([*split(drop_command), "v1", "--force"])


# Make sure we can create 3+ patches on the same version
@pytest.mark.integration
@pytest.mark.parametrize(
    "create_command,list_command,drop_command,test_project",
    [["app version create", "app version list", "app version drop", "napp_init_v2"]],
)
def test_nativeapp_version_create_3_patches(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_teardown,
    nativeapp_project_directory,
    create_command,
    list_command,
    drop_command,
    test_project,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()

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
            [*split(drop_command), "v1", "--force"]
        )
        assert result_drop.exit_code == 0

        # ensure there are no versions now
        actual = runner.invoke_with_connection_json(split(list_command))
        assert len(actual.json) == 0


@pytest.mark.integration
@pytest.mark.parametrize(
    "create_command,list_command,drop_command,test_project",
    [["app version create", "app version list", "app version drop", "napp_init_v2"]],
)
def test_nativeapp_version_create_patch_is_integer(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_teardown,
    nativeapp_project_directory,
    create_command,
    list_command,
    drop_command,
    test_project,
):
    with nativeapp_project_directory(test_project):
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
            [*split(drop_command), "v1", "--force"]
        )
        assert result_drop.exit_code == 0

        # ensure there are no versions now
        actual = runner.invoke_with_connection_json(split(list_command))
        assert len(actual.json) == 0


# Tests creating a version for a package that was not created by the CLI
# (doesn't have the magic CLI comment)
@pytest.mark.integration
@pytest.mark.parametrize(
    "create_command,list_command,drop_command,test_project",
    [["app version create", "app version list", "app version drop", "napp_init_v2"]],
)
def test_nativeapp_version_create_package_no_magic_comment(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_teardown,
    snapshot,
    nativeapp_project_directory,
    create_command,
    list_command,
    drop_command,
    test_project,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        result_create_abort = runner.invoke_with_connection_json(["app", "deploy"])
        assert result_create_abort.exit_code == 0

        # package exist
        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
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
        actual = runner.invoke_with_connection_json(split(list_command))
        for row in actual.json:
            # Remove date field
            row.pop("created_on", None)
        assert actual.json == snapshot


# Tests a simple flow of an existing project, executing snow app version create, drop and teardown, all with distribution=internal
@pytest.mark.integration
@pytest.mark.parametrize(
    "create_command,list_command,drop_command,test_project",
    [["app version create", "app version list", "app version drop", "napp_init_v2"]],
)
def test_nativeapp_version_create_and_drop_from_manifest(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_project_directory,
    create_command,
    list_command,
    drop_command,
    test_project,
):
    with nativeapp_project_directory(test_project) as project_dir:
        # not using pytest parameterization here because we need
        # to guarantee that the initial version gets created before the patches

        VERSIONS = [7, "v1", "1.0", "version 1"]
        PATCHES = [None, 10, "12"]

        for version_name in VERSIONS:
            manifest_path = project_dir / "app/manifest.yml"
            set_version_in_app_manifest(manifest_path, version_name)

            result_create = runner.invoke_with_connection_json(
                ["app", "version", "create", "--force", "--skip-git-check"]
            )
            assert result_create.exit_code == 0

            # app package contains correct version
            actual = runner.invoke_with_connection_json(["app", "version", "list"])
            assert contains_row_with(
                actual.json, {"version": normalize_identifier(version_name), "patch": 0}
            )

            result_drop = runner.invoke_with_connection_json(
                [*split(drop_command), "--force"]
            )
            assert result_drop.exit_code == 0
            actual = runner.invoke_with_connection_json(["app", "version", "list"])
            assert len(actual.json) == 0

        version_name = "V2"
        for patch_name in PATCHES:
            manifest_path = project_dir / "app/manifest.yml"
            set_version_in_app_manifest(manifest_path, version_name, patch_name)

            result_create = runner.invoke_with_connection_json(
                ["app", "version", "create", "--force", "--skip-git-check"]
            )
            assert result_create.exit_code == 0

            # app package contains version V2
            actual = runner.invoke_with_connection_json(["app", "version", "list"])
            assert contains_row_with(
                actual.json,
                {
                    "version": version_name,
                    "patch": int(patch_name) if patch_name else 0,
                },
            )

        result_drop = runner.invoke_with_connection_json(
            [*split(drop_command), version_name, "--force"]
        )
        assert result_drop.exit_code == 0
        actual = runner.invoke_with_connection_json(["app", "version", "list"])
        assert len(actual.json) == 0
