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
from textwrap import dedent
from unittest import mock

import pytest
import typer
from click import BadOptionUsage, ClickException
from snowflake.cli._plugins.nativeapp.constants import SPECIAL_COMMENT
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntity,
    ApplicationPackageEntityModel,
)
from snowflake.cli._plugins.nativeapp.exceptions import (
    ApplicationPackageDoesNotExistError,
)
from snowflake.cli._plugins.nativeapp.policy import (
    AllowAlwaysPolicy,
    AskAlwaysPolicy,
    DenyAlwaysPolicy,
)
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.utils import (
    APPLICATION_PACKAGE_ENTITY_MODULE,
    SQL_EXECUTOR_EXECUTE,
    mock_execute_helper,
    mock_snowflake_yml_file_v2,
)
from tests.testing_utils.files_and_dirs import create_named_file

allow_always_policy = AllowAlwaysPolicy()
ask_always_policy = AskAlwaysPolicy()
deny_always_policy = DenyAlwaysPolicy()


def _version_create(
    version: str | None,
    patch: int | None,
    force: bool,
    interactive: bool,
    skip_git_check: bool,
):
    dm = DefinitionManager()
    pd = dm.project_definition
    pkg_model: ApplicationPackageEntityModel = pd.entities["app_pkg"]
    return ApplicationPackageEntity.version_create(
        console=cc,
        project_root=dm.project_root,
        deploy_root=dm.project_root / pkg_model.deploy_root,
        bundle_root=dm.project_root / pkg_model.bundle_root,
        generated_root=(
            dm.project_root / pkg_model.deploy_root / pkg_model.generated_root
        ),
        artifacts=pkg_model.artifacts,
        package_name=pkg_model.fqn.name,
        package_role=pkg_model.meta.role,
        package_distribution=pkg_model.distribution,
        prune=True,
        recursive=True,
        paths=None,
        print_diff=True,
        validate=True,
        stage_fqn=f"{pkg_model.fqn.name}.{pkg_model.stage}",
        package_warehouse=pkg_model.meta.warehouse,
        post_deploy_hooks=pkg_model.meta.post_deploy,
        version=version,
        patch=patch,
        force=force,
        interactive=interactive,
        skip_git_check=skip_git_check,
    )


# Test get_existing_release_directive_info_for_version returns release directives info correctly
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_existing_release_direction_info(mock_execute, temp_dir, mock_cursor):
    version = "V1"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor(
                    [
                        {"name": "RD1", "version": version},
                        {"name": "RD2", "version": "V2"},
                        {"name": "RD3", "version": version},
                    ],
                    [],
                ),
                mock.call(
                    f"show release directives in application package app_pkg",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = DefinitionManager()
    pd = dm.project_definition
    pkg_model: ApplicationPackageEntityModel = pd.entities["app_pkg"]
    result = ApplicationPackageEntity.get_existing_release_directive_info_for_version(
        package_name=pkg_model.fqn.name,
        package_role=pkg_model.meta.role,
        version=version,
    )
    assert mock_execute.mock_calls == expected
    assert len(result) == 2


# Test add_new_version adds a new version to an app pkg correctly
@mock.patch(SQL_EXECUTOR_EXECUTE)
@pytest.mark.parametrize(
    ["version", "version_identifier"],
    [("V1", "V1"), ("1.0.0", '"1.0.0"'), ('"1.0.0"', '"1.0.0"')],
)
def test_add_version(mock_execute, temp_dir, mock_cursor, version, version_identifier):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (
                None,
                mock.call(
                    dedent(
                        f"""\
                        alter application package app_pkg
                            add version {version_identifier}
                            using @app_pkg.app_src.stage
                    """
                    ),
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = DefinitionManager()
    pd = dm.project_definition
    pkg_model: ApplicationPackageEntityModel = pd.entities["app_pkg"]
    ApplicationPackageEntity.add_new_version(
        console=cc,
        package_name=pkg_model.fqn.name,
        package_role=pkg_model.meta.role,
        stage_fqn=f"{pkg_model.fqn.name}.{pkg_model.stage}",
        version=version,
    )
    assert mock_execute.mock_calls == expected


# Test add_new_patch_to_version adds an "auto-increment" patch to an existing version
@mock.patch(SQL_EXECUTOR_EXECUTE)
@pytest.mark.parametrize(
    ["version", "version_identifier"],
    [("V1", "V1"), ("1.0.0", '"1.0.0"'), ('"1.0.0"', '"1.0.0"')],
)
def test_add_new_patch_auto(
    mock_execute, temp_dir, mock_cursor, version, version_identifier
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([{"version": version, "patch": 12}], []),
                mock.call(
                    dedent(
                        f"""\
                        alter application package app_pkg
                            add patch  for version {version_identifier}
                            using @app_pkg.app_src.stage
                    """
                    ),
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = DefinitionManager()
    pd = dm.project_definition
    pkg_model: ApplicationPackageEntityModel = pd.entities["app_pkg"]
    ApplicationPackageEntity.add_new_patch_to_version(
        console=cc,
        package_name=pkg_model.fqn.name,
        package_role=pkg_model.meta.role,
        stage_fqn=f"{pkg_model.fqn.name}.{pkg_model.stage}",
        version=version,
    )
    assert mock_execute.mock_calls == expected


# Test add_new_patch_to_version adds a custom patch to an existing version
@mock.patch(SQL_EXECUTOR_EXECUTE)
@pytest.mark.parametrize(
    ["version", "version_identifier"],
    [("V1", "V1"), ("1.0.0", '"1.0.0"'), ('"1.0.0"', '"1.0.0"')],
)
def test_add_new_patch_custom(
    mock_execute, temp_dir, mock_cursor, version, version_identifier
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([{"version": version, "patch": 12}], []),
                mock.call(
                    dedent(
                        f"""\
                        alter application package app_pkg
                            add patch 12 for version {version_identifier}
                            using @app_pkg.app_src.stage
                    """
                    ),
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = DefinitionManager()
    pd = dm.project_definition
    pkg_model: ApplicationPackageEntityModel = pd.entities["app_pkg"]
    ApplicationPackageEntity.add_new_patch_to_version(
        console=cc,
        package_name=pkg_model.fqn.name,
        package_role=pkg_model.meta.role,
        stage_fqn=f"{pkg_model.fqn.name}.{pkg_model.stage}",
        version=version,
        patch=12,
    )
    assert mock_execute.mock_calls == expected


# Test version create when user did not pass in a version AND we could not find a version in the manifest file either
@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.ApplicationPackageEntity.bundle",
    return_value=None,
)
@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.find_version_info_in_manifest_file",
    return_value=(None, None),
)
@pytest.mark.parametrize("force", [True, False])
@pytest.mark.parametrize("interactive", [True, False])
@pytest.mark.parametrize("skip_git_check", [True, False])
def test_process_no_version_from_user_no_version_in_manifest(
    mock_version_info_in_manifest,
    mock_bundle,
    force,
    interactive,
    skip_git_check,
    temp_dir,
):
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    with pytest.raises(ClickException):
        _version_create(
            version=None,
            patch=None,
            force=force,
            interactive=interactive,
            skip_git_check=skip_git_check,
        )  # last three parameters do not matter here, so it should succeed for all policies.
    mock_version_info_in_manifest.assert_called_once()


# Test version create when user passed in a version and patch AND version does not exist in app package
@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.ApplicationPackageEntity.get_existing_version_info",
    return_value=None,
)
@pytest.mark.parametrize("force", [True, False])
@pytest.mark.parametrize("interactive", [True, False])
@pytest.mark.parametrize("skip_git_check", [True, False])
def test_process_no_version_exists_throws_bad_option_exception_one(
    mock_existing_version_info,
    force,
    interactive,
    skip_git_check,
    temp_dir,
):
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    with pytest.raises(BadOptionUsage):
        _version_create(
            version="v1",
            patch=12,
            force=force,
            interactive=interactive,
            skip_git_check=skip_git_check,
        )  # last three parameters do not matter here, so it should succeed for all policies.


# Test version create when user passed in a version and patch AND app package does not exist
@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.ApplicationPackageEntity.get_existing_version_info",
    side_effect=ApplicationPackageDoesNotExistError("app_pkg"),
)
@pytest.mark.parametrize("force", [True, False])
@pytest.mark.parametrize("interactive", [True, False])
@pytest.mark.parametrize("skip_git_check", [True, False])
def test_process_no_version_exists_throws_bad_option_exception_two(
    mock_existing_version_info,
    force,
    interactive,
    skip_git_check,
    temp_dir,
):
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    with pytest.raises(BadOptionUsage):
        _version_create(
            version="v1",
            patch=12,
            force=force,
            interactive=interactive,
            skip_git_check=skip_git_check,
        )  # last three parameters do not matter here, so it should succeed for all policies.


# Test version create when there are no release directives matching the version AND no version exists for app pkg
@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.find_version_info_in_manifest_file",
    return_value=("manifest_version", None),
)
@mock.patch.object(
    ApplicationPackageEntity, "check_index_changes_in_git_repo", return_value=None
)
@mock.patch.object(ApplicationPackageEntity, "deploy", return_value=None)
@mock.patch.object(
    ApplicationPackageEntity,
    "get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch.object(
    ApplicationPackageEntity, "get_existing_version_info", return_value=None
)
@mock.patch.object(ApplicationPackageEntity, "add_new_version", return_value=None)
@pytest.mark.parametrize("force", [True, False])
@pytest.mark.parametrize("interactive", [True, False])
def test_process_no_existing_release_directives_or_versions(
    mock_add_new_version,
    mock_existing_version_info,
    mock_rd,
    mock_deploy,
    mock_check_git,
    mock_find_version,
    force,
    interactive,
    temp_dir,
    mock_cursor,
):
    version = "V1"

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    _version_create(
        version=version,
        patch=None,
        force=force,
        interactive=interactive,
        skip_git_check=False,
    )  # last three parameters do not matter here
    mock_find_version.assert_not_called()
    mock_check_git.assert_called_once()
    mock_rd.assert_called_once()
    mock_deploy.assert_called_once()
    mock_existing_version_info.assert_called_once()
    mock_add_new_version.assert_called_once()


# Test version create when there are no release directives matching the version AND a version exists for app pkg
@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.find_version_info_in_manifest_file",
)
@mock.patch.object(
    ApplicationPackageEntity, "check_index_changes_in_git_repo", return_value=None
)
@mock.patch.object(ApplicationPackageEntity, "deploy", return_value=None)
@mock.patch.object(
    ApplicationPackageEntity,
    "get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch.object(ApplicationPackageEntity, "get_existing_version_info")
@mock.patch.object(ApplicationPackageEntity, "add_new_version")
@mock.patch.object(
    ApplicationPackageEntity, "add_new_patch_to_version", return_value=None
)
@pytest.mark.parametrize("force", [True, False])
@pytest.mark.parametrize("interactive", [True, False])
def test_process_no_existing_release_directives_w_existing_version(
    mock_add_patch,
    mock_add_new_version,
    mock_existing_version_info,
    mock_rd,
    mock_deploy,
    mock_check_git,
    mock_find_version,
    force,
    interactive,
    temp_dir,
    mock_cursor,
):
    version = "V1"
    mock_existing_version_info.return_value = {
        "name": "My Package",
        "comment": SPECIAL_COMMENT,
        "owner": "PACKAGE_ROLE",
        "version": version,
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    _version_create(
        version=version,
        patch=12,
        force=force,
        interactive=interactive,
        skip_git_check=False,
    )  # last three parameters do not matter here
    mock_find_version.assert_not_called()
    mock_check_git.assert_called_once()
    mock_rd.assert_called_once()
    mock_deploy.assert_called_once()
    assert mock_existing_version_info.call_count == 2
    mock_add_new_version.assert_not_called()
    mock_add_patch.assert_called_once()


# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is False AND interactive mode is False AND --interactive is False
# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is False AND interactive mode is False AND --interactive is True AND  user does not want to proceed
# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is False AND interactive mode is True AND user does not want to proceed
@mock.patch.object(
    ApplicationPackageEntity, "check_index_changes_in_git_repo", return_value=None
)
@mock.patch.object(ApplicationPackageEntity, "deploy", return_value=None)
@mock.patch.object(
    ApplicationPackageEntity,
    "get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch.object(typer, "confirm", return_value=False)
@mock.patch.object(ApplicationPackageEntity, "get_existing_version_info")
@pytest.mark.parametrize(
    "interactive, expected_code",
    [
        (False, 1),
        (True, 0),
    ],
)
def test_process_existing_release_directives_user_does_not_proceed(
    mock_existing_version_info,
    mock_typer_confirm,
    mock_rd,
    mock_deploy,
    mock_check_git,
    interactive,
    expected_code,
    temp_dir,
    mock_cursor,
):
    version = "V1"
    mock_existing_version_info.return_value = {"version": version, "patch": 0}
    mock_rd.return_value = [
        {"name": "RD1", "version": version},
        {"name": "RD3", "version": version},
    ]

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    with pytest.raises(typer.Exit):
        _version_create(
            version=version,
            patch=12,
            force=False,
            interactive=interactive,
            skip_git_check=False,
        )
    mock_check_git.assert_called_once()
    mock_rd.assert_called_once()
    mock_deploy.assert_called_once()


# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is True
# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is False AND interactive mode is False AND --interactive is True AND user wants to proceed
# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is False AND interactive mode is True AND user wants to proceed
@mock.patch.object(
    ApplicationPackageEntity, "check_index_changes_in_git_repo", return_value=None
)
@mock.patch.object(ApplicationPackageEntity, "deploy", return_value=None)
@mock.patch.object(
    ApplicationPackageEntity,
    "get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch.object(
    ApplicationPackageEntity, "get_existing_version_info", return_value=None
)
@mock.patch.object(
    ApplicationPackageEntity, "add_new_patch_to_version", return_value=None
)
@mock.patch.object(typer, "confirm", return_value=True)
@pytest.mark.parametrize(
    "force, interactive",
    [
        (False, True),
        (True, True),
    ],
)
def test_process_existing_release_directives_w_existing_version_two(
    mock_typer_confirm,
    mock_add_patch,
    mock_existing_version_info,
    mock_rd,
    mock_deploy,
    mock_check_git,
    force,
    interactive,
    temp_dir,
    mock_cursor,
):
    version = "V1"
    mock_existing_version_info.return_value = {
        "name": "My Package",
        "comment": SPECIAL_COMMENT,
        "owner": "PACKAGE_ROLE",
        "version": version,
    }
    mock_rd.return_value = [
        {"name": "RD1", "version": version},
        {"name": "RD3", "version": version},
    ]

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    _version_create(
        version=version,
        patch=12,
        force=force,
        interactive=interactive,
        skip_git_check=False,
    )
    mock_check_git.assert_called_once()
    mock_rd.assert_called_once()
    mock_deploy.assert_called_once()
    assert mock_existing_version_info.call_count == 2
    mock_add_patch.assert_called_once()
