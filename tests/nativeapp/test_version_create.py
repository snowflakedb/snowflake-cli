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
from unittest.mock import MagicMock

import pytest
import typer
from click import BadOptionUsage, ClickException
from snowflake.cli._plugins.nativeapp.artifacts import VersionInfo
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
from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.factories import ApplicationPackageEntityModelFactory, PdfV2Factory
from tests.nativeapp.utils import (
    APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO,
    APPLICATION_PACKAGE_ENTITY_MODULE,
    SQL_EXECUTOR_EXECUTE,
    SQL_FACADE,
    SQL_FACADE_CREATE_VERSION,
    SQL_FACADE_SHOW_RELEASE_DIRECTIVES,
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
    label: str | None = None,
    console: AbstractConsole | None = None,
    from_stage: bool = False,
):
    dm = DefinitionManager()
    pd = dm.project_definition
    pkg_model: ApplicationPackageEntityModel = pd.entities["app_pkg"]
    ctx = WorkspaceContext(
        console=console or cc,
        project_root=dm.project_root,
        get_default_role=lambda: "mock_role",
        get_default_warehouse=lambda: "mock_warehouse",
    )
    pkg = ApplicationPackageEntity(pkg_model, ctx)
    return pkg.action_version_create(
        action_ctx=mock.Mock(spec=ActionContext),
        version=version,
        patch=patch,
        label=label,
        force=force,
        interactive=interactive,
        skip_git_check=skip_git_check,
        from_stage=from_stage,
    )


# Test get_existing_release_directive_info_for_version returns release directives info correctly
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_existing_release_direction_info(
    mock_execute, temporary_directory, mock_cursor, workspace_context
):
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
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    result = pkg.get_existing_release_directive_info_for_version(version=version)
    assert mock_execute.mock_calls == expected
    assert len(result) == 2


# Test add_new_version adds a new version to an app pkg correctly
@mock.patch(SQL_FACADE_CREATE_VERSION)
@pytest.mark.parametrize(
    "version",
    ["V1", "1.0.0", '"1.0.0"'],
)
def test_add_version(
    mock_create_version,
    temporary_directory,
    mock_cursor,
    version,
    workspace_context,
):
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = DefinitionManager()
    pd = dm.project_definition
    pkg_model: ApplicationPackageEntityModel = pd.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    pkg.add_new_version(version=version)

    mock_create_version.assert_called_once_with(
        package_name="app_pkg",
        version=version,
        path_to_version_directory=f"app_pkg.{pkg_model.stage}",
        role="package_role",
        label=None,
    )


# Test add_new_patch_to_version adds an "auto-increment" patch to an existing version
@mock.patch(SQL_EXECUTOR_EXECUTE)
@pytest.mark.parametrize(
    ["version", "version_identifier"],
    [("V1", "V1"), ("1.0.0", '"1.0.0"'), ('"1.0.0"', '"1.0.0"')],
)
def test_add_new_patch_auto(
    mock_execute,
    temporary_directory,
    mock_cursor,
    version,
    version_identifier,
    workspace_context,
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
                            add patch for version {version_identifier}
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
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    result_patch = pkg.add_new_patch_to_version(version=version)
    assert result_patch == 12

    assert mock_execute.mock_calls == expected


# Test add_new_patch_to_version adds a custom patch to an existing version
@mock.patch(SQL_EXECUTOR_EXECUTE)
@pytest.mark.parametrize(
    ["version", "version_identifier"],
    [("V1", "V1"), ("1.0.0", '"1.0.0"'), ('"1.0.0"', '"1.0.0"')],
)
def test_add_new_patch_custom(
    mock_execute,
    temporary_directory,
    mock_cursor,
    version,
    version_identifier,
    workspace_context,
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
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    result_patch = pkg.add_new_patch_to_version(version=version, patch=12)
    assert result_patch == 12
    assert mock_execute.mock_calls == expected


# Test version create when user did not pass in a version AND we could not find a version in the manifest file either
@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.ApplicationPackageEntity._bundle",
    return_value=None,
)
@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.find_version_info_in_manifest_file",
    return_value=VersionInfo(None, None, None),
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
    temporary_directory,
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
            label="version_label",
        )  # last three parameters do not matter here, so it should succeed for all policies.
    mock_version_info_in_manifest.assert_called_once()


# Test version create when user passed in a version and patch AND version does not exist in app package
@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.ApplicationPackageEntity.get_existing_version_info",
    return_value=None,
)
@mock.patch.object(ApplicationPackageEntity, "_bundle", return_value=None)
@pytest.mark.parametrize("force", [True, False])
@pytest.mark.parametrize("interactive", [True, False])
@pytest.mark.parametrize("skip_git_check", [True, False])
def test_process_no_version_exists_throws_bad_option_exception_one(
    mock_bundle,
    mock_existing_version_info,
    force,
    interactive,
    skip_git_check,
    temporary_directory,
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
@mock.patch.object(ApplicationPackageEntity, "_bundle", return_value=None)
@pytest.mark.parametrize("force", [True, False])
@pytest.mark.parametrize("interactive", [True, False])
@pytest.mark.parametrize("skip_git_check", [True, False])
def test_process_no_version_exists_throws_bad_option_exception_two(
    mock_bundle,
    mock_existing_version_info,
    force,
    interactive,
    skip_git_check,
    temporary_directory,
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
    return_value=VersionInfo("manifest_version", None, None),
)
@mock.patch.object(
    ApplicationPackageEntity, "check_index_changes_in_git_repo", return_value=None
)
@mock.patch.object(ApplicationPackageEntity, "_deploy", return_value=None)
@mock.patch.object(ApplicationPackageEntity, "_bundle", return_value=None)
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
    mock_bundle,
    mock_deploy,
    mock_check_git,
    mock_find_version,
    force,
    interactive,
    temporary_directory,
    mock_cursor,
):
    version = "V1"

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    result = _version_create(
        version=version,
        patch=None,
        force=force,
        interactive=interactive,
        skip_git_check=False,
    )  # last three parameters do not matter here

    assert result == VersionInfo(version, 0, None)

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
@mock.patch.object(ApplicationPackageEntity, "_deploy", return_value=None)
@mock.patch.object(ApplicationPackageEntity, "_bundle", return_value=None)
@mock.patch.object(
    ApplicationPackageEntity,
    "get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch.object(ApplicationPackageEntity, "get_existing_version_info")
@mock.patch.object(ApplicationPackageEntity, "add_new_version")
@mock.patch.object(ApplicationPackageEntity, "add_new_patch_to_version")
@pytest.mark.parametrize("force", [True, False])
@pytest.mark.parametrize("interactive", [True, False])
def test_process_no_existing_release_directives_w_existing_version(
    mock_add_patch,
    mock_add_new_version,
    mock_existing_version_info,
    mock_rd,
    mock_bundle,
    mock_deploy,
    mock_check_git,
    mock_find_version,
    force,
    interactive,
    temporary_directory,
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
    mock_add_patch.return_value = 12

    result = _version_create(
        version=version,
        patch=12,
        force=force,
        interactive=interactive,
        skip_git_check=False,
    )  # last three parameters do not matter here

    assert result == VersionInfo(version, 12, None)

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
@mock.patch.object(ApplicationPackageEntity, "_deploy", return_value=None)
@mock.patch.object(ApplicationPackageEntity, "_bundle", return_value=None)
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
    mock_bundle,
    mock_deploy,
    mock_check_git,
    interactive,
    expected_code,
    temporary_directory,
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
@mock.patch.object(ApplicationPackageEntity, "_deploy", return_value=None)
@mock.patch.object(ApplicationPackageEntity, "_bundle", return_value=None)
@mock.patch.object(
    ApplicationPackageEntity,
    "get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch.object(
    ApplicationPackageEntity, "get_existing_version_info", return_value=None
)
@mock.patch.object(ApplicationPackageEntity, "add_new_patch_to_version")
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
    mock_bundle,
    mock_deploy,
    mock_check_git,
    force,
    interactive,
    temporary_directory,
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
    mock_add_patch.return_value = 12

    result = _version_create(
        version=version,
        patch=12,
        force=force,
        interactive=interactive,
        skip_git_check=False,
    )

    assert result == VersionInfo(version, 12, None)

    mock_check_git.assert_called_once()
    mock_rd.assert_called_once()
    mock_deploy.assert_called_once()
    assert mock_existing_version_info.call_count == 2
    mock_add_patch.assert_called_once()


@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.find_version_info_in_manifest_file",
    return_value=VersionInfo(
        version_name="manifest_version", patch_number="2", label="manifest_label"
    ),
)
@mock.patch.object(ApplicationPackageEntity, "_deploy", return_value=None)
@mock.patch.object(ApplicationPackageEntity, "_bundle", return_value=None)
@mock.patch.object(
    ApplicationPackageEntity,
    "get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch.object(
    ApplicationPackageEntity, "get_existing_version_info", return_value=None
)
@mock.patch(
    f"{SQL_FACADE}.create_version_in_package",
    return_value=None,
)
def test_manifest_version_info_not_used(
    mock_create_version,
    mock_existing_version,
    mock_rd,
    mock_bundle,
    mock_deploy,
    mock_find_info_manifest,
    temporary_directory,
    mock_cursor,
):

    role = "package_role"
    stage = "app_src.stage"

    version_cli = "version name from cli"
    PdfV2Factory(
        entities=dict(
            app_pkg=ApplicationPackageEntityModelFactory(
                stage=stage, meta__role=role, meta__warehouse="pkg_wh"
            ),
        )
    )

    result = _version_create(
        version=version_cli,
        patch=None,
        label=None,
        skip_git_check=True,
        interactive=True,
        force=False,
    )

    assert result == VersionInfo(version_cli, 0, None)

    mock_create_version.assert_called_with(
        role=role,
        package_name="app_pkg",
        path_to_version_directory=f"app_pkg.{stage}",
        version=version_cli,
        label=None,
    )
    mock_find_info_manifest.assert_not_called()


@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.find_version_info_in_manifest_file",
    return_value=VersionInfo(
        version_name="manifest_version", patch_number="4", label="manifest_label"
    ),
)
@mock.patch.object(ApplicationPackageEntity, "_deploy", return_value=None)
@mock.patch.object(ApplicationPackageEntity, "_bundle", return_value=None)
@mock.patch.object(
    ApplicationPackageEntity,
    "get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch.object(
    ApplicationPackageEntity, "get_existing_version_info", return_value=True
)
@mock.patch(
    f"{SQL_FACADE}.add_patch_to_package_version",
)
@pytest.mark.parametrize("label", [None, "some label"])
@pytest.mark.parametrize("patch", [None, 2, 7])
def test_manifest_patch_is_not_used(
    mock_create_patch,
    mock_existing_version,
    mock_rd,
    mock_bundle,
    mock_deploy,
    mock_find_info_manifest,
    patch,
    label,
    temporary_directory,
    mock_cursor,
):

    role = "package_role"
    stage = "app_src.stage"

    version_cli = "version name from cli"
    PdfV2Factory(
        entities=dict(
            app_pkg=ApplicationPackageEntityModelFactory(
                stage=stage, meta__role=role, meta__warehouse="pkg_wh"
            ),
        )
    )
    mock_create_patch.return_value = patch or 0

    result = _version_create(
        version=version_cli,
        patch=patch,
        label=label,
        skip_git_check=True,
        interactive=True,
        force=False,
    )

    assert result == VersionInfo(version_cli, patch or 0, label)

    mock_create_patch.assert_called_with(
        role=role,
        package_name="app_pkg",
        path_to_version_directory=f"app_pkg.{stage}",
        version=version_cli,
        patch=patch,
        # ensure empty label is used to replace label from manifest.yml
        label=label,
    )
    mock_find_info_manifest.assert_not_called()


@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.find_version_info_in_manifest_file",
)
@mock.patch.object(ApplicationPackageEntity, "_deploy", return_value=None)
@mock.patch.object(ApplicationPackageEntity, "_bundle", return_value=None)
@mock.patch.object(
    ApplicationPackageEntity,
    "get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch.object(
    ApplicationPackageEntity,
    "get_existing_version_info",
    return_value=True,  # so we can use patch
)
@mock.patch(
    f"{SQL_FACADE}.add_patch_to_package_version",
)
@pytest.mark.parametrize("manifest_label", [None, "some label", ""])
@pytest.mark.parametrize("manifest_patch", [None, 4])
@pytest.mark.parametrize("cli_label", [None, "", "cli label"])
def test_version_from_manifest(
    mock_create_patch,
    mock_existing_version,
    mock_rd,
    mock_bundle,
    mock_deploy,
    mock_find_info_manifest,
    cli_label,
    manifest_patch,
    manifest_label,
    temporary_directory,
    mock_cursor,
):

    mock_find_info_manifest.return_value = VersionInfo(
        version_name="manifest_version",
        patch_number=manifest_patch,
        label=manifest_label,
    )

    role = "package_role"
    stage = "app_src.stage"

    PdfV2Factory(
        entities=dict(
            app_pkg=ApplicationPackageEntityModelFactory(
                stage=stage, meta__role=role, meta__warehouse="pkg_wh"
            ),
        )
    )
    mock_create_patch.return_value = manifest_patch

    # no version or patch through cli
    result = _version_create(
        version=None,
        patch=None,
        label=cli_label,
        skip_git_check=True,
        interactive=True,
        force=False,
    )
    expected_label = cli_label if cli_label is not None else manifest_label

    assert result == VersionInfo("manifest_version", manifest_patch, expected_label)

    mock_create_patch.assert_called_with(
        role=role,
        package_name="app_pkg",
        path_to_version_directory=f"app_pkg.{stage}",
        version="manifest_version",
        patch=manifest_patch,
        label=expected_label,
    )


@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.find_version_info_in_manifest_file",
)
@mock.patch.object(ApplicationPackageEntity, "_deploy", return_value=None)
@mock.patch.object(ApplicationPackageEntity, "_bundle", return_value=None)
@mock.patch.object(
    ApplicationPackageEntity,
    "get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch.object(
    ApplicationPackageEntity,
    "get_existing_version_info",
    return_value=True,  # so we can use patch
)
@mock.patch(
    f"{SQL_FACADE}.add_patch_to_package_version",
)
@pytest.mark.parametrize("manifest_label", [None, "some label", ""])
@pytest.mark.parametrize("cli_label", [None, "", "cli label"])
def test_patch_from_manifest(
    mock_create_patch,
    mock_existing_version,
    mock_rd,
    mock_bundle,
    mock_deploy,
    mock_find_info_manifest,
    cli_label,
    manifest_label,
    temporary_directory,
    mock_cursor,
):
    manifest_patch = 4
    cli_patch = 2
    mock_find_info_manifest.return_value = VersionInfo(
        version_name="manifest_version",
        patch_number=manifest_patch,
        label=manifest_label,
    )

    role = "package_role"
    stage = "app_src.stage"
    mock_console = MagicMock()
    PdfV2Factory(
        entities=dict(
            app_pkg=ApplicationPackageEntityModelFactory(
                stage=stage, meta__role=role, meta__warehouse="pkg_wh"
            ),
        )
    )
    mock_create_patch.return_value = cli_patch

    # patch through cli, but no version
    result = _version_create(
        version=None,
        patch=cli_patch,
        label=cli_label,
        skip_git_check=True,
        interactive=True,
        # Force to skip confirmation on patch override
        force=True,
        console=mock_console,
    )

    expected_label = cli_label if cli_label is not None else manifest_label
    assert result == VersionInfo("manifest_version", cli_patch, expected_label)

    mock_create_patch.assert_called_with(
        role=role,
        package_name="app_pkg",
        path_to_version_directory=f"app_pkg.{stage}",
        version="manifest_version",
        # cli patch overrides the manifest
        patch=cli_patch,
        label=expected_label,
    )
    mock_console.warning.assert_called_with(
        f"Cannot resolve version. Found patch: {manifest_patch} in manifest.yml which is different from provided patch {cli_patch}."
    )


@mock.patch(SQL_FACADE_CREATE_VERSION)
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES, return_value=[])
@mock.patch.object(ApplicationPackageEntity, "_deploy")
@mock.patch(
    APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO, return_value=[{"name": "app_pkg"}]
)
@mock.patch.object(
    ApplicationPackageEntity, "check_index_changes_in_git_repo", return_value=None
)
@mock.patch.object(
    ApplicationPackageEntity, "get_existing_version_info", return_value=None
)
@mock.patch.object(ApplicationPackageEntity, "_bundle")
def test_action_version_create_from_stage(
    mock_bundle,
    mock_get_existing_version_info,
    mock_check_git,
    mock_get_existing_pkg_info,
    mock_deploy,
    mock_show_release_directives,
    mock_create_version,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    version = "v1"
    result = application_package_entity.action_version_create(
        action_ctx=action_context,
        version=version,
        patch=None,
        label=None,
        skip_git_check=False,
        interactive=False,
        force=False,
        from_stage=True,
    )

    assert result == VersionInfo(version, 0, None)

    mock_check_git.assert_called_once()
    mock_show_release_directives.assert_called_once_with(
        package_name=pkg_model.fqn.name, role=pkg_model.meta.role
    )
    mock_get_existing_version_info.assert_called_once_with(version)
    mock_bundle.assert_called_once()
    mock_create_version.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        version=version,
        path_to_version_directory=application_package_entity.stage_path.full_path,
        role=pkg_model.meta.role,
        label=None,
    )

    # Deploy should not be called with --from-stage
    mock_deploy.assert_not_called()
