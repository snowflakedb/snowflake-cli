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
from unittest import mock

import pytest
import typer
from click import ClickException
from snowflake.cli._plugins.nativeapp.exceptions import (
    ApplicationPackageDoesNotExistError,
)
from snowflake.cli._plugins.nativeapp.policy import (
    AllowAlwaysPolicy,
    AskAlwaysPolicy,
    DenyAlwaysPolicy,
)
from snowflake.cli._plugins.nativeapp.version.version_processor import (
    NativeAppVersionDropProcessor,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager

from tests.nativeapp.patch_utils import mock_get_app_pkg_distribution_in_sf
from tests.nativeapp.utils import (
    APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO,
    APPLICATION_PACKAGE_ENTITY_MODULE,
    SQL_EXECUTOR_EXECUTE,
    TYPER_CONFIRM,
    mock_execute_helper,
    mock_snowflake_yml_file,
)
from tests.testing_utils.files_and_dirs import create_named_file

DROP_PROCESSOR = "NativeAppVersionDropProcessor"

allow_always_policy = AllowAlwaysPolicy()
ask_always_policy = AskAlwaysPolicy()
deny_always_policy = DenyAlwaysPolicy()


def _get_version_drop_processor():
    dm = DefinitionManager()

    return NativeAppVersionDropProcessor(
        project_definition=dm.project_definition.native_app,
        project_root=dm.project_root,
    )


# Test version drop process when there is no existing application package
@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO, return_value=None)
@pytest.mark.parametrize("force", [True, False])
@pytest.mark.parametrize("interactive", [True, False])
def test_process_has_no_existing_app_pkg(
    mock_get_existing, force, interactive, temp_dir
):

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_drop_processor()
    with pytest.raises(ApplicationPackageDoesNotExistError):
        processor.process(version="some_version", force=force, interactive=interactive)


# Test version drop process when user did not pass in a version AND we could not find a version in the manifest file either
@mock.patch(
    APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO,
    return_value={"owner": "package_role"},
)
@mock_get_app_pkg_distribution_in_sf()
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
def test_process_no_version_from_user_no_version_in_manifest(
    mock_version_info_in_manifest,
    mock_build_bundle,
    mock_distribution,
    mock_get_existing,
    force,
    interactive,
    temp_dir,
):

    mock_distribution.return_value = "internal"
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_drop_processor()
    with pytest.raises(ClickException):
        processor.process(version=None, force=force, interactive=interactive)
    mock_build_bundle.assert_called_once()
    mock_version_info_in_manifest.assert_called_once()


# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is False AND interactive mode is False AND --interactive is False
# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is False AND interactive mode is False AND --interactive is True AND user does not want to proceed
# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is False AND interactive mode is True AND user does not want to proceed
@mock.patch(
    APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO,
    return_value={"owner": "package_role"},
)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.ApplicationPackageEntity.bundle",
    return_value=None,
)
@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.find_version_info_in_manifest_file",
    return_value=("manifest_version", None),
)
@mock.patch(
    f"snowflake.cli._plugins.nativeapp.policy.{TYPER_CONFIRM}", return_value=False
)
@pytest.mark.parametrize(
    "interactive, expected_code",
    [
        (False, 1),
        (True, 0),
    ],
)
def test_process_drop_cannot_complete(
    mock_typer_confirm,
    mock_version_info_in_manifest,
    mock_build_bundle,
    mock_distribution,
    mock_get_existing,
    interactive,
    expected_code,
    temp_dir,
):

    mock_distribution.return_value = "internal"
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_drop_processor()
    with pytest.raises(typer.Exit):
        result = processor.process(version=None, force=False, interactive=interactive)
        assert result.exit_code == expected_code


# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is True
# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is False AND interactive mode is False AND --interactive is True AND user wants to proceed
# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is False AND interactive mode is True AND user wants to proceed
@mock.patch(
    APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO,
    return_value={"owner": "package_role"},
)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.ApplicationPackageEntity.bundle",
    return_value=None,
)
@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.find_version_info_in_manifest_file",
    return_value=("manifest_version", None),
)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(
    f"snowflake.cli._plugins.nativeapp.policy.{TYPER_CONFIRM}", return_value=True
)
@pytest.mark.parametrize("force", [True, False])
def test_process_drop_from_manifest(
    mock_typer_confirm,
    mock_execute,
    mock_version_info_in_manifest,
    mock_build_bundle,
    mock_distribution,
    mock_get_existing,
    force,
    temp_dir,
    mock_cursor,
):

    mock_distribution.return_value = "internal"
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
                    "alter application package app_pkg drop version manifest_version"
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
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_drop_processor()
    processor.process(version=None, force=force, interactive=True)
    assert mock_execute.mock_calls == expected


@mock.patch(
    APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO,
    return_value={"owner": "package_role"},
)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.ApplicationPackageEntity.bundle",
    return_value=None,
)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(
    f"snowflake.cli._plugins.nativeapp.policy.{TYPER_CONFIRM}", return_value=True
)
@pytest.mark.parametrize("force", [True, False])
@pytest.mark.parametrize(
    ["version", "version_identifier"],
    [("V1", "V1"), ("1.0.0", '"1.0.0"'), ('"1.0.0"', '"1.0.0"')],
)
def test_process_drop_specific_version(
    mock_typer_confirm,
    mock_execute,
    mock_build_bundle,
    mock_distribution,
    mock_get_existing,
    force,
    temp_dir,
    mock_cursor,
    version,
    version_identifier,
):

    mock_distribution.return_value = "internal"
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
                    f"alter application package app_pkg drop version {version_identifier}"
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
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_drop_processor()
    processor.process(version=version, force=force, interactive=True)
    assert mock_execute.mock_calls == expected
