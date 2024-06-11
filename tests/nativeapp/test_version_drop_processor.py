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
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.plugins.nativeapp.exceptions import (
    ApplicationPackageDoesNotExistError,
)
from snowflake.cli.plugins.nativeapp.policy import (
    AllowAlwaysPolicy,
    AskAlwaysPolicy,
    DenyAlwaysPolicy,
)
from snowflake.cli.plugins.nativeapp.version.version_processor import (
    NativeAppVersionDropProcessor,
)
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.patch_utils import mock_get_app_pkg_distribution_in_sf
from tests.nativeapp.utils import (
    FIND_VERSION_FROM_MANIFEST,
    NATIVEAPP_MANAGER_EXECUTE,
    TYPER_CONFIRM,
    VERSION_MODULE,
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
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.get_existing_app_pkg_info", return_value=None
)
@pytest.mark.parametrize(
    "policy_param", [allow_always_policy, ask_always_policy, deny_always_policy]
)
def test_process_has_no_existing_app_pkg(mock_get_existing, policy_param, temp_dir):

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_drop_processor()
    with pytest.raises(ApplicationPackageDoesNotExistError):
        processor.process(
            version="some_version", policy=policy_param, is_interactive=True
        )  # last two don't matter here


# Test version drop process when user did not pass in a version AND we could not find a version in the manifest file either
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.get_existing_app_pkg_info",
    return_value={"owner": "package_role"},
)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.build_bundle", return_value=None)
@mock.patch(FIND_VERSION_FROM_MANIFEST, return_value=(None, None))
@pytest.mark.parametrize(
    "policy_param", [allow_always_policy, ask_always_policy, deny_always_policy]
)
def test_process_no_version_from_user_no_version_in_manifest(
    mock_version_info_in_manifest,
    mock_build_bundle,
    mock_mismatch,
    mock_get_existing,
    policy_param,
    temp_dir,
):

    mock_mismatch.return_Value = "internal"
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_drop_processor()
    with pytest.raises(ClickException):
        processor.process(
            version=None, policy=policy_param, is_interactive=True
        )  # last two don't matter here
    mock_build_bundle.assert_called_once()
    mock_version_info_in_manifest.assert_called_once()


# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is False AND interactive mode is False AND --interactive is False
# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is False AND interactive mode is False AND --interactive is True AND user does not want to proceed
# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is False AND interactive mode is True AND user does not want to proceed
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.get_existing_app_pkg_info",
    return_value={"owner": "package_role"},
)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.build_bundle", return_value=None)
@mock.patch(FIND_VERSION_FROM_MANIFEST, return_value=("manifest_version", None))
@mock.patch(
    f"snowflake.cli.plugins.nativeapp.policy.{TYPER_CONFIRM}", return_value=False
)
@pytest.mark.parametrize(
    "policy_param, is_interactive_param, expected_code",
    [
        (deny_always_policy, False, 1),
        (ask_always_policy, True, 0),
        (ask_always_policy, True, 0),
    ],
)
def test_process_drop_cannot_complete(
    mock_typer_confirm,
    mock_version_info_in_manifest,
    mock_build_bundle,
    mock_mismatch,
    mock_get_existing,
    policy_param,
    is_interactive_param,
    expected_code,
    temp_dir,
):

    mock_mismatch.return_value = "internal"
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_drop_processor()
    with pytest.raises(typer.Exit):
        result = processor.process(
            version=None, policy=policy_param, is_interactive=is_interactive_param
        )
        assert result.exit_code == expected_code


# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is True
# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is False AND interactive mode is False AND --interactive is True AND user wants to proceed
# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is False AND interactive mode is True AND user wants to proceed
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.get_existing_app_pkg_info",
    return_value={"owner": "package_role"},
)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.build_bundle", return_value=None)
@mock.patch(FIND_VERSION_FROM_MANIFEST, return_value=("manifest_version", None))
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(
    f"snowflake.cli.plugins.nativeapp.policy.{TYPER_CONFIRM}", return_value=True
)
@pytest.mark.parametrize(
    "policy_param, is_interactive_param",
    [
        (allow_always_policy, False),
        (ask_always_policy, True),
        (ask_always_policy, True),
    ],
)
def test_process_drop_success(
    mock_typer_confirm,
    mock_execute,
    mock_version_info_in_manifest,
    mock_build_bundle,
    mock_mismatch,
    mock_get_existing,
    policy_param,
    is_interactive_param,
    temp_dir,
    mock_cursor,
):

    mock_mismatch.return_value = "internal"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
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
    processor.process(
        version=None, policy=policy_param, is_interactive=is_interactive_param
    )
    assert mock_execute.mock_calls == expected
