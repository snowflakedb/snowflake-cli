import unittest

import typer
from click import ClickException
from snowcli.cli.nativeapp.exceptions import ApplicationPackageDoesNotExistError
from snowcli.cli.nativeapp.policy import AllowAlwaysPolicy, AskAlwaysPolicy
from snowcli.cli.nativeapp.version.version_processor import (
    NativeAppVersionDropProcessor,
)
from snowcli.cli.project.definition_manager import DefinitionManager
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.utils import *
from tests.testing_utils.fixtures import *

DROP_PROCESSOR = "NativeAppVersionDropProcessor"

allow_always_policy = AllowAlwaysPolicy()
ask_always_policy = AskAlwaysPolicy()


def _get_version_drop_processor():
    dm = DefinitionManager()

    return NativeAppVersionDropProcessor(
        project_definition=dm.project_definition["native_app"],
        project_root=dm.project_root,
    )


# Test version drop process when there is no existing application package
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.get_existing_app_pkg_info", return_value=None
)
def test_process_has_no_existing_app_pkg(mock_get_existing, temp_dir):

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_drop_processor()
    with pytest.raises(ApplicationPackageDoesNotExistError):
        processor.process(
            version="some_version", policy=ask_always_policy
        )  # policy does not matter here


# Test version drop process when user did not pass in a version AND we could not find a version in the manifest file either
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.get_existing_app_pkg_info",
    return_value={"owner": "package_role"},
)
@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.build_bundle", return_value=None)
@mock.patch(FIND_VERSION_FROM_MANIFEST, return_value=(None, None))
@mock.patch(f"{VERSION_MODULE}.log.info")
def test_process_no_version_from_user_no_version_in_manifest(
    mock_log,
    mock_version_info_in_manifest,
    mock_build_bundle,
    mock_get_existing,
    temp_dir,
):
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_drop_processor()
    with pytest.raises(ClickException):
        processor.process(
            version=None, policy=allow_always_policy
        )  # policy does not matter here
    mock_log.assert_called_once()
    mock_build_bundle.assert_called_once()
    mock_version_info_in_manifest.assert_called_once()


# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is False AND interactive mode is False
# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is False AND interactive mode is True AND user does not want to proceed
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.get_existing_app_pkg_info",
    return_value={"owner": "package_role"},
)
@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.build_bundle", return_value=None)
@mock.patch(FIND_VERSION_FROM_MANIFEST, return_value=("manifest_version", None))
@mock.patch(f"{VERSION_MODULE}.is_user_in_interactive_mode")
@mock.patch(f"snowcli.cli.nativeapp.policy.{TYPER_CONFIRM}", return_value=False)
@pytest.mark.parametrize("is_interactive", [False, True])
def test_process_drop_cannot_complete(
    mock_typer_confirm,
    mock_is_interactive,
    mock_version_info_in_manifest,
    mock_build_bundle,
    mock_get_existing,
    is_interactive,
    temp_dir,
):
    mock_is_interactive.return_value = is_interactive
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_drop_processor()
    with pytest.raises(typer.Exit):
        result = processor.process(version=None, policy=ask_always_policy)
        assert result.exit_code == (
            not is_interactive
        )  # exit_code is 1 if is_interactive is False/0


# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is True
# Test version drop process when user did not pass in a version AND manifest file has a version in it AND --force is False AND interactive mode is True AND user wants to proceed
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.get_existing_app_pkg_info",
    return_value={"owner": "package_role"},
)
@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.build_bundle", return_value=None)
@mock.patch(FIND_VERSION_FROM_MANIFEST, return_value=("manifest_version", None))
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(f"{VERSION_MODULE}.is_user_in_interactive_mode", return_value=True)
@mock.patch(f"snowcli.cli.nativeapp.policy.{TYPER_CONFIRM}", return_value=True)
@pytest.mark.parametrize("var_policy", [allow_always_policy, ask_always_policy])
def test_process_drop_success(
    mock_typer_confirm,
    mock_is_interactive,
    mock_execute,
    mock_version_info_in_manifest,
    mock_build_bundle,
    mock_get_existing,
    var_policy,
    temp_dir,
    mock_cursor,
):
    mock_is_interactive.return_value = True

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
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_drop_processor()
    processor.process(version=None, policy=var_policy)
    assert mock_execute.mock_calls == expected
