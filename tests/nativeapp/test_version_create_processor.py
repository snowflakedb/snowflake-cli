import unittest
from textwrap import dedent

import typer
from click import BadOptionUsage, ClickException
from snowflake.cli.plugins.nativeapp.constants import SPECIAL_COMMENT
from snowflake.cli.plugins.nativeapp.exceptions import (
    ApplicationPackageDoesNotExistError,
)
from snowflake.cli.plugins.nativeapp.policy import (
    AllowAlwaysPolicy,
    AskAlwaysPolicy,
    DenyAlwaysPolicy,
)
from snowflake.cli.plugins.nativeapp.version.version_processor import (
    NativeAppVersionCreateProcessor,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.utils import *
from tests.testing_utils.fixtures import *

CREATE_PROCESSOR = "NativeAppVersionCreateProcessor"

allow_always_policy = AllowAlwaysPolicy()
ask_always_policy = AskAlwaysPolicy()
deny_always_policy = DenyAlwaysPolicy()


def _get_version_create_processor():
    dm = DefinitionManager()
    return NativeAppVersionCreateProcessor(
        project_definition=dm.project_definition["native_app"],
        project_root=dm.project_root,
    )


# Test get_existing_release_directive_info_for_version returns release directives info correctly
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_existing_release_direction_info(mock_execute, temp_dir, mock_cursor):
    version = "V1"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
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
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_create_processor()
    result = processor.get_existing_release_directive_info_for_version(version)
    assert mock_execute.mock_calls == expected
    assert len(result) == 2


# Test add_new_version adds a new version to an app pkg correctly
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_add_version(mock_execute, temp_dir, mock_cursor):
    version = "V1"
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
                    dedent(
                        f"""\
                        alter application package app_pkg
                            add version V1
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
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_create_processor()
    processor.add_new_version(version)
    assert mock_execute.mock_calls == expected


# Test add_new_patch_to_version adds an "auto-increment" patch to an existing version
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_add_new_patch_auto(mock_execute, temp_dir, mock_cursor):
    version = "V1"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([{"version": version, "patch": 12}], []),
                mock.call(
                    dedent(
                        f"""\
                        alter application package app_pkg
                            add patch  for version V1
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
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_create_processor()
    processor.add_new_patch_to_version(version)
    assert mock_execute.mock_calls == expected


# Test add_new_patch_to_version adds a custom patch to an existing version
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_add_new_patch_custom(mock_execute, temp_dir, mock_cursor):
    version = "V1"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([{"version": version, "patch": 12}], []),
                mock.call(
                    dedent(
                        f"""\
                        alter application package app_pkg
                            add patch 12 for version V1
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
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_create_processor()
    processor.add_new_patch_to_version(version, "12")
    assert mock_execute.mock_calls == expected


# Test version create when user did not pass in a version AND we could not find a version in the manifest file either
@mock.patch(FIND_VERSION_FROM_MANIFEST, return_value=(None, None))
@mock.patch(f"{VERSION_MODULE}.log.info")
@pytest.mark.parametrize(
    "policy_param", [allow_always_policy, ask_always_policy, deny_always_policy]
)
def test_process_no_version_from_user_no_version_in_manifest(
    mock_log, mock_version_info_in_manifest, policy_param, temp_dir
):
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_create_processor()
    with pytest.raises(ClickException):
        processor.process(
            version=None,
            patch=None,
            policy=policy_param,
            git_policy=policy_param,
            is_interactive=False,
        )  # last three parameters do not matter here, so it should succeed for all policies.
    mock_log.assert_called_once()
    mock_version_info_in_manifest.assert_called_once()


# Test version create when user passed in a version and patch AND version does not exist in app package
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.get_existing_version_info", return_value=None
)
@pytest.mark.parametrize(
    "policy_param", [allow_always_policy, ask_always_policy, deny_always_policy]
)
def test_process_no_version_exists_throws_bad_option_exception_one(
    mock_existing_version_info, policy_param, temp_dir
):
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_create_processor()
    with pytest.raises(BadOptionUsage):
        processor.process(
            version="v1",
            patch="12",
            policy=policy_param,
            git_policy=policy_param,
            is_interactive=False,
        )  # last three parameters do not matter here, so it should succeed for all policies.


# Test version create when user passed in a version and patch AND app package does not exist
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.get_existing_version_info",
    side_effect=ApplicationPackageDoesNotExistError("app_pkg"),
)
@pytest.mark.parametrize(
    "policy_param", [allow_always_policy, ask_always_policy, deny_always_policy]
)
def test_process_no_version_exists_throws_bad_option_exception_two(
    mock_existing_version_info, policy_param, temp_dir
):
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_create_processor()
    with pytest.raises(BadOptionUsage):
        processor.process(
            version="v1",
            patch="12",
            policy=policy_param,
            git_policy=policy_param,
            is_interactive=False,
        )  # last three parameters do not matter here, so it should succeed for all policies.


# Test version create when there are no release directives matching the version AND no version exists for app pkg
@mock.patch(FIND_VERSION_FROM_MANIFEST, return_value=("manifest_version", None))
@mock.patch(f"{VERSION_MODULE}.check_index_changes_in_git_repo", return_value=None)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.create_app_package", return_value=None
)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}._apply_package_scripts", return_value=None
)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.sync_deploy_root_with_stage",
    return_value=None,
)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.get_existing_version_info", return_value=None
)
@mock.patch(f"{VERSION_MODULE}.{CREATE_PROCESSOR}.add_new_version", return_value=None)
@pytest.mark.parametrize(
    "policy_param", [allow_always_policy, ask_always_policy, deny_always_policy]
)
def test_process_no_existing_release_directives_or_versions(
    mock_add_new_version,
    mock_existing_version_info,
    mock_rd,
    mock_sync,
    mock_apply_package_scripts,
    mock_execute,
    mock_create_app_pkg,
    mock_check_git,
    mock_find_version,
    policy_param,
    temp_dir,
    mock_cursor,
):
    version = "V1"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
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

    processor = _get_version_create_processor()
    processor.process(
        version=version,
        patch=None,
        policy=policy_param,
        git_policy=allow_always_policy,
        is_interactive=False,
    )  # last three parameters do not matter here
    assert mock_execute.mock_calls == expected
    mock_find_version.assert_not_called()
    mock_check_git.assert_called_once()
    mock_rd.assert_called_once()
    mock_create_app_pkg.assert_called_once()
    mock_apply_package_scripts.assert_called_once()
    mock_sync.assert_called_once()
    mock_existing_version_info.assert_called_once()
    mock_add_new_version.assert_called_once()


# Test version create when there are no release directives matching the version AND a version exists for app pkg
@mock.patch(
    "snowflake.cli.plugins.nativeapp.artifacts.find_version_info_in_manifest_file"
)
@mock.patch(f"{VERSION_MODULE}.check_index_changes_in_git_repo", return_value=None)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.create_app_package", return_value=None
)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}._apply_package_scripts", return_value=None
)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.sync_deploy_root_with_stage",
    return_value=None,
)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch(f"{VERSION_MODULE}.{CREATE_PROCESSOR}.get_existing_version_info")
@mock.patch(f"{VERSION_MODULE}.{CREATE_PROCESSOR}.add_new_version")
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.add_new_patch_to_version", return_value=None
)
@pytest.mark.parametrize(
    "policy_param", [allow_always_policy, ask_always_policy, deny_always_policy]
)
def test_process_no_existing_release_directives_w_existing_version(
    mock_add_patch,
    mock_add_new_version,
    mock_existing_version_info,
    mock_rd,
    mock_sync,
    mock_apply_package_scripts,
    mock_execute,
    mock_create_app_pkg,
    mock_check_git,
    mock_find_version,
    policy_param,
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
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
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

    processor = _get_version_create_processor()
    processor.process(
        version=version,
        patch=12,
        policy=policy_param,
        git_policy=allow_always_policy,
        is_interactive=False,
    )  # last three parameters do not matter here
    assert mock_execute.mock_calls == expected
    mock_find_version.assert_not_called()
    mock_check_git.assert_called_once()
    mock_rd.assert_called_once()
    mock_create_app_pkg.assert_called_once()
    mock_apply_package_scripts.assert_called_once()
    mock_sync.assert_called_once()
    assert mock_existing_version_info.call_count == 2
    mock_add_new_version.assert_not_called()
    mock_add_patch.assert_called_once()


# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is False AND interactive mode is False AND --interactive is False
# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is False AND interactive mode is False AND --interactive is True AND  user does not want to proceed
# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is False AND interactive mode is True AND user does not want to proceed
@mock.patch(f"{VERSION_MODULE}.check_index_changes_in_git_repo", return_value=None)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.create_app_package", return_value=None
)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}._apply_package_scripts", return_value=None
)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.sync_deploy_root_with_stage",
    return_value=None,
)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch(
    f"snowflake.cli.plugins.nativeapp.policy.{TYPER_CONFIRM}", return_value=False
)
@mock.patch(f"{VERSION_MODULE}.{CREATE_PROCESSOR}.get_existing_version_info")
@pytest.mark.parametrize(
    "policy_param, is_interactive_param, expected_code",
    [
        (deny_always_policy, False, 1),
        (ask_always_policy, True, 0),
        (ask_always_policy, True, 0),
    ],
)
def test_process_existing_release_directives_user_does_not_proceed(
    mock_existing_version_info,
    mock_typer_confirm,
    mock_rd,
    mock_sync,
    mock_apply_package_scripts,
    mock_execute,
    mock_create_app_pkg,
    mock_check_git,
    policy_param,
    is_interactive_param,
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
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
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

    processor = _get_version_create_processor()
    with pytest.raises(typer.Exit):
        result = processor.process(
            version=version,
            patch=12,
            policy=policy_param,
            git_policy=allow_always_policy,
            is_interactive=is_interactive_param,
        )
        assert result.exit_code == expected_code
    assert mock_execute.mock_calls == expected
    mock_check_git.assert_called_once()
    mock_rd.assert_called_once()
    mock_create_app_pkg.assert_called_once()
    mock_apply_package_scripts.assert_called_once()
    mock_sync.assert_called_once()


# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is True
# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is False AND interactive mode is False AND --interactive is True AND user wants to proceed
# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is False AND interactive mode is True AND user wants to proceed
@mock.patch(f"{VERSION_MODULE}.check_index_changes_in_git_repo", return_value=None)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.create_app_package", return_value=None
)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}._apply_package_scripts", return_value=None
)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.sync_deploy_root_with_stage",
    return_value=None,
)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.get_existing_version_info", return_value=None
)
@mock.patch(
    f"{VERSION_MODULE}.{CREATE_PROCESSOR}.add_new_patch_to_version", return_value=None
)
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
def test_process_existing_release_directives_w_existing_version_two(
    mock_typer_confirm,
    mock_add_patch,
    mock_existing_version_info,
    mock_rd,
    mock_sync,
    mock_apply_package_scripts,
    mock_execute,
    mock_create_app_pkg,
    mock_check_git,
    policy_param,
    is_interactive_param,
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
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
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

    processor = _get_version_create_processor()
    processor.process(
        version=version,
        patch=12,
        policy=policy_param,
        git_policy=allow_always_policy,
        is_interactive=is_interactive_param,
    )
    assert mock_execute.mock_calls == expected
    mock_check_git.assert_called_once()
    mock_rd.assert_called_once()
    mock_create_app_pkg.assert_called_once()
    mock_apply_package_scripts.assert_called_once()
    mock_sync.assert_called_once()
    assert mock_existing_version_info.call_count == 2
    mock_add_patch.assert_called_once()
