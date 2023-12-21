import unittest
from textwrap import dedent

import typer
from click import ClickException
from snowcli.cli.nativeapp.constants import SPECIAL_COMMENT
from snowcli.cli.nativeapp.policy import AllowAlwaysPolicy, AskAlwaysPolicy
from snowcli.cli.nativeapp.version.version_processor import (
    NativeAppVersionCreateProcessor,
)
from snowcli.cli.project.definition_manager import DefinitionManager
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.utils import *
from tests.testing_utils.fixtures import *

CREATE_PROCESSOR = "NativeAppVersionCreateProcessor"

allow_always_policy = AllowAlwaysPolicy()
ask_always_policy = AskAlwaysPolicy()


def _get_version_create_processor():
    dm = DefinitionManager()
    return NativeAppVersionCreateProcessor(
        project_definition=dm.project_definition["native_app"],
        project_root=dm.project_root,
    )


# Test get_existing_version_info returns version info correctly
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_existing_version_info(mock_execute, temp_dir, mock_cursor):
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
                        {
                            "name": "My Package",
                            "comment": "some comment",
                            "owner": "PACKAGE_ROLE",
                            "version": version,
                        }
                    ],
                    [],
                ),
                mock.call(
                    f"show versions like 'V1' in application package app_pkg",
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
    result = processor.get_existing_version_info(version)
    assert mock_execute.mock_calls == expected
    assert result["version"] == version


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
@mock.patch(f"{VERSION_MODULE}.{CREATE_PROCESSOR}.build_bundle", return_value=None)
@mock.patch(FIND_VERSION_FROM_MANIFEST, return_value=(None, None))
@mock.patch(f"{VERSION_MODULE}.log.info")
def test_process_no_version_from_user_no_version_in_manifest(
    mock_log, mock_version_info_in_manifest, mock_build_bundle, temp_dir
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
            None, None, policy=allow_always_policy
        )  # policy does not matter here
    mock_log.assert_called_once()
    mock_build_bundle.assert_called_once()
    mock_version_info_in_manifest.assert_called_once()


# Test version create when there are no release directives matching the version AND no version exists for app pkg
@mock.patch(f"{VERSION_MODULE}.{CREATE_PROCESSOR}.build_bundle", return_value=None)
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
    mock_build_bundle,
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
        version, 12, policy=allow_always_policy
    )  # policy does not matter here
    assert mock_execute.mock_calls == expected
    mock_build_bundle.assert_called_once()
    mock_find_version.assert_not_called()
    mock_check_git.assert_called_once()
    mock_rd.assert_called_once()
    mock_create_app_pkg.assert_called_once()
    mock_apply_package_scripts.assert_called_once()
    mock_sync.assert_called_once()
    mock_existing_version_info.assert_called_once()
    mock_add_new_version.assert_called_once()


# Test version create when there are no release directives matching the version AND a version exists for app pkg
@mock.patch(f"{VERSION_MODULE}.{CREATE_PROCESSOR}.build_bundle", return_value=None)
@mock.patch("snowcli.cli.nativeapp.artifacts.find_version_info_in_manifest_file")
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
    mock_build_bundle,
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
        version, 12, policy=allow_always_policy
    )  # policy does not matter here
    assert mock_execute.mock_calls == expected
    mock_build_bundle.assert_called_once()
    mock_find_version.assert_not_called()
    mock_check_git.assert_called_once()
    mock_rd.assert_called_once()
    mock_create_app_pkg.assert_called_once()
    mock_apply_package_scripts.assert_called_once()
    mock_sync.assert_called_once()
    mock_existing_version_info.assert_called_once()
    mock_add_new_version.assert_not_called()
    mock_add_patch.assert_called_once()


# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is False AND interactive mode is False
# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is False AND interactive mode is True AND user does not want to proceed
@mock.patch(f"{VERSION_MODULE}.{CREATE_PROCESSOR}.build_bundle", return_value=None)
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
@mock.patch(f"{VERSION_MODULE}.is_user_in_interactive_mode")
@mock.patch(f"snowcli.cli.nativeapp.policy.{TYPER_CONFIRM}", return_value=False)
@pytest.mark.parametrize("is_interactive", [False, True])
def test_process_existing_release_directives_user_does_not_proceed(
    mock_typer_confirm,
    mock_is_interactive,
    mock_rd,
    mock_sync,
    mock_apply_package_scripts,
    mock_execute,
    mock_create_app_pkg,
    mock_check_git,
    mock_build_bundle,
    is_interactive,
    temp_dir,
    mock_cursor,
):
    version = "V1"
    mock_is_interactive.return_value = is_interactive
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
        result = processor.process(version, 12, policy=ask_always_policy)
        assert result.exit_code == (not is_interactive)
    assert mock_execute.mock_calls == expected
    mock_build_bundle.assert_called_once()
    mock_check_git.assert_called_once()
    mock_rd.assert_called_once()
    mock_create_app_pkg.assert_called_once()
    mock_apply_package_scripts.assert_called_once()
    mock_sync.assert_called_once()


# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is True
# Test version create when there are release directives matching the version AND no version exists for app pkg AND --force is False AND interactive mode is True AND user wants to proceed
@mock.patch(f"{VERSION_MODULE}.{CREATE_PROCESSOR}.build_bundle", return_value=None)
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
@mock.patch(f"{VERSION_MODULE}.is_user_in_interactive_mode", return_value=True)
@mock.patch(f"snowcli.cli.nativeapp.policy.{TYPER_CONFIRM}", return_value=True)
@pytest.mark.parametrize("var_policy", [allow_always_policy, ask_always_policy])
def test_process_existing_release_directives_w_existing_version_two(
    mock_typer_confirm,
    mock_is_interactive,
    mock_add_patch,
    mock_existing_version_info,
    mock_rd,
    mock_sync,
    mock_apply_package_scripts,
    mock_execute,
    mock_create_app_pkg,
    mock_check_git,
    mock_build_bundle,
    var_policy,
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
    result = processor.process(
        version, 12, policy=var_policy
    )  # policy does not matter here
    assert mock_execute.mock_calls == expected
    mock_build_bundle.assert_called_once()
    mock_check_git.assert_called_once()
    mock_rd.assert_called_once()
    mock_create_app_pkg.assert_called_once()
    mock_apply_package_scripts.assert_called_once()
    mock_sync.assert_called_once()
    mock_existing_version_info.assert_called_once()
    mock_add_patch.assert_called_once()
