import unittest
from textwrap import dedent

import typer
from click import ClickException
from snowcli.cli.nativeapp.constants import SPECIAL_COMMENT
from snowcli.cli.nativeapp.exceptions import ApplicationPackageDoesNotExistError
from snowcli.cli.nativeapp.version.version_processor import (
    NativeAppVersionDropProcessor,
)
from snowcli.cli.project.definition_manager import DefinitionManager
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.utils import *
from tests.testing_utils.fixtures import *

DROP_PROCESSOR = "NativeAppVersionDropProcessor"


def _get_version_drop_processor():
    dm = DefinitionManager()
    return NativeAppVersionDropProcessor(
        project_definition=dm.project_definition["native_app"],
        project_root=dm.project_root,
    )


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
        processor.process("some_version")


@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.get_existing_app_pkg_info",
    return_value={"owner": "package_role"},
)
@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.build_bundle", return_value=None)
@mock.patch(
    "snowcli.cli.nativeapp.artifacts.find_version_info_in_manifest_file",
    return_value=(None, None),
)
def test_process_no_version_from_user_no_version_in_manifest(
    mock_version_info_in_manifest, mock_build_bundle, mock_get_existing, temp_dir
):

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_drop_processor()
    with pytest.raises(ClickException):
        processor.process(version=None)


@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.get_existing_app_pkg_info",
    return_value={"owner": "package_role"},
)
@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.build_bundle", return_value=None)
@mock.patch(
    "snowcli.cli.nativeapp.artifacts.find_version_info_in_manifest_file",
    return_value=("manifest_version", None),
)
@mock.patch(f"snowcli.cli.nativeapp.utils.ask_user_confirmation", return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_process_no_version_from_user_get_version_in_manifest(
    mock_execute,
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
        processor.process(version=None)


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

    processor = _get_version_drop_processor()
    result = processor.get_existing_release_directive_info_for_version(version)
    assert mock_execute.mock_calls == expected
    assert len(result) == 2


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

    processor = _get_version_drop_processor()
    processor.add_new_version(version)
    assert mock_execute.mock_calls == expected


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

    processor = _get_version_drop_processor()
    processor.add_new_patch_to_version(version)
    assert mock_execute.mock_calls == expected


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

    processor = _get_version_drop_processor()
    processor.add_new_patch_to_version(version, "12")
    assert mock_execute.mock_calls == expected


@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.build_bundle", return_value=None)
@mock.patch(
    "snowcli.cli.nativeapp.artifacts.find_version_info_in_manifest_file",
    return_value=(None, None),
)
def test_process_no_version(mock_find_version, mock_build_bundle, temp_dir):
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_version_drop_processor()
    with pytest.raises(ClickException):
        processor.process(None, None)


@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.build_bundle", return_value=None)
@mock.patch("snowcli.cli.nativeapp.artifacts.find_version_info_in_manifest_file")
@mock.patch(f"{VERSION_MODULE}.check_index_changes_in_git_repo", return_value=None)
@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.create_app_package", return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}._apply_package_scripts", return_value=None
)
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.sync_deploy_root_with_stage",
    return_value=None,
)
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.get_existing_version_info", return_value=None
)
@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.add_new_version", return_value=None)
def test_process_no_existing_release_directives(
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

    processor = _get_version_drop_processor()
    with pytest.raises(typer.Exit):
        result = processor.process(version, 12)
        assert result.exit_code == 0
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


@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.build_bundle", return_value=None)
@mock.patch("snowcli.cli.nativeapp.artifacts.find_version_info_in_manifest_file")
@mock.patch(f"{VERSION_MODULE}.check_index_changes_in_git_repo", return_value=None)
@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.create_app_package", return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}._apply_package_scripts", return_value=None
)
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.sync_deploy_root_with_stage",
    return_value=None,
)
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.get_existing_release_directive_info_for_version",
    return_value=None,
)
@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.get_existing_version_info")
@mock.patch(f"{VERSION_MODULE}.{DROP_PROCESSOR}.add_new_version")
@mock.patch(
    f"{VERSION_MODULE}.{DROP_PROCESSOR}.add_new_patch_to_version", return_value=None
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

    processor = _get_version_drop_processor()
    processor.process(version, 12)
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
