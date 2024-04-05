import os
from pathlib import Path
from textwrap import dedent
from typing import List, Set
from unittest import mock

import pytest
from click.exceptions import (
    ClickException,
    FileError,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.plugins.nativeapp.constants import (
    LOOSE_FILES_MAGIC_VERSION,
    NAME_COL,
    SPECIAL_COMMENT,
    SPECIAL_COMMENT_OLD,
)
from snowflake.cli.plugins.nativeapp.exceptions import (
    ApplicationPackageAlreadyExistsError,
    UnexpectedOwnerError,
)
from snowflake.cli.plugins.nativeapp.manager import (
    NativeAppManager,
    SnowflakeSQLExecutionError,
    _get_default_deploy_prune_value,
    _get_relative_paths_to_sync,
    ensure_correct_owner,
)
from snowflake.cli.plugins.object.stage.diff import (
    DiffResult,
    filter_from_diff,
)
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.patch_utils import (
    mock_connection,
    mock_get_app_pkg_distribution_in_sf,
)
from tests.nativeapp.utils import (
    NATIVEAPP_MANAGER_EXECUTE,
    NATIVEAPP_MANAGER_GET_EXISTING_APP_PKG_INFO,
    NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME,
    NATIVEAPP_MODULE,
    mock_execute_helper,
    mock_snowflake_yml_file,
)
from tests.testing_utils.files_and_dirs import create_named_file

mock_project_definition_override = {
    "native_app": {
        "application": {
            "name": "sample_application_name",
            "role": "sample_application_role",
        },
        "package": {
            "name": "sample_package_name",
            "role": "sample_package_role",
        },
    }
}


def _get_na_manager():
    dm = DefinitionManager()
    return NativeAppManager(
        project_definition=dm.project_definition.native_app,
        project_root=dm.project_root,
    )


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(f"{NATIVEAPP_MODULE}.stage_diff")
@mock.patch(f"{NATIVEAPP_MODULE}.sync_local_diff_with_stage")
def test_sync_deploy_root_with_stage(
    mock_local_diff_with_stage, mock_stage_diff, mock_execute, temp_dir, mock_cursor
):
    mock_execute.return_value = mock_cursor([{"CURRENT_ROLE()": "old_role"}], [])
    mock_diff_result = DiffResult(different=["setup.sql"])
    mock_stage_diff.return_value = mock_diff_result
    mock_local_diff_with_stage.return_value = None
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    assert mock_diff_result.has_changes()
    native_app_manager.sync_deploy_root_with_stage("new_role")

    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role new_role"),
        mock.call(f"create schema if not exists app_pkg.app_src"),
        mock.call(
            f"""
                    create stage if not exists app_pkg.app_src.stage
                    encryption = (TYPE = 'SNOWFLAKE_SSE')
                    DIRECTORY = (ENABLE = TRUE)"""
        ),
        mock.call("use role old_role"),
    ]
    assert mock_execute.mock_calls == expected
    mock_stage_diff.assert_called_once_with(
        native_app_manager.deploy_root, "app_pkg.app_src.stage"
    )
    mock_local_diff_with_stage.assert_called_once_with(
        role="new_role",
        deploy_root_path=native_app_manager.deploy_root,
        diff_result=mock_diff_result,
        stage_path="app_pkg.app_src.stage",
    )


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_app_pkg_distribution_in_snowflake(mock_execute, temp_dir, mock_cursor):

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
                        ("name", "app_pkg"),
                        ["owner", "package_role"],
                        ["distribution", "EXTERNAL"],
                    ],
                    [],
                ),
                mock.call("describe application package app_pkg"),
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

    native_app_manager = _get_na_manager()
    actual_distribution = native_app_manager.get_app_pkg_distribution_in_snowflake
    assert actual_distribution == "external"
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_app_pkg_distribution_in_snowflake_throws_programming_error(
    mock_execute, temp_dir, mock_cursor
):

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                ProgrammingError(
                    msg="Application package app_pkg does not exist or not authorized."
                ),
                mock.call("describe application package app_pkg"),
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

    native_app_manager = _get_na_manager()
    with pytest.raises(ProgrammingError):
        native_app_manager.get_app_pkg_distribution_in_snowflake

    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_app_pkg_distribution_in_snowflake_throws_execution_error(
    mock_execute, temp_dir, mock_cursor
):

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (mock_cursor([], []), mock.call("describe application package app_pkg")),
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

    native_app_manager = _get_na_manager()
    with pytest.raises(SnowflakeSQLExecutionError):
        native_app_manager.get_app_pkg_distribution_in_snowflake

    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_app_pkg_distribution_in_snowflake_throws_distribution_error(
    mock_execute, temp_dir, mock_cursor
):

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([("name", "app_pkg"), ["owner", "package_role"]], []),
                mock.call("describe application package app_pkg"),
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

    native_app_manager = _get_na_manager()
    with pytest.raises(ProgrammingError):
        native_app_manager.get_app_pkg_distribution_in_snowflake

    assert mock_execute.mock_calls == expected


@mock_get_app_pkg_distribution_in_sf()
def test_is_app_pkg_distribution_same_in_sf_w_arg(mock_mismatch, temp_dir):

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    create_named_file(
        file_name="snowflake.local.yml",
        dir_name=current_working_directory,
        contents=[
            dedent(
                """\
                    native_app:
                        package:
                            distribution: >-
                                EXTERNAL
                """
            )
        ],
    )

    native_app_manager = _get_na_manager()
    assert native_app_manager.verify_project_distribution("internal") is False
    mock_mismatch.assert_not_called()


@mock_get_app_pkg_distribution_in_sf()
def test_is_app_pkg_distribution_same_in_sf_no_mismatch(mock_mismatch, temp_dir):
    mock_mismatch.return_value = "external"

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    create_named_file(
        file_name="snowflake.local.yml",
        dir_name=current_working_directory,
        contents=[
            dedent(
                """\
                    native_app:
                        package:
                            distribution: >-
                                EXTERNAL
                """
            )
        ],
    )

    native_app_manager = _get_na_manager()
    assert native_app_manager.verify_project_distribution() is True


@mock_get_app_pkg_distribution_in_sf()
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
def test_is_app_pkg_distribution_same_in_sf_has_mismatch(
    mock_warning, mock_mismatch, temp_dir
):
    mock_mismatch.return_value = "external"

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    assert native_app_manager.verify_project_distribution() is False
    mock_warning.assert_called_once_with(
        "Application package app_pkg in your Snowflake account has distribution property external,\nwhich does not match the value specified in project definition file: internal.\n"
    )


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_existing_app_info_app_exists(mock_execute, temp_dir, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor(
                    [
                        {
                            "name": "MYAPP",
                            "comment": SPECIAL_COMMENT,
                            "version": LOOSE_FILES_MAGIC_VERSION,
                            "owner": "app_role",
                        }
                    ],
                    [],
                ),
                mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
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

    native_app_manager = _get_na_manager()
    show_obj_row = native_app_manager.get_existing_app_info()
    assert show_obj_row is not None
    assert show_obj_row[NAME_COL] == "MYAPP"
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_existing_app_info_app_does_not_exist(mock_execute, temp_dir, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([], []),
                mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
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

    native_app_manager = _get_na_manager()
    show_obj_row = native_app_manager.get_existing_app_info()
    assert show_obj_row is None
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_existing_app_pkg_info_app_pkg_exists(mock_execute, temp_dir, mock_cursor):
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
                            "name": "APP_PKG",
                            "comment": SPECIAL_COMMENT,
                            "version": LOOSE_FILES_MAGIC_VERSION,
                            "owner": "package_role",
                        }
                    ],
                    [],
                ),
                mock.call(
                    r"show application packages like 'APP\\_PKG'",
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
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    show_obj_row = native_app_manager.get_existing_app_pkg_info()
    assert show_obj_row is not None
    assert show_obj_row[NAME_COL] == "APP_PKG"
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_existing_app_pkg_info_app_pkg_does_not_exist(
    mock_execute, temp_dir, mock_cursor
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call(
                    r"show application packages like 'APP\\_PKG'",
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
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    show_obj_row = native_app_manager.get_existing_app_pkg_info()
    assert show_obj_row is None
    assert mock_execute.mock_calls == expected


@mock.patch("snowflake.cli.plugins.connection.util.get_context")
@mock.patch("snowflake.cli.plugins.connection.util.get_account")
@mock.patch("snowflake.cli.plugins.connection.util.get_snowsight_host")
@mock_connection()
def test_get_snowsight_url(
    mock_conn, mock_snowsight_host, mock_account, mock_context, temp_dir
):
    mock_conn.return_value = None
    mock_snowsight_host.return_value = "https://host"
    mock_context.return_value = "organization"
    mock_account.return_value = "account"

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    assert (
        native_app_manager.get_snowsight_url()
        == "https://host/organization/account/#/apps/application/MYAPP"
    )


def test_ensure_correct_owner():
    test_row = {"name": "some_name", "owner": "some_role", "comment": "some_comment"}
    assert (
        ensure_correct_owner(row=test_row, role="some_role", obj_name="some_name")
        is None
    )


def test_is_correct_owner_bad_owner():
    test_row = {"name": "some_name", "owner": "wrong_role", "comment": "some_comment"}
    with pytest.raises(UnexpectedOwnerError):
        ensure_correct_owner(row=test_row, role="right_role", obj_name="some_name")


# Test create_app_package() with no existing package available
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(NATIVEAPP_MANAGER_GET_EXISTING_APP_PKG_INFO, return_value=None)
def test_create_app_pkg_no_existing_package(
    mock_get_existing_app_pkg_info, mock_execute, temp_dir, mock_cursor
):
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
                        create application package app_pkg
                            comment = {SPECIAL_COMMENT}
                            distribution = internal
                    """
                    )
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

    native_app_manager = _get_na_manager()
    native_app_manager.create_app_package()
    assert mock_execute.mock_calls == expected
    mock_get_existing_app_pkg_info.assert_called_once()


# Test create_app_package() with incorrect owner
@mock.patch(NATIVEAPP_MANAGER_GET_EXISTING_APP_PKG_INFO)
def test_create_app_pkg_incorrect_owner(mock_get_existing_app_pkg_info, temp_dir):
    mock_get_existing_app_pkg_info.return_value = {
        "name": "APP_PKG",
        "comment": SPECIAL_COMMENT,
        "version": LOOSE_FILES_MAGIC_VERSION,
        "owner": "wrong_owner",
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    with pytest.raises(UnexpectedOwnerError):
        native_app_manager = _get_na_manager()
        native_app_manager.create_app_package()


# Test create_app_package() with distribution external AND variable mismatch
@mock.patch(NATIVEAPP_MANAGER_GET_EXISTING_APP_PKG_INFO)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME)
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
@pytest.mark.parametrize(
    "is_pkg_distribution_same",
    [False, True],
)
def test_create_app_pkg_external_distribution(
    mock_warning,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_get_existing_app_pkg_info,
    is_pkg_distribution_same,
    temp_dir,
):
    mock_is_distribution_same.return_value = is_pkg_distribution_same
    mock_get_distribution.return_value = "external"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "APP_PKG",
        "comment": "random",
        "version": LOOSE_FILES_MAGIC_VERSION,
        "owner": "PACKAGE_ROLE",
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    native_app_manager.create_app_package()
    if not is_pkg_distribution_same:
        mock_warning.assert_called_once_with(
            "Continuing to execute `snow app run` on application package app_pkg with distribution 'external'."
        )


# Test create_app_package() with distribution internal AND variable mismatch AND special comment is True
@mock.patch(NATIVEAPP_MANAGER_GET_EXISTING_APP_PKG_INFO)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME)
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
@pytest.mark.parametrize(
    "is_pkg_distribution_same, special_comment",
    [
        (False, SPECIAL_COMMENT),
        (False, SPECIAL_COMMENT_OLD),
        (True, SPECIAL_COMMENT),
        (True, SPECIAL_COMMENT_OLD),
    ],
)
def test_create_app_pkg_internal_distribution_special_comment(
    mock_warning,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_get_existing_app_pkg_info,
    is_pkg_distribution_same,
    special_comment,
    temp_dir,
):
    mock_is_distribution_same.return_value = is_pkg_distribution_same
    mock_get_distribution.return_value = "internal"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "APP_PKG",
        "comment": special_comment,
        "version": LOOSE_FILES_MAGIC_VERSION,
        "owner": "PACKAGE_ROLE",
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    native_app_manager.create_app_package()
    if not is_pkg_distribution_same:
        mock_warning.assert_called_once_with(
            "Continuing to execute `snow app run` on application package app_pkg with distribution 'internal'."
        )


# Test create_app_package() with distribution internal AND variable mismatch AND special comment is False
@mock.patch(NATIVEAPP_MANAGER_GET_EXISTING_APP_PKG_INFO)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME)
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
@pytest.mark.parametrize(
    "is_pkg_distribution_same",
    [False, True],
)
def test_create_app_pkg_internal_distribution_no_special_comment(
    mock_warning,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_get_existing_app_pkg_info,
    is_pkg_distribution_same,
    temp_dir,
):
    mock_is_distribution_same.return_value = is_pkg_distribution_same
    mock_get_distribution.return_value = "internal"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "APP_PKG",
        "comment": "dummy",
        "version": LOOSE_FILES_MAGIC_VERSION,
        "owner": "PACKAGE_ROLE",
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    with pytest.raises(ApplicationPackageAlreadyExistsError):
        native_app_manager.create_app_package()

    if not is_pkg_distribution_same:
        mock_warning.assert_called_once_with(
            "Continuing to execute `snow app run` on application package app_pkg with distribution 'internal'."
        )


def exists_mock(path: Path):
    if str(path) in ["/file", "/dir", "/dir/nested_file"]:
        return True
    else:
        return False


def is_dir_mock(path: Path):
    if str(path) == "/dir":
        return True
    else:
        return False


# Mocking Path to mimic the following directory structure:
# /file
# /dir/nested_file
@mock.patch(f"{NATIVEAPP_MODULE}.Path.is_dir", autospec=True)
@mock.patch(f"{NATIVEAPP_MODULE}.Path.exists", autospec=True)
@pytest.mark.parametrize(
    "files_to_sync,remote_paths,expected_exception",
    [
        [["file", "dir/nested_file"], set(), None],
        [["file", "file2", "dir/file3"], set(["file2", "dir/file3"]), None],
        [["file", "file2"], set(), FileError],
        [["dir/file3"], set(), FileError],
        [["dir"], set(), ClickException],
    ],
)
def test_get_full_file_paths_to_sync(
    path_mock_exists,
    path_mock_is_dir,
    files_to_sync: List[Path],
    remote_paths: Set[str],
    expected_exception: Exception,
):
    path_mock_exists.side_effect = exists_mock
    path_mock_is_dir.side_effect = is_dir_mock
    if expected_exception is None:
        result = _get_relative_paths_to_sync(files_to_sync, "/", remote_paths)
        assert len(result) == len(files_to_sync)
    else:
        with pytest.raises(expected_exception):
            _get_relative_paths_to_sync(files_to_sync, "/", remote_paths)


def test_filter_from_diff():
    diff = DiffResult()
    diff.different = [
        "different",
        "different-2",
        "dir/different",
        "dir/different-2",
    ]
    diff.only_local = [
        "only_local",
        "only_local-2",
        "dir/only_local",
        "dir/only_local-2",
    ]
    diff.only_on_stage = [
        "only_on_stage",
        "only_on_stage-2",
        "dir/only_on_stage",
        "dir/only_on_stage-2",
    ]

    paths_to_keep = set(
        [
            "different",
            "only-local",
            "only-stage",
            "dir/different",
            "dir/only-local",
            "dir/only-stage",
        ]
    )
    diff = filter_from_diff(diff, paths_to_keep, True)

    for path in diff.different:
        assert path in paths_to_keep
    for path in diff.only_local:
        assert path in paths_to_keep
    for path in diff.only_on_stage:
        assert path in paths_to_keep


# When prune flag is off, remote-only files are filtered out and a warning is printed
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
def test_filter_from_diff_no_prune(mock_warning):
    diff = DiffResult()
    diff.only_on_stage = [
        "only-stage.txt",
        "only-stage-2.txt",
    ]

    paths_to_keep = set(["only-stage.txt"])
    diff = filter_from_diff(diff, paths_to_keep, False)

    assert len(diff.only_on_stage) == 0
    mock_warning.assert_called_once_with(
        "The following files exist only on the stage:\n['only-stage.txt']\nUse the --prune flag to delete them from the stage."
    )


@pytest.mark.parametrize(
    "files_argument,expected_prune_value",
    [
        [None, True],
        [[], True],
        [["file1", "file2"], False],
    ],
)
def test_get_default_deploy_prune_value(files_argument, expected_prune_value):
    prune = _get_default_deploy_prune_value(files_argument)
    assert prune == expected_prune_value
