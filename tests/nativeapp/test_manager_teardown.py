import unittest

from snowcli.cli.nativeapp.manager import (
    SPECIAL_COMMENT,
    NativeAppManager,
    SnowflakeSQLExecutionError,
)
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.utils import (
    NATIVEAPP_MANAGER_EXECUTE,
    NATIVEAPP_MANAGER_TYPER_CONFIRM,
    NATIVEAPP_MODULE,
    mock_execute_helper,
    mock_get_app_pkg_distribution_in_sf,
    mock_snowflake_yml_file,
    quoted_override_yml_file,
)
from tests.testing_utils.fixtures import *


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_get_app_pkg_distribution_in_sf
@mock.patch(f"{NATIVEAPP_MODULE}.NativeAppManager.does_app_pkg_exist")
def test_idempotent_app_teardown(
    mock_exist, mock_mismatch, mock_execute, temp_dir, mock_cursor
):
    mock_exist.return_value = True
    mock_mismatch.return_value = "internal"
    side_effects, expected = mock_execute_helper(
        [
            # teardown app 1: app exists + is dropped
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
                            "version": "UNVERSIONED",
                            "owner": "APP_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
            ),
            (None, mock.call("drop application myapp")),
            (None, mock.call("use role old_role")),
            # teardown package 1: package exists + is dropped
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
                            "owner": "PACKAGE_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call(
                    "show application packages like 'APP_PKG'",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("drop application package app_pkg")),
            (None, mock.call("use role old_role")),
            # teardown app 2: app doesn't exist
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
            # teardown package 2: package doesn't exist
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call(
                    "show application packages like 'APP_PKG'",
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

    native_app_manager = NativeAppManager()
    native_app_manager.teardown()
    native_app_manager.teardown()
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_get_app_pkg_distribution_in_sf
@mock.patch(f"{NATIVEAPP_MODULE}.NativeAppManager.does_app_pkg_exist")
def test_teardown_without_app_instance(
    mock_exist, mock_mismatch, mock_execute, temp_dir, mock_cursor
):
    mock_exist.return_value = True
    mock_mismatch.return_value = "internal"
    side_effects, expected = mock_execute_helper(
        [
            # teardown app: app does not exist + is skipped
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
            # teardown package: package exists + is dropped
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
                            "owner": "PACKAGE_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call(
                    "show application packages like 'APP_PKG'",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("drop application package app_pkg")),
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

    native_app_manager = NativeAppManager()
    native_app_manager.teardown()
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_teardown_drop_app_fails(mock_execute, temp_dir, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            # teardown app: drop cannot be performed for any other reason
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
                            "version": "UNVERSIONED",
                            "owner": "APP_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
            ),
            (
                ProgrammingError(
                    msg="Object does not exist, or operation cannot be performed.",
                    errno=2043,
                ),
                mock.call("drop application myapp"),
            ),
            (None, mock.call("use role old_role")),
            # teardown package: never happens
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    with pytest.raises(SnowflakeSQLExecutionError):
        native_app_manager.teardown()

    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_get_app_pkg_distribution_in_sf
@mock.patch(f"{NATIVEAPP_MODULE}.NativeAppManager.does_app_pkg_exist")
def test_quoting_app_teardown(
    mock_exist, mock_mismatch, mock_execute, temp_dir, mock_cursor
):
    mock_exist.return_value = True
    mock_mismatch.return_value = "internal"

    side_effects, expected = mock_execute_helper(
        [
            # teardown app
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor(
                    [
                        {
                            "name": "My Application",
                            "comment": SPECIAL_COMMENT,
                            "version": "UNVERSIONED",
                            "owner": "APP_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call(
                    "show applications like 'My Application'", cursor_class=DictCursor
                ),
            ),
            (None, mock.call('drop application "My Application"')),
            (None, mock.call("use role old_role")),
            # teardown package
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
                            "comment": SPECIAL_COMMENT,
                            "owner": "PACKAGE_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call(
                    "show application packages like 'My Package'",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call('drop application package "My Package"')),
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
    create_named_file(
        file_name="snowflake.local.yml",
        dir=current_working_directory,
        contents=[quoted_override_yml_file],
    )

    native_app_manager = NativeAppManager()
    native_app_manager.teardown()
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(NATIVEAPP_MANAGER_TYPER_CONFIRM)
@mock.patch(f"{NATIVEAPP_MODULE}.log.info")
def test_drop_object_app_has_wrong_comment_no_drop(
    mock_info, mock_confirm, mock_execute, temp_dir, mock_cursor
):
    mock_confirm.return_value = False

    side_effects, expected = mock_execute_helper(
        [
            # teardown app: app has wrong comment
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role APP_ROLE")),
            (
                mock_cursor(
                    [
                        {
                            "name": "MYAPP",
                            "comment": "some other random comment",
                            "version": "UNVERSIONED",
                            "owner": "APP_ROLE",
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
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    assert (
        native_app_manager.drop_object(
            object_name="MYAPP",
            object_role="APP_ROLE",
            object_type="application",
            query_dict={"show": "show applications like", "drop": "drop application"},
        )
        is False
    )

    assert mock_execute.mock_calls == expected

    mock_info.assert_called_once_with("Did not drop application MYAPP.\n")


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(NATIVEAPP_MANAGER_TYPER_CONFIRM)
def test_drop_object_app_has_wrong_comment_yes_drop(
    mock_confirm, mock_execute, temp_dir, mock_cursor
):
    mock_confirm.return_value = True

    side_effects, expected = mock_execute_helper(
        [
            # teardown app: app has wrong comment
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role APP_ROLE")),
            (
                mock_cursor(
                    [
                        {
                            "name": "MYAPP",
                            "comment": "some other random comment",
                            "version": "UNVERSIONED",
                            "owner": "APP_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
            ),
            (None, mock.call("drop application MYAPP")),
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

    native_app_manager = NativeAppManager()
    assert (
        native_app_manager.drop_object(
            object_name="MYAPP",
            object_role="APP_ROLE",
            object_type="application",
            query_dict={"show": "show applications like", "drop": "drop application"},
        )
        is True
    )

    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(NATIVEAPP_MANAGER_TYPER_CONFIRM)
def test_drop_pkg_skip_comment_check(mock_confirm, mock_execute, temp_dir, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            # teardown package
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role PACKAGE_ROLE")),
            (
                mock_cursor(
                    [
                        {
                            "name": "APP_PKG",
                            "comment": "random",
                            "owner": "PACKAGE_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call(
                    "show application packages like 'APP_PKG'",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("drop application package APP_PKG")),
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

    native_app_manager = NativeAppManager()
    assert (
        native_app_manager.drop_object(
            object_name="APP_PKG",
            object_role="PACKAGE_ROLE",
            object_type="package",
            query_dict={
                "show": "show application packages like",
                "drop": "drop application package",
            },
            is_external_distribution=True,
        )
        is True
    )

    mock_confirm.assert_not_called()
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_drop_object_no_show_object(mock_execute, temp_dir, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (mock_cursor(["row"], []), mock.call("use role sample_package_role")),
            (
                mock_cursor([], []),
                mock.call(
                    "show application packages like 'SAMPLE_PACKAGE_NAME'",
                    cursor_class=DictCursor,
                ),
            ),
            (mock_cursor(["row"], []), mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )
    native_app_manager = NativeAppManager()

    dropped = native_app_manager.drop_object(
        object_name="sample_package_name",
        object_role="sample_package_role",
        object_type="package",
        query_dict={"show": "show application packages like"},
    )
    assert not dropped
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_drop_object(mock_execute, temp_dir, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (mock_cursor(["row"], []), mock.call("use role sample_package_role")),
            (
                mock_cursor(
                    [
                        {
                            "name": "SAMPLE_PACKAGE_NAME",
                            "owner": "SAMPLE_PACKAGE_ROLE",
                            "blank": "blank",
                            "comment": "GENERATED_BY_SNOWCLI",
                        }
                    ],
                    [],
                ),
                mock.call(
                    "show application packages like 'SAMPLE_PACKAGE_NAME'",
                    cursor_class=DictCursor,
                ),
            ),
            (
                mock_cursor(["row"], []),
                mock.call("drop application package sample_package_name"),
            ),
            (mock_cursor(["row"], []), mock.call("use role old_role")),
        ]
    )

    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    native_app_manager.drop_object(
        object_name="sample_package_name",
        object_role="sample_package_role",
        object_type="package",
        query_dict={
            "show": "show application packages like",
            "drop": "drop application package",
        },
    )
    assert mock_execute.mock_calls == expected


@mock_get_app_pkg_distribution_in_sf
@mock.patch(NATIVEAPP_MANAGER_TYPER_CONFIRM)
@mock.patch(f"{NATIVEAPP_MODULE}.NativeAppManager.drop_object")
@mock.patch(f"{NATIVEAPP_MODULE}.log.warning")
@mock.patch(f"{NATIVEAPP_MODULE}.NativeAppManager.does_app_pkg_exist")
def test_teardown_external_package_confirmation(
    mock_exist,
    mock_warning,
    mock_drop,
    mock_confirm,
    mock_mismatch,
    temp_dir,
    mock_cursor,
):
    mock_exist.return_value = True
    mock_drop.return_value = True
    mock_confirm.return_value = True
    mock_mismatch.return_value = "external"

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    native_app_manager.teardown()
    mock_confirm.assert_called_once()
    mock_warning.assert_any_call(
        "App pkg app_pkg in your Snowflake account has distribution property external,\nwhich does not match the value specified in project definition file: internal.\n"
    )
    mock_warning.assert_any_call(
        "Continuing to execute `snow app teardown` on app pkg app_pkg with distribution external.\n"
    )
    assert mock_warning.call_count == 2


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(NATIVEAPP_MANAGER_TYPER_CONFIRM)
@mock_get_app_pkg_distribution_in_sf
@mock.patch(f"{NATIVEAPP_MODULE}.log.warning")
@mock.patch(f"{NATIVEAPP_MODULE}.NativeAppManager.does_app_pkg_exist")
def test_teardown_full_flow_wrong_comment_for_all_internal_dist_package(
    mock_exist,
    mock_warning,
    mock_mismatch,
    mock_confirm,
    mock_execute,
    temp_dir,
    mock_cursor,
):
    mock_exist.return_value = True
    side_effects, expected = mock_execute_helper(
        [
            # teardown app: app exists with wrong comment, yes for confirmation
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
                            "comment": "random",
                            "version": "UNVERSIONED",
                            "owner": "APP_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
            ),
            (None, mock.call("drop application myapp")),
            (None, mock.call("use role old_role")),
            # teardown package
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
                            "comment": "random",
                            "owner": "PACKAGE_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call(
                    "show application packages like 'APP_PKG'",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("drop application package app_pkg")),
            (None, mock.call("use role old_role")),
        ]
    )

    mock_mismatch.return_value = "external"
    mock_confirm.side_effect = [True, True]
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    native_app_manager.teardown()
    assert mock_execute.mock_calls == expected
    assert mock_warning.call_count == 2
