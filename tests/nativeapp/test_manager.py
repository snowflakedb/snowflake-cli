import unittest
from textwrap import dedent

from snowcli.cli.nativeapp.manager import (
    LOOSE_FILES_MAGIC_VERSION,
    SPECIAL_COMMENT,
    NativeAppManager,
    SnowflakeSQLExecutionError,
)
from snowcli.cli.object.stage.diff import DiffResult
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.utils import (
    NATIVEAPP_MANAGER_EXECUTE,
    NATIVEAPP_MODULE,
    mock_connection,
    mock_execute_helper,
    mock_get_app_pkg_distribution_in_sf,
    mock_snowflake_yml_file,
)
from tests.testing_utils.fixtures import *

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
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    assert mock_diff_result.has_changes()
    native_app_manager.sync_deploy_root_with_stage("new_role")

    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role new_role"),
        mock.call(f"create schema if not exists app_pkg.app_src"),
        mock.call(
            f"""
                    create stage if not exists app_pkg.app_src.stage
                    encryption = (TYPE = 'SNOWFLAKE_SSE')"""
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
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
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
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
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
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
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
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    with pytest.raises(ProgrammingError):
        native_app_manager.get_app_pkg_distribution_in_snowflake

    assert mock_execute.mock_calls == expected


@mock_get_app_pkg_distribution_in_sf
def test_is_app_pkg_distribution_same_in_sf_no_mismatch(mock_mismatch, temp_dir):
    mock_mismatch.return_value = "external"

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    create_named_file(
        file_name="snowflake.local.yml",
        dir=current_working_directory,
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

    native_app_manager = NativeAppManager()
    assert native_app_manager.is_app_pkg_distribution_same_in_sf() is True


@mock_get_app_pkg_distribution_in_sf
@mock.patch(f"{NATIVEAPP_MODULE}.log.warning")
def test_is_app_pkg_distribution_same_in_sf_has_mismatch(
    mock_warning, mock_mismatch, temp_dir
):
    mock_mismatch.return_value = "external"

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    assert native_app_manager.is_app_pkg_distribution_same_in_sf() is False
    mock_warning.assert_called_once_with(
        "App pkg app_pkg in your Snowflake account has distribution property external,\nwhich does not match the value specified in project definition file: internal.\n"
    )


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_app_exists(mock_execute, temp_dir, mock_cursor):
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
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    assert native_app_manager.app_exists() is True
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_app_does_not_exist(mock_execute, temp_dir, mock_cursor):
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
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    assert native_app_manager.app_exists() is False
    assert mock_execute.mock_calls == expected


@mock.patch("snowcli.cli.connection.util.get_context")
@mock.patch("snowcli.cli.connection.util.get_account")
@mock.patch("snowcli.cli.connection.util.get_snowsight_host")
@mock_connection
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
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    assert (
        native_app_manager.get_snowsight_url()
        == "https://host/organization/account/#/apps/application/MYAPP"
    )
