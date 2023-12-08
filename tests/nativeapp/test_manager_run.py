import unittest
from textwrap import dedent

from snowcli.cli.nativeapp.manager import (
    LOOSE_FILES_MAGIC_VERSION,
    SPECIAL_COMMENT,
    ApplicationAlreadyExistsError,
    ApplicationPackageAlreadyExistsError,
    NativeAppManager,
    UnexpectedOwnerError,
)
from snowcli.cli.object.stage.diff import DiffResult
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.utils import (
    NATIVEAPP_MANAGER_APP_PKG_DISTRIBUTION_IN_SF,
    NATIVEAPP_MANAGER_EXECUTE,
    NATIVEAPP_MANAGER_EXECUTE_QUERIES,
    NATIVEAPP_MODULE,
    mock_connection,
    mock_execute_helper,
    mock_get_app_pkg_distribution_in_sf,
    mock_snowflake_yml_file,
    quoted_override_yml_file,
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
@mock_connection
def test_create_dev_app_w_warehouse_access_exception(
    mock_conn, mock_execute, temp_dir, mock_cursor
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                ProgrammingError(
                    msg="Object does not exist, or operation cannot be performed.",
                    errno=2043,
                ),
                mock.call("use warehouse app_warehouse"),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    assert not mock_diff_result.has_changes()

    with pytest.raises(ProgrammingError) as err:
        native_app_manager._create_dev_app(mock_diff_result)

    assert mock_execute.mock_calls == expected
    assert "Please grant usage privilege on warehouse to this role." in err.value.msg


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_noop(mock_conn, mock_execute, temp_dir, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
            (
                mock_cursor(
                    [
                        {
                            "name": "MYAPP",
                            "comment": SPECIAL_COMMENT,
                            "version": LOOSE_FILES_MAGIC_VERSION,
                            "owner": "APP_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
            ),
            (None, mock.call("alter application myapp set debug_mode = True")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    assert not mock_diff_result.has_changes()
    native_app_manager._create_dev_app(mock_diff_result)
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_recreate(mock_conn, mock_execute, temp_dir, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
            (
                mock_cursor(
                    [
                        {
                            "name": "MYAPP",
                            "comment": SPECIAL_COMMENT,
                            "version": LOOSE_FILES_MAGIC_VERSION,
                            "owner": "APP_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
            ),
            (
                None,
                mock.call(
                    "alter application myapp upgrade using @app_pkg.app_src.stage"
                ),
            ),
            (None, mock.call("alter application myapp set debug_mode = True")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult(different=["setup.sql"])
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    assert mock_diff_result.has_changes()
    native_app_manager._create_dev_app(mock_diff_result)
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_recreate_w_missing_warehouse_exception(
    mock_conn, mock_execute, temp_dir, mock_cursor
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
            (
                mock_cursor(
                    [
                        {
                            "name": "MYAPP",
                            "comment": SPECIAL_COMMENT,
                            "version": LOOSE_FILES_MAGIC_VERSION,
                            "owner": "APP_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
            ),
            (
                ProgrammingError(
                    msg="No active warehouse selected in the current session", errno=606
                ),
                mock.call(
                    "alter application myapp upgrade using @app_pkg.app_src.stage"
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult(different=["setup.sql"])
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    assert mock_diff_result.has_changes()

    with pytest.raises(ProgrammingError) as err:
        native_app_manager._create_dev_app(mock_diff_result)

    assert mock_execute.mock_calls == expected
    assert "Please provide a warehouse for the active session role" in err.value.msg


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_create_new(mock_conn, mock_execute, temp_dir, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
            (
                mock_cursor([], []),
                mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
            ),
            (
                None,
                mock.call(
                    f"""
                    create application myapp
                        from application package app_pkg
                        using @app_pkg.app_src.stage
                        debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file.replace("package_role", "app_role")],
    )

    native_app_manager = NativeAppManager()
    assert not mock_diff_result.has_changes()
    native_app_manager._create_dev_app(mock_diff_result)
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_create_new_w_missing_warehouse_exception(
    mock_conn, mock_execute, temp_dir, mock_cursor
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
            (
                mock_cursor([], []),
                mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
            ),
            (
                ProgrammingError(
                    msg="No active warehouse selected in the current session", errno=606
                ),
                mock.call(
                    f"""
                    create application myapp
                        from application package app_pkg
                        using @app_pkg.app_src.stage
                        debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )

    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file.replace("package_role", "app_role")],
    )

    native_app_manager = NativeAppManager()
    assert not mock_diff_result.has_changes()

    with pytest.raises(ProgrammingError) as err:
        native_app_manager._create_dev_app(mock_diff_result)

    assert "Please provide a warehouse for the active session role" in err.value.msg
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_create_new_quoted(
    mock_conn, mock_execute, temp_dir, mock_cursor
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
            (
                mock_cursor([], []),
                mock.call(
                    "show applications like 'My Application'", cursor_class=DictCursor
                ),
            ),
            (
                None,
                mock.call(
                    f"""
                    create application "My Application"
                        from application package "My Package"
                        using '@"My Package".app_src.stage'
                        debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[
            dedent(
                """\
            definition_version: 1
            native_app:
                name: '"My Native Application"'

                source_stage:
                    app_src.stage

                artifacts:
                - setup.sql
                - app/README.md
                - src: app/streamlit/*.py
                dest: ui/

                application:
                    name: >-
                        "My Application"
                    role: app_role
                    warehouse: app_warehouse
                    debug: true

                package:
                    name: >-
                        "My Package"
                    role: app_role
                    scripts:
                    - shared_content.sql
        """
            )
        ],
    )

    native_app_manager = NativeAppManager()
    assert not mock_diff_result.has_changes()
    native_app_manager._create_dev_app(mock_diff_result)
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_create_new_quoted_override(
    mock_conn, mock_execute, temp_dir, mock_cursor
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
            (
                mock_cursor([], []),
                mock.call(
                    "show applications like 'My Application'", cursor_class=DictCursor
                ),
            ),
            (
                None,
                mock.call(
                    f"""
                    create application "My Application"
                        from application package "My Package"
                        using '@"My Package".app_src.stage'
                        debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file.replace("package_role", "app_role")],
    )
    create_named_file(
        file_name="snowflake.local.yml",
        dir=current_working_directory,
        contents=[quoted_override_yml_file],
    )

    native_app_manager = NativeAppManager()
    assert not mock_diff_result.has_changes()
    native_app_manager._create_dev_app(mock_diff_result)
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@mock_connection
def test_create_dev_app_create_new_with_additional_privileges(
    mock_conn, mock_execute_queries, mock_execute_query, temp_dir, mock_cursor
):
    side_effects, mock_execute_query_expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
            (
                mock_cursor([], []),
                mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (
                mock_cursor([{"CURRENT_ROLE()": "app_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (None, mock.call("use role app_role")),
            (
                None,
                mock.call(
                    f"""
                    create application myapp
                        from application package app_pkg
                        using @app_pkg.app_src.stage
                        debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute_query.side_effect = side_effects

    mock_execute_queries_expected = [
        mock.call(
            dedent(
                f"""\
            grant install, develop on application package app_pkg to role app_role;
            grant usage on schema app_pkg.app_src to role app_role;
            grant read on stage app_pkg.app_src.stage to role app_role;
            """
            )
        )
    ]
    mock_execute_queries.side_effect = [None, None, None]

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    assert not mock_diff_result.has_changes()
    native_app_manager._create_dev_app(mock_diff_result)
    assert mock_execute_query.mock_calls == mock_execute_query_expected
    assert mock_execute_queries.mock_calls == mock_execute_queries_expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_bad_comment(mock_conn, mock_execute, temp_dir, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
            (
                mock_cursor(
                    [
                        {
                            "name": "MYAPP",
                            "comment": "bad comment",
                            "version": LOOSE_FILES_MAGIC_VERSION,
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
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    with pytest.raises(ApplicationAlreadyExistsError):
        native_app_manager = NativeAppManager()
        assert not mock_diff_result.has_changes()
        native_app_manager._create_dev_app(mock_diff_result)

    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_bad_version(mock_conn, mock_execute, temp_dir, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
            (
                mock_cursor(
                    [
                        {
                            "name": "MYAPP",
                            "comment": SPECIAL_COMMENT,
                            "version": "v1",
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
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    with pytest.raises(ApplicationAlreadyExistsError):
        native_app_manager = NativeAppManager()
        assert not mock_diff_result.has_changes()
        native_app_manager._create_dev_app(mock_diff_result)

    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_bad_owner(mock_conn, mock_execute, temp_dir, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
            (
                mock_cursor(
                    [
                        {
                            "name": "MYAPP",
                            "comment": SPECIAL_COMMENT,
                            "version": LOOSE_FILES_MAGIC_VERSION,
                            "owner": "accountadmin_or_something",
                        }
                    ],
                    [],
                ),
                mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    with pytest.raises(UnexpectedOwnerError):
        native_app_manager = NativeAppManager()
        assert not mock_diff_result.has_changes()
        native_app_manager._create_dev_app(mock_diff_result)

    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_app_pkg_no_show(mock_conn, mock_execute, temp_dir, mock_cursor):
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
                    "show application packages like 'APP_PKG'", cursor_class=DictCursor
                ),
            ),
            (
                None,
                mock.call(
                    f"""
                    create application package app_pkg
                        comment = {SPECIAL_COMMENT}
                        distribution = internal
                    """
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    native_app_manager.create_app_package()
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_app_pkg_bad_owner(mock_conn, mock_execute, temp_dir, mock_cursor):
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
                            "owner": "accountadmin_or_something",
                        }
                    ],
                    [],
                ),
                mock.call(
                    "show application packages like 'APP_PKG'", cursor_class=DictCursor
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    with pytest.raises(UnexpectedOwnerError):
        native_app_manager = NativeAppManager()
        native_app_manager.create_app_package()

    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
@mock_get_app_pkg_distribution_in_sf
def test_create_app_pkg_internal_distribution_special_comment(
    mock_mismatch, mock_conn, mock_execute, temp_dir, mock_cursor
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
                mock_cursor(
                    [
                        {
                            "name": "APP_PKG",
                            "comment": SPECIAL_COMMENT,
                            "version": LOOSE_FILES_MAGIC_VERSION,
                            "owner": "PACKAGE_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call(
                    "show application packages like 'APP_PKG'", cursor_class=DictCursor
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    native_app_manager.create_app_package()

    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
@mock_get_app_pkg_distribution_in_sf
def test_create_app_pkg_internal_distribution_wrong_comment(
    mock_mismatch, mock_conn, mock_execute, temp_dir, mock_cursor
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
                mock_cursor(
                    [
                        {
                            "name": "APP_PKG",
                            "comment": "random",
                            "version": LOOSE_FILES_MAGIC_VERSION,
                            "owner": "PACKAGE_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call(
                    "show application packages like 'APP_PKG'", cursor_class=DictCursor
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    with pytest.raises(ApplicationPackageAlreadyExistsError):
        native_app_manager = NativeAppManager()
        native_app_manager.create_app_package()

    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
@mock_get_app_pkg_distribution_in_sf
@mock.patch(f"{NATIVEAPP_MODULE}.log.warning")
def test_create_app_pkg_external_distribution(
    mock_warning, mock_mismatch, mock_conn, mock_execute, temp_dir, mock_cursor
):
    mock_mismatch.return_value = "external"
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
                            "comment": "random",
                            "version": LOOSE_FILES_MAGIC_VERSION,
                            "owner": "PACKAGE_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call(
                    "show application packages like 'APP_PKG'", cursor_class=DictCursor
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    native_app_manager.create_app_package()

    assert mock_execute.mock_calls == expected
    mock_warning.assert_any_call(
        "Continuing to execute `snow app run` on app pkg app_pkg with distribution 'external'.\n"
    )
    assert mock_warning.call_count == 2
