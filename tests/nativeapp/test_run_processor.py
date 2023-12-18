import unittest
from textwrap import dedent

from snowcli.cli.nativeapp.constants import (
    LOOSE_FILES_MAGIC_VERSION,
    SPECIAL_COMMENT,
)
from snowcli.cli.nativeapp.exceptions import (
    ApplicationAlreadyExistsError,
    ApplicationPackageAlreadyExistsError,
    UnexpectedOwnerError,
)
from snowcli.cli.nativeapp.run_processor import NativeAppRunProcessor
from snowcli.cli.object.stage.diff import DiffResult
from snowcli.cli.project.definition_manager import DefinitionManager
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.utils import *
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


def _get_na_manager():
    dm = DefinitionManager()
    return NativeAppRunProcessor(
        project_definition=dm.project_definition["native_app"],
        project_root=dm.project_root,
    )


# Test create_app_package() with no existing package available
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_PKG_INFO, return_value=None)
@mock_connection
def test_create_app_pkg_no_existing_package(
    mock_conn, mock_get_existing_app_pkg_info, mock_execute, temp_dir, mock_cursor
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
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    native_app_manager.create_app_package()
    assert mock_execute.mock_calls == expected
    mock_get_existing_app_pkg_info.assert_called_once()


# Test create_app_package() with incorrect owner
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_PKG_INFO)
@mock_connection
def test_create_app_pkg_incorrect_owner(
    mock_conn, mock_get_existing_app_pkg_info, temp_dir
):
    mock_get_existing_app_pkg_info.return_value = {
        "name": "APP_PKG",
        "comment": SPECIAL_COMMENT,
        "version": LOOSE_FILES_MAGIC_VERSION,
        "owner": "wrong_owner",
    }

    mock_conn.return_value = MockConnectionCtx()

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    with pytest.raises(UnexpectedOwnerError):
        native_app_manager = _get_na_manager()
        native_app_manager.create_app_package()


# Test create_app_package() with distribution external AND variable mismatch
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_PKG_INFO)
@mock_connection
@mock_get_app_pkg_distribution_in_sf
@mock.patch(NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME)
@mock.patch(f"{RUN_MODULE}.print")
@pytest.mark.parametrize(
    "is_pkg_distribution_same",
    [False, True],
)
def test_create_app_pkg_external_distribution(
    mock_warning,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_conn,
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
    mock_conn.return_value = MockConnectionCtx()

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    native_app_manager.create_app_package()
    if not is_pkg_distribution_same:
        mock_warning.assert_called_once_with(
            "Continuing to execute `snow app run` on app pkg app_pkg with distribution 'external'."
        )


# Test create_app_package() with distribution internal AND variable mismatch AND special comment is True
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_PKG_INFO)
@mock_connection
@mock_get_app_pkg_distribution_in_sf
@mock.patch(NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME)
@mock.patch(f"{RUN_MODULE}.print")
@pytest.mark.parametrize(
    "is_pkg_distribution_same",
    [False, True],
)
def test_create_app_pkg_internal_distribution_special_comment(
    mock_warning,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_conn,
    mock_get_existing_app_pkg_info,
    is_pkg_distribution_same,
    temp_dir,
):
    mock_is_distribution_same.return_value = is_pkg_distribution_same
    mock_get_distribution.return_value = "internal"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "APP_PKG",
        "comment": SPECIAL_COMMENT,
        "version": LOOSE_FILES_MAGIC_VERSION,
        "owner": "PACKAGE_ROLE",
    }
    mock_conn.return_value = MockConnectionCtx()

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    native_app_manager.create_app_package()
    if not is_pkg_distribution_same:
        mock_warning.assert_called_once_with(
            "Continuing to execute `snow app run` on app pkg app_pkg with distribution 'internal'."
        )


# Test create_app_package() with distribution internal AND variable mismatch AND special comment is False
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_PKG_INFO)
@mock_connection
@mock_get_app_pkg_distribution_in_sf
@mock.patch(NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME)
@mock.patch(f"{RUN_MODULE}.print")
@pytest.mark.parametrize(
    "is_pkg_distribution_same",
    [False, True],
)
def test_create_app_pkg_internal_distribution_no_special_comment(
    mock_warning,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_conn,
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
    mock_conn.return_value = MockConnectionCtx()

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    with pytest.raises(ApplicationPackageAlreadyExistsError):
        native_app_manager.create_app_package()

    if not is_pkg_distribution_same:
        mock_warning.assert_called_once_with(
            "Continuing to execute `snow app run` on app pkg app_pkg with distribution 'internal'."
        )


# Test create_dev_app with exception thrown trying to use the warehouse
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

    native_app_manager = _get_na_manager()
    assert not mock_diff_result.has_changes()

    with pytest.raises(ProgrammingError) as err:
        native_app_manager._create_dev_app(mock_diff_result)

    assert mock_execute.mock_calls == expected
    assert "Please grant usage privilege on warehouse to this role." in err.value.msg


# Test create_dev_app with no existing application AND create succeeds AND app role == package role
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_create_new_w_no_additional_privileges(
    mock_conn, mock_execute, mock_get_existing_app_info, temp_dir, mock_cursor
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
                None,
                mock.call(
                    dedent(
                        f"""\
                    create application myapp
                        from application package app_pkg
                        using @app_pkg.app_src.stage
                        debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                    )
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

    native_app_manager = _get_na_manager()
    assert not mock_diff_result.has_changes()
    native_app_manager._create_dev_app(mock_diff_result)
    assert mock_execute.mock_calls == expected


# Test create_dev_app with no existing application AND create succeeds AND app role != package role
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@mock_connection
def test_create_dev_app_create_new_with_additional_privileges(
    mock_conn,
    mock_execute_queries,
    mock_execute_query,
    mock_get_existing_app_info,
    temp_dir,
    mock_cursor,
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
                mock_cursor([{"CURRENT_ROLE()": "app_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (None, mock.call("use role app_role")),
            (
                None,
                mock.call(
                    dedent(
                        f"""\
                    create application myapp
                        from application package app_pkg
                        using @app_pkg.app_src.stage
                        debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                    )
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

    native_app_manager = _get_na_manager()
    assert not mock_diff_result.has_changes()
    native_app_manager._create_dev_app(mock_diff_result)
    assert mock_execute_query.mock_calls == mock_execute_query_expected
    assert mock_execute_queries.mock_calls == mock_execute_queries_expected


# Test create_dev_app with no existing application AND create throws an exception
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_create_new_w_missing_warehouse_exception(
    mock_conn, mock_execute, mock_get_existing_app_info, temp_dir, mock_cursor
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
                ProgrammingError(
                    msg="No active warehouse selected in the current session", errno=606
                ),
                mock.call(
                    dedent(
                        f"""\
                    create application myapp
                        from application package app_pkg
                        using @app_pkg.app_src.stage
                        debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                    )
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

    native_app_manager = _get_na_manager()
    assert not mock_diff_result.has_changes()

    with pytest.raises(ProgrammingError) as err:
        native_app_manager._create_dev_app(mock_diff_result)

    assert "Please provide a warehouse for the active session role" in err.value.msg
    assert mock_execute.mock_calls == expected


# Test create_dev_app with existing application AND bad comment AND good version
# Test create_dev_app with existing application AND bad comment AND bad version
# Test create_dev_app with existing application AND good comment AND bad version
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
@pytest.mark.parametrize(
    "comment, version",
    [
        ("dummy", LOOSE_FILES_MAGIC_VERSION),
        ("dummy", "dummy"),
        (SPECIAL_COMMENT, "dummy"),
    ],
)
def test_create_dev_app_incorrect_properties(
    mock_conn,
    mock_execute,
    mock_get_existing_app_info,
    comment,
    version,
    temp_dir,
    mock_cursor,
):
    mock_get_existing_app_info.return_value = {
        "name": "MYAPP",
        "comment": comment,
        "version": version,
        "owner": "APP_ROLE",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
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
        native_app_manager = _get_na_manager()
        assert not mock_diff_result.has_changes()
        native_app_manager._create_dev_app(mock_diff_result)

    assert mock_execute.mock_calls == expected


# Test create_dev_app with existing application AND incorrect owner
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_incorrect_owner(
    mock_conn, mock_execute, mock_get_existing_app_info, temp_dir, mock_cursor
):
    mock_get_existing_app_info.return_value = {
        "name": "MYAPP",
        "comment": SPECIAL_COMMENT,
        "version": LOOSE_FILES_MAGIC_VERSION,
        "owner": "accountadmin_or_something",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
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
        native_app_manager = _get_na_manager()
        assert not mock_diff_result.has_changes()
        native_app_manager._create_dev_app(mock_diff_result)

    assert mock_execute.mock_calls == expected


# Test create_dev_app with existing application AND diff has no changes
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_no_diff_changes(
    mock_conn, mock_execute, mock_get_existing_app_info, temp_dir, mock_cursor
):
    mock_get_existing_app_info.return_value = {
        "name": "MYAPP",
        "comment": SPECIAL_COMMENT,
        "version": LOOSE_FILES_MAGIC_VERSION,
        "owner": "APP_ROLE",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
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

    native_app_manager = _get_na_manager()
    assert not mock_diff_result.has_changes()
    native_app_manager._create_dev_app(mock_diff_result)
    assert mock_execute.mock_calls == expected


# Test create_dev_app with existing application AND diff has changes
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_w_diff_changes(
    mock_conn, mock_execute, mock_get_existing_app_info, temp_dir, mock_cursor
):
    mock_get_existing_app_info.return_value = {
        "name": "MYAPP",
        "comment": SPECIAL_COMMENT,
        "version": LOOSE_FILES_MAGIC_VERSION,
        "owner": "APP_ROLE",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
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

    native_app_manager = _get_na_manager()
    assert mock_diff_result.has_changes()
    native_app_manager._create_dev_app(mock_diff_result)
    assert mock_execute.mock_calls == expected


# Test create_dev_app with existing application AND alter throws an error
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_recreate_w_missing_warehouse_exception(
    mock_conn, mock_execute, mock_get_existing_app_info, temp_dir, mock_cursor
):
    mock_get_existing_app_info.return_value = {
        "name": "MYAPP",
        "comment": SPECIAL_COMMENT,
        "version": LOOSE_FILES_MAGIC_VERSION,
        "owner": "APP_ROLE",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use warehouse app_warehouse")),
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

    native_app_manager = _get_na_manager()
    assert mock_diff_result.has_changes()

    with pytest.raises(ProgrammingError) as err:
        native_app_manager._create_dev_app(mock_diff_result)

    assert mock_execute.mock_calls == expected
    assert "Please provide a warehouse for the active session role" in err.value.msg


# Test create_dev_app with no existing application AND quoted name scenario 1
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_create_new_quoted(
    mock_conn, mock_execute, mock_get_existing_app_info, temp_dir, mock_cursor
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
                None,
                mock.call(
                    dedent(
                        f"""\
                    create application "My Application"
                        from application package "My Package"
                        using '@"My Package".app_src.stage'
                        debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                    )
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

    native_app_manager = _get_na_manager()
    assert not mock_diff_result.has_changes()
    native_app_manager._create_dev_app(mock_diff_result)
    assert mock_execute.mock_calls == expected


# Test create_dev_app with no existing application AND quoted name scenario 2
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection
def test_create_dev_app_create_new_quoted_override(
    mock_conn, mock_execute, mock_get_existing_app_info, temp_dir, mock_cursor
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
                None,
                mock.call(
                    dedent(
                        f"""\
                    create application "My Application"
                        from application package "My Package"
                        using '@"My Package".app_src.stage'
                        debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                    )
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

    native_app_manager = _get_na_manager()
    assert not mock_diff_result.has_changes()
    native_app_manager._create_dev_app(mock_diff_result)
    assert mock_execute.mock_calls == expected
