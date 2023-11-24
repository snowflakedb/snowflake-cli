from textwrap import dedent
from unittest.mock import PropertyMock

from snowcli.cli.nativeapp.manager import (
    LOOSE_FILES_MAGIC_VERSIONS,
    SPECIAL_COMMENT,
    ApplicationAlreadyExistsError,
    CouldNotDropObjectError,
    NativeAppManager,
    UnexpectedOwnerError,
)
from snowcli.cli.object.stage.diff import DiffResult
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

from tests.testing_utils.fixtures import *

NATIVEAPP_MODULE = "snowcli.cli.nativeapp.manager"
NATIVEAPP_MANAGER_EXECUTE = f"{NATIVEAPP_MODULE}.NativeAppManager._execute_query"
NATIVEAPP_MANAGER_EXECUTE_QUERIES = (
    f"{NATIVEAPP_MODULE}.NativeAppManager._execute_queries"
)


mock_connection = mock.patch(
    "snowcli.cli.common.cli_global_context._CliGlobalContextAccess.connection",
    new_callable=PropertyMock,
)


mock_snowflake_yml_file = dedent(
    """\
        definition_version: 1
        native_app:
            name: myapp

            source_stage:
                app_src.stage

            artifacts:
                - setup.sql
                - app/README.md
                - src: app/streamlit/*.py
                  dest: ui/

            application:
                name: myapp
                role: app_role
                warehouse: app_warehouse
                debug: true

            package:
                name: app_pkg
                role: package_role
                scripts:
                    - shared_content.sql
    """
)

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

quoted_override_yml_file = dedent(
    """\
        native_app:
            application:
                name: >-
                    "My Application"
            package:
                name: >-
                    "My Package"
    """
)


def mock_execute_helper(mock_input: list):
    side_effects, expected = map(list, zip(*mock_input))
    return side_effects, expected


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
                mock.call("show application packages like 'SAMPLE_PACKAGE_NAME'"),
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
    with pytest.raises(
        CouldNotDropObjectError,
        match="Role sample_package_role does not own any application package with the name sample_package_name!",
    ):
        native_app_manager.drop_object(
            object_name="sample_package_name",
            object_role="sample_package_role",
            object_type="package",
            query_dict={"show": "show application packages like"},
        )
        assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_drop_object_no_special_comment(mock_execute, temp_dir, mock_cursor):
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
                            "comment": "NOT_GENERATED_BY_SNOWCLI",
                        }
                    ],
                    [],
                ),
                mock.call("show application packages like 'sample_package_name'"),
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
    with pytest.raises(
        CouldNotDropObjectError,
        match="Application Package sample_package_name was not created by SnowCLI. Cannot drop the application package.",
    ):
        native_app_manager.drop_object(
            object_name="sample_package_name",
            object_role="sample_package_role",
            object_type="package",
            query_dict={
                "show": "show application packages like",
            },
        )
        assert mock_execute.mock_calls == expected


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
                            "version": "UNVERSIONED",
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
                            "version": "UNVERSIONED",
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
@pytest.mark.parametrize("loose_files_magic_version", LOOSE_FILES_MAGIC_VERSIONS)
def test_create_dev_app_bad_comment(
    mock_conn, mock_execute, loose_files_magic_version, temp_dir, mock_cursor
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
                            "comment": "bad comment",
                            "version": loose_files_magic_version,
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
                            "version": "UNVERSIONED",
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
                            "version": "UNVERSIONED",
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


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_quoting_app_teardown(mock_execute, temp_dir, mock_cursor):
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
