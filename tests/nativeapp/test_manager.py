import os
import pytest
from textwrap import dedent
from unittest import mock

from snowcli.cli.nativeapp.manager import (
    CouldNotDropObjectError,
    NativeAppManager,
    ApplicationAlreadyExistsError,
    UnexpectedOwnerError,
    SPECIAL_COMMENT,
    LOOSE_FILES_MAGIC_VERSION,
)
from snowcli.cli.stage.diff import DiffResult
from snowflake.connector.cursor import DictCursor


from tests.testing_utils.fixtures import *

NATIVEAPP_MODULE = "snowcli.cli.nativeapp.manager"
NATIVEAPP_MANAGER_EXECUTE = f"{NATIVEAPP_MODULE}.NativeAppManager._execute_query"

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


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(f"{NATIVEAPP_MODULE}.stage_diff")
@mock.patch(f"{NATIVEAPP_MODULE}.sync_local_diff_with_stage")
def test_sync_deploy_root_with_stage(
    mock_local_diff_with_stage, mock_stage_diff, mock_execute, temp_dir, mock_cursor
):
    mock_execute.return_value = mock_cursor([{"CURRENT_ROLE()": "old_role"}], [])
    mock_diff_result = DiffResult(different="setup.sql")
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
    mock_execute.side_effect = [
        mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
        mock_cursor(["row"], []),
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
        mock_cursor(["row"], []),
        mock_cursor(["row"], []),
    ]

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
    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role sample_package_role"),
        mock.call("show application packages like 'sample_package_name'"),
        mock.call("drop application package sample_package_name"),
        mock.call("use role old_role"),
    ]
    mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_drop_object_no_show_object(mock_execute, temp_dir, mock_cursor):
    mock_execute.side_effect = [
        mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
        mock_cursor(["row"], []),
        mock_cursor([], []),
        mock_cursor(["row"], []),
    ]
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
        expected = [
            mock.call("select current_role()", cursor_class=DictCursor),
            mock.call("use role sample_package_role"),
            mock.call("show application packages like 'sample_package_name'"),
            mock.call("use role old_role"),
        ]
        mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_drop_object_no_special_comment(mock_execute, temp_dir, mock_cursor):
    mock_execute.side_effect = [
        mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
        mock_cursor(["row"], []),
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
        mock_cursor(["row"], []),
    ]

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
        expected = [
            mock.call("select current_role()", cursor_class=DictCursor),
            mock.call("use role sample_package_role"),
            mock.call("show application packages like 'sample_package_name'"),
            mock.call("use role old_role"),
        ]
        mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_create_dev_app_noop(mock_execute, temp_dir, mock_cursor):
    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role app_role"),
        mock.call("use warehouse app_warehouse"),
        mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
        mock.call("alter application myapp set debug_mode = True"),
        mock.call("use role old_role"),
    ]
    # 1:1 with expected calls; these are return values
    mock_execute.side_effect = [
        mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
        None,
        None,
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
        None,
        None,
    ]

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
def test_create_dev_app_recreate(mock_execute, temp_dir, mock_cursor):
    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role app_role"),
        mock.call("use warehouse app_warehouse"),
        mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
        mock.call("alter application myapp upgrade using @app_pkg.app_src.stage"),
        mock.call("alter application myapp set debug_mode = True"),
        mock.call("use role old_role"),
    ]
    # 1:1 with expected calls; these are return values
    mock_execute.side_effect = [
        mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
        None,
        None,
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
        None,
        None,
        None,
    ]

    mock_diff_result = DiffResult(different="setup.sql")
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
def test_create_dev_app_create_new(mock_execute, temp_dir, mock_cursor):
    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role app_role"),
        mock.call("use warehouse app_warehouse"),
        mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
        mock.call(
            f"""
                create application myapp
                    from application package app_pkg
                    using @app_pkg.app_src.stage
                    debug_mode = True
                    comment = {SPECIAL_COMMENT}
                """
        ),
        mock.call("use role old_role"),
    ]
    # 1:1 with expected calls; these are return values
    mock_execute.side_effect = [
        mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
        None,
        None,
        mock_cursor([], []),
        None,
        None,
    ]

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
def test_create_dev_app_create_new_with_additional_privileges(
    mock_execute, temp_dir, mock_cursor
):
    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role app_role"),
        mock.call("use warehouse app_warehouse"),
        mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role package_role"),
        mock.call(
            f"""
                        grant install, develop on application package app_pkg to role app_role;
                        """
        ),
        mock.call("use role app_role"),
        mock.call(
            f"""
                create application myapp
                    from application package app_pkg
                    using @app_pkg.app_src.stage
                    debug_mode = True
                    comment = {SPECIAL_COMMENT}
                """
        ),
        mock.call("use role old_role"),
    ]
    # 1:1 with expected calls; these are return values
    mock_execute.side_effect = [
        mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
        None,
        None,
        mock_cursor([], []),
        mock_cursor([{"CURRENT_ROLE()": "app_role"}], []),
        None,
        None,
        None,
        None,
        None,
    ]

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
def test_create_dev_app_bad_comment(mock_execute, temp_dir, mock_cursor):
    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role app_role"),
        mock.call("use warehouse app_warehouse"),
        mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
        mock.call("use role old_role"),
    ]
    # 1:1 with expected calls; these are return values
    mock_execute.side_effect = [
        mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
        None,
        None,
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
        None,
    ]

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
def test_create_dev_app_bad_version(mock_execute, temp_dir, mock_cursor):
    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role app_role"),
        mock.call("use warehouse app_warehouse"),
        mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
        mock.call("use role old_role"),
    ]
    # 1:1 with expected calls; these are return values
    mock_execute.side_effect = [
        mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
        None,
        None,
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
        None,
    ]

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
def test_create_dev_app_bad_owner(mock_execute, temp_dir, mock_cursor):
    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role app_role"),
        mock.call("use warehouse app_warehouse"),
        mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
        mock.call("use role old_role"),
    ]
    # 1:1 with expected calls; these are return values
    mock_execute.side_effect = [
        mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
        None,
        None,
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
        None,
    ]

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
    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role app_role"),
        mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
        mock.call("use role old_role"),
    ]
    # 1:1 with expected calls; these are return values
    mock_execute.side_effect = [
        mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
        None,
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
        None,
    ]

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
    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role app_role"),
        mock.call("show applications like 'MYAPP'", cursor_class=DictCursor),
        mock.call("use role old_role"),
    ]
    # 1:1 with expected calls; these are return values
    mock_execute.side_effect = [
        mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
        None,
        mock_cursor([], []),
        None,
    ]

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    assert native_app_manager.app_exists() is False
    assert mock_execute.mock_calls == expected


@mock.patch("snowcli.cli.connection.util.get_deployment")
@mock.patch("snowcli.cli.connection.util.get_account")
@mock.patch("snowcli.cli.connection.util.get_snowsight_host")
@mock.patch(
    "snowcli.cli.common.sql_execution.snow_cli_global_context_manager.get_connection"
)
def test_get_snowsight_url(
    mock_conn, mock_snowsight_host, mock_account, mock_deployment, temp_dir
):
    mock_conn.return_value = None
    mock_snowsight_host.return_value = "https://host"
    mock_deployment.return_value = "deployment"
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
        == "https://host/deployment/account/#/apps/application/MYAPP"
    )


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_quoting_app_teardown(mock_execute, temp_dir, mock_cursor):
    expected = [
        # teardown app
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role app_role"),
        mock.call("show applications like 'My Application'", cursor_class=DictCursor),
        mock.call('drop application "My Application"'),
        mock.call("use role old_role"),
        # teardown package
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role package_role"),
        mock.call(
            "show application packages like 'My Package'", cursor_class=DictCursor
        ),
        mock.call('drop application package "My Package"'),
        mock.call("use role old_role"),
    ]
    # 1:1 with expected calls; these are return values
    mock_execute.side_effect = [
        # teardown app
        mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
        None,
        mock_cursor(
            [
                {
                    "name": "My Application",
                    "comment": SPECIAL_COMMENT,
                    "version": LOOSE_FILES_MAGIC_VERSION,
                    "owner": "APP_ROLE",
                }
            ],
            [],
        ),
        None,
        None,
        # teardown package
        mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
        None,
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
        None,
        None,
    ]

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
