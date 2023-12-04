from textwrap import dedent
from unittest.mock import PropertyMock

from snowcli.cli.nativeapp.manager import (
    SPECIAL_COMMENT,
    CouldNotDropObjectError,
    NativeAppManager,
)
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
def test_idempotent_app_teardown(mock_execute, temp_dir, mock_cursor):
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
def test_teardown_without_app_instance(mock_execute, temp_dir, mock_cursor):
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
def test_teardown_could_not_drop_app(mock_execute, temp_dir, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            # teardown app: app does not exist + is skipped
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
    with pytest.raises(CouldNotDropObjectError):
        native_app_manager.teardown()

    assert mock_execute.mock_calls == expected


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
