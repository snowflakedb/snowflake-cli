# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from textwrap import dedent
from unittest import mock
from unittest.mock import MagicMock

import pytest
import typer
from click import UsageError
from snowflake.cli._plugins.nativeapp.constants import (
    LOOSE_FILES_MAGIC_VERSION,
    SPECIAL_COMMENT,
)
from snowflake.cli._plugins.nativeapp.exceptions import (
    ApplicationCreatedExternallyError,
    ApplicationPackageDoesNotExistError,
    UnexpectedOwnerError,
)
from snowflake.cli._plugins.nativeapp.policy import (
    AllowAlwaysPolicy,
    AskAlwaysPolicy,
    DenyAlwaysPolicy,
)
from snowflake.cli._plugins.nativeapp.run_processor import (
    NativeAppRunProcessor,
    SameAccountInstallMethod,
)
from snowflake.cli._plugins.stage.diff import DiffResult
from snowflake.cli.api.errno import (
    APPLICATION_NO_LONGER_AVAILABLE,
    APPLICATION_OWNS_EXTERNAL_OBJECTS,
    CANNOT_UPGRADE_FROM_LOOSE_FILES_TO_VERSION,
    DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    NO_WAREHOUSE_SELECTED_IN_SESSION,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.patch_utils import (
    mock_connection,
)
from tests.nativeapp.utils import (
    NATIVEAPP_MANAGER_EXECUTE,
    NATIVEAPP_MODULE,
    RUN_PROCESSOR_GET_EXISTING_APP_INFO,
    TYPER_CONFIRM,
    mock_execute_helper,
    mock_snowflake_yml_file,
    quoted_override_yml_file,
)
from tests.testing_utils.files_and_dirs import create_named_file
from tests.testing_utils.fixtures import MockConnectionCtx

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

allow_always_policy = AllowAlwaysPolicy()
ask_always_policy = AskAlwaysPolicy()
deny_always_policy = DenyAlwaysPolicy()


def _get_na_run_processor():
    dm = DefinitionManager()
    return NativeAppRunProcessor(
        project_definition=dm.project_definition.native_app,
        project_root=dm.project_root,
    )


# Test create_dev_app with exception thrown trying to use the warehouse
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
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
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (
                ProgrammingError(
                    msg="Object does not exist, or operation cannot be performed.",
                    errno=DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
                ),
                mock.call("use warehouse app_warehouse"),
            ),
            (
                None,
                mock.call("use warehouse old_wh"),
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
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    assert not mock_diff_result.has_changes()

    with pytest.raises(ProgrammingError) as err:
        run_processor.create_or_upgrade_app(
            policy=MagicMock(),
            install_method=SameAccountInstallMethod.unversioned_dev(),
        )

    assert mock_execute.mock_calls == expected
    assert (
        "Could not use warehouse app_warehouse. Object does not exist, or operation cannot be performed."
        in err.value.msg
    )


# Test create_dev_app with no existing application AND create succeeds AND app role == package role
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
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
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                None,
                mock.call(
                    dedent(
                        f"""\
                    create application myapp
                        from application package app_pkg using @app_pkg.app_src.stage debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                    )
                ),
            ),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file.replace("package_role", "app_role")],
    )

    run_processor = _get_na_run_processor()
    assert not mock_diff_result.has_changes()
    run_processor.create_or_upgrade_app(
        policy=MagicMock(), install_method=SameAccountInstallMethod.unversioned_dev()
    )
    assert mock_execute.mock_calls == expected


# Test create_dev_app with no existing application AND create returns a warning
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
@mock_connection()
@pytest.mark.parametrize(
    "existing_app_info",
    [
        None,
        {
            "name": "MYAPP",
            "comment": SPECIAL_COMMENT,
            "version": LOOSE_FILES_MAGIC_VERSION,
            "owner": "APP_ROLE",
        },
    ],
)
def test_create_or_upgrade_dev_app_with_warning(
    mock_conn,
    mock_warning,
    mock_execute,
    mock_get_existing_app_info,
    temp_dir,
    mock_cursor,
    existing_app_info,
):
    status_messages = ["App created/upgraded", "Warning: some warning"]
    status_cursor = mock_cursor(
        [(msg,) for msg in status_messages],
        ["status"],
    )
    create_or_upgrade_calls = (
        [
            (
                status_cursor,
                mock.call(
                    dedent(
                        f"""\
                create application myapp
                    from application package app_pkg using @app_pkg.app_src.stage debug_mode = True
                    comment = {SPECIAL_COMMENT}
                """
                    )
                ),
            ),
        ]
        if existing_app_info is None
        else [
            (
                status_cursor,
                mock.call(
                    "alter application myapp upgrade using @app_pkg.app_src.stage"
                ),
            ),
            (None, mock.call("alter application myapp set debug_mode = True")),
        ]
    )

    mock_get_existing_app_info.return_value = existing_app_info
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            *create_or_upgrade_calls,
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file.replace("package_role", "app_role")],
    )

    run_processor = _get_na_run_processor()
    assert not mock_diff_result.has_changes()
    run_processor.create_or_upgrade_app(
        policy=MagicMock(), install_method=SameAccountInstallMethod.unversioned_dev()
    )
    assert mock_execute.mock_calls == expected

    mock_warning.assert_has_calls([mock.call(msg) for msg in status_messages])


# Test create_dev_app with no existing application AND create succeeds AND app role != package role
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
def test_create_dev_app_create_new_with_additional_privileges(
    mock_conn,
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
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                mock_cursor([{"CURRENT_ROLE()": "app_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                None,
                mock.call(
                    "grant install, develop on application package app_pkg to role app_role"
                ),
            ),
            (
                None,
                mock.call("grant usage on schema app_pkg.app_src to role app_role"),
            ),
            (
                None,
                mock.call("grant read on stage app_pkg.app_src.stage to role app_role"),
            ),
            (None, mock.call("use role app_role")),
            (
                None,
                mock.call(
                    dedent(
                        f"""\
                    create application myapp
                        from application package app_pkg using @app_pkg.app_src.stage debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                    )
                ),
            ),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute_query.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    assert not mock_diff_result.has_changes()
    run_processor.create_or_upgrade_app(
        policy=MagicMock(), install_method=SameAccountInstallMethod.unversioned_dev()
    )
    assert mock_execute_query.mock_calls == mock_execute_query_expected


# Test create_dev_app with no existing application AND create throws an exception
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
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
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                ProgrammingError(
                    msg="No active warehouse selected in the current session",
                    errno=NO_WAREHOUSE_SELECTED_IN_SESSION,
                ),
                mock.call(
                    dedent(
                        f"""\
                    create application myapp
                        from application package app_pkg using @app_pkg.app_src.stage debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                    )
                ),
            ),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )

    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file.replace("package_role", "app_role")],
    )

    run_processor = _get_na_run_processor()
    assert not mock_diff_result.has_changes()

    with pytest.raises(ProgrammingError) as err:
        run_processor.create_or_upgrade_app(
            policy=MagicMock(),
            install_method=SameAccountInstallMethod.unversioned_dev(),
        )

    assert "Please provide a warehouse for the active session role" in err.value.msg
    assert mock_execute.mock_calls == expected


# Test create_dev_app with existing application AND bad comment AND good version
# Test create_dev_app with existing application AND bad comment AND bad version
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
@pytest.mark.parametrize(
    "comment, version",
    [
        ("dummy", LOOSE_FILES_MAGIC_VERSION),
        ("dummy", "dummy"),
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
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    with pytest.raises(ApplicationCreatedExternallyError):
        run_processor = _get_na_run_processor()
        assert not mock_diff_result.has_changes()
        run_processor.create_or_upgrade_app(
            policy=MagicMock(),
            install_method=SameAccountInstallMethod.unversioned_dev(),
        )

    assert mock_execute.mock_calls == expected


# Test create_dev_app with existing application AND incorrect owner
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
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
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    with pytest.raises(UnexpectedOwnerError):
        run_processor = _get_na_run_processor()
        assert not mock_diff_result.has_changes()
        run_processor.create_or_upgrade_app(
            policy=MagicMock(),
            install_method=SameAccountInstallMethod.unversioned_dev(),
        )

    assert mock_execute.mock_calls == expected


# Test create_dev_app with existing application AND diff has no changes
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
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
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                None,
                mock.call(
                    "alter application myapp upgrade using @app_pkg.app_src.stage"
                ),
            ),
            (None, mock.call("alter application myapp set debug_mode = True")),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    assert not mock_diff_result.has_changes()
    run_processor.create_or_upgrade_app(
        policy=MagicMock(), install_method=SameAccountInstallMethod.unversioned_dev()
    )
    assert mock_execute.mock_calls == expected


# Test create_dev_app with existing application AND diff has changes
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
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
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                None,
                mock.call(
                    "alter application myapp upgrade using @app_pkg.app_src.stage"
                ),
            ),
            (None, mock.call("alter application myapp set debug_mode = True")),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult(different=["setup.sql"])
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    assert mock_diff_result.has_changes()
    run_processor.create_or_upgrade_app(
        policy=MagicMock(), install_method=SameAccountInstallMethod.unversioned_dev()
    )
    assert mock_execute.mock_calls == expected


# Test create_dev_app with existing application AND alter throws an error
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
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
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                ProgrammingError(
                    msg="No active warehouse selected in the current session",
                    errno=NO_WAREHOUSE_SELECTED_IN_SESSION,
                ),
                mock.call(
                    "alter application myapp upgrade using @app_pkg.app_src.stage"
                ),
            ),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult(different=["setup.sql"])
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    assert mock_diff_result.has_changes()

    with pytest.raises(ProgrammingError) as err:
        run_processor.create_or_upgrade_app(
            policy=MagicMock(),
            install_method=SameAccountInstallMethod.unversioned_dev(),
        )

    assert mock_execute.mock_calls == expected
    assert "Please provide a warehouse for the active session role" in err.value.msg


# Test create_dev_app with no existing application AND quoted name scenario 1
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
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
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                None,
                mock.call(
                    dedent(
                        f"""\
                    create application "My Application"
                        from application package "My Package" using '@"My Package".app_src.stage' debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                    )
                ),
            ),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
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

    run_processor = _get_na_run_processor()
    assert not mock_diff_result.has_changes()
    run_processor.create_or_upgrade_app(
        policy=MagicMock(), install_method=SameAccountInstallMethod.unversioned_dev()
    )
    assert mock_execute.mock_calls == expected


# Test create_dev_app with no existing application AND quoted name scenario 2
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
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
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                None,
                mock.call(
                    dedent(
                        f"""\
                    create application "My Application"
                        from application package "My Package" using '@"My Package".app_src.stage' debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                    )
                ),
            ),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    mock_diff_result = DiffResult()
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file.replace("package_role", "app_role")],
    )
    create_named_file(
        file_name="snowflake.local.yml",
        dir_name=current_working_directory,
        contents=[quoted_override_yml_file],
    )

    run_processor = _get_na_run_processor()
    assert not mock_diff_result.has_changes()
    run_processor.create_or_upgrade_app(
        policy=MagicMock(), install_method=SameAccountInstallMethod.unversioned_dev()
    )
    assert mock_execute.mock_calls == expected


# Test run existing app info
# AND app package has been dropped
# AND user wants to drop app
# AND drop succeeds
# AND app is created successfully.
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock_connection()
def test_create_dev_app_recreate_app_when_orphaned(
    mock_conn,
    mock_get_existing_app_info,
    mock_execute,
    temp_dir,
    mock_cursor,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "comment": SPECIAL_COMMENT,
        "owner": "app_role",
        "version": LOOSE_FILES_MAGIC_VERSION,
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                ProgrammingError(errno=APPLICATION_NO_LONGER_AVAILABLE),
                mock.call(
                    "alter application myapp upgrade using @app_pkg.app_src.stage"
                ),
            ),
            (None, mock.call("drop application myapp")),
            (
                mock_cursor([{"CURRENT_ROLE()": "app_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                None,
                mock.call(
                    "grant install, develop on application package app_pkg to role app_role"
                ),
            ),
            (
                None,
                mock.call("grant usage on schema app_pkg.app_src to role app_role"),
            ),
            (
                None,
                mock.call("grant read on stage app_pkg.app_src.stage to role app_role"),
            ),
            (None, mock.call("use role app_role")),
            (
                None,
                mock.call(
                    dedent(
                        f"""\
                    create application myapp
                        from application package app_pkg using @app_pkg.app_src.stage debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                    )
                ),
            ),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    run_processor.create_or_upgrade_app(
        policy=MagicMock(), install_method=SameAccountInstallMethod.unversioned_dev()
    )
    assert mock_execute.mock_calls == expected


# Test run existing app info
# AND app package has been dropped
# AND user wants to drop app
# AND drop requires cascade
# AND drop succeeds
# AND app is created successfully.
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock_connection()
def test_create_dev_app_recreate_app_when_orphaned_requires_cascade(
    mock_conn,
    mock_get_existing_app_info,
    mock_execute,
    temp_dir,
    mock_cursor,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "comment": SPECIAL_COMMENT,
        "owner": "app_role",
        "version": LOOSE_FILES_MAGIC_VERSION,
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                ProgrammingError(errno=APPLICATION_NO_LONGER_AVAILABLE),
                mock.call(
                    "alter application myapp upgrade using @app_pkg.app_src.stage"
                ),
            ),
            (
                ProgrammingError(errno=APPLICATION_OWNS_EXTERNAL_OBJECTS),
                mock.call("drop application myapp"),
            ),
            (
                mock_cursor([{"CURRENT_ROLE()": "app_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (
                mock_cursor(
                    [
                        [None, "mypool", "COMPUTE_POOL"],
                    ],
                    [],
                ),
                mock.call("show objects owned by application myapp"),
            ),
            (None, mock.call("drop application myapp cascade")),
            (
                mock_cursor([{"CURRENT_ROLE()": "app_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                None,
                mock.call(
                    "grant install, develop on application package app_pkg to role app_role"
                ),
            ),
            (
                None,
                mock.call("grant usage on schema app_pkg.app_src to role app_role"),
            ),
            (
                None,
                mock.call("grant read on stage app_pkg.app_src.stage to role app_role"),
            ),
            (None, mock.call("use role app_role")),
            (
                None,
                mock.call(
                    dedent(
                        f"""\
                    create application myapp
                        from application package app_pkg using @app_pkg.app_src.stage debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                    )
                ),
            ),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    run_processor.create_or_upgrade_app(
        policy=MagicMock(), install_method=SameAccountInstallMethod.unversioned_dev()
    )
    assert mock_execute.mock_calls == expected


# Test run existing app info
# AND app package has been dropped
# AND user wants to drop app
# AND drop requires cascade
# AND we can't see which objects are owned by the app
# AND drop succeeds
# AND app is created successfully.
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock_connection()
def test_create_dev_app_recreate_app_when_orphaned_requires_cascade_unknown_objects(
    mock_conn,
    mock_get_existing_app_info,
    mock_execute,
    temp_dir,
    mock_cursor,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "comment": SPECIAL_COMMENT,
        "owner": "app_role",
        "version": LOOSE_FILES_MAGIC_VERSION,
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                ProgrammingError(errno=APPLICATION_NO_LONGER_AVAILABLE),
                mock.call(
                    "alter application myapp upgrade using @app_pkg.app_src.stage"
                ),
            ),
            (
                ProgrammingError(errno=APPLICATION_OWNS_EXTERNAL_OBJECTS),
                mock.call("drop application myapp"),
            ),
            (
                mock_cursor([{"CURRENT_ROLE()": "app_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (
                ProgrammingError(errno=APPLICATION_NO_LONGER_AVAILABLE),
                mock.call("show objects owned by application myapp"),
            ),
            (None, mock.call("drop application myapp cascade")),
            (
                mock_cursor([{"CURRENT_ROLE()": "app_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                None,
                mock.call(
                    "grant install, develop on application package app_pkg to role app_role"
                ),
            ),
            (
                None,
                mock.call("grant usage on schema app_pkg.app_src to role app_role"),
            ),
            (
                None,
                mock.call("grant read on stage app_pkg.app_src.stage to role app_role"),
            ),
            (None, mock.call("use role app_role")),
            (
                None,
                mock.call(
                    dedent(
                        f"""\
                    create application myapp
                        from application package app_pkg using @app_pkg.app_src.stage debug_mode = True
                        comment = {SPECIAL_COMMENT}
                    """
                    )
                ),
            ),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    run_processor.create_or_upgrade_app(
        policy=MagicMock(), install_method=SameAccountInstallMethod.unversioned_dev()
    )
    assert mock_execute.mock_calls == expected


# Test upgrade app method for release directives AND throws warehouse error
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
@pytest.mark.parametrize(
    "policy_param", [allow_always_policy, ask_always_policy, deny_always_policy]
)
def test_upgrade_app_warehouse_error(
    mock_conn, mock_execute, policy_param, temp_dir, mock_cursor
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (
                ProgrammingError(
                    msg="Object does not exist, or operation cannot be performed.",
                    errno=DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
                ),
                mock.call("use warehouse app_warehouse"),
            ),
            (
                None,
                mock.call("use warehouse old_wh"),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    with pytest.raises(ProgrammingError):
        run_processor.create_or_upgrade_app(
            policy_param,
            is_interactive=True,
            install_method=SameAccountInstallMethod.release_directive(),
        )
    assert mock_execute.mock_calls == expected


# Test upgrade app method for release directives AND existing app info AND bad owner
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock_connection()
@pytest.mark.parametrize(
    "policy_param", [allow_always_policy, ask_always_policy, deny_always_policy]
)
def test_upgrade_app_incorrect_owner(
    mock_conn,
    mock_get_existing_app_info,
    mock_execute,
    policy_param,
    temp_dir,
    mock_cursor,
):
    mock_get_existing_app_info.return_value = {
        "name": "APP",
        "comment": SPECIAL_COMMENT,
        "owner": "wrong_owner",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    with pytest.raises(UnexpectedOwnerError):
        run_processor.create_or_upgrade_app(
            policy=policy_param,
            is_interactive=True,
            install_method=SameAccountInstallMethod.release_directive(),
        )
    assert mock_execute.mock_calls == expected


# Test upgrade app method for release directives AND existing app info AND upgrade succeeds
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock_connection()
@pytest.mark.parametrize(
    "policy_param", [allow_always_policy, ask_always_policy, deny_always_policy]
)
def test_upgrade_app_succeeds(
    mock_conn,
    mock_get_existing_app_info,
    mock_execute,
    policy_param,
    temp_dir,
    mock_cursor,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "comment": SPECIAL_COMMENT,
        "owner": "app_role",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (None, mock.call("alter application myapp upgrade ")),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    run_processor.create_or_upgrade_app(
        policy=policy_param,
        is_interactive=True,
        install_method=SameAccountInstallMethod.release_directive(),
    )
    assert mock_execute.mock_calls == expected


# Test upgrade app method for release directives AND existing app info AND upgrade fails due to generic error
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock_connection()
@pytest.mark.parametrize(
    "policy_param", [allow_always_policy, ask_always_policy, deny_always_policy]
)
def test_upgrade_app_fails_generic_error(
    mock_conn,
    mock_get_existing_app_info,
    mock_execute,
    policy_param,
    temp_dir,
    mock_cursor,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "comment": SPECIAL_COMMENT,
        "owner": "app_role",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                ProgrammingError(
                    errno=1234,
                ),
                mock.call("alter application myapp upgrade "),
            ),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    with pytest.raises(ProgrammingError):
        run_processor.create_or_upgrade_app(
            policy=policy_param,
            is_interactive=True,
            install_method=SameAccountInstallMethod.release_directive(),
        )
    assert mock_execute.mock_calls == expected


# Test upgrade app method for release directives AND existing app info AND upgrade fails due to upgrade restriction error AND --force is False AND interactive mode is False AND --interactive is False
# Test upgrade app method for release directives AND existing app info AND upgrade fails due to upgrade restriction error AND --force is False AND interactive mode is False AND --interactive is True AND  user does not want to proceed
# Test upgrade app method for release directives AND existing app info AND upgrade fails due to upgrade restriction error AND --force is False AND interactive mode is True AND user does not want to proceed
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(
    f"snowflake.cli._plugins.nativeapp.policy.{TYPER_CONFIRM}", return_value=False
)
@mock_connection()
@pytest.mark.parametrize(
    "policy_param, is_interactive_param, expected_code",
    [(deny_always_policy, False, 1), (ask_always_policy, True, 0)],
)
def test_upgrade_app_fails_upgrade_restriction_error(
    mock_conn,
    mock_typer_confirm,
    mock_get_existing_app_info,
    mock_execute,
    policy_param,
    is_interactive_param,
    expected_code,
    temp_dir,
    mock_cursor,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "comment": SPECIAL_COMMENT,
        "owner": "app_role",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                ProgrammingError(
                    errno=CANNOT_UPGRADE_FROM_LOOSE_FILES_TO_VERSION,
                ),
                mock.call("alter application myapp upgrade "),
            ),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    with pytest.raises(typer.Exit):
        result = run_processor.create_or_upgrade_app(
            policy_param,
            is_interactive=is_interactive_param,
            install_method=SameAccountInstallMethod.release_directive(),
        )
        assert result.exit_code == expected_code
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock_connection()
def test_versioned_app_upgrade_to_unversioned(
    mock_conn,
    mock_get_existing_app_info,
    mock_execute,
    temp_dir,
    mock_cursor,
):
    """
    Ensure that attempting to upgrade from a versioned dev mode
    application to an unversioned one can succeed given a permissive policy.
    """
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "comment": SPECIAL_COMMENT,
        "owner": "app_role",
        "version": "v1",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                ProgrammingError(
                    msg="Some Error Message.",
                    errno=93045,
                ),
                mock.call(
                    "alter application myapp upgrade using @app_pkg.app_src.stage"
                ),
            ),
            (None, mock.call("drop application myapp")),
            (
                mock_cursor([{"CURRENT_ROLE()": "app_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                None,
                mock.call(
                    "grant install, develop on application package app_pkg to role app_role"
                ),
            ),
            (
                None,
                mock.call("grant usage on schema app_pkg.app_src to role app_role"),
            ),
            (
                None,
                mock.call("grant read on stage app_pkg.app_src.stage to role app_role"),
            ),
            (None, mock.call("use role app_role")),
            (
                None,
                mock.call(
                    dedent(
                        f"""\
            create application myapp
                from application package app_pkg using @app_pkg.app_src.stage debug_mode = True
                comment = {SPECIAL_COMMENT}
            """
                    )
                ),
            ),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    run_processor.create_or_upgrade_app(
        policy=AllowAlwaysPolicy(),
        is_interactive=False,
        install_method=SameAccountInstallMethod.unversioned_dev(),
    )
    assert mock_execute.mock_calls == expected


# Test upgrade app method for release directives AND existing app info AND upgrade fails due to upgrade restriction error AND --force is True AND drop fails
# Test upgrade app method for release directives AND existing app info AND upgrade fails due to upgrade restriction error AND --force is False AND interactive mode is False AND --interactive is True AND user wants to proceed AND drop fails
# Test upgrade app method for release directives AND existing app info AND upgrade fails due to upgrade restriction error AND --force is False AND interactive mode is True AND user wants to proceed AND drop fails
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(
    f"snowflake.cli._plugins.nativeapp.policy.{TYPER_CONFIRM}", return_value=True
)
@mock_connection()
@pytest.mark.parametrize(
    "policy_param, is_interactive_param",
    [(allow_always_policy, False), (ask_always_policy, True)],
)
def test_upgrade_app_fails_drop_fails(
    mock_conn,
    mock_typer_confirm,
    mock_get_existing_app_info,
    mock_execute,
    policy_param,
    is_interactive_param,
    temp_dir,
    mock_cursor,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "comment": SPECIAL_COMMENT,
        "owner": "app_role",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                ProgrammingError(
                    errno=CANNOT_UPGRADE_FROM_LOOSE_FILES_TO_VERSION,
                ),
                mock.call("alter application myapp upgrade "),
            ),
            (
                ProgrammingError(
                    errno=1234,
                ),
                mock.call("drop application myapp"),
            ),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    with pytest.raises(ProgrammingError):
        run_processor.create_or_upgrade_app(
            policy_param,
            is_interactive=is_interactive_param,
            install_method=SameAccountInstallMethod.release_directive(),
        )
    assert mock_execute.mock_calls == expected


# Test upgrade app method for release directives AND existing app info AND user wants to drop app AND drop succeeds AND app is created successfully.
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(
    f"snowflake.cli._plugins.nativeapp.policy.{TYPER_CONFIRM}", return_value=True
)
@mock_connection()
@pytest.mark.parametrize("policy_param", [allow_always_policy, ask_always_policy])
def test_upgrade_app_recreate_app(
    mock_conn,
    mock_typer_confirm,
    mock_get_existing_app_info,
    mock_execute,
    policy_param,
    temp_dir,
    mock_cursor,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "comment": SPECIAL_COMMENT,
        "owner": "app_role",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                ProgrammingError(
                    errno=CANNOT_UPGRADE_FROM_LOOSE_FILES_TO_VERSION,
                ),
                mock.call("alter application myapp upgrade "),
            ),
            (None, mock.call("drop application myapp")),
            (
                mock_cursor([{"CURRENT_ROLE()": "app_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                None,
                mock.call(
                    "grant install, develop on application package app_pkg to role app_role"
                ),
            ),
            (
                None,
                mock.call("grant usage on schema app_pkg.app_src to role app_role"),
            ),
            (
                None,
                mock.call("grant read on stage app_pkg.app_src.stage to role app_role"),
            ),
            (None, mock.call("use role app_role")),
            (
                None,
                mock.call(
                    dedent(
                        f"""\
            create application myapp
                from application package app_pkg  
                comment = {SPECIAL_COMMENT}
            """
                    )
                ),
            ),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    run_processor.create_or_upgrade_app(
        policy_param,
        is_interactive=True,
        install_method=SameAccountInstallMethod.release_directive(),
    )
    assert mock_execute.mock_calls == expected


# Test upgrade app method for version AND no existing version info
@mock.patch(
    "snowflake.cli._plugins.nativeapp.run_processor.NativeAppRunProcessor.get_existing_version_info",
    return_value=None,
)
@pytest.mark.parametrize(
    "policy_param", [allow_always_policy, ask_always_policy, deny_always_policy]
)
def test_upgrade_app_from_version_throws_usage_error_one(
    mock_existing, policy_param, temp_dir, mock_bundle_map
):

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    with pytest.raises(UsageError):
        run_processor.process(
            bundle_map=mock_bundle_map,
            policy=policy_param,
            version="v1",
            is_interactive=True,
        )


# Test upgrade app method for version AND no existing app package from version info
@mock.patch(
    "snowflake.cli._plugins.nativeapp.run_processor.NativeAppRunProcessor.get_existing_version_info",
    side_effect=ApplicationPackageDoesNotExistError("app_pkg"),
)
@pytest.mark.parametrize(
    "policy_param", [allow_always_policy, ask_always_policy, deny_always_policy]
)
def test_upgrade_app_from_version_throws_usage_error_two(
    mock_existing, policy_param, temp_dir, mock_bundle_map
):

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    with pytest.raises(UsageError):
        run_processor.process(
            bundle_map=mock_bundle_map,
            policy=policy_param,
            version="v1",
            is_interactive=True,
        )


# Test upgrade app method for version AND existing app info AND user wants to drop app AND drop succeeds AND app is created successfully
@mock.patch(
    "snowflake.cli._plugins.nativeapp.run_processor.NativeAppRunProcessor.get_existing_version_info",
    return_value={"key": "val"},
)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(RUN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(
    f"snowflake.cli._plugins.nativeapp.policy.{TYPER_CONFIRM}", return_value=True
)
@mock_connection()
@pytest.mark.parametrize("policy_param", [allow_always_policy, ask_always_policy])
def test_upgrade_app_recreate_app_from_version(
    mock_conn,
    mock_typer_confirm,
    mock_get_existing_app_info,
    mock_execute,
    mock_existing,
    policy_param,
    temp_dir,
    mock_cursor,
    mock_bundle_map,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "comment": SPECIAL_COMMENT,
        "owner": "app_role",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
            (
                ProgrammingError(
                    errno=CANNOT_UPGRADE_FROM_LOOSE_FILES_TO_VERSION,
                ),
                mock.call("alter application myapp upgrade using version v1 "),
            ),
            (None, mock.call("drop application myapp")),
            (
                mock_cursor([{"CURRENT_ROLE()": "app_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                None,
                mock.call(
                    "grant install, develop on application package app_pkg to role app_role"
                ),
            ),
            (
                None,
                mock.call("grant usage on schema app_pkg.app_src to role app_role"),
            ),
            (
                None,
                mock.call("grant read on stage app_pkg.app_src.stage to role app_role"),
            ),
            (None, mock.call("use role app_role")),
            (
                None,
                mock.call(
                    dedent(
                        f"""\
            create application myapp
                from application package app_pkg using version v1  debug_mode = True
                comment = {SPECIAL_COMMENT}
            """
                    )
                ),
            ),
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    run_processor = _get_na_run_processor()
    run_processor.process(
        bundle_map=mock_bundle_map,
        policy=policy_param,
        version="v1",
        is_interactive=True,
    )
    assert mock_execute.mock_calls == expected


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
                            "patch": 0,
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
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    processor = _get_na_run_processor()
    result = processor.get_existing_version_info(version)
    assert mock_execute.mock_calls == expected
    assert result["version"] == version
