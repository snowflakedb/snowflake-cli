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

from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest
from click import ClickException
from snowflake.cli.api.errno import (
    DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    NO_WAREHOUSE_SELECTED_IN_SESSION,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.plugins.nativeapp.exceptions import (
    InvalidScriptError,
    MissingScriptError,
)
from snowflake.cli.plugins.nativeapp.run_processor import NativeAppRunProcessor
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.patch_utils import mock_connection
from tests.nativeapp.utils import (
    NATIVEAPP_MANAGER_EXECUTE,
    NATIVEAPP_MANAGER_EXECUTE_QUERIES,
)
from tests.testing_utils.fixtures import MockConnectionCtx


def _get_na_manager(working_dir):
    dm = DefinitionManager(working_dir)
    return NativeAppRunProcessor(
        project_definition=dm.project_definition.native_app,
        project_root=dm.project_root,
    )


@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
@pytest.mark.parametrize(
    "project_definition_files, expected_calls",
    [
        (
            "napp_project_1",  # With connection warehouse, without PDF warehouse
            [
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ],
        ),
        (
            "napp_project_with_pkg_warehouse",  # With connection warehouse, with PDF warehouse
            [
                mock.call("select current_warehouse()", cursor_class=DictCursor),
                mock.call("use warehouse myapp_pkg_warehouse"),
                mock.call("use warehouse MockWarehouse"),
            ],
        ),
    ],
    indirect=["project_definition_files"],
)
def test_package_scripts_with_conn_info(
    mock_conn,
    mock_execute_query,
    mock_execute_queries,
    project_definition_files,
    expected_calls,
    mock_cursor,
):
    mock_conn.return_value = MockConnectionCtx()
    working_dir: Path = project_definition_files[0].parent
    # Only consequential for "select current_warehouse()"
    mock_execute_query.return_value = mock_cursor(
        [{"CURRENT_WAREHOUSE()": "MockWarehouse"}], []
    )
    native_app_manager = _get_na_manager(str(working_dir))
    native_app_manager._apply_package_scripts()  # noqa: SLF001
    assert mock_execute_query.mock_calls == expected_calls
    assert mock_execute_queries.mock_calls == [
        mock.call(
            dedent(
                f"""\
                    -- package script (1/2)

                    create schema if not exists myapp_pkg_polly.my_shared_content;
                    grant usage on schema myapp_pkg_polly.my_shared_content
                      to share in application package myapp_pkg_polly;
                """
            )
        ),
        mock.call(
            dedent(
                f"""\
                    -- package script (2/2)

                    create or replace table myapp_pkg_polly.my_shared_content.shared_table (
                      col1 number,
                      col2 varchar
                    );
                    grant select on table myapp_pkg_polly.my_shared_content.shared_table
                      to share in application package myapp_pkg_polly;
                """
            )
        ),
    ]


# Without connection warehouse, without PDF warehouse
@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_package_scripts_without_conn_info_throws_error(
    mock_conn,
    mock_execute_query,
    mock_execute_queries,
    project_definition_files,
    mock_cursor,
):
    mock_conn.return_value = MockConnectionCtx(warehouse=None)
    working_dir: Path = project_definition_files[0].parent
    mock_execute_query.return_value = mock_cursor([{"CURRENT_WAREHOUSE()": None}], [])
    native_app_manager = _get_na_manager(str(working_dir))
    with pytest.raises(ClickException) as err:
        native_app_manager._apply_package_scripts()  # noqa: SLF001

    assert "Application package warehouse cannot be empty." in err.value.message
    assert mock_execute_query.mock_calls == []
    assert mock_execute_queries.mock_calls == []


# Without connection warehouse, with PDF warehouse
@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
@pytest.mark.parametrize(
    "project_definition_files", ["napp_project_with_pkg_warehouse"], indirect=True
)
def test_package_scripts_without_conn_info_succeeds(
    mock_conn,
    mock_execute_query,
    mock_execute_queries,
    project_definition_files,
    mock_cursor,
):
    mock_conn.return_value = MockConnectionCtx(warehouse=None)
    working_dir: Path = project_definition_files[0].parent
    mock_execute_query.return_value = mock_cursor([{"CURRENT_WAREHOUSE()": None}], [])
    native_app_manager = _get_na_manager(str(working_dir))
    native_app_manager._apply_package_scripts()  # noqa: SLF001

    assert mock_execute_query.mock_calls == [
        mock.call("select current_warehouse()", cursor_class=DictCursor),
        mock.call("use warehouse myapp_pkg_warehouse"),
    ]
    assert mock_execute_queries.mock_calls == [
        mock.call(
            dedent(
                f"""\
                    -- package script (1/2)

                    create schema if not exists myapp_pkg_polly.my_shared_content;
                    grant usage on schema myapp_pkg_polly.my_shared_content
                      to share in application package myapp_pkg_polly;
                """
            )
        ),
        mock.call(
            dedent(
                f"""\
                    -- package script (2/2)

                    create or replace table myapp_pkg_polly.my_shared_content.shared_table (
                      col1 number,
                      col2 varchar
                    );
                    grant select on table myapp_pkg_polly.my_shared_content.shared_table
                      to share in application package myapp_pkg_polly;
                """
            )
        ),
    ]


@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_missing_package_script(mock_execute, project_definition_files):
    working_dir: Path = project_definition_files[0].parent
    native_app_manager = _get_na_manager(str(working_dir))
    with pytest.raises(MissingScriptError):
        (working_dir / "002-shared.sql").unlink()
        native_app_manager._apply_package_scripts()  # noqa: SLF001

    # even though the second script was the one missing, nothing should be executed
    assert mock_execute.mock_calls == []


@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_invalid_package_script(mock_execute, project_definition_files):
    working_dir: Path = project_definition_files[0].parent
    native_app_manager = _get_na_manager(str(working_dir))
    with pytest.raises(InvalidScriptError):
        second_file = working_dir / "002-shared.sql"
        second_file.unlink()
        second_file.write_text("select * from {{ package_name")
        native_app_manager._apply_package_scripts()  # noqa: SLF001

    # even though the second script was the one missing, nothing should be executed
    assert mock_execute.mock_calls == []


@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_undefined_var_package_script(mock_execute, project_definition_files):
    working_dir: Path = project_definition_files[0].parent
    native_app_manager = _get_na_manager(str(working_dir))
    with pytest.raises(InvalidScriptError):
        second_file = working_dir / "001-shared.sql"
        second_file.unlink()
        second_file.write_text("select * from {{ abc }}")
        native_app_manager._apply_package_scripts()  # noqa: SLF001

    assert mock_execute.mock_calls == []


@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_package_scripts_w_missing_warehouse_exception(
    mock_conn,
    mock_execute_query,
    mock_execute_queries,
    project_definition_files,
    mock_cursor,
):
    mock_conn.return_value = MockConnectionCtx()
    mock_execute_query.side_effect = [
        mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
        None,
        None,
    ]

    mock_execute_queries.side_effect = ProgrammingError(
        msg="No active warehouse selected in the current session.",
        errno=NO_WAREHOUSE_SELECTED_IN_SESSION,
    )

    working_dir: Path = project_definition_files[0].parent
    native_app_manager = _get_na_manager(str(working_dir))

    with pytest.raises(ProgrammingError) as err:
        native_app_manager._apply_package_scripts()  # noqa: SLF001

    assert "Please provide a warehouse for the active session role" in err.value.msg


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_package_scripts_w_warehouse_access_exception(
    mock_conn,
    mock_execute_query,
    project_definition_files,
    mock_cursor,
):
    side_effects = [
        mock_cursor([{"CURRENT_WAREHOUSE()": "old_wh"}], []),
        ProgrammingError(
            msg="Object does not exist, or operation cannot be performed.",
            errno=DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
        ),
        None,
    ]

    mock_conn.return_value = MockConnectionCtx()
    mock_execute_query.side_effect = side_effects

    working_dir: Path = project_definition_files[0].parent
    native_app_manager = _get_na_manager(str(working_dir))

    with pytest.raises(ProgrammingError) as err:
        native_app_manager._apply_package_scripts()  # noqa: SLF001

    assert (
        "Could not use warehouse MockWarehouse. Object does not exist, or operation cannot be performed."
        in err.value.msg
    )
