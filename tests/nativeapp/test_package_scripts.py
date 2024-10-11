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
from snowflake.cli._plugins.nativeapp.exceptions import (
    InvalidTemplateInFileError,
    MissingScriptError,
)
from snowflake.cli._plugins.nativeapp.run_processor import NativeAppRunProcessor
from snowflake.cli._plugins.nativeapp.sf_sql_facade import (
    SnowflakeSQLFacade,
    UnknownSQLError,
    UserScriptError,
)
from snowflake.cli.api.errno import (
    DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    NO_WAREHOUSE_SELECTED_IN_SESSION,
)
from snowflake.cli.api.exceptions import (
    CouldNotUseObjectError,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.connector import ProgrammingError

from tests.nativeapp.factories import PdfV10Factory, ProjectV10Factory
from tests.nativeapp.patch_utils import mock_connection
from tests.nativeapp.utils import (
    SQL_EXECUTOR_EXECUTE,
    SQL_EXECUTOR_EXECUTE_QUERIES,
)
from tests.testing_utils.fixtures import MockConnectionCtx


def _get_na_manager(working_dir):
    dm = DefinitionManager(working_dir)
    return NativeAppRunProcessor(
        project_definition=dm.project_definition.native_app,
        project_root=dm.project_root,
    )


def use_project_with_package_scripts():
    package_script_1 = dedent(
        """\
        -- package script (1/2)
        create schema if not exists {{ package_name }}.my_shared_content;
        grant usage on schema {{ package_name }}.my_shared_content
        to share in application package {{ package_name }};
        """
    )
    package_script_2 = dedent(
        """\
        grant select on table {{ package_name }}.my_shared_content.shared_table
          to share in application package {{ package_name }};
        """
    )

    ProjectV10Factory(
        pdf__native_app__name="myapp",
        pdf__native_app__package__name="myapp_pkg_polly",
        pdf__native_app__artifacts=["setup.sql"],
        pdf__native_app__package__scripts=["001-shared.sql", "002-shared.sql"],
        files={"001-shared.sql": package_script_1, "002-shared.sql": package_script_2},
    )

    rendered_script_1 = dedent(
        """\
        -- package script (1/2)
        create schema if not exists myapp_pkg_polly.my_shared_content;
        grant usage on schema myapp_pkg_polly.my_shared_content
        to share in application package myapp_pkg_polly;
        """
    )
    rendered_script_2 = dedent(
        """\
        grant select on table myapp_pkg_polly.my_shared_content.shared_table
          to share in application package myapp_pkg_polly;
        """
    )
    return {"001-shared.sql": rendered_script_1, "002-shared.sql": rendered_script_2}


@mock_connection()
def test_package_scripts_with_conn_warehouse(
    mock_conn,
    temp_dir,
):
    scripts = use_project_with_package_scripts()
    with mock.patch.object(
        SnowflakeSQLFacade, "execute_user_script"
    ) as mock_execute_user_script:
        mock_execute_user_script.return_value = None
        native_app_manager = _get_na_manager(str(temp_dir))
        native_app_manager._apply_package_scripts()  # noqa: SLF001
        assert mock_execute_user_script.call_count == 2
        expected_calls = [
            mock.call(query[1], query[0], "role", "wh")
            for query in list(scripts.items())
        ]
        mock_execute_user_script.assert_has_calls(expected_calls, any_order=False)


@mock_connection()
def test_package_scripts_with_pdf_warehouse(
    mock_conn,
    temp_dir,
):
    scripts = use_project_with_package_scripts()
    PdfV10Factory.with_filename("snowflake.local.yml")(
        native_app__package__warehouse="myapp_pkg_warehouse"
    )
    with mock.patch.object(
        SnowflakeSQLFacade, "execute_user_script"
    ) as mock_execute_user_script:
        mock_execute_user_script.return_value = None
        native_app_manager = _get_na_manager(str(temp_dir))
        native_app_manager._apply_package_scripts()  # noqa: SLF001
        assert mock_execute_user_script.call_count == 2
        expected_calls = [
            mock.call(query[1], query[0], "role", "myapp_pkg_warehouse")
            for query in list(scripts.items())
        ]
        mock_execute_user_script.assert_has_calls(expected_calls, any_order=False)


# Without connection warehouse, with PDF warehouse
@mock_connection()
def test_package_scripts_without_conn_warehouse_with_pkg_warehouse(mock_conn, temp_dir):
    mock_conn.return_value = MockConnectionCtx(warehouse=None)
    scripts = use_project_with_package_scripts()
    PdfV10Factory.with_filename("snowflake.local.yml")(
        native_app__package__warehouse="myapp_pkg_warehouse"
    )

    with mock.patch.object(
        SnowflakeSQLFacade, "execute_user_script"
    ) as mock_execute_user_script:
        mock_execute_user_script.return_value = None
        native_app_manager = _get_na_manager(str(temp_dir))
        native_app_manager._apply_package_scripts()  # noqa: SLF001
        assert mock_execute_user_script.call_count == 2
        expected_calls = [
            mock.call(query[1], query[0], "MockRole", "myapp_pkg_warehouse")
            for query in list(scripts.items())
        ]
        mock_execute_user_script.assert_has_calls(expected_calls, any_order=False)


@pytest.mark.parametrize(
    "error_thrown, error_raised, error_messages",
    [
        (
            ProgrammingError(errno=NO_WAREHOUSE_SELECTED_IN_SESSION),
            UserScriptError,
            [
                "Failed to run script 001-shared.sql",
                "Please provide a warehouse in your project definition file, config.toml file, or via command line",
            ],
        ),
        (ProgrammingError(), UserScriptError, ["Failed to run script 001-shared.sql"]),
        (
            Exception(),
            UnknownSQLError,
            ["Unknown SQL error occurred", "Failed to run script 001-shared.sql"],
        ),
    ],
)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
def test_package_scripts_catches_raises_errors(
    mock_conn,
    mock_execute_query,
    mock_execute_queries,
    mock_cursor,
    error_thrown,
    error_raised,
    error_messages,
    temp_dir,
):
    use_project_with_package_scripts()
    mock_conn.return_value = MockConnectionCtx(warehouse=None)

    side_effects = [
        mock_cursor([("MockRole",)], []),
    ]

    mock_execute_query.side_effects = side_effects
    mock_execute_queries.side_effect = error_thrown

    native_app_manager = _get_na_manager(str(temp_dir))
    with pytest.raises(error_raised) as err:
        native_app_manager._apply_package_scripts()  # noqa: SLF001

    for error_message in error_messages:
        assert error_message in err.value.message


@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
@mock_connection()
@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_missing_package_script(mock_conn, mock_execute, project_definition_files):
    mock_conn.return_value = MockConnectionCtx()
    working_dir: Path = project_definition_files[0].parent
    native_app_manager = _get_na_manager(str(working_dir))
    with pytest.raises(MissingScriptError):
        (working_dir / "002-shared.sql").unlink()
        native_app_manager._apply_package_scripts()  # noqa: SLF001

    # even though the second script was the one missing, nothing should be executed
    assert mock_execute.mock_calls == []


@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
@mock_connection()
@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_invalid_package_script(mock_conn, mock_execute, project_definition_files):
    mock_conn.return_value = MockConnectionCtx()
    working_dir: Path = project_definition_files[0].parent
    native_app_manager = _get_na_manager(str(working_dir))
    with pytest.raises(InvalidTemplateInFileError):
        second_file = working_dir / "002-shared.sql"
        second_file.unlink()
        second_file.write_text("select * from {{ package_name")
        native_app_manager._apply_package_scripts()  # noqa: SLF001

    # even though the second script was the one missing, nothing should be executed
    assert mock_execute.mock_calls == []


@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
@mock_connection()
@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_undefined_var_package_script(
    mock_conn, mock_execute, project_definition_files
):
    mock_conn.return_value = MockConnectionCtx()
    working_dir: Path = project_definition_files[0].parent
    native_app_manager = _get_na_manager(str(working_dir))
    with pytest.raises(InvalidTemplateInFileError):
        second_file = working_dir / "001-shared.sql"
        second_file.unlink()
        second_file.write_text("select * from {{ abc }}")
        native_app_manager._apply_package_scripts()  # noqa: SLF001

    assert mock_execute.mock_calls == []


# TODO: Move to tests for execute_user_script
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_package_scripts_w_warehouse_access_exception(
    mock_conn,
    mock_execute_query,
    project_definition_files,
    mock_cursor,
):
    side_effects = [
        mock_cursor([("accountadmin",)], []),
        mock_cursor([("old_wh",)], []),
        ProgrammingError(errno=DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED),
        None,
    ]

    mock_conn.return_value = MockConnectionCtx()
    mock_execute_query.side_effect = side_effects

    working_dir: Path = project_definition_files[0].parent
    native_app_manager = _get_na_manager(str(working_dir))

    with pytest.raises(CouldNotUseObjectError) as err:
        native_app_manager._apply_package_scripts()  # noqa: SLF001

    assert (
        "Could not use warehouse MockWarehouse. Object does not exist, or operation cannot be performed."
        in err.value.message
    )
