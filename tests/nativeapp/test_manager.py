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
from __future__ import annotations

import json
import os
from pathlib import Path
from textwrap import dedent
from typing import Optional
from unittest import mock
from unittest.mock import call

import pytest
from click import ClickException
from snowflake.cli.api.errno import DOES_NOT_EXIST_OR_NOT_AUTHORIZED
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.plugins.nativeapp.artifacts import BundleMap
from snowflake.cli.plugins.nativeapp.constants import (
    LOOSE_FILES_MAGIC_VERSION,
    NAME_COL,
    SPECIAL_COMMENT,
    SPECIAL_COMMENT_OLD,
)
from snowflake.cli.plugins.nativeapp.exceptions import (
    ApplicationPackageAlreadyExistsError,
    ApplicationPackageDoesNotExistError,
    NoEventTableForAccount,
    SetupScriptFailedValidation,
    UnexpectedOwnerError,
)
from snowflake.cli.plugins.nativeapp.manager import (
    NativeAppManager,
    SnowflakeSQLExecutionError,
    _get_stage_paths_to_sync,
    ensure_correct_owner,
)
from snowflake.cli.plugins.stage.diff import (
    DiffResult,
    StagePath,
)
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.patch_utils import (
    mock_connection,
    mock_get_app_pkg_distribution_in_sf,
)
from tests.nativeapp.utils import (
    NATIVEAPP_MANAGER_ACCOUNT_EVENT_TABLE,
    NATIVEAPP_MANAGER_BUILD_BUNDLE,
    NATIVEAPP_MANAGER_DEPLOY,
    NATIVEAPP_MANAGER_EXECUTE,
    NATIVEAPP_MANAGER_GET_EXISTING_APP_PKG_INFO,
    NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME,
    NATIVEAPP_MODULE,
    mock_execute_helper,
    mock_snowflake_yml_file,
    quoted_override_yml_file,
    touch,
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


def _get_na_manager(working_dir: Optional[str] = None):
    dm = DefinitionManager(working_dir)
    return NativeAppManager(
        project_definition=dm.project_definition.native_app,
        project_root=dm.project_root,
    )


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(f"{NATIVEAPP_MODULE}.compute_stage_diff")
@mock.patch(f"{NATIVEAPP_MODULE}.sync_local_diff_with_stage")
def test_sync_deploy_root_with_stage(
    mock_local_diff_with_stage,
    mock_compute_stage_diff,
    mock_execute,
    temp_dir,
    mock_cursor,
):
    mock_execute.return_value = mock_cursor([{"CURRENT_ROLE()": "old_role"}], [])
    mock_diff_result = DiffResult(different=[StagePath("setup.sql")])
    mock_compute_stage_diff.return_value = mock_diff_result
    mock_local_diff_with_stage.return_value = None
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    assert mock_diff_result.has_changes()
    mock_bundle_map = mock.Mock(spec=BundleMap)
    native_app_manager.sync_deploy_root_with_stage(
        bundle_map=mock_bundle_map,
        role="new_role",
        prune=True,
        recursive=True,
        stage_fqn=native_app_manager.stage_fqn,
    )

    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role new_role"),
        mock.call(f"create schema if not exists app_pkg.app_src"),
        mock.call(
            f"""
                    create stage if not exists app_pkg.app_src.stage
                    encryption = (TYPE = 'SNOWFLAKE_SSE')
                    DIRECTORY = (ENABLE = TRUE)"""
        ),
        mock.call("use role old_role"),
    ]
    assert mock_execute.mock_calls == expected
    mock_compute_stage_diff.assert_called_once_with(
        native_app_manager.deploy_root, "app_pkg.app_src.stage"
    )
    mock_local_diff_with_stage.assert_called_once_with(
        role="new_role",
        deploy_root_path=native_app_manager.deploy_root,
        diff_result=mock_diff_result,
        stage_fqn="app_pkg.app_src.stage",
    )


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(f"{NATIVEAPP_MODULE}.sync_local_diff_with_stage")
@mock.patch(f"{NATIVEAPP_MODULE}.compute_stage_diff")
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
@pytest.mark.parametrize(
    "prune,only_on_stage_files,expected_warn",
    [
        [
            True,
            [StagePath("only-stage.txt")],
            False,
        ],
        [
            False,
            [StagePath("only-stage-1.txt"), StagePath("only-stage-2.txt")],
            True,
        ],
    ],
)
def test_sync_deploy_root_with_stage_prune(
    mock_warning,
    mock_compute_stage_diff,
    mock_local_diff_with_stage,
    mock_execute,
    prune,
    only_on_stage_files,
    expected_warn,
    temp_dir,
):
    mock_compute_stage_diff.return_value = DiffResult(only_on_stage=only_on_stage_files)
    create_named_file(
        file_name="snowflake.yml",
        dir_name=os.getcwd(),
        contents=[mock_snowflake_yml_file],
    )
    native_app_manager = _get_na_manager()

    mock_bundle_map = mock.Mock(spec=BundleMap)
    native_app_manager.sync_deploy_root_with_stage(
        bundle_map=mock_bundle_map,
        role="new_role",
        prune=prune,
        recursive=True,
        stage_fqn=native_app_manager.stage_fqn,
    )

    if expected_warn:
        files_str = "\n".join([str(f) for f in only_on_stage_files])
        warn_message = f"""The following files exist only on the stage:
{files_str}

Use the --prune flag to delete them from the stage."""
        mock_warning.assert_called_once_with(warn_message)
    else:
        mock_warning.assert_not_called()


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
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
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
                    msg="Application package app_pkg does not exist or not authorized.",
                    errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED,
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
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
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
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
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
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    with pytest.raises(ProgrammingError):
        native_app_manager.get_app_pkg_distribution_in_snowflake

    assert mock_execute.mock_calls == expected


@mock_get_app_pkg_distribution_in_sf()
def test_is_app_pkg_distribution_same_in_sf_w_arg(mock_mismatch, temp_dir):

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    create_named_file(
        file_name="snowflake.local.yml",
        dir_name=current_working_directory,
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

    native_app_manager = _get_na_manager()
    assert native_app_manager.verify_project_distribution("internal") is False
    mock_mismatch.assert_not_called()


@mock_get_app_pkg_distribution_in_sf()
def test_is_app_pkg_distribution_same_in_sf_no_mismatch(mock_mismatch, temp_dir):
    mock_mismatch.return_value = "external"

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    create_named_file(
        file_name="snowflake.local.yml",
        dir_name=current_working_directory,
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

    native_app_manager = _get_na_manager()
    assert native_app_manager.verify_project_distribution() is True


@mock_get_app_pkg_distribution_in_sf()
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
def test_is_app_pkg_distribution_same_in_sf_has_mismatch(
    mock_warning, mock_mismatch, temp_dir
):
    mock_mismatch.return_value = "external"

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    assert native_app_manager.verify_project_distribution() is False
    mock_warning.assert_called_once_with(
        "Application package app_pkg in your Snowflake account has distribution property external,\nwhich does not match the value specified in project definition file: internal.\n"
    )


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_existing_app_info_app_exists(mock_execute, temp_dir, mock_cursor):
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
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    show_obj_row = native_app_manager.get_existing_app_info()
    assert show_obj_row is not None
    assert show_obj_row[NAME_COL] == "MYAPP"
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_existing_app_info_app_does_not_exist(mock_execute, temp_dir, mock_cursor):
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
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    show_obj_row = native_app_manager.get_existing_app_info()
    assert show_obj_row is None
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_existing_app_pkg_info_app_pkg_exists(mock_execute, temp_dir, mock_cursor):
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
                            "owner": "package_role",
                        }
                    ],
                    [],
                ),
                mock.call(
                    r"show application packages like 'APP\\_PKG'",
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

    native_app_manager = _get_na_manager()
    show_obj_row = native_app_manager.get_existing_app_pkg_info()
    assert show_obj_row is not None
    assert show_obj_row[NAME_COL] == "APP_PKG"
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_existing_app_pkg_info_app_pkg_does_not_exist(
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
                mock_cursor([], []),
                mock.call(
                    r"show application packages like 'APP\\_PKG'",
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

    native_app_manager = _get_na_manager()
    show_obj_row = native_app_manager.get_existing_app_pkg_info()
    assert show_obj_row is None
    assert mock_execute.mock_calls == expected


# With connection warehouse, with PDF warehouse
# Without connection warehouse, with PDF warehouse
@mock.patch("snowflake.cli.plugins.connection.util.get_context")
@mock.patch("snowflake.cli.plugins.connection.util.get_account")
@mock.patch("snowflake.cli.plugins.connection.util.get_snowsight_host")
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
@pytest.mark.parametrize(
    "warehouse, fallback_warehouse_call, fallback_side_effect",
    [
        (
            "MockWarehouse",
            [mock.call("use warehouse MockWarehouse")],
            [None],
        ),
        (
            None,
            [],
            [],
        ),
    ],
)
def test_get_snowsight_url_with_pdf_warehouse(
    mock_conn,
    mock_execute_query,
    mock_snowsight_host,
    mock_account,
    mock_context,
    warehouse,
    fallback_warehouse_call,
    fallback_side_effect,
    temp_dir,
    mock_cursor,
):
    mock_conn.return_value = MockConnectionCtx(warehouse=warehouse)
    mock_snowsight_host.return_value = "https://host"
    mock_context.return_value = "organization"
    mock_account.return_value = "account"

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_WAREHOUSE()": warehouse}], []),
                mock.call("select current_warehouse()", cursor_class=DictCursor),
            ),
            (None, mock.call("use warehouse app_warehouse")),
        ]
    )
    mock_execute_query.side_effect = side_effects + fallback_side_effect

    native_app_manager = _get_na_manager()
    assert (
        native_app_manager.get_snowsight_url()
        == "https://host/organization/account/#/apps/application/MYAPP"
    )
    assert mock_execute_query.mock_calls == expected + fallback_warehouse_call


# With connection warehouse, without PDF warehouse
# Without connection warehouse, without PDF warehouse
@mock.patch("snowflake.cli.plugins.connection.util.get_context")
@mock.patch("snowflake.cli.plugins.connection.util.get_account")
@mock.patch("snowflake.cli.plugins.connection.util.get_snowsight_host")
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_connection()
@pytest.mark.parametrize(
    "project_definition_files, warehouse, expected_calls, fallback_side_effect",
    [
        (
            "napp_project_1",
            "MockWarehouse",
            [mock.call("select current_warehouse()", cursor_class=DictCursor)],
            [None],
        ),
        (
            "napp_project_1",
            None,
            [],
            [],
        ),
    ],
    indirect=["project_definition_files"],
)
def test_get_snowsight_url_without_pdf_warehouse(
    mock_conn,
    mock_execute_query,
    mock_snowsight_host,
    mock_account,
    mock_context,
    project_definition_files,
    warehouse,
    expected_calls,
    fallback_side_effect,
    mock_cursor,
):
    mock_conn.return_value = MockConnectionCtx(warehouse=warehouse)
    mock_snowsight_host.return_value = "https://host"
    mock_context.return_value = "organization"
    mock_account.return_value = "account"

    working_dir: Path = project_definition_files[0].parent

    mock_execute_query.side_effect = [
        mock_cursor([{"CURRENT_WAREHOUSE()": warehouse}], [])
    ] + fallback_side_effect

    native_app_manager = _get_na_manager(str(working_dir))
    if warehouse:
        assert (
            native_app_manager.get_snowsight_url()
            == "https://host/organization/account/#/apps/application/MYAPP_POLLY"
        )
    else:
        with pytest.raises(ClickException) as err:
            native_app_manager.get_snowsight_url()
        assert "Application warehouse cannot be empty." in err.value.message

    assert mock_execute_query.mock_calls == expected_calls


def test_ensure_correct_owner():
    test_row = {"name": "some_name", "owner": "some_role", "comment": "some_comment"}
    assert (
        ensure_correct_owner(row=test_row, role="some_role", obj_name="some_name")
        is None
    )


def test_is_correct_owner_bad_owner():
    test_row = {"name": "some_name", "owner": "wrong_role", "comment": "some_comment"}
    with pytest.raises(UnexpectedOwnerError):
        ensure_correct_owner(row=test_row, role="right_role", obj_name="some_name")


# Test create_app_package() with no existing package available
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(NATIVEAPP_MANAGER_GET_EXISTING_APP_PKG_INFO, return_value=None)
def test_create_app_pkg_no_existing_package(
    mock_get_existing_app_pkg_info, mock_execute, temp_dir, mock_cursor
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
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    native_app_manager.create_app_package()
    assert mock_execute.mock_calls == expected
    mock_get_existing_app_pkg_info.assert_called_once()


# Test create_app_package() with incorrect owner
@mock.patch(NATIVEAPP_MANAGER_GET_EXISTING_APP_PKG_INFO)
def test_create_app_pkg_incorrect_owner(mock_get_existing_app_pkg_info, temp_dir):
    mock_get_existing_app_pkg_info.return_value = {
        "name": "APP_PKG",
        "comment": SPECIAL_COMMENT,
        "version": LOOSE_FILES_MAGIC_VERSION,
        "owner": "wrong_owner",
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    with pytest.raises(UnexpectedOwnerError):
        native_app_manager = _get_na_manager()
        native_app_manager.create_app_package()


# Test create_app_package() with distribution external AND variable mismatch
@mock.patch(NATIVEAPP_MANAGER_GET_EXISTING_APP_PKG_INFO)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME)
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
@pytest.mark.parametrize(
    "is_pkg_distribution_same",
    [False, True],
)
def test_create_app_pkg_external_distribution(
    mock_warning,
    mock_is_distribution_same,
    mock_get_distribution,
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

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    native_app_manager.create_app_package()
    if not is_pkg_distribution_same:
        mock_warning.assert_called_once_with(
            "Continuing to execute `snow app run` on application package app_pkg with distribution 'external'."
        )


# Test create_app_package() with distribution internal AND variable mismatch AND special comment is True
@mock.patch(NATIVEAPP_MANAGER_GET_EXISTING_APP_PKG_INFO)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME)
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
@pytest.mark.parametrize(
    "is_pkg_distribution_same, special_comment",
    [
        (False, SPECIAL_COMMENT),
        (False, SPECIAL_COMMENT_OLD),
        (True, SPECIAL_COMMENT),
        (True, SPECIAL_COMMENT_OLD),
    ],
)
def test_create_app_pkg_internal_distribution_special_comment(
    mock_warning,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_get_existing_app_pkg_info,
    is_pkg_distribution_same,
    special_comment,
    temp_dir,
):
    mock_is_distribution_same.return_value = is_pkg_distribution_same
    mock_get_distribution.return_value = "internal"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "APP_PKG",
        "comment": special_comment,
        "version": LOOSE_FILES_MAGIC_VERSION,
        "owner": "PACKAGE_ROLE",
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    native_app_manager.create_app_package()
    if not is_pkg_distribution_same:
        mock_warning.assert_called_once_with(
            "Continuing to execute `snow app run` on application package app_pkg with distribution 'internal'."
        )


# Test create_app_package() with distribution internal AND variable mismatch AND special comment is False
@mock.patch(NATIVEAPP_MANAGER_GET_EXISTING_APP_PKG_INFO)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME)
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
@pytest.mark.parametrize(
    "is_pkg_distribution_same",
    [False, True],
)
def test_create_app_pkg_internal_distribution_no_special_comment(
    mock_warning,
    mock_is_distribution_same,
    mock_get_distribution,
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

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    with pytest.raises(ApplicationPackageAlreadyExistsError):
        native_app_manager.create_app_package()

    if not is_pkg_distribution_same:
        mock_warning.assert_called_once_with(
            "Continuing to execute `snow app run` on application package app_pkg with distribution 'internal'."
        )


@pytest.mark.parametrize(
    "paths_to_sync,expected_result",
    [
        [
            ["deploy/dir"],
            ["dir/nested_file1", "dir/nested_file2", "dir/nested_dir/nested_file3"],
        ],
        [["deploy/dir/nested_dir"], ["dir/nested_dir/nested_file3"]],
        [
            ["deploy/file", "deploy/dir/nested_dir/nested_file3"],
            ["file", "dir/nested_dir/nested_file3"],
        ],
    ],
)
def test_get_paths_to_sync(
    temp_dir,
    paths_to_sync,
    expected_result,
):
    touch("deploy/file")
    touch("deploy/dir/nested_file1")
    touch("deploy/dir/nested_file2")
    touch("deploy/dir/nested_dir/nested_file3")

    paths_to_sync = [Path(p) for p in paths_to_sync]
    result = _get_stage_paths_to_sync(paths_to_sync, Path("deploy/"))
    assert result.sort() == [StagePath(p) for p in expected_result].sort()


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_validate_passing(mock_execute, temp_dir, mock_cursor):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temp_dir,
        contents=[mock_snowflake_yml_file],
    )

    success_data = dict(status="SUCCESS")
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([[json.dumps(success_data)]], []),
                mock.call(
                    "call system$validate_native_app_setup('@app_pkg.app_src.stage')"
                ),
            ),
        ]
    )
    mock_execute.side_effect = side_effects

    native_app_manager = _get_na_manager()
    native_app_manager.validate()

    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
def test_validate_passing_with_warnings(
    mock_warning, mock_execute, temp_dir, mock_cursor
):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temp_dir,
        contents=[mock_snowflake_yml_file],
    )

    warning_file = "@STAGE/setup_script.sql"
    warning_cause = "APPLICATION ROLE should be created with IF NOT EXISTS."
    warning = dict(
        message=f"Warning in file {warning_file}: {warning_cause}",
        cause=warning_cause,
        errorCode="093352",
        fileName=warning_file,
        line=11,
        column=35,
    )
    failure_data = dict(status="SUCCESS", errors=[], warnings=[warning])
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([[json.dumps(failure_data)]], []),
                mock.call(
                    "call system$validate_native_app_setup('@app_pkg.app_src.stage')"
                ),
            ),
        ]
    )
    mock_execute.side_effect = side_effects

    native_app_manager = _get_na_manager()
    native_app_manager.validate()

    warn_message = f"{warning['message']} (error code {warning['errorCode']})"
    mock_warning.assert_called_once_with(warn_message)
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
def test_validate_failing(mock_warning, mock_execute, temp_dir, mock_cursor):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temp_dir,
        contents=[mock_snowflake_yml_file],
    )

    error_file = "@STAGE/empty.sql"
    error_cause = "Empty SQL statement."
    error = dict(
        message=f"Error in file {error_file}: {error_cause}",
        cause=error_cause,
        errorCode="000900",
        fileName=error_file,
        line=-1,
        column=-1,
    )
    warning_file = "@STAGE/setup_script.sql"
    warning_cause = "APPLICATION ROLE should be created with IF NOT EXISTS."
    warning = dict(
        message=f"Warning in file {warning_file}: {warning_cause}",
        cause=warning_cause,
        errorCode="093352",
        fileName=warning_file,
        line=11,
        column=35,
    )
    failure_data = dict(status="FAIL", errors=[error], warnings=[warning])
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([[json.dumps(failure_data)]], []),
                mock.call(
                    "call system$validate_native_app_setup('@app_pkg.app_src.stage')"
                ),
            ),
        ]
    )
    mock_execute.side_effect = side_effects

    native_app_manager = _get_na_manager()
    with pytest.raises(
        SetupScriptFailedValidation,
        match="Snowflake Native App setup script failed validation.",
    ):
        native_app_manager.validate()

    warn_message = f"{warning['message']} (error code {warning['errorCode']})"
    error_message = f"{error['message']} (error code {error['errorCode']})"
    mock_warning.assert_has_calls(
        [call(warn_message), call(error_message)], any_order=False
    )
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_validate_query_error(mock_execute, temp_dir, mock_cursor):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temp_dir,
        contents=[mock_snowflake_yml_file],
    )

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([], []),
                mock.call(
                    "call system$validate_native_app_setup('@app_pkg.app_src.stage')"
                ),
            ),
        ]
    )
    mock_execute.side_effect = side_effects

    native_app_manager = _get_na_manager()
    with pytest.raises(SnowflakeSQLExecutionError):
        native_app_manager.validate()

    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_validate_not_deployed(mock_execute, temp_dir, mock_cursor):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temp_dir,
        contents=[mock_snowflake_yml_file],
    )

    side_effects, expected = mock_execute_helper(
        [
            (
                ProgrammingError(
                    msg="Application package app_pkg does not exist or not authorized.",
                    errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED,
                ),
                mock.call(
                    "call system$validate_native_app_setup('@app_pkg.app_src.stage')"
                ),
            ),
        ]
    )
    mock_execute.side_effect = side_effects

    native_app_manager = _get_na_manager()
    with pytest.raises(ApplicationPackageDoesNotExistError, match="app_pkg"):
        native_app_manager.validate()

    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_BUILD_BUNDLE)
@mock.patch(NATIVEAPP_MANAGER_DEPLOY)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_validate_use_scratch_stage(
    mock_execute, mock_deploy, mock_build_bundle, temp_dir, mock_cursor
):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temp_dir,
        contents=[mock_snowflake_yml_file],
    )

    success_data = dict(status="SUCCESS")
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([[json.dumps(success_data)]], []),
                mock.call(
                    "call system$validate_native_app_setup('@app_pkg.app_src.stage_snowflake_cli_scratch')"
                ),
            ),
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call(
                    f"drop stage if exists app_pkg.app_src.stage_snowflake_cli_scratch"
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    native_app_manager = _get_na_manager()
    native_app_manager.validate(use_scratch_stage=True)

    mock_build_bundle.assert_called_once()
    mock_deploy.assert_called_with(
        bundle_map=mock_build_bundle.return_value,
        prune=True,
        recursive=True,
        stage_fqn=native_app_manager.scratch_stage_fqn,
        validate=False,
        print_diff=False,
    )
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_BUILD_BUNDLE)
@mock.patch(NATIVEAPP_MANAGER_DEPLOY)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_validate_failing_drops_scratch_stage(
    mock_execute, mock_deploy, mock_build_bundle, temp_dir, mock_cursor
):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temp_dir,
        contents=[mock_snowflake_yml_file],
    )

    error_file = "@STAGE/empty.sql"
    error_cause = "Empty SQL statement."
    error = dict(
        message=f"Error in file {error_file}: {error_cause}",
        cause=error_cause,
        errorCode="000900",
        fileName=error_file,
        line=-1,
        column=-1,
    )
    failure_data = dict(status="FAIL", errors=[error], warnings=[])
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([[json.dumps(failure_data)]], []),
                mock.call(
                    "call system$validate_native_app_setup('@app_pkg.app_src.stage_snowflake_cli_scratch')"
                ),
            ),
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call(
                    f"drop stage if exists app_pkg.app_src.stage_snowflake_cli_scratch"
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    native_app_manager = _get_na_manager()
    with pytest.raises(
        SetupScriptFailedValidation,
        match="Snowflake Native App setup script failed validation.",
    ):
        native_app_manager.validate(use_scratch_stage=True)

    mock_build_bundle.assert_called_once()
    mock_deploy.assert_called_with(
        bundle_map=mock_build_bundle.return_value,
        prune=True,
        recursive=True,
        stage_fqn=native_app_manager.scratch_stage_fqn,
        validate=False,
        print_diff=False,
    )
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_validate_raw_returns_data(mock_execute, temp_dir, mock_cursor):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temp_dir,
        contents=[mock_snowflake_yml_file],
    )

    error_file = "@STAGE/empty.sql"
    error_cause = "Empty SQL statement."
    error = dict(
        message=f"Error in file {error_file}: {error_cause}",
        cause=error_cause,
        errorCode="000900",
        fileName=error_file,
        line=-1,
        column=-1,
    )
    warning_file = "@STAGE/setup_script.sql"
    warning_cause = "APPLICATION ROLE should be created with IF NOT EXISTS."
    warning = dict(
        message=f"Warning in file {warning_file}: {warning_cause}",
        cause=warning_cause,
        errorCode="093352",
        fileName=warning_file,
        line=11,
        column=35,
    )
    failure_data = dict(status="FAIL", errors=[error], warnings=[warning])
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([[json.dumps(failure_data)]], []),
                mock.call(
                    "call system$validate_native_app_setup('@app_pkg.app_src.stage')"
                ),
            ),
        ]
    )
    mock_execute.side_effect = side_effects

    native_app_manager = _get_na_manager()
    assert (
        native_app_manager.get_validation_result(use_scratch_stage=False)
        == failure_data
    )
    assert mock_execute.mock_calls == expected


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_account_event_table(mock_execute, temp_dir, mock_cursor):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temp_dir,
        contents=[mock_snowflake_yml_file],
    )

    event_table = "db.schema.event_table"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([dict(key="EVENT_TABLE", value=event_table)], []),
                mock.call(
                    "show parameters like 'event_table' in account",
                    cursor_class=DictCursor,
                ),
            ),
        ]
    )
    mock_execute.side_effect = side_effects

    native_app_manager = _get_na_manager()
    assert native_app_manager.account_event_table == event_table


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_account_event_table_not_set_up(mock_execute, temp_dir, mock_cursor):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temp_dir,
        contents=[mock_snowflake_yml_file],
    )

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([], []),
                mock.call(
                    "show parameters like 'event_table' in account",
                    cursor_class=DictCursor,
                ),
            ),
        ]
    )
    mock_execute.side_effect = side_effects

    native_app_manager = _get_na_manager()
    assert native_app_manager.account_event_table == ""


@mock.patch(
    NATIVEAPP_MANAGER_ACCOUNT_EVENT_TABLE,
    return_value="db.schema.event_table",
    new_callable=mock.PropertyMock,
)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_events(mock_execute, mock_account_event_table, temp_dir, mock_cursor):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temp_dir,
        contents=[mock_snowflake_yml_file],
    )

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([dict(TIMESTAMP="2020-01-01T00:00:00Z", VALUE="test")], []),
                mock.call(
                    dedent(
                        f"""\
                        select timestamp, value::varchar value
                        from db.schema.event_table
                        where resource_attributes:"snow.database.name" = 'MYAPP'
                        order by timestamp asc;"""
                    ),
                    cursor_class=DictCursor,
                ),
            ),
        ]
    )
    mock_execute.side_effect = side_effects

    native_app_manager = _get_na_manager()
    assert native_app_manager.get_events() == [
        dict(TIMESTAMP="2020-01-01T00:00:00Z", VALUE="test")
    ]
    assert mock_execute.mock_calls == expected


@mock.patch(
    NATIVEAPP_MANAGER_ACCOUNT_EVENT_TABLE,
    return_value="db.schema.event_table",
    new_callable=mock.PropertyMock,
)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_get_events_quoted_app_name(
    mock_execute, mock_account_event_table, temp_dir, mock_cursor
):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temp_dir,
        contents=[mock_snowflake_yml_file],
    )
    create_named_file(
        file_name="snowflake.local.yml",
        dir_name=temp_dir,
        contents=[quoted_override_yml_file],
    )

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([dict(TIMESTAMP="2020-01-01T00:00:00Z", VALUE="test")], []),
                mock.call(
                    dedent(
                        f"""\
                        select timestamp, value::varchar value
                        from db.schema.event_table
                        where resource_attributes:"snow.database.name" = 'My Application'
                        order by timestamp asc;"""
                    ),
                    cursor_class=DictCursor,
                ),
            ),
        ]
    )
    mock_execute.side_effect = side_effects

    native_app_manager = _get_na_manager()
    assert native_app_manager.get_events() == [
        dict(TIMESTAMP="2020-01-01T00:00:00Z", VALUE="test")
    ]
    assert mock_execute.mock_calls == expected


@mock.patch(
    NATIVEAPP_MANAGER_ACCOUNT_EVENT_TABLE,
    return_value=None,
    new_callable=mock.PropertyMock,
)
def test_get_events_no_event_table(mock_account_event_table, temp_dir, mock_cursor):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temp_dir,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = _get_na_manager()
    with pytest.raises(NoEventTableForAccount):
        native_app_manager.get_events()
