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
from datetime import datetime
from pathlib import Path
from textwrap import dedent
from typing import Optional
from unittest import mock
from unittest.mock import call

import pytest
from click import ClickException
from snowflake.cli._plugins.nativeapp.constants import (
    LOOSE_FILES_MAGIC_VERSION,
    NAME_COL,
    SPECIAL_COMMENT,
    SPECIAL_COMMENT_OLD,
)
from snowflake.cli._plugins.nativeapp.entities.application import (
    ApplicationEntity,
    ApplicationEntityModel,
)
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntity,
    ApplicationPackageEntityModel,
)
from snowflake.cli._plugins.nativeapp.exceptions import (
    ApplicationPackageAlreadyExistsError,
    ApplicationPackageDoesNotExistError,
    NoEventTableForAccount,
    ObjectPropertyNotFoundError,
    SetupScriptFailedValidation,
)
from snowflake.cli._plugins.nativeapp.sf_facade import get_snowflake_facade
from snowflake.cli._plugins.stage.diff import (
    DiffResult,
    StagePathType,
)
from snowflake.cli._plugins.stage.manager import DefaultStagePathParts
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.entities.utils import (
    EntityActions,
    _get_stage_paths_to_sync,
    sync_deploy_root_with_stage,
)
from snowflake.cli.api.errno import (
    DOES_NOT_EXIST_OR_NOT_AUTHORIZED,
)
from snowflake.cli.api.exceptions import (
    DoesNotExistOrUnauthorizedError,
    SnowflakeSQLExecutionError,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.project.util import extract_schema
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

from tests.conftest import MockConnectionCtx
from tests.nativeapp.patch_utils import (
    mock_connection,
    mock_get_app_pkg_distribution_in_sf,
)
from tests.nativeapp.utils import (
    APP_PACKAGE_ENTITY_DEPLOY,
    APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO,
    APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME,
    ENTITIES_UTILS_MODULE,
    SQL_EXECUTOR_EXECUTE,
    SQL_FACADE_ALTER_APP_PKG_PROPERTIES,
    SQL_FACADE_CREATE_APP_PKG,
    SQL_FACADE_CREATE_SCHEMA,
    SQL_FACADE_CREATE_STAGE,
    SQL_FACADE_GET_ACCOUNT_EVENT_TABLE,
    SQL_FACADE_GET_UI_PARAMETER,
    SQL_FACADE_STAGE_EXISTS,
    mock_execute_helper,
    mock_snowflake_yml_file_v2,
    quoted_override_yml_file_v2,
    touch,
)
from tests.testing_utils.files_and_dirs import create_named_file


def _get_dm(working_dir: Optional[str] = None):
    return DefinitionManager(working_dir)


def _get_wm(working_dir: Optional[str] = None):
    dm = _get_dm(working_dir)
    return WorkspaceManager(
        project_definition=dm.project_definition,
        project_root=dm.project_root,
    )


@mock.patch(SQL_FACADE_STAGE_EXISTS)
@mock.patch(SQL_FACADE_CREATE_SCHEMA)
@mock.patch(SQL_FACADE_CREATE_STAGE)
@mock.patch(f"{ENTITIES_UTILS_MODULE}.compute_stage_diff")
@mock.patch(f"{ENTITIES_UTILS_MODULE}.sync_local_diff_with_stage")
@pytest.mark.parametrize("stage_exists", [True, False])
def test_sync_deploy_root_with_stage(
    mock_local_diff_with_stage,
    mock_compute_stage_diff,
    mock_create_stage,
    mock_create_schema,
    mock_stage_exists,
    temporary_directory,
    mock_cursor,
    stage_exists,
):
    mock_stage_exists.return_value = stage_exists
    mock_diff_result = DiffResult(different=[StagePathType("setup.sql")])
    mock_compute_stage_diff.return_value = mock_diff_result
    mock_local_diff_with_stage.return_value = None
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )
    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]

    assert mock_diff_result.has_changes()
    mock_bundle_map = mock.Mock(spec=BundleMap)
    package_name = pkg_model.fqn.name
    stage_fqn = f"{package_name}.{pkg_model.stage}"
    stage_schema = extract_schema(stage_fqn)
    sync_deploy_root_with_stage(
        console=cc,
        deploy_root=dm.project_root / pkg_model.deploy_root,
        package_name=package_name,
        bundle_map=mock_bundle_map,
        role="new_role",
        prune=True,
        recursive=True,
        stage_path=DefaultStagePathParts.from_fqn(stage_fqn),
    )

    mock_stage_exists.assert_called_once_with(stage_fqn)
    if not stage_exists:
        mock_create_schema.assert_called_once_with(stage_schema, database=package_name)
        mock_create_stage.assert_called_once_with(stage_fqn)
    mock_compute_stage_diff.assert_called_once_with(
        local_root=dm.project_root / pkg_model.deploy_root,
        stage_path=DefaultStagePathParts.from_fqn("app_pkg.app_src.stage"),
    )
    mock_local_diff_with_stage.assert_called_once_with(
        role="new_role",
        deploy_root_path=dm.project_root / pkg_model.deploy_root,
        diff_result=mock_diff_result,
        stage_full_path="app_pkg.app_src.stage",
    )


@mock.patch(SQL_FACADE_STAGE_EXISTS)
@mock.patch(SQL_FACADE_CREATE_SCHEMA)
@mock.patch(SQL_FACADE_CREATE_STAGE)
@mock.patch(f"{ENTITIES_UTILS_MODULE}.compute_stage_diff")
@mock.patch(f"{ENTITIES_UTILS_MODULE}.sync_local_diff_with_stage")
@pytest.mark.parametrize("stage_exists", [True, False])
def test_sync_deploy_root_with_stage_subdir(
    mock_local_diff_with_stage,
    mock_compute_stage_diff,
    mock_create_stage,
    mock_create_schema,
    mock_stage_exists,
    temporary_directory,
    mock_cursor,
    stage_exists,
):
    mock_stage_exists.return_value = stage_exists
    mock_diff_result = DiffResult(different=[StagePathType("setup.sql")])
    mock_compute_stage_diff.return_value = mock_diff_result
    mock_local_diff_with_stage.return_value = None
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )
    dm = _get_dm()
    # add subdir replace
    dm.project_definition.entities["app_pkg"].stage_subdirectory = "v1"
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]

    assert mock_diff_result.has_changes()
    mock_bundle_map = mock.Mock(spec=BundleMap)
    package_name = pkg_model.fqn.name
    stage_fqn = f"{package_name}.{pkg_model.stage}"
    stage_full_path = f"{stage_fqn}/v1"
    stage_schema = extract_schema(stage_fqn)
    sync_deploy_root_with_stage(
        console=cc,
        deploy_root=dm.project_root / pkg_model.deploy_root,
        package_name=package_name,
        bundle_map=mock_bundle_map,
        role="new_role",
        prune=True,
        recursive=True,
        stage_path=DefaultStagePathParts.from_fqn(stage_fqn, "v1"),
    )

    mock_stage_exists.assert_called_once_with(stage_fqn)
    if not stage_exists:
        mock_create_schema.assert_called_once_with(stage_schema, database=package_name)
        mock_create_stage.assert_called_once_with(stage_fqn)
    mock_compute_stage_diff.assert_called_once_with(
        local_root=dm.project_root / pkg_model.deploy_root,
        stage_path=DefaultStagePathParts.from_fqn(stage_fqn, "v1"),
    )
    mock_local_diff_with_stage.assert_called_once_with(
        role="new_role",
        deploy_root_path=dm.project_root / pkg_model.deploy_root,
        diff_result=mock_diff_result,
        stage_full_path=stage_full_path,
    )


@mock.patch(SQL_FACADE_STAGE_EXISTS)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(f"{ENTITIES_UTILS_MODULE}.sync_local_diff_with_stage")
@mock.patch(f"{ENTITIES_UTILS_MODULE}.compute_stage_diff")
@pytest.mark.parametrize(
    "prune,only_on_stage_files,expected_warn",
    [
        [
            True,
            [StagePathType("only-stage.txt")],
            False,
        ],
        [
            False,
            [StagePathType("only-stage-1.txt"), StagePathType("only-stage-2.txt")],
            True,
        ],
    ],
)
def test_sync_deploy_root_with_stage_prune(
    mock_compute_stage_diff,
    mock_local_diff_with_stage,
    mock_execute,
    mock_stage_exists,
    prune,
    only_on_stage_files,
    expected_warn,
    temporary_directory,
):
    mock_stage_exists.return_value = True
    mock_compute_stage_diff.return_value = DiffResult(only_on_stage=only_on_stage_files)
    create_named_file(
        file_name="snowflake.yml",
        dir_name=os.getcwd(),
        contents=[mock_snowflake_yml_file_v2],
    )
    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]

    mock_bundle_map = mock.Mock(spec=BundleMap)
    package_name = pkg_model.fqn.name
    stage_fqn = f"{package_name}.{pkg_model.stage}"
    mock_console = mock.MagicMock()
    sync_deploy_root_with_stage(
        console=mock_console,
        deploy_root=dm.project_root / pkg_model.deploy_root,
        package_name=package_name,
        bundle_map=mock_bundle_map,
        role="new_role",
        prune=prune,
        recursive=True,
        stage_path=DefaultStagePathParts.from_fqn(stage_fqn),
    )

    if expected_warn:
        files_str = "\n".join([str(f) for f in only_on_stage_files])
        warn_message = f"""The following files exist only on the stage:
{files_str}

Use the --prune flag to delete them from the stage."""
        mock_console.warning.assert_called_once_with(warn_message)
    else:
        mock_console.warning.assert_not_called()


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_app_pkg_distribution_in_snowflake(
    mock_execute, temporary_directory, mock_cursor, workspace_context
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
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
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    actual_distribution = pkg.get_app_pkg_distribution_in_snowflake()
    assert actual_distribution == "external"
    assert mock_execute.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_app_pkg_distribution_in_snowflake_throws_programming_error(
    mock_execute, temporary_directory, mock_cursor, workspace_context
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (
                DoesNotExistOrUnauthorizedError(
                    msg="Application package app_pkg does not exist or not authorized.",
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
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)

    with pytest.raises(DoesNotExistOrUnauthorizedError):
        pkg.get_app_pkg_distribution_in_snowflake()

    assert mock_execute.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_app_pkg_distribution_in_snowflake_throws_execution_error(
    mock_execute, temporary_directory, mock_cursor, workspace_context
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
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
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)

    with pytest.raises(SnowflakeSQLExecutionError):
        pkg.get_app_pkg_distribution_in_snowflake()

    assert mock_execute.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_app_pkg_distribution_in_snowflake_throws_distribution_error(
    mock_execute, temporary_directory, mock_cursor, workspace_context
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
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
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)

    with pytest.raises(ObjectPropertyNotFoundError) as err:
        pkg.get_app_pkg_distribution_in_snowflake()

    assert mock_execute.mock_calls == expected
    assert err.match(
        dedent(
            f"""\
        Could not find the 'distribution' attribute for application package app_pkg in the output of SQL query:
        'describe application package app_pkg'
        """
        )
    )


@mock_get_app_pkg_distribution_in_sf()
def test_is_app_pkg_distribution_same_in_sf_w_arg(
    mock_mismatch, temporary_directory, workspace_context
):
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    create_named_file(
        file_name="snowflake.local.yml",
        dir_name=current_working_directory,
        contents=[
            dedent(
                """\
                    entities:
                        app_pkg:
                            distribution: >-
                                EXTERNAL
                """
            )
        ],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    assert not pkg.verify_project_distribution(expected_distribution="internal")
    mock_mismatch.assert_not_called()


@mock_get_app_pkg_distribution_in_sf()
def test_is_app_pkg_distribution_same_in_sf_no_mismatch(
    mock_mismatch, temporary_directory, workspace_context
):
    mock_mismatch.return_value = "external"

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    create_named_file(
        file_name="snowflake.local.yml",
        dir_name=current_working_directory,
        contents=[
            dedent(
                """\
                    entities:
                        app_pkg:
                            distribution: >-
                                EXTERNAL
                """
            )
        ],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    assert pkg.verify_project_distribution()


@mock_get_app_pkg_distribution_in_sf()
def test_is_app_pkg_distribution_same_in_sf_has_mismatch(
    mock_mismatch, temporary_directory, workspace_context
):
    mock_mismatch.return_value = "external"

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    workspace_context.console = mock.MagicMock()
    assert not pkg.verify_project_distribution()
    workspace_context.console.warning.assert_called_once_with(
        "Application package app_pkg in your Snowflake account has distribution property external,\nwhich does not match the value specified in project definition file: internal.\n"
    )


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_existing_app_info_app_exists(
    mock_execute, temporary_directory, mock_cursor, workspace_context
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
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
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    app_model: ApplicationEntityModel = dm.project_definition.entities["myapp"]
    app = ApplicationEntity(app_model, workspace_context)
    show_obj_row = get_snowflake_facade().get_existing_app_info(app.name, app.role)
    assert show_obj_row is not None
    assert show_obj_row[NAME_COL] == "MYAPP"
    assert mock_execute.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_existing_app_info_app_does_not_exist(
    mock_execute, temporary_directory, mock_cursor, workspace_context
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
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
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    app_model: ApplicationEntityModel = dm.project_definition.entities["myapp"]
    app = ApplicationEntity(app_model, workspace_context)
    show_obj_row = get_snowflake_facade().get_existing_app_info(app.name, app.role)
    assert show_obj_row is None
    assert mock_execute.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_existing_app_pkg_info_app_pkg_exists(
    mock_execute, temporary_directory, mock_cursor, workspace_context
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
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
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    show_obj_row = pkg.get_existing_app_pkg_info()
    assert show_obj_row is not None
    assert show_obj_row[NAME_COL] == "APP_PKG"
    assert mock_execute.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_existing_app_pkg_info_app_pkg_does_not_exist(
    mock_execute, temporary_directory, mock_cursor, workspace_context
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
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
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    show_obj_row = pkg.get_existing_app_pkg_info()
    assert show_obj_row is None
    assert mock_execute.mock_calls == expected


# With connection warehouse, with PDF warehouse
# Without connection warehouse, with PDF warehouse
@mock.patch("snowflake.cli._plugins.connection.util.get_context")
@mock.patch("snowflake.cli._plugins.connection.util.get_account")
@mock.patch("snowflake.cli._plugins.connection.util.get_snowsight_host")
@mock.patch(SQL_EXECUTOR_EXECUTE)
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
    temporary_directory,
    mock_cursor,
    workspace_context,
):
    mock_conn.return_value = MockConnectionCtx(warehouse=warehouse)
    mock_snowsight_host.return_value = "https://host"
    mock_context.return_value = "organization"
    mock_account.return_value = "account"

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([(warehouse,)], []),
                mock.call("select current_warehouse()"),
            ),
            (None, mock.call("use warehouse app_warehouse")),
        ]
    )
    mock_execute_query.side_effect = side_effects + fallback_side_effect

    dm = _get_dm()
    app_model: ApplicationEntityModel = dm.project_definition.entities["myapp"]
    app = ApplicationEntity(app_model, workspace_context)
    assert (
        app.get_snowsight_url()
        == "https://host/organization/account/#/apps/application/MYAPP"
    )
    assert mock_execute_query.mock_calls == expected + fallback_warehouse_call


# With connection warehouse, without PDF warehouse
# Without connection warehouse, without PDF warehouse
@mock.patch("snowflake.cli._plugins.connection.util.get_context")
@mock.patch("snowflake.cli._plugins.connection.util.get_account")
@mock.patch("snowflake.cli._plugins.connection.util.get_snowsight_host")
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@pytest.mark.parametrize(
    "project_definition_files, warehouse, expected_calls, fallback_side_effect",
    [
        (
            "napp_project_2",
            "MockWarehouse",
            [mock.call("select current_warehouse()")],
            [None],
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
    workspace_context,
):
    mock_conn.return_value = MockConnectionCtx(warehouse=warehouse)
    mock_snowsight_host.return_value = "https://host"
    mock_context.return_value = "organization"
    mock_account.return_value = "account"

    working_dir: Path = project_definition_files[0].parent

    mock_execute_query.side_effect = [
        mock_cursor([(warehouse,)], [])
    ] + fallback_side_effect

    dm = _get_dm(str(working_dir))
    app_model: ApplicationEntityModel = dm.project_definition.entities["myapp_polly"]
    app = ApplicationEntity(app_model, workspace_context)
    workspace_context.get_default_warehouse = lambda: warehouse
    assert (
        app.get_snowsight_url()
        == "https://host/organization/account/#/apps/application/MYAPP_POLLY"
    )
    assert mock_execute_query.mock_calls == expected_calls


# Test create_app_package() with no existing package available
@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO, return_value=None)
@mock.patch(SQL_FACADE_GET_UI_PARAMETER, return_value="ENABLED")
@mock.patch(SQL_FACADE_CREATE_APP_PKG)
@mock.patch("snowflake.cli.api.config.get_config_value")
@pytest.mark.parametrize("feature_flag", [True, False, None])
def test_given_no_existing_pkg_when_create_app_pkg_then_success_and_respect_release_channels_flag(
    mock_get_config_value,
    mock_create_app_pkg,
    mock_get_ui_parameter,
    mock_get_existing_app_pkg_info,
    temporary_directory,
    workspace_context,
    feature_flag,
):
    mock_get_config_value.return_value = feature_flag

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)

    pkg.create_app_package()

    mock_get_existing_app_pkg_info.assert_called_once()
    mock_create_app_pkg.assert_called_once_with(
        package_name="app_pkg",
        distribution="internal",
        enable_release_channels=feature_flag,
        role="package_role",
    )
    mock_get_config_value.assert_called_once_with(
        "cli", "features", key="enable_release_channels", default=None
    )


@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME)
@mock.patch(SQL_FACADE_GET_UI_PARAMETER, return_value="ENABLED")
@mock.patch(SQL_FACADE_ALTER_APP_PKG_PROPERTIES)
@mock.patch("snowflake.cli.api.config.get_config_value")
@pytest.mark.parametrize("feature_flag", [True, False, None])
def test_given_existing_app_package_with_feature_flag_set_when_create_pkg_then_set_pkg_property_to_same_value(
    mock_get_config_value,
    mock_alter_app_pkg_properties,
    mock_get_ui_parameter,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_get_existing_app_pkg_info,
    temporary_directory,
    workspace_context,
    feature_flag,
):
    mock_get_config_value.return_value = feature_flag
    mock_is_distribution_same.return_value = True
    mock_get_distribution.return_value = "internal"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "APP_PKG",
        "comment": SPECIAL_COMMENT,
        "version": LOOSE_FILES_MAGIC_VERSION,
        "owner": "PACKAGE_ROLE",
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )
    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    workspace_context.console = mock.MagicMock()

    pkg.create_app_package()

    mock_alter_app_pkg_properties.assert_called_once_with(
        package_name="app_pkg",
        enable_release_channels=feature_flag,
        role="package_role",
    )
    mock_get_config_value.assert_called_once_with(
        "cli", "features", key="enable_release_channels", default=None
    )


# Test create_app_package() with a different owner
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME, return_value=True)
@mock.patch(SQL_FACADE_GET_UI_PARAMETER, return_value="ENABLED")
def test_create_app_pkg_different_owner(
    mock_get_ui_parameter,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_get_existing_app_pkg_info,
    mock_execute,
    temporary_directory,
    mock_cursor,
    workspace_context,
):
    mock_get_existing_app_pkg_info.return_value = {
        "name": "APP_PKG",
        "comment": SPECIAL_COMMENT,
        "version": LOOSE_FILES_MAGIC_VERSION,
        "owner": "wrong_owner",
        "distribution": "internal",
    }
    mock_get_distribution.return_value = "internal"

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    # Invoke create when the package already exists, but the owner is the current role.
    # This is expected to succeed with no warnings.
    pkg.create_app_package()

    mock_execute.assert_not_called()


# Test create_app_package() with distribution external AND variable mismatch
@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME)
@pytest.mark.parametrize(
    "is_pkg_distribution_same",
    [False, True],
)
@mock.patch(SQL_FACADE_GET_UI_PARAMETER, return_value="ENABLED")
def test_create_app_pkg_external_distribution(
    mock_get_ui_parameter,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_get_existing_app_pkg_info,
    is_pkg_distribution_same,
    temporary_directory,
    workspace_context,
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
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    workspace_context.console = mock.MagicMock()
    pkg.create_app_package()
    if not is_pkg_distribution_same:
        workspace_context.console.warning.assert_called_once_with(
            "Continuing to execute `snow app run` on application package app_pkg with distribution 'external'."
        )


# Test create_app_package() with distribution internal AND variable mismatch AND special comment is True
@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME)
@pytest.mark.parametrize(
    "is_pkg_distribution_same, special_comment",
    [
        (False, SPECIAL_COMMENT),
        (False, SPECIAL_COMMENT_OLD),
        (True, SPECIAL_COMMENT),
        (True, SPECIAL_COMMENT_OLD),
    ],
)
@mock.patch(SQL_FACADE_GET_UI_PARAMETER, return_value="ENABLED")
def test_create_app_pkg_internal_distribution_special_comment(
    mock_get_ui_parameter,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_get_existing_app_pkg_info,
    is_pkg_distribution_same,
    special_comment,
    temporary_directory,
    workspace_context,
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
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    workspace_context.console = mock.MagicMock()
    pkg.create_app_package()
    if not is_pkg_distribution_same:
        workspace_context.console.warning.assert_called_once_with(
            "Continuing to execute `snow app run` on application package app_pkg with distribution 'internal'."
        )


# Test create_app_package() with distribution internal AND variable mismatch AND special comment is False
@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME)
@pytest.mark.parametrize(
    "is_pkg_distribution_same",
    [False, True],
)
def test_create_app_pkg_internal_distribution_no_special_comment(
    mock_is_distribution_same,
    mock_get_distribution,
    mock_get_existing_app_pkg_info,
    is_pkg_distribution_same,
    temporary_directory,
    workspace_context,
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
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    workspace_context.console = mock.MagicMock()
    with pytest.raises(ApplicationPackageAlreadyExistsError):
        pkg.create_app_package()

    if not is_pkg_distribution_same:
        workspace_context.console.warning.assert_called_once_with(
            "Continuing to execute `snow app run` on application package app_pkg with distribution 'internal'."
        )


# Test create_app_package() with existing package without special comment
@mock.patch(APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO)
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_existing_app_pkg_without_special_comment(
    mock_execute,
    mock_get_existing_app_pkg_info,
    mock_get_distribution,
    mock_is_distribution_same,
    temporary_directory,
    mock_cursor,
    workspace_context,
):
    mock_get_existing_app_pkg_info.return_value = {
        "name": "APP_PKG",
        "comment": "NOT_SPECIAL_COMMENT",
        "version": LOOSE_FILES_MAGIC_VERSION,
        "owner": "package_role",
    }
    mock_get_distribution.return_value = "internal"
    mock_is_distribution_same.return_value = True

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    with pytest.raises(ApplicationPackageAlreadyExistsError):
        pkg.create_app_package()


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
    temporary_directory,
    paths_to_sync,
    expected_result,
):
    touch("deploy/file")
    touch("deploy/dir/nested_file1")
    touch("deploy/dir/nested_file2")
    touch("deploy/dir/nested_dir/nested_file3")

    paths_to_sync = [Path(p) for p in paths_to_sync]
    result = _get_stage_paths_to_sync(paths_to_sync, Path("deploy/"))
    assert result.sort() == [StagePathType(p) for p in expected_result].sort()


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_validate_passing(mock_execute, temporary_directory, mock_cursor):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temporary_directory,
        contents=[mock_snowflake_yml_file_v2],
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

    wm = _get_wm()
    wm.perform_action(
        "app_pkg",
        EntityActions.VALIDATE,
        interactive=False,
        force=True,
        use_scratch_stage=False,
    )

    assert mock_execute.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch("snowflake.cli._plugins.workspace.manager.cc.warning")
def test_validate_passing_with_warnings(
    mock_warning, mock_execute, temporary_directory, mock_cursor
):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temporary_directory,
        contents=[mock_snowflake_yml_file_v2],
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

    wm = _get_wm()
    wm.perform_action(
        "app_pkg",
        EntityActions.VALIDATE,
        interactive=False,
        force=True,
        use_scratch_stage=False,
    )

    warn_message = f"{warning['message']} (error code {warning['errorCode']})"
    mock_warning.assert_called_once_with(warn_message)
    assert mock_execute.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch("snowflake.cli._plugins.workspace.manager.cc.warning")
def test_validate_failing(mock_warning, mock_execute, temporary_directory, mock_cursor):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temporary_directory,
        contents=[mock_snowflake_yml_file_v2],
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

    wm = _get_wm()
    with pytest.raises(
        SetupScriptFailedValidation,
        match="Snowflake Native App setup script failed validation.",
    ):
        wm.perform_action(
            "app_pkg",
            EntityActions.VALIDATE,
            interactive=False,
            force=True,
            use_scratch_stage=False,
        )

    warn_message = f"{warning['message']} (error code {warning['errorCode']})"
    error_message = f"{error['message']} (error code {error['errorCode']})"
    mock_warning.assert_has_calls(
        [call(warn_message), call(error_message)], any_order=False
    )
    assert mock_execute.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_validate_query_error(mock_execute, temporary_directory, mock_cursor):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temporary_directory,
        contents=[mock_snowflake_yml_file_v2],
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

    wm = _get_wm()
    with pytest.raises(SnowflakeSQLExecutionError):
        wm.perform_action(
            "app_pkg",
            EntityActions.VALIDATE,
            interactive=False,
            force=True,
            use_scratch_stage=False,
        )

    assert mock_execute.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_validate_not_deployed(mock_execute, temporary_directory, mock_cursor):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temporary_directory,
        contents=[mock_snowflake_yml_file_v2],
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

    wm = _get_wm()
    with pytest.raises(ApplicationPackageDoesNotExistError, match="app_pkg"):
        wm.perform_action(
            "app_pkg",
            EntityActions.VALIDATE,
            interactive=False,
            force=True,
            use_scratch_stage=False,
        )

    assert mock_execute.mock_calls == expected


@mock.patch(APP_PACKAGE_ENTITY_DEPLOY)
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_validate_use_scratch_stage(
    mock_execute, mock_deploy, temporary_directory, mock_cursor
):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temporary_directory,
        contents=[mock_snowflake_yml_file_v2],
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
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
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

    wm = _get_wm()
    wm.perform_action(
        "app_pkg",
        EntityActions.VALIDATE,
        interactive=False,
        force=True,
        use_scratch_stage=True,
    )

    pd = wm._project_definition  # noqa: SLF001
    pkg_model: ApplicationPackageEntityModel = pd.entities["app_pkg"]
    mock_deploy.assert_called_with(
        action_ctx=wm.action_ctx,
        bundle_map=None,
        prune=True,
        recursive=True,
        paths=[],
        print_diff=False,
        validate=False,
        stage_path=DefaultStagePathParts.from_fqn(
            f"{pkg_model.fqn.name}.{pkg_model.scratch_stage}"
        ),
        interactive=False,
        force=True,
        run_post_deploy_hooks=False,
    )
    assert mock_execute.mock_calls == expected


@mock.patch(APP_PACKAGE_ENTITY_DEPLOY)
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_validate_failing_drops_scratch_stage(
    mock_execute, mock_deploy, temporary_directory, mock_cursor
):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temporary_directory,
        contents=[mock_snowflake_yml_file_v2],
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
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
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

    wm = _get_wm()
    with pytest.raises(
        SetupScriptFailedValidation,
        match="Snowflake Native App setup script failed validation.",
    ):
        wm.perform_action(
            "app_pkg",
            EntityActions.VALIDATE,
            interactive=False,
            force=True,
            use_scratch_stage=True,
        )

    pd = wm._project_definition  # noqa: SLF001
    pkg_model: ApplicationPackageEntityModel = pd.entities["app_pkg"]
    mock_deploy.assert_called_with(
        action_ctx=wm.action_ctx,
        bundle_map=None,
        prune=True,
        recursive=True,
        paths=[],
        print_diff=False,
        validate=False,
        stage_path=DefaultStagePathParts.from_fqn(
            f"{pkg_model.fqn.name}.{pkg_model.scratch_stage}"
        ),
        interactive=False,
        force=True,
        run_post_deploy_hooks=False,
    )
    assert mock_execute.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_validate_raw_returns_data(mock_execute, temporary_directory, mock_cursor):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temporary_directory,
        contents=[mock_snowflake_yml_file_v2],
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

    wm = _get_wm()
    pkg = wm.get_entity("app_pkg")
    assert (
        pkg.get_validation_result(
            action_ctx=wm.action_ctx,
            use_scratch_stage=False,
            interactive=False,
            force=True,
        )
        == failure_data
    )
    assert mock_execute.mock_calls == expected


@pytest.mark.parametrize(
    ["since", "expected_since_clause"],
    [
        pytest.param("", "", id="no_since"),
        pytest.param(
            "1 hour",
            "and timestamp >= sysdate() - interval '1 hour'",
            id="since_interval",
        ),
        pytest.param(
            datetime(2024, 1, 1),
            "and timestamp >= '2024-01-01 00:00:00'",
            id="since_datetime",
        ),
    ],
)
@pytest.mark.parametrize(
    ["until", "expected_until_clause"],
    [
        pytest.param("", "", id="no_until"),
        pytest.param(
            "20 minutes",
            "and timestamp <= sysdate() - interval '20 minutes'",
            id="until_interval",
        ),
        pytest.param(
            datetime(2024, 1, 1),
            "and timestamp <= '2024-01-01 00:00:00'",
            id="until_datetime",
        ),
    ],
)
@pytest.mark.parametrize(
    ["scopes", "expected_scopes_clause"],
    [
        pytest.param([], "", id="no_scopes"),
        pytest.param(["scope_1"], "and scope:name in ('scope_1')", id="single_scope"),
        pytest.param(
            ["scope_1", "scope_2"],
            "and scope:name in ('scope_1','scope_2')",
            id="multiple_scopes",
        ),
    ],
)
@pytest.mark.parametrize(
    ["types", "expected_types_clause"],
    [
        pytest.param([], "", id="no_types"),
        pytest.param(["log"], "and record_type in ('log')", id="single_type"),
        pytest.param(
            ["log", "span"], "and record_type in ('log','span')", id="multiple_types"
        ),
    ],
)
@pytest.mark.parametrize(
    ["consumer_org", "consumer_account", "consumer_app_hash", "expected_app_clause"],
    [
        pytest.param(
            "",
            "",
            "",
            f"resource_attributes:\"snow.database.name\" = 'MYAPP'",
            id="no_consumer",
        ),
        pytest.param(
            "testorg",
            "testacc",
            "",
            (
                f"resource_attributes:\"snow.application.package.name\" = 'APP_PKG' "
                f"and resource_attributes:\"snow.application.consumer.organization\" = 'TESTORG' "
                f"and resource_attributes:\"snow.application.consumer.name\" = 'TESTACC'"
            ),
            id="with_consumer",
        ),
        pytest.param(
            "testorg",
            "testacc",
            "428cdba48b74dfbbb333d5ea2cc51a78ecc56ce2",
            (
                f"resource_attributes:\"snow.application.package.name\" = 'APP_PKG' "
                f"and resource_attributes:\"snow.application.consumer.organization\" = 'TESTORG' "
                f"and resource_attributes:\"snow.application.consumer.name\" = 'TESTACC' "
                f"and resource_attributes:\"snow.database.hash\" = '428cdba48b74dfbbb333d5ea2cc51a78ecc56ce2'"
            ),
            id="with_consumer_app_hash",
        ),
    ],
)
@pytest.mark.parametrize(
    ["first", "expected_first_clause"],
    [
        pytest.param(-1, "", id="no_first"),
        pytest.param(0, "limit 0", id="first_0"),
        pytest.param(10, "limit 10", id="first_10"),
    ],
)
@pytest.mark.parametrize(
    ["last", "expected_last_clause"],
    [
        pytest.param(-1, "", id="no_last"),
        pytest.param(0, "limit 0", id="last_0"),
        pytest.param(20, "limit 20", id="last_20"),
    ],
)
@mock.patch(
    SQL_FACADE_GET_ACCOUNT_EVENT_TABLE,
    return_value="db.schema.event_table",
)
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_events(
    mock_execute,
    mock_account_event_table,
    temporary_directory,
    mock_cursor,
    since,
    expected_since_clause,
    until,
    expected_until_clause,
    types,
    expected_types_clause,
    consumer_org,
    consumer_account,
    consumer_app_hash,
    expected_app_clause,
    scopes,
    expected_scopes_clause,
    first,
    expected_first_clause,
    last,
    expected_last_clause,
    workspace_context,
):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temporary_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    events = [dict(TIMESTAMP=datetime(2024, 1, 1), VALUE="test")] * 100
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor(events, []),
                mock.call(
                    dedent(
                        f"""\
                        select * from (
                            select timestamp, value::varchar value
                            from db.schema.event_table
                            where ({expected_app_clause})
                            {expected_since_clause}
                            {expected_until_clause}
                            {expected_types_clause}
                            {expected_scopes_clause}
                            order by timestamp desc
                            {expected_last_clause}
                        ) order by timestamp asc
                        {expected_first_clause}
                        """
                    ),
                    cursor_class=DictCursor,
                ),
            ),
        ]
    )
    mock_execute.side_effect = side_effects

    def get_events():
        dm = _get_dm()
        pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities[
            "app_pkg"
        ]
        app_model: ApplicationEntityModel = dm.project_definition.entities["myapp"]
        app = ApplicationEntity(app_model, workspace_context)
        return app.get_events(
            package_name=pkg_model.fqn.name,
            since=since,
            until=until,
            record_types=types,
            scopes=scopes,
            consumer_org=consumer_org,
            consumer_account=consumer_account,
            consumer_app_hash=consumer_app_hash,
            first=first,
            last=last,
        )

    if first >= 0 and last >= 0:
        # Filtering on first and last events at the same time doesn't make sense
        with pytest.raises(ValueError):
            get_events()
    else:
        assert get_events() == events
        assert mock_execute.mock_calls == expected


@mock.patch(
    SQL_FACADE_GET_ACCOUNT_EVENT_TABLE,
    return_value="db.schema.event_table",
)
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_events_quoted_app_name(
    mock_execute,
    mock_account_event_table,
    temporary_directory,
    mock_cursor,
    workspace_context,
):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temporary_directory,
        contents=[mock_snowflake_yml_file_v2],
    )
    create_named_file(
        file_name="snowflake.local.yml",
        dir_name=temporary_directory,
        contents=[quoted_override_yml_file_v2],
    )

    events = [dict(TIMESTAMP=datetime(2024, 1, 1), VALUE="test")] * 100
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor(events, []),
                mock.call(
                    dedent(
                        f"""\
                        select * from (
                            select timestamp, value::varchar value
                            from db.schema.event_table
                            where (resource_attributes:"snow.database.name" = 'My Application')
                        
                        
                        
                        
                            order by timestamp desc
                        
                        ) order by timestamp asc
                        
                        """
                    ),
                    cursor_class=DictCursor,
                ),
            ),
        ]
    )
    mock_execute.side_effect = side_effects

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    app_model: ApplicationEntityModel = dm.project_definition.entities["myapp"]
    app = ApplicationEntity(app_model, workspace_context)
    assert app.get_events(package_name=pkg_model.fqn.name) == events
    assert mock_execute.mock_calls == expected


@mock.patch(SQL_FACADE_GET_ACCOUNT_EVENT_TABLE)
def test_get_events_no_event_table(
    mock_account_event_table, temporary_directory, mock_cursor, workspace_context
):
    mock_account_event_table.return_value = None
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temporary_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    app_model: ApplicationEntityModel = dm.project_definition.entities["myapp"]
    app = ApplicationEntity(app_model, workspace_context)
    with pytest.raises(NoEventTableForAccount):
        app.get_events(package_name=pkg_model.fqn.name)


@mock.patch(
    SQL_FACADE_GET_ACCOUNT_EVENT_TABLE,
    return_value="db.schema.non_existent_event_table",
)
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_get_events_event_table_dne_or_unauthorized(
    mock_execute,
    mock_account_event_table,
    temporary_directory,
    mock_cursor,
    workspace_context,
):
    side_effects, expected = mock_execute_helper(
        [
            (
                ProgrammingError(
                    msg="Object 'db.schema.non_existent_event_table' does not exist or not authorized.",
                    errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED,
                ),
                mock.call(
                    dedent(
                        f"""\
                        select * from (
                            select timestamp, value::varchar value
                            from db.schema.non_existent_event_table
                            where (resource_attributes:"snow.database.name" = 'MYAPP')
                            
                            
                            
                            
                            order by timestamp desc
                            
                        ) order by timestamp asc
                        
                        """
                    ),
                    cursor_class=DictCursor,
                ),
            ),
        ]
    )
    mock_execute.side_effect = side_effects

    create_named_file(
        file_name="snowflake.yml",
        dir_name=temporary_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    app_model: ApplicationEntityModel = dm.project_definition.entities["myapp"]
    app = ApplicationEntity(app_model, workspace_context)
    with pytest.raises(ClickException) as err:
        app.get_events(package_name=pkg_model.fqn.name)

    assert mock_execute.mock_calls == expected
    assert err.match(
        dedent(
            """\
                    Event table 'db.schema.non_existent_event_table' does not exist or you are not authorized to perform this operation.
                    Please check your EVENT_TABLE parameter to ensure that it is set to a valid event table."""
        )
    )


@mock.patch(
    SQL_FACADE_GET_ACCOUNT_EVENT_TABLE,
    return_value="db.schema.event_table",
)
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_stream_events(
    mock_execute,
    mock_account_event_table,
    temporary_directory,
    mock_cursor,
    workspace_context,
):
    create_named_file(
        file_name="snowflake.yml",
        dir_name=temporary_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    events = [
        [],
        [dict(TIMESTAMP=datetime(2024, 1, 1), VALUE="test")] * 10,
        [dict(TIMESTAMP=datetime(2024, 1, 2), VALUE="test")] * 10,
    ]
    last = 20
    side_effects, expected = mock_execute_helper(
        [
            # Initial call
            # Returns no events
            (
                mock_cursor(events[0], []),
                mock.call(
                    dedent(
                        f"""\
                        select * from (
                            select timestamp, value::varchar value
                            from db.schema.event_table
                            where (resource_attributes:"snow.database.name" = 'MYAPP')
                            
                            
                            
                            
                            order by timestamp desc
                            limit {last}
                        ) order by timestamp asc
                        
                        """
                    ),
                    cursor_class=DictCursor,
                ),
            ),
            # Second call
            # No where clause on the timestamp since previous call didn't return any events
            # Returns some events
            (
                mock_cursor(events[1], []),
                mock.call(
                    dedent(
                        f"""\
                        select * from (
                            select timestamp, value::varchar value
                            from db.schema.event_table
                            where (resource_attributes:"snow.database.name" = 'MYAPP')
                            
                            
                            
                            
                            order by timestamp desc
                            
                        ) order by timestamp asc
                        
                        """
                    ),
                    cursor_class=DictCursor,
                ),
            ),
            # Third call
            # Where clause on the timestamp >= the last timestamp of the previous call
            (
                mock_cursor(events[2], []),
                mock.call(
                    dedent(
                        """\
                        select * from (
                            select timestamp, value::varchar value
                            from db.schema.event_table
                            where (resource_attributes:"snow.database.name" = 'MYAPP')
                            and timestamp >= '2024-01-01 00:00:00'
                            
                            
                            
                            order by timestamp desc
                            
                        ) order by timestamp asc
                        
                        """
                    ),
                    cursor_class=DictCursor,
                ),
            ),
        ]
    )
    mock_execute.side_effect = side_effects

    dm = _get_dm()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["app_pkg"]
    app_model: ApplicationEntityModel = dm.project_definition.entities["myapp"]
    app = ApplicationEntity(app_model, workspace_context)
    stream = app.stream_events(
        package_name=pkg_model.fqn.name, interval_seconds=0, last=last
    )
    for call in events:
        for event in call:
            assert next(stream) == event
    assert mock_execute.mock_calls == expected

    try:
        stream.throw(KeyboardInterrupt)
    except StopIteration:
        pass
    else:
        pytest.fail("stream_events didn't end when receiving a KeyboardInterrupt")
