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

from pathlib import Path
from unittest import mock

import yaml
from snowflake.cli._plugins.nativeapp.constants import (
    LOOSE_FILES_MAGIC_VERSION,
    SPECIAL_COMMENT,
)
from snowflake.cli._plugins.workspace.action_context import ActionContext
from snowflake.cli.api.entities.application_package_entity import (
    ApplicationPackageEntity,
)
from snowflake.cli.api.project.schemas.entities.application_package_entity_model import (
    ApplicationPackageEntityModel,
)
from snowflake.cli.api.project.schemas.entities.common import SqlScriptHookType
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.utils import (
    APP_PACKAGE_ENTITY,
    APPLICATION_PACKAGE_ENTITY_MODULE,
    SQL_EXECUTOR_EXECUTE,
    mock_execute_helper,
)


def _get_app_pkg_entity(project_directory):
    with project_directory("workspaces_simple") as project_root:
        with Path(project_root / "snowflake.yml").open() as definition_file_path:
            project_definition = yaml.safe_load(definition_file_path)
            model = ApplicationPackageEntityModel(
                **project_definition["entities"]["pkg"]
            )
            mock_console = mock.MagicMock()
            action_ctx = ActionContext(
                console=mock_console,
                project_root=project_root,
                default_role="app_role",
                default_warehouse="wh",
            )
            return ApplicationPackageEntity(model), action_ctx, mock_console


def test_bundle(project_directory):
    app_pkg, bundle_ctx, mock_console = _get_app_pkg_entity(project_directory)

    bundle_result = app_pkg.action_bundle(bundle_ctx)

    deploy_root = bundle_result.deploy_root()
    assert (deploy_root / "README.md").exists()
    assert (deploy_root / "manifest.yml").exists()
    assert (deploy_root / "setup_script.sql").exists()


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(f"{APP_PACKAGE_ENTITY}.execute_post_deploy_hooks")
@mock.patch(f"{APP_PACKAGE_ENTITY}.validate_setup_script")
@mock.patch(f"{APPLICATION_PACKAGE_ENTITY_MODULE}.sync_deploy_root_with_stage")
def test_deploy(
    mock_sync,
    mock_validate,
    mock_execute_post_deploy_hooks,
    mock_execute,
    project_directory,
    mock_cursor,
):
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
                            "name": "PKG",
                            "comment": SPECIAL_COMMENT,
                            "version": LOOSE_FILES_MAGIC_VERSION,
                            "owner": "app_role",
                        }
                    ],
                    [],
                ),
                mock.call(
                    r"show application packages like 'PKG'",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor(
                    [
                        ("name", "pkg"),
                        ["owner", "app_role"],
                        ["distribution", "internal"],
                    ],
                    [],
                ),
                mock.call("describe application package pkg"),
            ),
            (None, mock.call("use role old_role")),
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    app_pkg, bundle_ctx, mock_console = _get_app_pkg_entity(project_directory)

    app_pkg.action_deploy(
        bundle_ctx, prune=False, recursive=False, paths=["a/b", "c"], validate=True
    )

    mock_sync.assert_called_once_with(
        console=mock_console,
        deploy_root=Path("output/deploy"),
        package_name="pkg",
        stage_schema="app_src",
        bundle_map=mock.ANY,
        role="app_role",
        prune=False,
        recursive=False,
        stage_fqn="pkg.app_src.stage",
        local_paths_to_sync=["a/b", "c"],
        print_diff=True,
    )
    mock_validate.assert_called_once()
    mock_execute_post_deploy_hooks.assert_called_once_with(
        console=mock_console,
        project_root=bundle_ctx.project_root,
        post_deploy_hooks=[
            SqlScriptHookType(sql_script="scripts/package_post_deploy1.sql"),
            SqlScriptHookType(sql_script="scripts/package_post_deploy2.sql"),
        ],
        package_name="pkg",
        package_warehouse="wh",
    )
    assert mock_execute.mock_calls == expected
