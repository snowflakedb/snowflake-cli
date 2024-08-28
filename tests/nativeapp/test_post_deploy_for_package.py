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

import pytest
from snowflake.cli._plugins.nativeapp.exceptions import MissingScriptError
from snowflake.cli._plugins.nativeapp.manager import NativeAppManager
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.connector import ProgrammingError

from tests.nativeapp.patch_utils import mock_connection
from tests.nativeapp.utils import (
    CLI_GLOBAL_TEMPLATE_CONTEXT,
    SQL_EXECUTOR_EXECUTE,
    SQL_EXECUTOR_EXECUTE_QUERIES,
)
from tests.testing_utils.fixtures import MockConnectionCtx


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_package_post_deploy_scripts(
    mock_conn,
    mock_cli_ctx,
    mock_execute_queries,
    mock_execute_query,
    project_directory,
):
    mock_conn.return_value = MockConnectionCtx()
    with project_directory("napp_post_deploy") as project_dir:
        dm = DefinitionManager(project_dir)
        manager = NativeAppManager(
            project_definition=dm.project_definition.native_app,
            project_root=dm.project_root,
        )
        mock_cli_ctx.return_value = dm.template_context

        manager.execute_package_post_deploy_hooks()

        assert mock_execute_query.mock_calls == [
            mock.call("use database myapp_pkg_test_user"),
            mock.call("use database myapp_pkg_test_user"),
        ]
        assert mock_execute_queries.mock_calls == [
            # Verify template variables were expanded correctly
            mock.call(
                dedent(
                    """\
                    -- package post-deploy script (1/2)

                    select myapp;
                    select package_bar;
                    """
                )
            ),
            mock.call("-- package post-deploy script (2/2)\n"),
        ]


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_package_post_deploy_scripts_with_no_scripts(
    mock_conn,
    mock_cli_ctx,
    mock_execute_queries,
    mock_execute_query,
    project_directory,
):
    mock_conn.return_value = MockConnectionCtx()
    with project_directory("napp_project_1") as project_dir:
        dm = DefinitionManager(project_dir)
        manager = NativeAppManager(
            project_definition=dm.project_definition.native_app,
            project_root=dm.project_root,
        )
        mock_cli_ctx.return_value = dm.template_context

        manager.execute_package_post_deploy_hooks()

        assert mock_execute_query.mock_calls == []
        assert mock_execute_queries.mock_calls == []


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_package_post_deploy_scripts_with_non_existing_scripts(
    mock_conn,
    mock_cli_ctx,
    project_directory,
):
    mock_conn.return_value = MockConnectionCtx()
    with project_directory("napp_post_deploy_missing_file") as project_dir:
        dm = DefinitionManager(project_dir)
        manager = NativeAppManager(
            project_definition=dm.project_definition.native_app,
            project_root=dm.project_root,
        )
        mock_cli_ctx.return_value = dm.template_context

        with pytest.raises(MissingScriptError) as err:
            manager.execute_package_post_deploy_hooks()

        assert (
            err.value.message
            == 'Script "scripts/package_missing_script.sql" does not exist'
        )


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_package_post_deploy_scripts_with_sql_error(
    mock_conn,
    mock_cli_ctx,
    mock_execute_query,
    project_directory,
):
    mock_conn.return_value = MockConnectionCtx()
    with project_directory("napp_post_deploy") as project_dir:
        dm = DefinitionManager(project_dir)
        manager = NativeAppManager(
            project_definition=dm.project_definition.native_app,
            project_root=dm.project_root,
        )
        mock_cli_ctx.return_value = dm.template_context
        mock_execute_query.side_effect = ProgrammingError()

        with pytest.raises(ProgrammingError):
            manager.execute_package_post_deploy_hooks()


@mock.patch.dict(os.environ, {"USER": "test_user"})
def test_package_scripts_and_post_deploy_found(
    project_directory,
):
    with project_directory(
        "napp_post_deploy",
        {"native_app": {"package": {"scripts": ["scripts/package_post_deploy2.sql"]}}},
    ) as project_dir:

        with pytest.raises(SchemaValidationError) as err:
            dm = DefinitionManager(project_dir)
            NativeAppManager(
                project_definition=dm.project_definition.native_app,
                project_root=dm.project_root,
            )

        assert (
            "package.scripts and package.post_deploy fields cannot be used together"
            in err.value.message
        )
