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
from pydantic import ValidationError
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.schemas.native_app.application import (
    ApplicationPostDeployHook,
)
from snowflake.cli.plugins.nativeapp.exceptions import MissingScriptError
from snowflake.cli.plugins.nativeapp.run_processor import NativeAppRunProcessor

from tests.nativeapp.patch_utils import mock_connection
from tests.nativeapp.utils import (
    NATIVEAPP_MANAGER_EXECUTE,
    NATIVEAPP_MANAGER_EXECUTE_QUERIES,
    RUN_PROCESSOR_APP_POST_DEPLOY_HOOKS,
)
from tests.testing_utils.fixtures import MockConnectionCtx

CLI_GLOBAL_TEMPLATE_CONTEXT = (
    "snowflake.cli.api.cli_global_context._CliGlobalContextAccess.template_context"
)
MOCK_CONNECTION_DB = "tests.testing_utils.fixtures.MockConnectionCtx.database"
MOCK_CONNECTION_WH = "tests.testing_utils.fixtures.MockConnectionCtx.warehouse"


def _get_run_processor(working_dir):
    dm = DefinitionManager(working_dir)
    return NativeAppRunProcessor(
        project_definition=dm.project_definition.native_app,
        project_root=dm.project_root,
    )


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_sql_scripts(
    mock_conn,
    mock_cli_ctx,
    mock_execute_queries,
    mock_execute_query,
    project_directory,
):
    mock_conn.return_value = MockConnectionCtx()
    mock_cli_ctx.return_value = {
        "ctx": {"native_app": {"name": "myapp"}, "env": {"foo": "bar"}}
    }
    with project_directory("napp_post_deploy") as project_dir:
        processor = _get_run_processor(str(project_dir))

        processor._execute_post_deploy_hooks()  # noqa SLF001

        assert mock_execute_query.mock_calls == [
            mock.call("use database myapp_test_user"),
            mock.call("use database myapp_test_user"),
        ]
        assert mock_execute_queries.mock_calls == [
            # Verify template variables were expanded correctly
            mock.call(
                dedent(
                    """\
                -- app post-deploy script (1/2)

                select myapp;
                select bar;
                """
                )
            ),
            mock.call("-- app post-deploy script (2/2)\n"),
        ]


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock_connection()
@mock.patch(MOCK_CONNECTION_DB, new_callable=mock.PropertyMock)
@mock.patch(MOCK_CONNECTION_WH, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
def test_sql_scripts_with_no_warehouse_no_database(
    mock_conn_wh,
    mock_conn_db,
    mock_conn,
    mock_cli_ctx,
    mock_execute_queries,
    mock_execute_query,
    project_directory,
):
    mock_conn_wh.return_value = None
    mock_conn_db.return_value = None
    mock_conn.return_value = MockConnectionCtx(None)
    mock_cli_ctx.return_value = {
        "ctx": {"native_app": {"name": "myapp"}, "env": {"foo": "bar"}}
    }
    with project_directory("napp_post_deploy") as project_dir:
        processor = _get_run_processor(str(project_dir))

        processor._execute_post_deploy_hooks()  # noqa SLF001

        # Verify no "use warehouse"
        # Verify "use database" applies to current application
        assert mock_execute_query.mock_calls == [
            mock.call("use database myapp_test_user"),
            mock.call("use database myapp_test_user"),
        ]
        assert mock_execute_queries.mock_calls == [
            mock.call(
                dedent(
                    """\
                -- app post-deploy script (1/2)

                select myapp;
                select bar;
                """
                )
            ),
            mock.call("-- app post-deploy script (2/2)\n"),
        ]


@mock_connection()
def test_missing_sql_script(
    mock_conn,
    project_directory,
):
    mock_conn.return_value = MockConnectionCtx()
    with project_directory("napp_post_deploy_missing_file") as project_dir:
        processor = _get_run_processor(str(project_dir))

        with pytest.raises(MissingScriptError) as err:
            processor._execute_post_deploy_hooks()  # noqa SLF001


@mock.patch(RUN_PROCESSOR_APP_POST_DEPLOY_HOOKS, new_callable=mock.PropertyMock)
@mock_connection()
def test_invalid_hook_type(
    mock_conn,
    mock_deploy_hooks,
    project_directory,
):
    mock_hook = mock.Mock()
    mock_hook.invalid_type = "invalid_type"
    mock_hook.sql_script = None
    mock_deploy_hooks.return_value = [mock_hook]
    mock_conn.return_value = MockConnectionCtx()
    with project_directory("napp_post_deploy") as project_dir:
        processor = _get_run_processor(str(project_dir))

        with pytest.raises(ValueError) as err:
            processor._execute_post_deploy_hooks()  # noqa SLF001
        assert "Unsupported application post-deploy hook type" in str(err)


@pytest.mark.parametrize(
    "args,expected_error",
    [
        ({"sql_script": "/path"}, None),
        ({}, "missing the following field: 'sql_script'"),
    ],
)
def test_post_deploy_hook_schema(args, expected_error):
    if expected_error:
        with pytest.raises(ValidationError) as err:
            ApplicationPostDeployHook(**args)

        assert expected_error in str(SchemaValidationError(err.value))
    else:
        ApplicationPostDeployHook(**args)
