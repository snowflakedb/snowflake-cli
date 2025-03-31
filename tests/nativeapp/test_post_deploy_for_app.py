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
from typing import Optional
from unittest import mock

import pytest
from pydantic import ValidationError
from snowflake.cli._plugins.nativeapp.entities.application import (
    ApplicationEntity,
    ApplicationEntityModel,
)
from snowflake.cli._plugins.nativeapp.exceptions import MissingScriptError
from snowflake.cli.api.exceptions import InvalidTemplateError
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.schemas.entities.common import PostDeployHook

from tests.conftest import MockConnectionCtx
from tests.nativeapp.factories import (
    ApplicationEntityModelFactory,
    ApplicationPackageEntityModelFactory,
    ProjectV2Factory,
)
from tests.nativeapp.patch_utils import mock_connection
from tests.nativeapp.utils import (
    CLI_GLOBAL_TEMPLATE_CONTEXT,
    SQL_FACADE_EXECUTE_USER_SCRIPT,
)

MOCK_CONNECTION_DB = "tests.testing_utils.conftest.MockConnectionCtx.database"
MOCK_CONNECTION_WH = "tests.testing_utils.conftest.MockConnectionCtx.warehouse"

DEFAULT_POST_DEPLOY_FILENAME_1 = "scripts/app_post_deploy1.sql"
DEFAULT_POST_DEPLOY_CONTENT_1 = dedent(
    """\
        -- app post-deploy script (1/2)

        select myapp;
        select bar;
        """
)

DEFAULT_POST_DEPLOY_CONTENT_2 = "-- app post-deploy script (2/2)\n"
DEFAULT_POST_DEPLOY_FILENAME_2 = "scripts/app_post_deploy2.sql"


def app_post_deploy_project_factory(
    custom_post_deploy_content_1: Optional[str] = None,
    custom_post_deploy_content_2: Optional[str] = None,
) -> None:
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg",
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
                meta__post_deploy=[
                    {"sql_script": DEFAULT_POST_DEPLOY_FILENAME_1},
                    {"sql_script": DEFAULT_POST_DEPLOY_FILENAME_2},
                ],
            ),
        ),
        pdf__env__foo="bar",
        files={
            DEFAULT_POST_DEPLOY_FILENAME_1: custom_post_deploy_content_1
            or DEFAULT_POST_DEPLOY_CONTENT_1,
            DEFAULT_POST_DEPLOY_FILENAME_2: custom_post_deploy_content_2
            or DEFAULT_POST_DEPLOY_CONTENT_2,
        },
    )


@mock.patch(SQL_FACADE_EXECUTE_USER_SCRIPT)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_sql_scripts(
    mock_conn,
    mock_cli_ctx,
    mock_sqlfacade_execute_user_script,
    temporary_directory,
    workspace_context,
):
    mock_conn.return_value = MockConnectionCtx()
    app_post_deploy_project_factory()

    dm = DefinitionManager()
    mock_cli_ctx.return_value = dm.template_context
    app_model: ApplicationEntityModel = dm.project_definition.entities["app"]
    app = ApplicationEntity(app_model, workspace_context)
    app.execute_post_deploy_hooks()

    assert mock_sqlfacade_execute_user_script.mock_calls == [
        mock.call(
            queries=DEFAULT_POST_DEPLOY_CONTENT_1,
            script_name=DEFAULT_POST_DEPLOY_FILENAME_1,
            role=app.role,
            warehouse=app.warehouse,
            database=app.name,
        ),
        mock.call(
            queries=DEFAULT_POST_DEPLOY_CONTENT_2,
            script_name=DEFAULT_POST_DEPLOY_FILENAME_2,
            role=app.role,
            warehouse=app.warehouse,
            database=app.name,
        ),
    ]


@mock.patch(SQL_FACADE_EXECUTE_USER_SCRIPT)
@mock_connection()
def test_missing_sql_script(
    mock_sqlfacade_execute_user_script, mock_conn, project_directory, workspace_context
):
    mock_conn.return_value = MockConnectionCtx()
    with project_directory("napp_post_deploy_missing_file_v2") as project_dir:
        dm = DefinitionManager()
        app_model: ApplicationEntityModel = dm.project_definition.entities["myapp"]
        app = ApplicationEntity(app_model, workspace_context)

        with pytest.raises(MissingScriptError) as err:
            app.execute_post_deploy_hooks()

        assert err.value.message == 'Script "scripts/missing.sql" does not exist'


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
            PostDeployHook(**args)

        assert expected_error in str(SchemaValidationError(err.value))
    else:
        PostDeployHook(**args)


@pytest.mark.parametrize(
    "template_syntax", [("<% ctx.env.test %>"), ("&{ ctx.env.test }")]
)
@mock.patch(SQL_FACADE_EXECUTE_USER_SCRIPT)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_app_post_deploy_with_template(
    mock_conn,
    mock_cli_ctx,
    mock_sqlfacade_execute_user_script,
    project_directory,
    template_syntax,
    workspace_context,
    temporary_directory,
):
    mock_conn.return_value = MockConnectionCtx()
    mock_cli_ctx.return_value = {"ctx": {"env": {"test": "test_value"}}}

    # edit scripts/app_post_deploy1.sql to include template variables
    app_post_deploy_project_factory(
        custom_post_deploy_content_1=dedent(
            f"""\
                -- app post-deploy script (1/2)

                select '{template_syntax}';
                """
        )
    )
    dm = DefinitionManager()
    app_model: ApplicationEntityModel = dm.project_definition.entities["app"]
    app = ApplicationEntity(app_model, workspace_context)

    app.execute_post_deploy_hooks()

    assert mock_sqlfacade_execute_user_script.mock_calls == [
        mock.call(
            queries=dedent(
                f"""\
                -- app post-deploy script (1/2)

                select 'test_value';
                """
            ),
            script_name=DEFAULT_POST_DEPLOY_FILENAME_1,
            role=app.role,
            warehouse=app.warehouse,
            database=app.name,
        ),
        mock.call(
            queries=DEFAULT_POST_DEPLOY_CONTENT_2,
            script_name=DEFAULT_POST_DEPLOY_FILENAME_2,
            role=app.role,
            warehouse=app.warehouse,
            database=app.name,
        ),
    ]


@mock.patch(SQL_FACADE_EXECUTE_USER_SCRIPT)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_app_post_deploy_with_mixed_syntax_template(
    mock_conn,
    mock_cli_ctx,
    mock_sqlfacade_execute_user_script,
    project_directory,
    workspace_context,
    temporary_directory,
):
    mock_conn.return_value = MockConnectionCtx()
    mock_cli_ctx.return_value = {"ctx": {"env": {"test": "test_value"}}}

    # edit scripts/app_post_deploy1.sql to include template variables
    app_post_deploy_project_factory(
        custom_post_deploy_content_1=dedent(
            """\
            -- app post-deploy script (1/2)

            select '<% ctx.env.test %>';
            select '&{ ctx.env.test }';
            """
        )
    )

    dm = DefinitionManager()
    app_model: ApplicationEntityModel = dm.project_definition.entities["app"]
    app = ApplicationEntity(app_model, workspace_context)

    with pytest.raises(InvalidTemplateError) as err:
        app.execute_post_deploy_hooks()

    assert (
        "The SQL query in scripts/app_post_deploy1.sql mixes &{ ... } syntax and <% ... %> syntax."
        == str(err.value)
    )
