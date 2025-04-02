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
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntity,
    ApplicationPackageEntityModel,
)
from snowflake.cli._plugins.nativeapp.exceptions import MissingScriptError
from snowflake.cli._plugins.nativeapp.sf_facade_exceptions import UserScriptError
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.entities.utils import execute_post_deploy_hooks
from snowflake.cli.api.exceptions import InvalidTemplateError
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.project.errors import SchemaValidationError

from tests.conftest import MockConnectionCtx
from tests.nativeapp.factories import (
    ApplicationEntityModelFactory,
    ApplicationPackageEntityModelFactory,
    ProjectV2Factory,
    ProjectV11Factory,
)
from tests.nativeapp.patch_utils import mock_connection
from tests.nativeapp.utils import (
    CLI_GLOBAL_TEMPLATE_CONTEXT,
    SQL_FACADE_EXECUTE_USER_SCRIPT,
)

DEFAULT_POST_DEPLOY_FILENAME_1 = "scripts/pkg_post_deploy1.sql"
DEFAULT_POST_DEPLOY_CONTENT_1 = dedent(
    """\
    -- package post-deploy script (1/2)

    select myapp;
    select package_bar;
    """
)

DEFAULT_POST_DEPLOY_FILENAME_2 = "scripts/pkg_post_deploy2.sql"
DEFAULT_POST_DEPLOY_CONTENT_2 = "-- package post-deploy script (2/2)\n"


def pkg_post_deploy_project_factory(
    custom_post_deploy_content_1: Optional[str] = None,
    custom_post_deploy_content_2: Optional[str] = None,
) -> None:
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg",
                meta__post_deploy=[
                    {"sql_script": DEFAULT_POST_DEPLOY_FILENAME_1},
                    {"sql_script": DEFAULT_POST_DEPLOY_FILENAME_2},
                ],
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
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
def test_package_post_deploy_scripts(
    mock_conn,
    mock_cli_ctx,
    mock_sqlfacade_execute_user_script,
    project_directory,
    mock_cursor,
    workspace_context,
    temporary_directory,
):
    mock_conn.return_value = MockConnectionCtx()

    pkg_post_deploy_project_factory()

    dm = DefinitionManager()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    mock_cli_ctx.return_value = dm.template_context

    pkg.execute_post_deploy_hooks()

    assert mock_sqlfacade_execute_user_script.mock_calls == [
        mock.call(
            queries=DEFAULT_POST_DEPLOY_CONTENT_1,
            script_name=DEFAULT_POST_DEPLOY_FILENAME_1,
            role=pkg.role,
            warehouse=pkg.warehouse,
            database=pkg.name,
        ),
        mock.call(
            queries=DEFAULT_POST_DEPLOY_CONTENT_2,
            script_name=DEFAULT_POST_DEPLOY_FILENAME_2,
            role=pkg.role,
            warehouse=pkg.warehouse,
            database=pkg.name,
        ),
    ]


@mock.patch(SQL_FACADE_EXECUTE_USER_SCRIPT)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_package_post_deploy_scripts_with_no_scripts(
    mock_conn,
    mock_cli_ctx,
    mock_sqlfacade_execute_user_script,
    project_directory,
    workspace_context,
    temporary_directory,
):
    mock_conn.return_value = MockConnectionCtx()
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg",
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
            ),
        ),
        pdf__env__foo="bar",
    )

    dm = DefinitionManager()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["pkg"]
    mock_cli_ctx.return_value = dm.template_context

    execute_post_deploy_hooks(
        console=cc,
        project_root=temporary_directory,
        post_deploy_hooks=pkg_model.meta.post_deploy,
        deployed_object_type=pkg_model.get_type(),
        database_name=pkg_model.fqn.name,
        role_name=workspace_context.default_role,
        warehouse_name=workspace_context.default_warehouse,
    )

    assert mock_sqlfacade_execute_user_script.mock_calls == []


@mock.patch(SQL_FACADE_EXECUTE_USER_SCRIPT)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_package_post_deploy_scripts_with_non_existing_scripts(
    mock_conn,
    mock_cli_ctx,
    mock_sqlfacade_execute_user_script,
    project_directory,
    mock_cursor,
    workspace_context,
    temporary_directory,
):
    mock_conn.return_value = MockConnectionCtx()

    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg",
                meta__post_deploy=[
                    {"sql_script": "scripts/package_missing_script.sql"},
                ],
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
            ),
        ),
        pdf__env__foo="bar",
    )

    dm = DefinitionManager()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["pkg"]
    mock_cli_ctx.return_value = dm.template_context

    with pytest.raises(MissingScriptError) as err:
        execute_post_deploy_hooks(
            console=cc,
            project_root=temporary_directory,
            post_deploy_hooks=pkg_model.meta.post_deploy,
            deployed_object_type=pkg_model.get_type(),
            database_name=pkg_model.fqn.name,
            role_name=workspace_context.default_role,
            warehouse_name=workspace_context.default_warehouse,
        )

    assert (
        err.value.message
        == 'Script "scripts/package_missing_script.sql" does not exist'
    )


@mock.patch(SQL_FACADE_EXECUTE_USER_SCRIPT)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_package_post_deploy_scripts_with_sql_error(
    mock_conn,
    mock_cli_ctx,
    mock_sqlfacade_execute_user_script,
    project_directory,
    workspace_context,
    temporary_directory,
):
    mock_conn.return_value = MockConnectionCtx()

    pkg_post_deploy_project_factory()

    dm = DefinitionManager()
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["pkg"]
    mock_cli_ctx.return_value = dm.template_context
    mock_sqlfacade_execute_user_script.side_effect = UserScriptError(
        "script.sql", "Error message."
    )

    with pytest.raises(UserScriptError):
        execute_post_deploy_hooks(
            console=cc,
            project_root=temporary_directory,
            post_deploy_hooks=pkg_model.meta.post_deploy,
            deployed_object_type=pkg_model.get_type(),
            database_name=pkg_model.fqn.name,
            role_name=workspace_context.default_role,
            warehouse_name=workspace_context.default_warehouse,
        )


@mock.patch.dict(os.environ, {"USER": "test_user"})
def test_package_scripts_and_post_deploy_found(
    temporary_directory,
):
    ProjectV11Factory(
        pdf__native_app__package__scripts=["scripts/package_script1.sql"],
        pdf__native_app__artifacts=["README.md", "setup.sql", "manifest.yml"],
        pdf__native_app__package__post_deploy=[
            {"sql_script": "post_deploy1.sql"},
        ],
        pdf__native_app__package__warehouse="non_existent_warehouse",
        files={
            "README.md": "",
            "setup.sql": "select 1",
            "manifest.yml": "\n",
            "scripts/package_script1.sql": "\n",
            "post_deploy1.sql": "\n",
        },
    )

    with pytest.raises(SchemaValidationError) as err:
        DefinitionManager().project_definition  # noqa

    assert (
        "package.scripts and package.post_deploy fields cannot be used together"
        in err.value.message
    )


@pytest.mark.parametrize(
    "template_syntax", [("<% ctx.env.test %>"), ("&{ ctx.env.test }")]
)
@mock.patch(SQL_FACADE_EXECUTE_USER_SCRIPT)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_package_post_deploy_scripts_with_templates(
    mock_conn,
    mock_cli_ctx,
    mock_sqlfacade_execute_user_script,
    project_directory,
    template_syntax,
    mock_cursor,
    workspace_context,
    temporary_directory,
):
    mock_conn.return_value = MockConnectionCtx()

    pkg_post_deploy_project_factory(
        custom_post_deploy_content_1=dedent(
            f"""\
            -- package post-deploy script (1/2)

            select '{template_syntax}';
            """
        )
    )

    dm = DefinitionManager(context_overrides={"ctx": {"env": {"test": "test_value"}}})
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["pkg"]
    pkg = ApplicationPackageEntity(pkg_model, workspace_context)
    mock_cli_ctx.return_value = dm.template_context

    pkg.execute_post_deploy_hooks()

    assert mock_sqlfacade_execute_user_script.mock_calls == [
        mock.call(
            queries=dedent(
                """\
            -- package post-deploy script (1/2)

            select 'test_value';
            """
            ),
            script_name=DEFAULT_POST_DEPLOY_FILENAME_1,
            role=pkg.role,
            warehouse=pkg.warehouse,
            database=pkg.name,
        ),
        mock.call(
            queries=DEFAULT_POST_DEPLOY_CONTENT_2,
            script_name=DEFAULT_POST_DEPLOY_FILENAME_2,
            role=pkg.role,
            warehouse=pkg.warehouse,
            database=pkg.name,
        ),
    ]


@mock.patch(SQL_FACADE_EXECUTE_USER_SCRIPT)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_package_post_deploy_scripts_with_mix_syntax_templates(
    mock_conn,
    mock_cli_ctx,
    mock_sqlfacade_execute_user_script,
    project_directory,
    workspace_context,
    temporary_directory,
):
    mock_conn.return_value = MockConnectionCtx()
    pkg_post_deploy_project_factory(
        custom_post_deploy_content_1=dedent(
            """\
            -- package post-deploy script (1/2)

            select '<% ctx.env.test %>';
            select '&{ ctx.env.test }';
            """
        )
    )

    dm = DefinitionManager(context_overrides={"ctx": {"env": {"test": "test_value"}}})
    pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities["pkg"]
    mock_cli_ctx.return_value = dm.template_context

    with pytest.raises(InvalidTemplateError) as err:
        execute_post_deploy_hooks(
            console=cc,
            project_root=temporary_directory,
            post_deploy_hooks=pkg_model.meta.post_deploy,
            deployed_object_type=pkg_model.get_type(),
            database_name=pkg_model.fqn.name,
            role_name=workspace_context.default_role,
            warehouse_name=workspace_context.default_warehouse,
        )

    assert (
        "The SQL query in scripts/pkg_post_deploy1.sql mixes &{ ... } syntax and <% ... %> syntax."
        == str(err.value)
    )
