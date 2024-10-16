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
from snowflake.cli._plugins.nativeapp.entities.application import (
    ApplicationEntity,
    ApplicationEntityModel,
)
from snowflake.cli._plugins.nativeapp.exceptions import MissingScriptError
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.entities.utils import execute_post_deploy_hooks
from snowflake.cli.api.exceptions import InvalidTemplate
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.schemas.entities.common import PostDeployHook

from tests.nativeapp.factories import (
    ApplicationEntityModelFactory,
    ApplicationPackageEntityModelFactory,
    ProjectV2Factory,
)
from tests.nativeapp.patch_utils import mock_connection
from tests.nativeapp.utils import (
    CLI_GLOBAL_TEMPLATE_CONTEXT,
    SQL_EXECUTOR_EXECUTE,
    SQL_EXECUTOR_EXECUTE_QUERIES,
)
from tests.testing_utils.fixtures import MockConnectionCtx

MOCK_CONNECTION_DB = "tests.testing_utils.fixtures.MockConnectionCtx.database"
MOCK_CONNECTION_WH = "tests.testing_utils.fixtures.MockConnectionCtx.warehouse"


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_sql_scripts(
    mock_conn,
    mock_cli_ctx,
    mock_execute_queries,
    mock_execute_query,
    temp_dir,
):
    mock_conn.return_value = MockConnectionCtx()
    post_deploy_1 = dedent(
        """\
            -- app post-deploy script (1/2)

            select myapp;
            select bar;
            """
    )
    post_deploy_2 = "-- app post-deploy script (2/2)\n"

    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg",
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
                meta__post_deploy=[
                    {"sql_script": "scripts/app_post_deploy1.sql"},
                    {"sql_script": "scripts/app_post_deploy2.sql"},
                ],
            ),
        ),
        pdf__env__foo="bar",
        files={
            "scripts/app_post_deploy1.sql": post_deploy_1,
            "scripts/app_post_deploy2.sql": post_deploy_2,
        },
    )

    dm = DefinitionManager()
    mock_cli_ctx.return_value = dm.template_context
    app_model: ApplicationEntityModel = dm.project_definition.entities["app"]
    ApplicationEntity.execute_post_deploy_hooks(
        console=cc,
        project_root=dm.project_root,
        post_deploy_hooks=app_model.meta.post_deploy,
        app_name=app_model.fqn.name,
        app_warehouse=app_model.meta.warehouse or "MockWarehouse",
    )

    mock_execute_query.assert_has_calls(
        [
            mock.call(f"use database {app_model.fqn.name}"),
            mock.call(f"use database {app_model.fqn.name}"),
        ]
    )
    assert mock_execute_queries.mock_calls == [
        mock.call(post_deploy_1),
        mock.call(post_deploy_2),
    ]


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
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
    with project_directory("napp_post_deploy_v2") as project_dir:
        dm = DefinitionManager()
        app_model: ApplicationEntityModel = dm.project_definition.entities["myapp"]
        mock_cli_ctx.return_value = dm.template_context

        # Directly testing the function without the use_warehouse
        # that ApplicationEntity.execute_post_deploy_hooks adds
        execute_post_deploy_hooks(
            console=cc,
            project_root=dm.project_root,
            post_deploy_hooks=app_model.meta.post_deploy,
            deployed_object_type="application",
            database_name=app_model.fqn.name,
        )

        # Verify no "use warehouse"
        # Verify "use database" applies to current application
        assert mock_execute_query.mock_calls == [
            mock.call("use database myapp"),
            mock.call("use database myapp"),
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


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
def test_missing_sql_script(
    mock_execute_query,
    mock_conn,
    project_directory,
):
    mock_conn.return_value = MockConnectionCtx()
    with project_directory("napp_post_deploy_missing_file_v2") as project_dir:
        dm = DefinitionManager()
        app_model: ApplicationEntityModel = dm.project_definition.entities["myapp"]

        with pytest.raises(MissingScriptError) as err:
            ApplicationEntity.execute_post_deploy_hooks(
                console=cc,
                project_root=dm.project_root,
                post_deploy_hooks=app_model.meta.post_deploy,
                app_name=app_model.fqn.name,
                app_warehouse=app_model.meta.warehouse or "MockWarehouse",
            )

        assert err.value.message == 'Script "scripts/missing.sql" does not exist'


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
def test_invalid_hook_type(
    mock_conn,
    mock_execute_query,
    project_directory,
):
    mock_hook = mock.Mock()
    mock_hook.invalid_type = "invalid_type"
    mock_hook.sql_script = None
    mock_conn.return_value = MockConnectionCtx()
    with project_directory("napp_post_deploy_v2") as project_dir:
        dm = DefinitionManager()
        app_model: ApplicationEntityModel = dm.project_definition.entities["myapp"]

        with pytest.raises(ValueError) as err:
            ApplicationEntity.execute_post_deploy_hooks(
                console=cc,
                project_root=dm.project_root,
                post_deploy_hooks=[mock_hook],
                app_name=app_model.fqn.name,
                app_warehouse=app_model.meta.warehouse or "MockWarehouse",
            )
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
            PostDeployHook(**args)

        assert expected_error in str(SchemaValidationError(err.value))
    else:
        PostDeployHook(**args)


@pytest.mark.parametrize(
    "template_syntax", [("<% ctx.env.test %>"), ("&{ ctx.env.test }")]
)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_app_post_deploy_with_template(
    mock_conn,
    mock_cli_ctx,
    mock_execute_queries,
    mock_execute_query,
    project_directory,
    template_syntax,
):
    mock_conn.return_value = MockConnectionCtx()
    mock_cli_ctx.return_value = {"ctx": {"env": {"test": "test_value"}}}

    with project_directory("napp_post_deploy_v2") as project_dir:
        # edit scripts/app_post_deploy1.sql to include template variables
        with open(project_dir / "scripts" / "app_post_deploy1.sql", "w") as f:
            f.write(
                dedent(
                    f"""\
                    -- app post-deploy script (1/2)

                    select '{template_syntax}';
                    """
                )
            )
        dm = DefinitionManager()
        app_model: ApplicationEntityModel = dm.project_definition.entities["myapp"]

        ApplicationEntity.execute_post_deploy_hooks(
            console=cc,
            project_root=dm.project_root,
            post_deploy_hooks=app_model.meta.post_deploy,
            app_name=app_model.fqn.name,
            app_warehouse=app_model.meta.warehouse or "MockWarehouse",
        )

        mock_execute_query.assert_has_calls(
            [
                mock.call(f"use database {app_model.fqn.name}"),
                mock.call(f"use database {app_model.fqn.name}"),
            ]
        )
        assert mock_execute_queries.mock_calls == [
            # Verify template variables were expanded correctly
            mock.call(
                dedent(
                    """\
                -- app post-deploy script (1/2)

                select 'test_value';
                """
                )
            ),
            mock.call("-- app post-deploy script (2/2)\n"),
        ]


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_app_post_deploy_with_mixed_syntax_template(
    mock_conn,
    mock_cli_ctx,
    mock_execute_queries,
    mock_execute_query,
    project_directory,
):
    mock_conn.return_value = MockConnectionCtx()
    mock_cli_ctx.return_value = {"ctx": {"env": {"test": "test_value"}}}

    with project_directory("napp_post_deploy_v2") as project_dir:
        # edit scripts/app_post_deploy1.sql to include template variables
        with open(project_dir / "scripts" / "app_post_deploy1.sql", "w") as f:
            f.write(
                dedent(
                    """\
                    -- app post-deploy script (1/2)

                    select '<% ctx.env.test %>';
                    select '&{ ctx.env.test }';
                    """
                )
            )
        dm = DefinitionManager()
        app_model: ApplicationEntityModel = dm.project_definition.entities["myapp"]

        with pytest.raises(InvalidTemplate) as err:
            ApplicationEntity.execute_post_deploy_hooks(
                console=cc,
                project_root=dm.project_root,
                post_deploy_hooks=app_model.meta.post_deploy,
                app_name=app_model.fqn.name,
                app_warehouse=app_model.meta.warehouse or "MockWarehouse",
            )

        assert (
            "The SQL query in scripts/app_post_deploy1.sql mixes &{ ... } syntax and <% ... %> syntax."
            == str(err.value)
        )
