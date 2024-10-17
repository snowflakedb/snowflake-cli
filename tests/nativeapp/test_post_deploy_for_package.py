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
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntity,
    ApplicationPackageEntityModel,
)
from snowflake.cli._plugins.nativeapp.exceptions import MissingScriptError
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.entities.utils import execute_post_deploy_hooks
from snowflake.cli.api.exceptions import InvalidTemplate
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.connector import ProgrammingError

from tests.nativeapp.patch_utils import mock_connection
from tests.nativeapp.utils import (
    CLI_GLOBAL_TEMPLATE_CONTEXT,
    SQL_EXECUTOR_EXECUTE,
    SQL_EXECUTOR_EXECUTE_QUERIES,
    mock_execute_helper,
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
    mock_cursor,
):
    mock_conn.return_value = MockConnectionCtx()
    with project_directory("napp_post_deploy_v2") as project_dir:
        dm = DefinitionManager(project_dir)
        pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities[
            "myapp_pkg"
        ]
        mock_cli_ctx.return_value = dm.template_context
        side_effects, expected = mock_execute_helper(
            [
                (
                    mock_cursor([("MockWarehouse",)], []),
                    mock.call("select current_warehouse()"),
                ),
                (None, mock.call("use database myapp_pkg_test_user")),
                (None, mock.call("use database myapp_pkg_test_user")),
            ]
        )
        mock_execute_query.side_effect = side_effects

        ApplicationPackageEntity.execute_post_deploy_hooks(
            console=cc,
            project_root=project_dir,
            post_deploy_hooks=pkg_model.meta.post_deploy,
            package_name=pkg_model.fqn.name,
            package_warehouse=pkg_model.meta.warehouse or "MockWarehouse",
        )

        assert mock_execute_query.mock_calls == expected
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
    with project_directory(
        "napp_project_2",
        {"entities": {"myapp_pkg_polly": {"meta": {"post_deploy": []}}}},
    ) as project_dir:
        dm = DefinitionManager(project_dir)
        pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities[
            "myapp_pkg_polly"
        ]
        mock_cli_ctx.return_value = dm.template_context

        execute_post_deploy_hooks(
            console=cc,
            project_root=project_dir,
            post_deploy_hooks=pkg_model.meta.post_deploy,
            deployed_object_type=pkg_model.get_type(),
            database_name=pkg_model.fqn.name,
        )

        assert mock_execute_query.mock_calls == []
        assert mock_execute_queries.mock_calls == []


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_package_post_deploy_scripts_with_non_existing_scripts(
    mock_conn, mock_cli_ctx, mock_execute_query, project_directory, mock_cursor
):
    mock_conn.return_value = MockConnectionCtx()
    with project_directory("napp_post_deploy_missing_file_v2") as project_dir:
        dm = DefinitionManager(project_dir)
        pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities[
            "myapp_pkg"
        ]
        mock_cli_ctx.return_value = dm.template_context

        side_effects, expected = mock_execute_helper(
            [
                (
                    mock_cursor([("MockWarehouse",)], []),
                    mock.call("select current_warehouse()"),
                ),
            ]
        )
        mock_execute_query.side_effect = side_effects

        with pytest.raises(MissingScriptError) as err:
            execute_post_deploy_hooks(
                console=cc,
                project_root=project_dir,
                post_deploy_hooks=pkg_model.meta.post_deploy,
                deployed_object_type=pkg_model.get_type(),
                database_name=pkg_model.fqn.name,
            )

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
    with project_directory("napp_post_deploy_v2") as project_dir:
        dm = DefinitionManager(project_dir)
        pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities[
            "myapp_pkg"
        ]
        mock_cli_ctx.return_value = dm.template_context
        mock_execute_query.side_effect = ProgrammingError()

        with pytest.raises(ProgrammingError):
            execute_post_deploy_hooks(
                console=cc,
                project_root=project_dir,
                post_deploy_hooks=pkg_model.meta.post_deploy,
                deployed_object_type=pkg_model.get_type(),
                database_name=pkg_model.fqn.name,
            )


@mock.patch.dict(os.environ, {"USER": "test_user"})
def test_package_scripts_and_post_deploy_found(
    project_directory,
):
    with project_directory(
        "napp_post_deploy",
        {"native_app": {"package": {"scripts": ["scripts/package_post_deploy2.sql"]}}},
    ) as project_dir:
        with pytest.raises(SchemaValidationError) as err:
            DefinitionManager(project_dir).project_definition  # noqa

        assert (
            "package.scripts and package.post_deploy fields cannot be used together"
            in err.value.message
        )


@pytest.mark.parametrize(
    "template_syntax", [("<% ctx.env.test %>"), ("&{ ctx.env.test }")]
)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(SQL_EXECUTOR_EXECUTE_QUERIES)
@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, new_callable=mock.PropertyMock)
@mock.patch.dict(os.environ, {"USER": "test_user"})
@mock_connection()
def test_package_post_deploy_scripts_with_templates(
    mock_conn,
    mock_cli_ctx,
    mock_execute_queries,
    mock_execute_query,
    project_directory,
    template_syntax,
    mock_cursor,
):
    mock_conn.return_value = MockConnectionCtx()
    with project_directory("napp_post_deploy_v2") as project_dir:
        # edit scripts/package_post_deploy1.sql to include template variables
        with open(project_dir / "scripts" / "package_post_deploy1.sql", "w") as f:
            f.write(
                dedent(
                    f"""\
                    -- package post-deploy script (1/2)

                    select '{template_syntax}';
                    """
                )
            )

        dm = DefinitionManager(project_dir, {"ctx": {"env": {"test": "test_value"}}})
        pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities[
            "myapp_pkg"
        ]
        mock_cli_ctx.return_value = dm.template_context

        side_effects, expected = mock_execute_helper(
            [
                (
                    mock_cursor([("MockWarehouse",)], []),
                    mock.call("select current_warehouse()"),
                ),
                (None, mock.call("use database myapp_pkg_test_user")),
                (None, mock.call("use database myapp_pkg_test_user")),
            ]
        )
        mock_execute_query.side_effect = side_effects

        ApplicationPackageEntity.execute_post_deploy_hooks(
            console=cc,
            project_root=project_dir,
            post_deploy_hooks=pkg_model.meta.post_deploy,
            package_name=pkg_model.fqn.name,
            package_warehouse=pkg_model.meta.warehouse or "MockWarehouse",
        )

        assert mock_execute_query.mock_calls == expected
        assert mock_execute_queries.mock_calls == [
            # Verify template variables were expanded correctly
            mock.call(
                dedent(
                    """\
                    -- package post-deploy script (1/2)

                    select 'test_value';
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
def test_package_post_deploy_scripts_with_mix_syntax_templates(
    mock_conn,
    mock_cli_ctx,
    mock_execute_queries,
    mock_execute_query,
    project_directory,
):
    mock_conn.return_value = MockConnectionCtx()
    with project_directory("napp_post_deploy_v2") as project_dir:
        # edit scripts/package_post_deploy1.sql to include template variables
        with open(project_dir / "scripts" / "package_post_deploy1.sql", "w") as f:
            f.write(
                dedent(
                    """\
                    -- package post-deploy script (1/2)

                    select '<% ctx.env.test %>';
                    select '&{ ctx.env.test }';
                    """
                )
            )

        dm = DefinitionManager(project_dir, {"ctx": {"env": {"test": "test_value"}}})
        pkg_model: ApplicationPackageEntityModel = dm.project_definition.entities[
            "myapp_pkg"
        ]
        mock_cli_ctx.return_value = dm.template_context

        with pytest.raises(InvalidTemplate) as err:
            execute_post_deploy_hooks(
                console=cc,
                project_root=project_dir,
                post_deploy_hooks=pkg_model.meta.post_deploy,
                deployed_object_type=pkg_model.get_type(),
                database_name=pkg_model.fqn.name,
            )

        assert (
            "The SQL query in scripts/package_post_deploy1.sql mixes &{ ... } syntax and <% ... %> syntax."
            == str(err.value)
        )
