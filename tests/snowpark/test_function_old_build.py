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

import json
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest
from snowflake.cli.api.errno import DOES_NOT_EXIST_OR_NOT_AUTHORIZED
from snowflake.connector import ProgrammingError

from tests_common import IS_WINDOWS

if IS_WINDOWS:
    pytest.skip("Requires further refactor to work on Windows", allow_module_level=True)


mock_session_has_warehouse = mock.patch(
    "snowflake.cli.api.sql_execution.SqlExecutionMixin.session_has_warehouse",
    lambda _: True,
)


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager")
@mock_session_has_warehouse
def test_deploy_function(
    mock_object_manager,
    mock_connector,
    mock_ctx,
    runner,
    project_directory,
):
    mock_object_manager.return_value.describe.side_effect = ProgrammingError(
        errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED
    )
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    with project_directory("snowpark_functions") as project_dir:
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.dev_deployment') comment='deployments managed by Snowflake CLI'",
        f"put file://{Path(project_dir).resolve()}/app.py @MockDatabase.MockSchema.dev_deployment/my_snowpark_project/"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace function IDENTIFIER('MockDatabase.MockSchema.func1')(a string default 'default value', b variant)
            copy grants
            returns string
            language python
            runtime_version=3.10
            imports=('@MockDatabase.MockSchema.dev_deployment/my_snowpark_project/app.py')
            handler='app.func1_handler'
            packages=()
            """
        ).strip(),
    ]


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager")
@mock_session_has_warehouse
def test_deploy_function_with_external_access(
    mock_object_manager,
    mock_connector,
    mock_ctx,
    runner,
    project_directory,
):
    mock_object_manager.return_value.show.return_value = [
        {"name": "external_1", "type": "EXTERNAL_ACCESS"},
        {"name": "external_2", "type": "EXTERNAL_ACCESS"},
    ]
    mock_object_manager.return_value.describe.side_effect = ProgrammingError(
        errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED
    )
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    with project_directory("snowpark_function_external_access") as project_dir:
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.dev_deployment') comment='deployments managed by Snowflake CLI'",
        f"put file://{Path(project_dir).resolve()}/app.py @MockDatabase.MockSchema.dev_deployment/my_snowpark_project/"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace function IDENTIFIER('MockDatabase.MockSchema.func1')(a string, b variant)
            copy grants
            returns string
            language python
            runtime_version=3.10
            imports=('@MockDatabase.MockSchema.dev_deployment/my_snowpark_project/app.py')
            handler='app.func1_handler'
            packages=()
            external_access_integrations=(external_1, external_2)
            secrets=('cred'=cred_name, 'other'=other_name)
            """
        ).strip(),
    ]


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager")
@mock_session_has_warehouse
def test_deploy_function_secrets_without_external_access(
    mock_object_manager,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    os_agnostic_snapshot,
):
    mock_object_manager.return_value.show.return_value = [
        {"name": "external_1", "type": "EXTERNAL_ACCESS"},
        {"name": "external_2", "type": "EXTERNAL_ACCESS"},
    ]
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("snowpark_function_secrets_without_external_access"):
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ],
        )

    assert result.exit_code == 1, result.output
    assert result.output == os_agnostic_snapshot


@mock.patch("snowflake.connector.connect")
@mock_session_has_warehouse
def test_deploy_function_no_changes(
    mock_connector,
    runner,
    mock_ctx,
    mock_cursor,
    project_directory,
):
    rows = [
        ("packages", '["foo==1.2.3", "bar>=3.0.0"]'),
        ("handler", "app.func1_handler"),
        ("returns", "string"),
        ("imports", "dev_deployment/my_snowpark_project/app.py"),
        ("runtime_version", "3.10"),
    ]

    queries, result, project_dir = _deploy_function(
        rows,
        mock_connector,
        runner,
        mock_ctx,
        mock_cursor,
        project_directory,
        "--replace",
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [
        {
            "object": "MockDatabase.MockSchema.func1(a string default 'default value', b variant)",
            "status": "packages updated",
            "type": "function",
        }
    ]
    assert queries == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.dev_deployment') comment='deployments managed by Snowflake CLI'",
        f"put file://{Path(project_dir).resolve()}/app.py @MockDatabase.MockSchema.dev_deployment/my_snowpark_project/ auto_compress=false parallel=4 overwrite=True",
    ]


@mock.patch("snowflake.connector.connect")
@mock_session_has_warehouse
def test_deploy_function_needs_update_because_packages_changes(
    mock_connector,
    runner,
    mock_ctx,
    mock_cursor,
    project_directory,
):
    rows = [
        ("packages", '["foo==1.2.3"]'),
        ("handler", "main.py:app"),
        ("returns", "table(variant)"),
    ]

    queries, result, project_dir = _deploy_function(
        rows,
        mock_connector,
        runner,
        mock_ctx,
        mock_cursor,
        project_directory,
        "--replace",
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [
        {
            "object": "MockDatabase.MockSchema.func1(a string default 'default value', b variant)",
            "status": "definition updated",
            "type": "function",
        }
    ]
    assert queries == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.dev_deployment') comment='deployments managed by Snowflake CLI'",
        f"put file://{Path(project_dir).resolve()}/app.py @MockDatabase.MockSchema.dev_deployment/my_snowpark_project/ auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace function IDENTIFIER('MockDatabase.MockSchema.func1')(a string default 'default value', b variant)
            copy grants
            returns string
            language python
            runtime_version=3.10
            imports=('@MockDatabase.MockSchema.dev_deployment/my_snowpark_project/app.py')
            handler='app.func1_handler'
            packages=('foo==1.2.3','bar>=3.0.0')
            """
        ).strip(),
    ]


@mock.patch("snowflake.connector.connect")
@mock_session_has_warehouse
def test_deploy_function_needs_update_because_handler_changes(
    mock_connector,
    runner,
    mock_ctx,
    mock_cursor,
    project_directory,
):
    rows = [
        ("packages", '["foo==1.2.3", "bar>=3.0.0"]'),
        ("handler", "main.py:oldApp"),
        ("returns", "table(variant)"),
    ]

    queries, result, project_dir = _deploy_function(
        rows,
        mock_connector,
        runner,
        mock_ctx,
        mock_cursor,
        project_directory,
        "--replace",
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [
        {
            "object": "MockDatabase.MockSchema.func1(a string default 'default value', b variant)",
            "status": "definition updated",
            "type": "function",
        }
    ]
    assert queries == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.dev_deployment') comment='deployments managed by Snowflake CLI'",
        f"put file://{Path(project_dir).resolve()}/app.py @MockDatabase.MockSchema.dev_deployment/my_snowpark_project/"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace function IDENTIFIER('MockDatabase.MockSchema.func1')(a string default 'default value', b variant)
            copy grants
            returns string
            language python
            runtime_version=3.10
            imports=('@MockDatabase.MockSchema.dev_deployment/my_snowpark_project/app.py')
            handler='app.func1_handler'
            packages=('foo==1.2.3','bar>=3.0.0')
            """
        ).strip(),
    ]


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock_session_has_warehouse
def test_deploy_function_fully_qualified_name_duplicated_database(
    mock_om_show,
    mock_om_describe,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    alter_snowflake_yml,
    os_agnostic_snapshot,
):
    number_of_functions_in_project = 6
    mock_om_describe.side_effect = [
        ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
    ] * number_of_functions_in_project
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("snowpark_function_fully_qualified_name") as tmp_dir:
        result = runner.invoke(["snowpark", "deploy"])
        assert result.output == os_agnostic_snapshot(name="database error")


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock_session_has_warehouse
def test_deploy_function_fully_qualified_name_duplicated_schema(
    mock_om_show,
    mock_om_describe,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    alter_snowflake_yml,
    os_agnostic_snapshot,
):
    number_of_functions_in_project = 6
    mock_om_describe.side_effect = [
        ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
    ] * number_of_functions_in_project
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("snowpark_function_fully_qualified_name") as tmp_dir:
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.5.name",
            value="custom_schema.fqn_function_error",
        )
        result = runner.invoke(["snowpark", "deploy"])
        assert result.output == os_agnostic_snapshot(name="schema error")


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock_session_has_warehouse
def test_deploy_function_fully_qualified_name(
    mock_om_show,
    mock_om_describe,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    alter_snowflake_yml,
    os_agnostic_snapshot,
):
    number_of_functions_in_project = 6
    mock_om_describe.side_effect = [
        ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
    ] * number_of_functions_in_project
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("snowpark_function_fully_qualified_name") as tmp_dir:
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.5.name",
            value="fqn_function3",
        )
        result = runner.invoke(["snowpark", "deploy"])
        assert result.exit_code == 0
        assert result.output == os_agnostic_snapshot(name="ok")


@pytest.mark.parametrize(
    "parameter_type,default_value",
    [
        ("string", None),
        ("string", ""),
        ("int", None),
        ("variant", None),
        ("bool", None),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager")
@mock_session_has_warehouse
def test_deploy_function_with_empty_default_value(
    mock_object_manager,
    mock_connector,
    mock_ctx,
    runner,
    project_directory,
    alter_snowflake_yml,
    parameter_type,
    default_value,
):
    mock_object_manager.return_value.describe.side_effect = ProgrammingError(
        errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED
    )
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    with project_directory("snowpark_functions") as project_dir:
        snowflake_yml = project_dir / "snowflake.yml"
        for param, value in [("type", parameter_type), ("default", default_value)]:
            alter_snowflake_yml(
                snowflake_yml,
                parameter_path=f"snowpark.functions.0.signature.0.{param}",
                value=value,
            )
        alter_snowflake_yml(
            snowflake_yml,
            parameter_path=f"snowpark.functions.0.runtime",
            value="3.10",
        )
        result = runner.invoke(
            ["snowpark", "deploy", "--format", "json"], catch_exceptions=False
        )
    default_value_json = default_value
    if default_value is None:
        default_value_json = "null"
    elif parameter_type == "string":
        default_value_json = f"'{default_value}'"

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [
        {
            "object": f"MockDatabase.MockSchema.func1(a {parameter_type} default {default_value_json}, b variant)",
            "status": "created",
            "type": "function",
        }
    ]


@mock.patch("snowflake.connector.connect")
def test_execute_function(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "execute",
            "function",
            "functionName(42, 'string')",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "select functionName(42, 'string')"


def _deploy_function(
    rows,
    mock_connector,
    runner,
    mock_ctx,
    mock_cursor,
    project_directory,
    *args,
):
    ctx = mock_ctx(mock_cursor(rows=rows, columns=[]))
    mock_connector.return_value = ctx
    with (
        mock.patch(
            "snowflake.cli._plugins.snowpark.commands.ObjectManager.describe"
        ) as om_describe,
        mock.patch(
            "snowflake.cli._plugins.snowpark.commands.ObjectManager.show"
        ) as om_show,
    ):
        om_describe.return_value = rows

        with project_directory("snowpark_functions") as temp_dir:
            (Path(temp_dir) / "requirements.snowflake.txt").write_text(
                "foo==1.2.3\nbar>=3.0.0"
            )
            result = runner.invoke(
                [
                    "snowpark",
                    "deploy",
                    "--format",
                    "json",
                    *args,
                ]
            )
    queries = ctx.get_queries()
    return queries, result, temp_dir
