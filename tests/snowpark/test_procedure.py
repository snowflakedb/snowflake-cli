import json
from pathlib import Path
from textwrap import dedent
from unittest import mock
from unittest.mock import call

from snowflake.cli.api.constants import ObjectType
from snowflake.connector import ProgrammingError


def test_deploy_function_no_procedure(runner, project_directory):
    with project_directory("empty_project"):
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ],
        )
    assert result.exit_code == 1
    assert "No snowpark project definition found" in result.output


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager")
def test_deploy_procedure(
    mock_object_manager,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
):

    mock_object_manager.return_value.describe.side_effect = ProgrammingError(
        "does not exist or not authorized"
    )
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("snowpark_procedures") as tmp:
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ]
        )

    assert result.exit_code == 0, result.output
    mock_object_manager.return_value.describe.assert_has_calls(
        [
            call(object_type=str(ObjectType.PROCEDURE), name="procedureName(string)"),
            call(object_type=str(ObjectType.PROCEDURE), name="test()"),
        ]
    )
    assert ctx.get_queries() == [
        "create stage if not exists dev_deployment comment='deployments managed by snowcli'",
        f"put file://{Path(tmp).resolve()}/app.zip @dev_deployment/my_snowpark_project auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace procedure procedureName(name string)
            returns string
            language python
            runtime_version=3.8
            imports=('@dev_deployment/my_snowpark_project/app.zip')
            handler='hello'
            packages=()
            """
        ).strip(),
        dedent(
            """\
            create or replace procedure test()
            returns string
            language python
            runtime_version=3.10
            imports=('@dev_deployment/my_snowpark_project/app.zip')
            handler='test'
            packages=()
            """
        ).strip(),
    ]


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager")
def test_deploy_procedure_with_external_access(
    mock_object_manager,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
):
    mock_object_manager.return_value.describe.side_effect = ProgrammingError(
        "does not exist or not authorized"
    )
    mock_object_manager.return_value.show.return_value = [
        {"name": "external_1", "type": "EXTERNAL_ACCESS"},
        {"name": "external_2", "type": "EXTERNAL_ACCESS"},
    ]

    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("snowpark_procedure_external_access") as project_dir:
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ]
        )

    assert result.exit_code == 0, result.output
    mock_object_manager.return_value.describe.assert_has_calls(
        [
            call(object_type=str(ObjectType.PROCEDURE), name="procedureName(string)"),
        ]
    )
    assert ctx.get_queries() == [
        "create stage if not exists dev_deployment comment='deployments managed by snowcli'",
        f"put file://{Path(project_dir).resolve()}/app.zip @dev_deployment/my_snowpark_project"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace procedure procedureName(name string)
            returns string
            language python
            runtime_version=3.8
            imports=('@dev_deployment/my_snowpark_project/app.zip')
            handler='app.hello'
            packages=()
            external_access_integrations=(external_1,external_2)
            secrets=('cred'=cred_name,'other'=other_name)
            """
        ).strip(),
    ]


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager")
def test_deploy_procedure_secrets_without_external_access(
    mock_object_manager,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    snapshot,
):
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    mock_object_manager.return_value.show.return_value = [
        {"name": "external_1", "type": "EXTERNAL_ACCESS"},
        {"name": "external_2", "type": "EXTERNAL_ACCESS"},
    ]

    with project_directory("snowpark_procedure_secrets_without_external_access"):
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 1, result.output
    assert result.output == snapshot


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager")
def test_deploy_procedure_fails_if_integration_does_not_exists(
    mock_object_manager,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    snapshot,
):
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    mock_object_manager.return_value.show.return_value = [
        {"name": "external_1", "type": "EXTERNAL_ACCESS"},
    ]

    with project_directory("snowpark_procedure_external_access"):
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 1, result.output
    assert result.output == snapshot


@mock.patch(
    "snowflake.cli.plugins.snowpark.commands._check_if_all_defined_integrations_exists"
)
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager")
def test_deploy_procedure_fails_if_object_exists_and_no_replace(
    mock_object_manager,
    _,
    runner,
    mock_cursor,
    project_directory,
    snapshot,
):
    mock_object_manager.return_value.describe.return_value = mock_cursor(
        [
            ("packages", "[]"),
            ("handler", "hello"),
            ("returns", "string"),
        ],
        columns=["key", "value"],
    )

    with project_directory("snowpark_procedures"):
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ]
        )

    assert result.exit_code == 1
    assert result.output == snapshot


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager")
def test_deploy_procedure_replace_nothing_to_update(
    mock_object_manager,
    mock_conn,
    runner,
    mock_cursor,
    mock_ctx,
    project_directory,
):
    mock_object_manager.return_value.describe.side_effect = [
        mock_cursor(
            [
                ("packages", "[]"),
                ("handler", "hello"),
                ("returns", "string"),
            ],
            columns=["key", "value"],
        ),
        mock_cursor(
            [
                ("packages", "[]"),
                ("handler", "test"),
                ("returns", "string"),
            ],
            columns=["key", "value"],
        ),
    ]
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("snowpark_procedures"):
        result = runner.invoke(["snowpark", "deploy", "--replace", "--format", "json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [
        {
            "object": "procedureName(name string)",
            "status": "packages updated",
            "type": "procedure",
        },
        {"object": "test()", "status": "packages updated", "type": "procedure"},
    ]


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager")
def test_deploy_procedure_replace_updates_single_object(
    mock_object_manager,
    mock_conn,
    runner,
    mock_cursor,
    mock_ctx,
    project_directory,
):
    mock_object_manager.return_value.describe.side_effect = [
        mock_cursor(
            [
                ("packages", "[]"),
                ("handler", "hello"),
                ("returns", "string"),
            ],
            columns=["key", "value"],
        ),
        mock_cursor(
            [
                ("packages", "[]"),
                ("handler", "foo"),
                ("returns", "string"),
            ],
            columns=["key", "value"],
        ),
    ]
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("snowpark_procedures"):
        result = runner.invoke(["snowpark", "deploy", "--replace", "--format", "json"])

    assert result.exit_code == 0
    assert json.loads(result.output) == [
        {
            "object": "procedureName(name string)",
            "status": "packages updated",
            "type": "procedure",
        },
        {"object": "test()", "status": "definition updated", "type": "procedure"},
    ]


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager")
def test_deploy_procedure_replace_creates_missing_object(
    mock_object_manager,
    mock_conn,
    runner,
    mock_cursor,
    mock_ctx,
    project_directory,
):
    mock_object_manager.return_value.describe.side_effect = [
        mock_cursor(
            [
                ("packages", "[]"),
                ("handler", "hello"),
                ("returns", "string"),
            ],
            columns=["key", "value"],
        ),
        ProgrammingError("does not exist or not authorized"),
    ]
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("snowpark_procedures"):
        result = runner.invoke(["snowpark", "deploy", "--replace", "--format", "json"])

    assert result.exit_code == 0
    assert json.loads(result.output) == [
        {
            "object": "procedureName(name string)",
            "status": "packages updated",
            "type": "procedure",
        },
        {"object": "test()", "status": "created", "type": "procedure"},
    ]


@mock.patch("snowflake.connector.connect")
def test_execute_procedure(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "execute",
            "procedure",
            "procedureName(42, 'string')",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "call procedureName(42, 'string')"


@mock.patch(
    "snowflake.cli.api.commands.project_initialisation._create_project_template"
)
def test_init_procedure(mock_create_project_template, runner, temp_dir):
    runner.invoke(["snowpark", "init", "my_project2"])
    mock_create_project_template.assert_called_once_with(
        "default_snowpark", project_directory="my_project2"
    )
