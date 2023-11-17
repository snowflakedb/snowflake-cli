import json
from textwrap import dedent
from unittest.mock import call

from snowcli.cli.constants import SnowparkObjectType
from snowflake.connector import ProgrammingError

from tests.testing_utils.fixtures import *


def test_deploy_function_no_procedure(runner, project_directory):
    with project_directory("empty_project"):
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ],
        )
    assert result.exit_code == 1
    assert "No procedures or functions were specified in project" in result.output


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.commands.ObjectManager.describe")
def test_deploy_procedure(
    mock_describe,
    mock_conn,
    runner,
    mock_cursor,
    mock_ctx,
    project_directory,
):
    mock_describe.side_effect = ProgrammingError("does not exist or not authorized")
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("snowpark_procedures"):
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ]
        )

    assert result.exit_code == 0, result.output
    mock_describe.assert_has_calls(
        [
            call(SnowparkObjectType.PROCEDURE.value, "procedureName(string)"),
            call(SnowparkObjectType.PROCEDURE.value, "test()"),
        ]
    )
    assert ctx.get_queries() == [
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        f"put file://app.zip @deployments/procedurename_name_string"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace procedure procedureName(name string)
            returns string
            language python
            runtime_version=3.8
            imports=('@deployments/procedurename_name_string/app.zip')
            handler='app.hello'
            packages=()
            """
        ),
        f"put file://app.zip @deployments/test"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace procedure test()
            returns string
            language python
            runtime_version=3.8
            imports=('@deployments/test/app.zip')
            handler='app.test'
            packages=()
            """
        ),
    ]


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.commands._alter_procedure_artifact")
@mock.patch("snowcli.cli.snowpark.commands.ObjectManager.describe")
def test_deploy_procedure_with_coverage(
    mock_describe,
    mock_alter_procedure_artifact,
    mock_conn,
    runner,
    mock_ctx,
    snapshot,
    temp_dir,
    project_directory,
):
    mock_alter_procedure_artifact.return_value = "snowpark_coverage.measure_coverage"

    mock_describe.side_effect = ProgrammingError("does not exist or not authorized")
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory(
        "snowpark_procedures",
        {
            "procedures": [
                {
                    "name": "foo",
                    "signature": [{"name": "name", "type": "string"}],
                    "handler": "foo.func",
                    "returns": "variant",
                }
            ]
        },
    ):
        result = runner.invoke(["snowpark", "deploy", "--install-coverage-wrapper"])

    assert result.exit_code == 0, result.output
    mock_describe.assert_has_calls(
        [call(SnowparkObjectType.PROCEDURE.value, "foo(string)")]
    )
    assert ctx.get_queries() == [
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        f"put file://app.zip @deployments/foo_name_string"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace procedure foo(name string)
            returns variant
            language python
            runtime_version=3.8
            imports=('@deployments/foo_name_string/app.zip')
            handler='snowpark_coverage.measure_coverage'
            packages=('coverage')
            """
        ),
    ]


def test_coverage_wrapper_does_not_work_for_multiple_procedures(
    project_directory, runner
):
    with project_directory("snowpark_procedures"):
        result = runner.invoke(["snowpark", "deploy", "--install-coverage-wrapper"])
    assert result.exit_code == 1
    assert "Using coverage wrapper is currently limited" in result.output


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.commands.ObjectManager.describe")
def test_deploy_procedure_fails_if_object_exists_and_no_replace(
    mock_describe,
    mock_conn,
    runner,
    mock_cursor,
    mock_ctx,
    project_directory,
):
    mock_describe.side_effect = [
        mock_cursor(
            [
                ("packages", '["foo=1.2.3", "bar>=3.0.0"]'),
                ("handler", "main.py:app"),
                ("returns", "table(variant)"),
            ],
            columns=["key", "value"],
        ),
    ]
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("snowpark_procedures"):
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ]
        )

    assert result.exit_code == 1
    assert "Procedure procedureName(name string) already exists." in result.output


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.commands.ObjectManager.describe")
def test_deploy_procedure_replace_nothing_to_update(
    mock_describe,
    mock_conn,
    runner,
    mock_cursor,
    mock_ctx,
    project_directory,
):
    mock_describe.side_effect = [
        mock_cursor(
            [
                ("packages", "[]"),
                ("handler", "app.hello"),
                ("returns", "string"),
            ],
            columns=["key", "value"],
        ),
        mock_cursor(
            [
                ("packages", "[]"),
                ("handler", "app.test"),
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
        {"object": "test()", "status": "packages updated", "type": "procedure"},
    ]


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.commands.ObjectManager.describe")
def test_deploy_procedure_replace_updates_single_object(
    mock_describe,
    mock_conn,
    runner,
    mock_cursor,
    mock_ctx,
    project_directory,
):
    mock_describe.side_effect = [
        mock_cursor(
            [
                ("packages", "[]"),
                ("handler", "app.hello"),
                ("returns", "string"),
            ],
            columns=["key", "value"],
        ),
        mock_cursor(
            [
                ("packages", "[]"),
                ("handler", "app.foo"),
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
@mock.patch("snowcli.cli.snowpark.commands.ObjectManager.describe")
def test_deploy_procedure_replace_creates_missing_object(
    mock_describe,
    mock_conn,
    runner,
    mock_cursor,
    mock_ctx,
    project_directory,
):
    mock_describe.side_effect = [
        mock_cursor(
            [
                ("packages", "[]"),
                ("handler", "app.hello"),
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


@mock.patch("snowcli.cli.common.project_initialisation._create_project_template")
def test_init_procedure(mock_create_project_template, runner, temp_dir):
    runner.invoke(["snowpark", "init", "my_project2"])
    mock_create_project_template.assert_called_once_with(
        "default_snowpark", project_directory="my_project2"
    )
