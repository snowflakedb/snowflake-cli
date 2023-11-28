import json
from pathlib import Path
from textwrap import dedent
from unittest import mock
from unittest.mock import call

import pytest
from snowcli.cli.constants import ObjectType
from snowcli.exception import SecretsWithoutExternalAccessIntegrationError
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
@mock.patch("snowcli.cli.snowpark.commands.ObjectManager.describe")
def test_deploy_procedure(
    mock_describe,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
):
    mock_describe.side_effect = ProgrammingError("does not exist or not authorized")
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
    mock_describe.assert_has_calls(
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
            runtime_version=3.8
            imports=('@dev_deployment/my_snowpark_project/app.zip')
            handler='test'
            packages=()
            """
        ).strip(),
    ]


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.commands.ObjectManager.describe")
def test_deploy_procedure_with_external_access(
    mock_describe,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
):
    mock_describe.side_effect = ProgrammingError("does not exist or not authorized")
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
    mock_describe.assert_has_calls(
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
@mock.patch("snowcli.cli.snowpark.commands.ObjectManager.describe")
def test_deploy_procedure_secrets_without_external_access(
    mock_describe,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
):
    mock_describe.side_effect = ProgrammingError("does not exist or not authorized")
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("snowpark_procedure_secrets_without_external_access"):
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 1, result.output
    assert result.output.__contains__(
        "Can not provide secrets without external access integration"
    )


@mock.patch("snowflake.connector.connect")
@mock.patch(
    "snowcli.cli.snowpark.commands._alter_procedure_artifact_with_coverage_wrapper"
)
@mock.patch("snowcli.cli.snowpark.commands.ObjectManager.describe")
def test_deploy_procedure_with_coverage(
    mock_describe,
    mock_alter_procedure_artifact,
    mock_conn,
    runner,
    mock_ctx,
    temp_dir,
    project_directory,
):
    mock_alter_procedure_artifact.return_value = "snowpark_coverage.measure_coverage"

    mock_describe.side_effect = ProgrammingError("does not exist or not authorized")
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("snowpark_procedures_coverage") as tmp:
        result = runner.invoke(["snowpark", "deploy", "--install-coverage-wrapper"])

    assert result.exit_code == 0, result.output
    mock_describe.assert_has_calls(
        [call(object_type=str(ObjectType.PROCEDURE), name="foo(string)")]
    )
    assert ctx.get_queries() == [
        "create stage if not exists dev_deployment comment='deployments managed by snowcli'",
        f"put file://{Path(tmp).resolve()}/app.zip @dev_deployment/my_snowpark_project auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace procedure foo(name string)
            returns variant
            language python
            runtime_version=3.8
            imports=('@dev_deployment/my_snowpark_project/app.zip')
            handler='snowpark_coverage.measure_coverage'
            packages=('coverage')
            """
        ).strip(),
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


@mock.patch("snowcli.cli.common.project_initialisation._create_project_template")
def test_init_procedure(mock_create_project_template, runner, temp_dir):
    runner.invoke(["snowpark", "init", "my_project2"])
    mock_create_project_template.assert_called_once_with(
        "default_snowpark", project_directory="my_project2"
    )
