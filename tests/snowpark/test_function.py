import json
from pathlib import Path
from textwrap import dedent
from unittest import mock

from snowflake.connector import ProgrammingError


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.commands.ObjectManager.describe")
def test_deploy_function(
    mock_describe,
    mock_connector,
    mock_ctx,
    runner,
    project_directory,
):
    mock_describe.side_effect = ProgrammingError("does not exist or not authorized")
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    with project_directory("snowpark_functions") as tmp:
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists dev_deployment comment='deployments managed by snowcli'",
        f"put file://{Path(tmp).resolve()}/app.zip @dev_deployment/my_snowpark_project"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace function func1(a string, b variant)
            returns string
            language python
            runtime_version=3.8
            imports=('@dev_deployment/my_snowpark_project/app.zip')
            handler='app.func1_handler'
            packages=()
            """
        ),
    ]


@mock.patch("snowflake.connector.connect")
def test_deploy_function_no_changes(
    mock_connector,
    runner,
    mock_ctx,
    mock_cursor,
    project_directory,
):
    rows = [
        ("packages", '["foo=1.2.3", "bar>=3.0.0"]'),
        ("handler", "app.func1_handler"),
        ("returns", "string"),
    ]

    queries, result, tmp = _deploy_function(
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
            "object": "func1(a string, b variant)",
            "status": "packages updated",
            "type": "function",
        }
    ]
    assert queries == [
        "create stage if not exists dev_deployment comment='deployments managed by snowcli'",
        f"put file://{Path(tmp).resolve()}/app.zip @dev_deployment/my_snowpark_project auto_compress=false parallel=4 overwrite=True",
        "describe function func1(string, variant)",
    ]


@mock.patch("snowflake.connector.connect")
def test_deploy_function_needs_update_because_packages_changes(
    mock_connector,
    runner,
    mock_ctx,
    mock_cursor,
    project_directory,
):
    rows = [
        ("packages", '["foo=1.2.3"]'),
        ("handler", "main.py:app"),
        ("returns", "table(variant)"),
    ]

    queries, result, tmp = _deploy_function(
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
            "object": "func1(a string, b variant)",
            "status": "definition updated",
            "type": "function",
        }
    ]
    assert queries == [
        "create stage if not exists dev_deployment comment='deployments managed by snowcli'",
        f"put file://{Path(tmp).resolve()}/app.zip @dev_deployment/my_snowpark_project auto_compress=false parallel=4 overwrite=True",
        "describe function func1(string, variant)",
        dedent(
            """\
            create or replace function func1(a string, b variant)
            returns string
            language python
            runtime_version=3.8
            imports=('@dev_deployment/my_snowpark_project/app.zip')
            handler='app.func1_handler'
            packages=('foo=1.2.3','bar>=3.0.0')
            """
        ),
    ]


@mock.patch("snowflake.connector.connect")
def test_deploy_function_needs_update_because_handler_changes(
    mock_connector,
    runner,
    mock_ctx,
    mock_cursor,
    project_directory,
):
    rows = [
        ("packages", '["foo=1.2.3", "bar>=3.0.0"]'),
        ("handler", "main.py:oldApp"),
        ("returns", "table(variant)"),
    ]

    queries, result, tmp = _deploy_function(
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
            "object": "func1(a string, b variant)",
            "status": "definition updated",
            "type": "function",
        }
    ]
    assert queries == [
        "create stage if not exists dev_deployment comment='deployments managed by snowcli'",
        f"put file://{Path(tmp).resolve()}/app.zip @dev_deployment/my_snowpark_project"
        f" auto_compress=false parallel=4 overwrite=True",
        "describe function func1(string, variant)",
        dedent(
            """\
            create or replace function func1(a string, b variant)
            returns string
            language python
            runtime_version=3.8
            imports=('@dev_deployment/my_snowpark_project/app.zip')
            handler='app.func1_handler'
            packages=('foo=1.2.3','bar>=3.0.0')
            """
        ),
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

    assert result.exit_code == 0, result._output
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
    with project_directory("snowpark_functions") as temp_dir:
        (Path(temp_dir) / "requirements.snowflake.txt").write_text(
            "foo=1.2.3\nbar>=3.0.0"
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
