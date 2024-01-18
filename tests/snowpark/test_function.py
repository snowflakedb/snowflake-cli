import json
from pathlib import Path
from textwrap import dedent
from unittest import mock

from snowflake.connector import ProgrammingError


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager")
def test_deploy_function(
    mock_object_manager,
    mock_connector,
    mock_ctx,
    runner,
    project_directory,
):
    mock_object_manager.return_value.describe.side_effect = ProgrammingError(
        "does not exist or not authorized"
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
        "create stage if not exists dev_deployment comment='deployments managed by snowcli'",
        f"put file://{Path(project_dir).resolve()}/app.zip @dev_deployment/my_snowpark_project"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace function func1(a string, b variant)
            returns string
            language python
            runtime_version=3.10
            imports=('@dev_deployment/my_snowpark_project/app.zip')
            handler='app.func1_handler'
            packages=()
            """
        ).strip(),
    ]


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager")
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
        "does not exist or not authorized"
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
        "create stage if not exists dev_deployment comment='deployments managed by snowcli'",
        f"put file://{Path(project_dir).resolve()}/app.zip @dev_deployment/my_snowpark_project"
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
            external_access_integrations=(external_1,external_2)
            secrets=('cred'=cred_name,'other'=other_name)
            """
        ).strip(),
    ]


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager")
def test_deploy_function_secrets_without_external_access(
    mock_object_manager,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    snapshot,
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
    assert result.output == snapshot


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
            "object": "func1(a string, b variant)",
            "status": "packages updated",
            "type": "function",
        }
    ]
    assert queries == [
        "create stage if not exists dev_deployment comment='deployments managed by snowcli'",
        f"put file://{Path(project_dir).resolve()}/app.zip @dev_deployment/my_snowpark_project auto_compress=false parallel=4 overwrite=True",
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
            "object": "func1(a string, b variant)",
            "status": "definition updated",
            "type": "function",
        }
    ]
    assert queries == [
        "create stage if not exists dev_deployment comment='deployments managed by snowcli'",
        f"put file://{Path(project_dir).resolve()}/app.zip @dev_deployment/my_snowpark_project auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace function func1(a string, b variant)
            returns string
            language python
            runtime_version=3.10
            imports=('@dev_deployment/my_snowpark_project/app.zip')
            handler='app.func1_handler'
            packages=('foo=1.2.3','bar>=3.0.0')
            """
        ).strip(),
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
            "object": "func1(a string, b variant)",
            "status": "definition updated",
            "type": "function",
        }
    ]
    assert queries == [
        "create stage if not exists dev_deployment comment='deployments managed by snowcli'",
        f"put file://{Path(project_dir).resolve()}/app.zip @dev_deployment/my_snowpark_project"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace function func1(a string, b variant)
            returns string
            language python
            runtime_version=3.10
            imports=('@dev_deployment/my_snowpark_project/app.zip')
            handler='app.func1_handler'
            packages=('foo=1.2.3','bar>=3.0.0')
            """
        ).strip(),
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
    with mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager") as om:

        om.return_value.describe.return_value = rows

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
