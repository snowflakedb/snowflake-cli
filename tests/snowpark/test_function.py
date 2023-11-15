import json
from tempfile import TemporaryDirectory
from textwrap import dedent

from snowflake.connector import ProgrammingError

from tests.testing_utils.fixtures import *


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.commands.FunctionManager.describe")
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
    with project_directory("snowpark_functions"):
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        f"put file://app.zip @deployments/func1_a_string_b_variant"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace function func1(a string, b variant)
            returns string
            language python
            runtime_version=3.8
            imports=('@deployments/func1_a_string_b_variant/app.zip')
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

    queries, result = _deploy_function(
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
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        "describe function func1(string, variant)",
        f"put file://app.zip @deployments/func1_a_string_b_variant auto_compress=false parallel=4 overwrite=True",
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

    queries, result = _deploy_function(
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
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        "describe function func1(string, variant)",
        f"put file://app.zip @deployments/func1_a_string_b_variant auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace function func1(a string, b variant)
            returns string
            language python
            runtime_version=3.8
            imports=('@deployments/func1_a_string_b_variant/app.zip')
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

    queries, result = _deploy_function(
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
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        "describe function func1(string, variant)",
        f"put file://app.zip @deployments/func1_a_string_b_variant auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace function func1(a string, b variant)
            returns string
            language python
            runtime_version=3.8
            imports=('@deployments/func1_a_string_b_variant/app.zip')
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


@mock.patch("snowflake.connector.connect")
def test_describe_function_from_signature(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "describe",
            "function",
            "functionName(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result._output
    assert ctx.get_query() == "describe function functionName(int, string, variant)"


@mock.patch("snowflake.connector.connect")
def test_describe_function_from_name(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "describe",
            "function",
            "functionName(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result._output
    assert ctx.get_query() == "describe function functionName(int, string, variant)"


@mock.patch("snowflake.connector.connect")
def test_list_function(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "list",
            "function",
            "--like",
            "foo_bar%",
        ]
    )

    assert result.exit_code == 0, result._output
    assert ctx.get_query() == "show user functions like 'foo_bar%'"


@mock.patch("snowflake.connector.connect")
def test_drop_function_from_signature(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "drop",
            "function",
            "functionName(int, string, variant)",
        ]
    )
    print(result.output)
    assert result.exit_code == 0, result._output
    assert ctx.get_query() == "drop function functionName(int, string, variant)"


@mock.patch("snowflake.connector.connect")
def test_drop_function_from_name(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "drop",
            "function",
            "functionName(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result._output
    assert ctx.get_query() == "drop function functionName(int, string, variant)"


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
    return queries, result
