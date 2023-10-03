from tempfile import TemporaryDirectory
from textwrap import dedent

from snowflake.connector import ProgrammingError

from tests.testing_utils.fixtures import *


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.function.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.function.commands.TemporaryDirectory")
def test_deploy_function_unknown_name(
    mock_tmp_dir, mock_package_create, mock_connector, runner, project_directory
):
    with project_directory("snowpark_functions"):
        result = runner.invoke(
            ["snowpark", "function", "deploy", "unknownFunc"],
        )
    assert result.exit_code == 1
    assert "Function 'unknownFunc' is not defined" in result.output


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.function.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.function.commands.TemporaryDirectory")
def test_deploy_function_no_functions(
    mock_tmp_dir, mock_package_create, mock_connector, runner, project_directory
):
    with project_directory("empty_project"):
        result = runner.invoke(
            [
                "snowpark",
                "function",
                "deploy",
            ],
        )
    assert result.exit_code == 1
    assert "No functions were specified in project" in result.output


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.function.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.function.commands.TemporaryDirectory")
def test_deploy_function(
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx_function_not_exist,
    project_directory,
):
    tmp_dir_1 = TemporaryDirectory()
    tmp_dir_2 = TemporaryDirectory()
    mock_tmp_dir.side_effect = [tmp_dir_1, tmp_dir_2]
    ctx = mock_ctx_function_not_exist()
    mock_connector.return_value = ctx
    with project_directory("snowpark_functions"):
        result = runner.invoke(
            [
                "snowpark",
                "function",
                "deploy",
                "func1",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        "describe function func1(string, variant)",
        f"put file://{tmp_dir_1.name}/app.zip @deployments/func1_a_string_b_variant"
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
    mock_package_create.assert_called_once_with("ask", True, "ask")


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.function.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.function.commands.TemporaryDirectory")
def test_deploy_function_selected_multiple_names(
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx_function_not_exist,
    project_directory,
):
    tmp_dir_1 = TemporaryDirectory()
    tmp_dir_2 = TemporaryDirectory()
    mock_tmp_dir.side_effect = [tmp_dir_1, tmp_dir_2]

    ctx = mock_ctx_function_not_exist()
    mock_connector.return_value = ctx
    with project_directory("snowpark_functions"):
        result = runner.invoke(
            ["snowpark", "function", "deploy", "func1", "func2"],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        # FIRST FUNCTION
        "describe function func1(string, variant)",
        f"put file://{tmp_dir_1.name}/app.zip @deployments/func1_a_string_b_variant"
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
        # SECOND FUNCTION
        "describe function func2()",
        f"put file://{tmp_dir_2.name}/app.zip @deployments/func2"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace function func2()
            returns variant
            language python
            runtime_version=3.8
            imports=('@deployments/func2/app.zip')
            handler='app.func2_handler'
            packages=()
            """
        ),
    ]
    mock_package_create.assert_called_once_with("ask", True, "ask")


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.function.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.function.commands.TemporaryDirectory")
def test_deploy_function_no_changes(
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx,
    mock_cursor,
    temp_dir,
    project_directory,
):
    rows = [
        ("packages", '["foo=1.2.3", "bar>=3.0.0"]'),
        ("handler", "app.func1_handler"),
        ("returns", "string"),
    ]

    artifact_path, queries, result = _deploy_function(
        rows,
        mock_connector,
        runner,
        temp_dir,
        mock_ctx,
        mock_cursor,
        mock_tmp_dir,
        project_directory,
        "--replace",
    )

    assert result.exit_code == 0, result.output
    assert "No packages to update. Deployment complete" in result.output
    assert queries == [
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        "describe function func1(string, variant)",
        f"put file://{artifact_path} @deployments/func1_a_string_b_variant auto_compress=false parallel=4 overwrite=True",
    ]
    mock_package_create.assert_called_once_with("ask", True, "ask")


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.function.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.function.commands.TemporaryDirectory")
def test_deploy_function_needs_update_because_packages_changes(
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx_function_not_exist,
    mock_cursor,
    temp_dir,
    project_directory,
):
    rows = [
        ("packages", '["foo=1.2.3"]'),
        ("handler", "main.py:app"),
        ("returns", "table(variant)"),
    ]

    artifact_path, queries, result = _deploy_function(
        rows,
        mock_connector,
        runner,
        temp_dir,
        mock_ctx_function_not_exist,
        mock_cursor,
        mock_tmp_dir,
        project_directory,
    )

    assert result.exit_code == 0, result.output
    assert queries == [
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        "describe function func1(string, variant)",
        f"put file://{artifact_path} @deployments/func1_a_string_b_variant auto_compress=false parallel=4 overwrite=True",
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
    mock_package_create.assert_called_once_with("ask", True, "ask")


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.function.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.function.commands.TemporaryDirectory")
def test_deploy_function_needs_update_because_handler_changes(
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx_function_not_exist,
    mock_cursor,
    temp_dir,
    project_directory,
):
    rows = [
        ("packages", '["foo=1.2.3", "bar>=3.0.0"]'),
        ("handler", "main.py:oldApp"),
        ("returns", "table(variant)"),
    ]

    artifact_path, queries, result = _deploy_function(
        rows,
        mock_connector,
        runner,
        temp_dir,
        mock_ctx_function_not_exist,
        mock_cursor,
        mock_tmp_dir,
        project_directory,
    )

    assert result.exit_code == 0, result.output
    assert queries == [
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        "describe function func1(string, variant)",
        f"put file://{artifact_path} @deployments/func1_a_string_b_variant auto_compress=false parallel=4 overwrite=True",
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
    mock_package_create.assert_called_once_with("ask", True, "ask")


@mock.patch("snowflake.connector.connect")
def test_execute_function(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "function",
            "execute",
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
            "function",
            "describe",
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
            "function",
            "describe",
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
            "function",
            "list",
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
            "function",
            "drop",
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
            "function",
            "drop",
            "functionName(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result._output
    assert ctx.get_query() == "drop function functionName(int, string, variant)"


def _deploy_function(
    rows,
    mock_connector,
    runner,
    execute_in_tmp_dir,
    mock_ctx,
    mock_cursor,
    mock_tmp_dir,
    project_directory,
    *args,
):
    tmp_dir = TemporaryDirectory()
    mock_tmp_dir.return_value = tmp_dir
    ctx = mock_ctx(mock_cursor(rows=rows, columns=[]))
    mock_connector.return_value = ctx
    (Path(execute_in_tmp_dir) / "requirements.snowflake.txt").write_text(
        "foo=1.2.3\nbar>=3.0.0"
    )
    app = Path(execute_in_tmp_dir) / "app.zip"
    app.touch()
    with project_directory("snowpark_functions"):
        result = runner.invoke_with_config(
            [
                "snowpark",
                "function",
                "deploy",
                "func1",
                *args,
            ]
        )
    queries = ctx.get_queries()
    artifact_path = f"{tmp_dir.name}/{app.name}"
    return artifact_path, queries, result


@pytest.fixture
def mock_ctx_function_not_exist(mock_cursor):
    class _MockConnectionCtx(MockConnectionCtx):
        def __init__(self, cursor=None, *args, **kwargs):
            super().__init__(cursor, *args, **kwargs)

        def execute_string(self, query: str, **kwargs):
            self.queries.append(query)
            if query == "describe function func1(string, variant)":
                raise ProgrammingError(
                    "Function 'FUNCTIONNAME' does not exist or not authorized"
                )
            return (self.cs,)

    return lambda cursor=mock_cursor(["row"], []): _MockConnectionCtx(cursor)
