from tempfile import TemporaryDirectory
from textwrap import dedent

from tests.testing_utils.fixtures import *


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.function.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.function.commands.TemporaryDirectory")
def test_deploy_function_unknown_name(
    mock_tmp_dir, mock_package_create, mock_connector, runner, project_file
):
    with project_file("snowpark_functions"):
        result = runner.invoke(
            ["snowpark", "function", "deploy", "unknownFunc"],
        )
    assert result.exit_code == 1
    assert "Function 'unknownFunc' is not defined" in result.output


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.function.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.function.commands.TemporaryDirectory")
def test_deploy_function_no_functions(
    mock_tmp_dir, mock_package_create, mock_connector, runner, project_file
):
    with project_file("empty_project"):
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
    mock_tmp_dir, mock_package_create, mock_connector, runner, mock_ctx, project_file
):
    tmp_dir_1 = TemporaryDirectory()
    tmp_dir_2 = TemporaryDirectory()
    mock_tmp_dir.side_effect = [tmp_dir_1, tmp_dir_2]

    ctx = mock_ctx()
    mock_connector.return_value = ctx
    with project_file("snowpark_functions"):
        result = runner.invoke(
            [
                "snowpark",
                "function",
                "deploy",
                "--replace",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        # FIRST FUNCTION
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
def test_deploy_function_selected_name(
    mock_tmp_dir, mock_package_create, mock_connector, runner, mock_ctx, project_file
):
    tmp_dir_1 = TemporaryDirectory()
    tmp_dir_2 = TemporaryDirectory()
    mock_tmp_dir.side_effect = [tmp_dir_1, tmp_dir_2]

    ctx = mock_ctx()
    mock_connector.return_value = ctx
    with project_file("snowpark_functions"):
        result = runner.invoke(
            ["snowpark", "function", "deploy", "func1"],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        # FIRST FUNCTION
        f"put file://{tmp_dir_1.name}/app.zip @deployments/func1_a_string_b_variant"
        f" auto_compress=false parallel=4 overwrite=False",
        dedent(
            """\
            create function func1(a string, b variant)
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
@mock.patch("snowcli.cli.snowpark.function.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.function.commands.TemporaryDirectory")
def test_deploy_function_selected_multiple_names(
    mock_tmp_dir, mock_package_create, mock_connector, runner, mock_ctx, project_file
):
    tmp_dir_1 = TemporaryDirectory()
    tmp_dir_2 = TemporaryDirectory()
    mock_tmp_dir.side_effect = [tmp_dir_1, tmp_dir_2]

    ctx = mock_ctx()
    mock_connector.return_value = ctx
    with project_file("snowpark_functions"):
        result = runner.invoke(
            ["snowpark", "function", "deploy", "func1", "func2"],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        # FIRST FUNCTION
        f"put file://{tmp_dir_1.name}/app.zip @deployments/func1_a_string_b_variant"
        f" auto_compress=false parallel=4 overwrite=False",
        dedent(
            """\
            create function func1(a string, b variant)
            returns string
            language python
            runtime_version=3.8
            imports=('@deployments/func1_a_string_b_variant/app.zip')
            handler='app.func1_handler'
            packages=()
            """
        ),
        # SECOND FUNCTION
        f"put file://{tmp_dir_2.name}/app.zip @deployments/func2"
        f" auto_compress=false parallel=4 overwrite=False",
        dedent(
            """\
            create function func2()
            returns variant
            language python
            runtime_version=3.8
            imports=('@deployments/func2/app.zip')
            handler='app.func2_handler'
            packages=()
            """
        ),
    ]


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.function.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.function.commands.TemporaryDirectory")
def test_update_function_no_changes(
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx,
    mock_cursor,
    snapshot,
    temp_dir,
):
    rows = [
        ("packages", '["foo=1.2.3", "bar>=3.0.0"]'),
        ("handler", "main.py:app"),
        ("returns", "table(variant)"),
    ]

    artifact_path, queries, result = _update_function(
        rows,
        mock_connector,
        runner,
        temp_dir,
        mock_ctx,
        mock_cursor,
        mock_tmp_dir,
    )

    assert result.exit_code == 0, result._output
    assert "No packages to update. Deployment complete" in result.output
    assert queries == [
        "describe function functionName(a string, b number)",
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        f"put file://{artifact_path} @deployments/functionname_a_string_b_number auto_compress=false parallel=4 overwrite=True",
    ]
    mock_package_create.assert_called_once_with("ask", True, "ask")


def _update_function(
    rows,
    mock_connector,
    runner,
    execute_in_tmp_dir,
    mock_ctx,
    mock_cursor,
    mock_tmp_dir,
):
    tmp_dir = TemporaryDirectory()
    mock_tmp_dir.return_value = tmp_dir
    ctx = mock_ctx(mock_cursor(rows=rows, columns=[]))
    mock_connector.return_value = ctx
    (Path(execute_in_tmp_dir) / "requirements.snowflake.txt").write_text(
        "foo=1.2.3\nbar>=3.0.0"
    )
    app = Path(execute_in_tmp_dir) / "app.py"
    app.touch()
    result = runner.invoke_with_config(
        [
            "snowpark",
            "function",
            "update",
            "functionName(a string, b number)",
            "--file",
            str(app),
            "--handler",
            "main.py:app",
            "--returns",
            "table(variant)",
        ]
    )
    queries = ctx.get_queries()
    artifact_path = f"{tmp_dir.name}/{app.name}"
    return artifact_path, queries, result


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.function.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.function.commands.TemporaryDirectory")
def test_update_function_needs_update_because_packages_changes(
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx,
    mock_cursor,
    snapshot,
    temp_dir,
):
    rows = [
        ("packages", '["foo=1.2.3"]'),
        ("handler", "main.py:app"),
        ("returns", "table(variant)"),
    ]

    artifact_path, queries, result = _update_function(
        rows,
        mock_connector,
        runner,
        temp_dir,
        mock_ctx,
        mock_cursor,
        mock_tmp_dir,
    )

    assert result.exit_code == 0, result._output
    assert queries == [
        "describe function functionName(a string, b number)",
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        f"put file://{artifact_path} @deployments/functionname_a_string_b_number auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace function functionName(a string, b number)
            returns table(variant)
            language python
            runtime_version=3.8
            imports=('@deployments/functionname_a_string_b_number/app.zip')
            handler='main.py:app'
            packages=('foo=1.2.3','bar>=3.0.0')
            """
        ),
    ]
    mock_package_create.assert_called_once_with("ask", True, "ask")


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.function.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.function.commands.TemporaryDirectory")
def test_update_function_needs_update_because_handler_changes(
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx,
    mock_cursor,
    snapshot,
    temp_dir,
):
    rows = [
        ("packages", '["foo=1.2.3", "bar>=3.0.0"]'),
        ("handler", "main.py:oldApp"),
        ("returns", "table(variant)"),
    ]

    artifact_path, queries, result = _update_function(
        rows,
        mock_connector,
        runner,
        temp_dir,
        mock_ctx,
        mock_cursor,
        mock_tmp_dir,
    )

    assert result.exit_code == 0, result._output
    assert queries == [
        "describe function functionName(a string, b number)",
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        f"put file://{artifact_path} @deployments/functionname_a_string_b_number auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace function functionName(a string, b number)
            returns table(variant)
            language python
            runtime_version=3.8
            imports=('@deployments/functionname_a_string_b_number/app.zip')
            handler='main.py:app'
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
