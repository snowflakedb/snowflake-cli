from tempfile import TemporaryDirectory
from textwrap import dedent

from snowflake.connector import ProgrammingError

from tests.testing_utils.fixtures import *


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.procedure.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.procedure.commands.TemporaryDirectory")
def test_deploy_procedure(
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx_procedure_not_exist,
    temp_dir,
):
    tmp_dir = TemporaryDirectory()
    mock_tmp_dir.return_value = tmp_dir

    ctx = mock_ctx_procedure_not_exist()
    mock_connector.return_value = ctx

    tmp_dir_2 = temp_dir
    local_dir = Path(tmp_dir_2)
    (local_dir / "requirements.snowflake.txt").write_text("foo=1.2.3\nbar>=3.0.0")

    app = local_dir / "app.py"
    app.touch()

    result = runner.invoke(
        [
            "snowpark",
            "procedure",
            "deploy",
            "procedureName(a string, b number)",
            "--file",
            str(app),
            "--handler",
            "main.py:app",
            "--returns",
            "table(variant)",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "describe procedure procedureName(string, number)",
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        f"put file://{tmp_dir.name}/{app.name} @deployments/procedurename_a_string_b_number"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace procedure procedureName(a string, b number)
            returns table(variant)
            language python
            runtime_version=3.8
            imports=('@deployments/procedurename_a_string_b_number/app.zip')
            handler='main.py:app'
            packages=('foo=1.2.3','bar>=3.0.0')
            """
        ),
    ]
    mock_package_create.assert_called_once_with("ask", True, "ask")


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.procedure.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.procedure.commands.TemporaryDirectory")
@mock.patch("snowcli.cli.snowpark.procedure.commands._replace_handler_in_zip")
def test_deploy_procedure_with_coverage(
    mock_coverage,
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx_procedure_not_exist,
    snapshot,
    temp_dir,
):
    tmp_dir = TemporaryDirectory()
    mock_tmp_dir.return_value = tmp_dir

    mock_coverage.return_value = "snowpark_coverage.measure_coverage"

    ctx = mock_ctx_procedure_not_exist()
    mock_connector.return_value = ctx

    local_dir = Path(temp_dir)
    (local_dir / "requirements.snowflake.txt").write_text("foo=1.2.3\nbar>=3.0.0")

    app = local_dir / "app.py"
    app.touch()

    result = runner.invoke(
        [
            "snowpark",
            "procedure",
            "deploy",
            "procedureName(a string, b number)",
            "--file",
            str(app),
            "--handler",
            "main.py:app",
            "--returns",
            "table(variant)",
            "--install-coverage-wrapper",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "describe procedure procedureName(string, number)",
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        f"put file://{tmp_dir.name}/{app.name} @deployments/procedurename_a_string_b_number"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace procedure procedureName(a string, b number)
            returns table(variant)
            language python
            runtime_version=3.8
            imports=('@deployments/procedurename_a_string_b_number/app.zip')
            handler='snowpark_coverage.measure_coverage'
            packages=('foo=1.2.3','bar>=3.0.0')
            """
        ),
    ]
    mock_package_create.assert_called_once_with("ask", True, "ask")
    mock_coverage.assert_called_once_with(
        proc_name="procedureName",
        proc_signature="(a string, b number)",
        handler="main.py:app",
        coverage_reports_stage="deployments",
        coverage_reports_stage_path="/procedurename_a_string_b_number/coverage",
        temp_dir=tmp_dir.name,
        zip_file_path=tmp_dir.name + "/app.py",
    )


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.procedure.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.procedure.commands.TemporaryDirectory")
def test_deploy_procedure_noting_to_be_updated(
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx,
    temp_dir,
    mock_cursor,
):
    rows = [
        ("packages", '["foo=1.2.3", "bar>=3.0.0"]'),
        ("handler", "main.py:app"),
        ("returns", "table(variant)"),
    ]
    artifact_path, ctx, result = _deploy_procedure(
        temp_dir,
        mock_connector,
        mock_ctx,
        mock_cursor,
        mock_tmp_dir,
        rows,
        runner,
        "--replace",
    )

    assert result.exit_code == 0, result.output
    assert "No packages to update. Deployment complete" in result.output
    assert ctx.get_queries() == [
        "describe procedure procedureName(string, number)",
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        f"put file://{artifact_path} @deployments/procedurename_a_string_b_number auto_compress=false parallel=4 overwrite=True",
    ]
    mock_package_create.assert_called_once_with("ask", True, "ask")


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.procedure.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.procedure.commands.TemporaryDirectory")
def test_deploy_procedure_update_because_packages_changed(
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx_procedure_not_exist,
    temp_dir,
    mock_cursor,
):
    rows = [
        ("packages", '["foo=1.2.3"]'),
        ("handler", "main.py:app"),
        ("returns", "table(variant)"),
    ]
    artifact_path, ctx, result = _deploy_procedure(
        temp_dir,
        mock_connector,
        mock_ctx_procedure_not_exist,
        mock_cursor,
        mock_tmp_dir,
        rows,
        runner,
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "describe procedure procedureName(string, number)",
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        f"put file://{artifact_path} @deployments/procedurename_a_string_b_number auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace procedure procedureName(a string, b number)
            returns table(variant)
            language python
            runtime_version=3.8
            imports=('@deployments/procedurename_a_string_b_number/app.zip')
            handler='main.py:app'
            packages=('foo=1.2.3','bar>=3.0.0')
            """
        ),
    ]
    mock_package_create.assert_called_once_with("ask", True, "ask")


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.procedure.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.procedure.commands.TemporaryDirectory")
def test_deploy_procedure_update_because_handler_changed(
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx_procedure_not_exist,
    temp_dir,
    mock_cursor,
):
    rows = [
        ("packages", '["foo=1.2.3", "bar>=3.0.0"]'),
        ("handler", "main.py:oldApp"),
        ("returns", "table(variant)"),
    ]
    artifact_path, ctx, result = _deploy_procedure(
        temp_dir,
        mock_connector,
        mock_ctx_procedure_not_exist,
        mock_cursor,
        mock_tmp_dir,
        rows,
        runner,
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "describe procedure procedureName(string, number)",
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        f"put file://{artifact_path} @deployments/procedurename_a_string_b_number auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace procedure procedureName(a string, b number)
            returns table(variant)
            language python
            runtime_version=3.8
            imports=('@deployments/procedurename_a_string_b_number/app.zip')
            handler='main.py:app'
            packages=('foo=1.2.3','bar>=3.0.0')
            """
        ),
    ]
    mock_package_create.assert_called_once_with("ask", True, "ask")


@mock.patch("snowflake.connector.connect")
def test_execute_procedure(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "procedure",
            "execute",
            "procedureName(42, 'string')",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "call procedureName(42, 'string')"


@mock.patch("snowflake.connector.connect")
def test_describe_procedure_from_signature(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "procedure",
            "describe",
            "procedureName(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "describe procedure procedureName(int, string, variant)"


@mock.patch("snowflake.connector.connect")
def test_describe_procedure_from_name(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "procedure",
            "describe",
            "procedureName(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "describe procedure procedureName(int, string, variant)"


@mock.patch("snowflake.connector.connect")
def test_list_procedure(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "procedure",
            "list",
            "--like",
            "foo_bar%",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "show user procedures like 'foo_bar%'"


@mock.patch("snowflake.connector.connect")
def test_drop_procedure_from_signature(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "procedure",
            "drop",
            "procedureName(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "drop procedure procedureName(int, string, variant)"


@mock.patch("snowflake.connector.connect")
def test_drop_procedure_from_name(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "procedure",
            "drop",
            "procedureName(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "drop procedure procedureName(int, string, variant)"


def _deploy_procedure(
    execute_in_tmp_dir,
    mock_connector,
    mock_ctx,
    mock_cursor,
    mock_tmp_dir,
    rows,
    runner,
    *args,
):
    tmp_dir = TemporaryDirectory()
    mock_tmp_dir.return_value = tmp_dir
    ctx = mock_ctx(mock_cursor(rows=rows, columns=[]))
    mock_connector.return_value = ctx
    local_dir = Path(execute_in_tmp_dir)
    (local_dir / "requirements.snowflake.txt").write_text("foo=1.2.3\nbar>=3.0.0")
    app = local_dir / "app.py"
    app.touch()
    artifact_path = f"{tmp_dir.name}/{app.name}"

    result = runner.invoke(
        [
            "snowpark",
            "procedure",
            "deploy",
            "procedureName(a string, b number)",
            "--file",
            str(app),
            "--handler",
            "main.py:app",
            "--returns",
            "table(variant)",
            *args,
        ]
    )
    return artifact_path, ctx, result


@pytest.fixture
def mock_ctx_procedure_not_exist(mock_cursor):
    class _MockConnectionCtx(MockConnectionCtx):
        def __init__(self, cursor=None, *args, **kwargs):
            super().__init__(cursor, *args, **kwargs)

        def execute_string(self, query: str, **kwargs):
            self.queries.append(query)
            if query == "describe procedure procedureName(string, number)":
                raise ProgrammingError(
                    "Procedure 'PROCEDURENAME' does not exist or not authorized"
                )
            return (self.cs,)

    return lambda cursor=mock_cursor(["row"], []): _MockConnectionCtx(cursor)


@mock.patch("snowcli.cli.common.project_initialisation._create_project_template")
def test_init_procedure(mock_create_project_template, runner, temp_dir):
    runner.invoke(["snowpark", "procedure", "init", "my_project2"])
    mock_create_project_template.assert_called_once_with(
        "default_procedure", project_directory="my_project2"
    )
