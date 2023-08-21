import contextlib
import os
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from textwrap import dedent
from unittest import mock


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.procedure.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.procedure.commands.TemporaryDirectory")
def test_create_procedure(
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx,
    snapshot,
    execute_in_tmp_dir,
):
    tmp_dir = TemporaryDirectory()
    mock_tmp_dir.return_value = tmp_dir

    ctx = mock_ctx()
    mock_connector.return_value = ctx

    tmp_dir_2 = execute_in_tmp_dir.name
    local_dir = Path(tmp_dir_2)
    (local_dir / "requirements.snowflake.txt").write_text("foo=1.2.3\nbar>=3.0.0")

    app = local_dir / "app.py"
    app.touch()

    result = runner.invoke_with_config(
        [
            "snowpark",
            "procedure",
            "create",
            "--name",
            "procedureName",
            "--file",
            str(app),
            "--handler",
            "main.py:app",
            "--return-type",
            "table(variant)",
            "--input-parameters",
            "(a string, b number)",
            "--overwrite",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        f"put file://{tmp_dir.name}/{app.name} @deployments/procedurenamea_string_b_number"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace procedure procedureName(a string, b number)
            returns table(variant)
            language python
            runtime_version=3.8
            imports=('@deployments/procedurenamea_string_b_number/app.zip')
            handler='main.py:app'
            packages=('foo=1.2.3','bar>=3.0.0')
            """
        ),
    ]
    mock_package_create.assert_called_once_with("ask", True, "ask")


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.procedure.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark.procedure.commands.TemporaryDirectory")
@mock.patch("snowcli.cli.snowpark.procedure.commands.replace_handler_in_zip")
def test_create_procedure_with_coverage(
    mock_coverage,
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx,
    snapshot,
    execute_in_tmp_dir,
):
    tmp_dir = TemporaryDirectory()
    mock_tmp_dir.return_value = tmp_dir

    mock_coverage.return_value = "snowpark_coverage.measure_coverage"

    ctx = mock_ctx()
    mock_connector.return_value = ctx

    tmp_dir_2 = execute_in_tmp_dir.name
    local_dir = Path(tmp_dir_2)
    (local_dir / "requirements.snowflake.txt").write_text("foo=1.2.3\nbar>=3.0.0")

    app = local_dir / "app.py"
    app.touch()

    result = runner.invoke_with_config(
        [
            "snowpark",
            "procedure",
            "create",
            "--name",
            "procedureName",
            "--file",
            str(app),
            "--handler",
            "main.py:app",
            "--return-type",
            "table(variant)",
            "--input-parameters",
            "(a string, b number)",
            "--overwrite",
            "--install-coverage-wrapper",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists deployments comment='deployments managed by snowcli'",
        f"put file://{tmp_dir.name}/{app.name} @deployments/procedurenamea_string_b_number"
        f" auto_compress=false parallel=4 overwrite=True",
        dedent(
            """\
            create or replace procedure procedureName(a string, b number)
            returns table(variant)
            language python
            runtime_version=3.8
            imports=('@deployments/procedurenamea_string_b_number/app.zip')
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
        coverage_reports_stage_path="/procedurenamea_string_b_number/coverage",
        temp_dir=tmp_dir.name,
        zip_file_path=tmp_dir.name + "/app.py",
    )


@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.cli.snowpark.procedure.commands.snowpark_package")
@mock.patch("snowcli.cli.snowpark_shared.tempfile.TemporaryDirectory")
def test_update_procedure(
    mock_tmp_dir,
    mock_package_create,
    mock_connector,
    runner,
    mock_ctx,
    snapshot,
    execute_in_tmp_dir,
):
    tmp_dir = TemporaryDirectory()
    mock_tmp_dir.return_value = tmp_dir

    ctx = mock_ctx()
    mock_connector.return_value = ctx
    tmp_dir_2 = execute_in_tmp_dir.name
    local_dir = Path(tmp_dir_2)
    (local_dir / "requirements.snowflake.txt").write_text("foo=1.2.3\nbar>=3.0.0")

    app = local_dir / "app.py"
    app.touch()

    os.chdir(local_dir)
    result = runner.invoke_with_config(
        [
            "snowpark",
            "procedure",
            "update",
            "--name",
            "functionName",
            "--file",
            str(app),
            "--handler",
            "main.py:app",
            "--return-type",
            "table(variant)",
            "--input-parameters",
            "(a string, b number)",
            "--replace-always",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        dedent(
            f"""\
use role MockRole;
use warehouse MockWarehouse;
use database MockDatabase;
use schema MockSchema;
desc PROCEDURE functionName(string, number);"""
        ),
        dedent(
            f"""\
use role MockRole;
use warehouse MockWarehouse;
use database MockDatabase;
use schema MockSchema;


create stage if not exists MockDatabase.MockSchema.deployments comment='deployments managed by snowcli';


put file://{tmp_dir.name}/{app.name} @MockDatabase.MockSchema.deployments/functionnamea_string_b_number auto_compress=false parallel=4 overwrite=True;"""
        ),
        dedent(
            f"""\
use role MockRole;
use warehouse MockWarehouse;
use database MockDatabase;
use schema MockSchema;
CREATE OR REPLACE  PROCEDURE functionName(a string, b number)
         RETURNS table(variant)
         LANGUAGE PYTHON
         RUNTIME_VERSION=3.8
         IMPORTS=('@MockDatabase.MockSchema.deployments/functionnamea_string_b_number/app.zip')
         HANDLER='main.py:app'
         PACKAGES=('foo=1.2.3','bar>=3.0.0')
         ;


describe PROCEDURE functionName(string, number);"""
        ),
    ]
    mock_package_create.assert_called_once_with("ask", True, "ask")


@mock.patch("snowflake.connector.connect")
def test_execute_procedure(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke_with_config(
        [
            "snowpark",
            "procedure",
            "execute",
            "--procedure",
            "procedureName(42, 'string')",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "call procedureName(42, 'string')"


@mock.patch("snowflake.connector.connect")
def test_describe_procedure_from_signature(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke_with_config(
        [
            "snowpark",
            "procedure",
            "describe",
            "--procedure",
            "procedureName(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "describe procedure procedureName(int, string, variant)"


@mock.patch("snowflake.connector.connect")
def test_describe_procedure_from_name(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke_with_config(
        [
            "snowpark",
            "procedure",
            "describe",
            "--name",
            "procedureName",
            "--input-parameters",
            "(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "describe procedure procedureName(int, string, variant)"


@mock.patch("snowflake.connector.connect")
def test_list_procedure(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke_with_config(
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
    result = runner.invoke_with_config(
        [
            "snowpark",
            "procedure",
            "drop",
            "--procedure",
            "procedureName(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "drop procedure procedureName(int, string, variant)"


@mock.patch("snowflake.connector.connect")
def test_drop_procedure_from_name(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke_with_config(
        [
            "snowpark",
            "procedure",
            "drop",
            "--name",
            "procedureName",
            "--input-parameters",
            "(int, string, variant)",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "drop procedure procedureName(int, string, variant)"
