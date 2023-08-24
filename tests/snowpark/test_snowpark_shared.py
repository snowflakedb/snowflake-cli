import logging
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, ANY
from zipfile import ZipFile

import pytest
import typer

import snowcli.cli.snowpark_shared as shared
from tests.testing_utils.fixtures import *


@mock.patch("tests.snowpark.test_snowpark_shared.shared.connect_to_snowflake")
@mock.patch("tests.snowpark.test_snowpark_shared.shared.print_db_cursor")
def test_snowpark_create_procedure(mock_print, mock_connect, temp_dir, app_zip, caplog):

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.ctx.database = "test_db"
    mock_conn.ctx.schema = "public"

    with caplog.at_level(logging.DEBUG, logger="snowcli.cli.snowpark_shared"):
        result = shared.snowpark_create_procedure(
            "function", "dev", "hello", Path(app_zip), "app.hello", "()", "str", False
        )

    assert "Uploading deployment file to stage..." in caplog.text
    assert "Creating str..." in caplog.text
    mock_conn.upload_file_to_stage.assert_called_with(
        file_path=ANY,
        destination_stage="test_db.public.deployments",
        path="/hello",
        database="test_db",
        schema="public",
        overwrite=False,
        role=ANY,
        warehouse=ANY,
    )

    mock_conn.create_procedure.assert_called_with(
        name="hello",
        input_parameters="()",
        return_type="str",
        handler="app.hello",
        imports="@test_db.public.deployments/hello/app.zip",
        database="test_db",
        schema="public",
        role=ANY,
        warehouse=ANY,
        overwrite=False,
        packages=[],
        execute_as_caller=False,
    )
    mock_print.assert_called()


def test_validate_configuration_with_no_environment(caplog):
    with pytest.raises(typer.Abort):
        with caplog.at_level(logging.ERROR):
            result = shared.validate_configuration(None, "dev")

    assert "The dev environment is not configured in app.toml" in caplog.text


def test_validate_configuration_with_env():
    result = shared.validate_configuration("something", "dev")
    assert result is None


def test_snowpark_update_function_with_coverage_wrapper(caplog):

    with caplog.at_level(logging.ERROR):
        with pytest.raises(typer.Abort):
            shared.snowpark_update(
                type="function",
                environment="dev",
                name="hello",
                file=Path("app.zip"),
                handler="app.hello",
                input_parameters="()",
                return_type="str",
                replace=False,
                install_coverage_wrapper=True,
            )

    assert (
        "You cannot install a code coverage wrapper on a function, only a procedure."
        in caplog.text
    )


def test_replace_handler_in_zip(temp_dir, app_zip):
    result = shared.replace_handler_in_zip(
        proc_name="hello",
        proc_signature="()",
        handler="app.hello",
        temp_dir=temp_dir,
        zip_file_path=app_zip,
        coverage_reports_stage="@example",
        coverage_reports_stage_path="test_db.public.example",
    )
    assert os.path.isfile(app_zip)

    with ZipFile(app_zip, "r") as zip:
        assert "snowpark_coverage.py" in zip.namelist()
        with zip.open("snowpark_coverage.py") as coverage:
            coverage_file = coverage.readlines()
            assert b"        return app.hello(*args,**kwargs)\n" in coverage_file
            assert b"    import app\n" in coverage_file


def test_replace_handler_in_zip_with_wrong_handler(caplog, temp_dir, app_zip):
    with caplog.at_level(logging.ERROR):
        with pytest.raises(typer.Abort):
            result = shared.replace_handler_in_zip(
                proc_name="hello",
                proc_signature="()",
                handler="app.hello.world",
                temp_dir=temp_dir,
                zip_file_path=app_zip,
                coverage_reports_stage="@example",
                coverage_reports_stage_path="test_db.public.example",
            )

    assert (
        "To install a code coverage wrapper, your handler must be in the format <module>.<function>"
        in caplog.text
    )

def test_describe_procedure_without_name_and_input_parameters():
    with pytest.raises(typer.BadParameter) as e:
        result = shared.snowpark_describe_procedure(
            type="function",
            environment="dev",
            name="",
            input_parameters="",
            signature=""
        )
        assert "Please provide either a function name and input " in e.message

