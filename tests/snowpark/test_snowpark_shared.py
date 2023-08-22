import logging
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, ANY

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
            "str", "dev", "hello", Path(app_zip), "app.hello", "()", "str", False
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
    with pytest.raises(typer.Abort) as e:
        with caplog.at_level(logging.ERROR):
            result = shared.validate_configuration(None,"dev")

    assert "The dev environment is not configured in app.toml" in caplog.text

def test_validate_configuration_with_env():
    result = shared.validate_configuration("something","dev")
    assert result is None

@mock.patch("tests.snowpark.test_snowpark_shared.shared.connect_to_snowflake")
def test_snowpark_update(mock_connection):
    mock_conn = MagicMock()
    mock_connection.return_value = mock_conn
    mock_conn.ctx.database = "test_db"
    mock_conn.ctx.schema = "public"

    pass



