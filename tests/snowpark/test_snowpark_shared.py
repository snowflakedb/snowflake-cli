import logging
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest
import snowcli.cli.snowpark_shared as shared
from tests.testing_utils.fixtures import *

@mock.patch("tests.snowpark.test_snowpark_shared.shared.connect_to_snowflake")
@mock.patch("tests.snowpark.test_snowpark_shared.shared.print_db_cursor")
def test_snowpark_create_procedure(mock_print, mock_connect,temp_dir, app_zip, caplog):

    mock_connect = MagicMock()
    mock_connect.ctx.database = "test_db"
    mock_connect.ctx.schema = "public"
    mock_connect.upload_file_to_stage = MagicMock(return_value="Upload_result")

    mock_print = MagicMock()

    with caplog.at_level(logging.DEBUG, logger="snowcli.cli.snowpark_shared"):
        result = shared.snowpark_create_procedure("str","dev","hello",Path(app_zip),"app.hello","()","str",False)
    q = mock_connect.upload_file_to_stage.called
    w = mock_connect.mock_calls
    assert "Uploading deployment file to stage..." in caplog.text
    assert "Creating str..." in caplog.text
    mock_connect.upload_file_to_stage.assert_called()
    #mock_print.assert_called_once_with(mock_connect.upload_file_to_stage.return_value)

