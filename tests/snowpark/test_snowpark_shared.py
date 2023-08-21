from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest
import snowcli.cli.snowpark_shared as shared

@mock.patch("tests.snowpark.test_snowpark_shared.shared.connect_to_snowflake")
def test_snowpark_create_procedure(mock_connect):

    mock_connect = MagicMock()
    result = shared.snowpark_create_procedure("1","2","3",Path("4"),"5","6","7",False)

    mock_connect.assert_called_with(environment="2")