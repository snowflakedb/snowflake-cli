import pytest
from unittest import mock

from tests_integration.snowflake_connector import snowflake_session


@mock.patch("snowcli.cli.streamlit.print_db_cursor")
def test_streamlit_list(mock_print, runner, snowflake_session):
    runner.invoke(["streamlit", "list"])

    expected_values = snowflake_session.execute_string("show streamlits")

    print(expected_values)
    assert False