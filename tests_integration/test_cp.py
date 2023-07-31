import pytest

from unittest import mock
from tests_integration.snowflake_connector import snowflake_session
from tests_integration.test_utils import (
    row_from_mock,
    row_from_snowflake_session,
    contains_row_with,
    not_contains_row_with,
)


@pytest.mark.skip(reason="Not yet enabled on our account")
@pytest.mark.integration
@mock.patch("snowcli.cli.snowpark.cp.print_db_cursor")
def test_cp(mock_print, runner, snowflake_session):
    cp_name = "test_compute_pool_snowcli"

    runner.invoke_with_config_and_integration_connection(
        [
            "snowpark",
            "compute-pool",
            "create",
            "--name",
            cp_name,
            "--num",
            1,
            "--family",
            "STANDARD_1",
        ]
    )
    assert contains_row_with(
        row_from_mock(mock_print),
        {"status": f"Compute Pool {cp_name.upper()} successfully created."},
    )

    runner.invoke_with_config_and_integration_connection(["snowpark", "cp", "list"])
    expect = snowflake_session.execute_string(f"show compute pools like '{cp_name}'")
    assert contains_row_with(
        row_from_mock(mock_print), row_from_snowflake_session(expect)[0]
    )

    runner.invoke_with_config_and_integration_connection(
        ["snowpark", "compute-pool", "stop", cp_name]
    )
    assert contains_row_with(
        row_from_mock(mock_print),
        {"status": "Statement executed successfully."},
    )

    runner.invoke_with_config_and_integration_connection(
        ["snowpark", "cp", "drop", cp_name]
    )
    assert contains_row_with(
        row_from_mock(mock_print),
        {"status": f"{cp_name.upper()} successfully dropped."},
    )
    expect = snowflake_session.execute_string(f"show compute pools like '{cp_name}'")
    assert not_contains_row_with(row_from_snowflake_session(expect), {"name": cp_name})
