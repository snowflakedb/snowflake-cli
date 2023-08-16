import pytest

from tests_integration.snowflake_connector import snowflake_session
from tests_integration.test_utils import (
    row_from_snowflake_session,
    contains_row_with,
    not_contains_row_with,
)


@pytest.mark.skip(reason="Not yet enabled on our account")
@pytest.mark.integration
def test_cp(runner, snowflake_session):
    cp_name = "test_compute_pool_snowcli"

    result = runner.invoke_integration(
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
        result.json,
        {"status": f"Compute Pool {cp_name.upper()} successfully created."},
    )

    result = runner.invoke_integration(["snowpark", "cp", "list"])
    expect = snowflake_session.execute_string(f"show compute pools like '{cp_name}'")
    assert contains_row_with(result.json, row_from_snowflake_session(expect)[0])

    result = runner.invoke_integration(["snowpark", "compute-pool", "stop", cp_name])
    assert contains_row_with(
        result.json,
        {"status": "Statement executed successfully."},
    )

    result = runner.invoke_integration(["snowpark", "cp", "drop", cp_name])
    assert contains_row_with(
        result.json,
        {"status": f"{cp_name.upper()} successfully dropped."},
    )
    expect = snowflake_session.execute_string(f"show compute pools like '{cp_name}'")
    assert not_contains_row_with(row_from_snowflake_session(expect), {"name": cp_name})
