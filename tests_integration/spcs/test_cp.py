import time

import pytest

from tests_integration.test_utils import (
    contains_row_with,
    not_contains_row_with,
    row_from_snowflake_session,
)


@pytest.mark.integration
def test_cp(runner, snowflake_session):
    cp_name = f"test_compute_pool_snowcli_{int(time.time())}"

    result = runner.invoke_with_connection_json(
        [
            "spcs",
            "pool",
            "create",
            "--name",
            cp_name,
            "--min-nodes",
            1,
            "--family",
            "STANDARD_1",
        ]
    )
    assert result.json, result.output
    assert "status" in result.json
    assert (
        f"Compute Pool {cp_name.upper()} successfully created." in result.json["status"]
    )

    expect = snowflake_session.execute_string(f"show compute pools like '{cp_name}'")
    result = runner.invoke_with_connection_json(["object", "list", "compute-pool"])

    assert result.json, result.output
    assert contains_row_with(result.json, row_from_snowflake_session(expect)[0])

    result = runner.invoke_with_connection_json(["spcs", "pool", "stop", cp_name])
    assert contains_row_with(
        result.json,
        {"status": "Statement executed successfully."},
    )

    result = runner.invoke_with_connection_json(
        ["object", "drop", "compute-pool", cp_name]
    )
    assert contains_row_with(
        result.json,
        {"status": f"{cp_name.upper()} successfully dropped."},
    )
    expect = snowflake_session.execute_string(f"show compute pools like '{cp_name}'")
    assert not_contains_row_with(row_from_snowflake_session(expect), {"name": cp_name})
