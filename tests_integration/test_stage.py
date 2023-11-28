import os
import tempfile
from pathlib import Path

import pytest

from tests_integration.test_utils import (
    contains_row_with,
    not_contains_row_with,
    row_from_snowflake_session,
)


@pytest.mark.integration
def test_stage(runner, snowflake_session, test_database, tmp_path):
    stage_name = "test_stage"

    result = runner.invoke_with_connection_json(
        ["object", "stage", "create", stage_name]
    )
    assert contains_row_with(
        result.json,
        {"status": f"Stage area {stage_name.upper()} successfully created."},
    )

    result = runner.invoke_with_connection_json(["object", "list", "stage"])
    expect = snowflake_session.execute_string(f"show stages like '{stage_name}'")
    assert contains_row_with(result.json, row_from_snowflake_session(expect)[0])

    filename = "test.txt"
    with tempfile.TemporaryDirectory() as td:
        file_path = os.path.join(td, filename)
        Path(file_path).touch()

        result = runner.invoke_with_connection_json(
            ["object", "stage", "copy", file_path, f"@{stage_name}"]
        )
        assert result.exit_code == 0, result.output
        assert contains_row_with(
            result.json,
            {"source": filename, "target": filename, "status": "UPLOADED"},
        )

    result = runner.invoke_with_connection_json(["object", "stage", "list", stage_name])
    expect = snowflake_session.execute_string(f"list @{stage_name}")
    assert result.json == row_from_snowflake_session(expect)

    # Operation fails because directory exists
    result = runner.invoke_with_connection_json(
        ["object", "stage", "copy", f"@{stage_name}", tmp_path.parent.__str__()]
    )
    assert result.exit_code == 0, result.output
    assert contains_row_with(result.json, {"file": filename, "status": "DOWNLOADED"})
    assert os.path.isfile(tmp_path.parent / filename)

    result = runner.invoke_with_connection_json(
        ["object", "stage", "remove", stage_name, f"/{filename}"]
    )
    assert contains_row_with(
        result.json,
        {"name": f"{stage_name}/{filename}", "result": "removed"},
    )
    expect = snowflake_session.execute_string(f"list @{stage_name}")
    assert not_contains_row_with(
        row_from_snowflake_session(expect), {"name": f"{stage_name}/{filename}"}
    )

    result = runner.invoke_with_connection_json(["object", "drop", "stage", stage_name])
    assert contains_row_with(
        result.json,
        {"status": f"{stage_name.upper()} successfully dropped."},
    )
    expect = snowflake_session.execute_string(f"show stages like '%{stage_name}%'")
    assert row_from_snowflake_session(expect) == []
