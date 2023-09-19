import tempfile
from pathlib import Path

import pytest
import os

from unittest import mock
from tempfile import NamedTemporaryFile
from tests_integration.snowflake_connector import test_database, snowflake_session
from tests_integration.test_utils import (
    row_from_snowflake_session,
    contains_row_with,
    not_contains_row_with,
)


@pytest.mark.integration
def test_stage(runner, snowflake_session, test_database, tmp_path):
    stage_name = "test_stage"

    result = runner.invoke_integration(["stage", "create", stage_name])
    assert contains_row_with(
        result.json,
        {"status": f"Stage area {stage_name.upper()} successfully created."},
    )

    result = runner.invoke_integration(["stage", "list"])
    expect = snowflake_session.execute_string(f"show stages like '{stage_name}'")
    assert contains_row_with(result.json, row_from_snowflake_session(expect)[0])

    filename = "test.txt"
    with tempfile.TemporaryDirectory() as td:
        file_path = os.path.join(td, filename)
        Path(file_path).touch()

        result = runner.invoke_integration(["stage", "put", file_path, stage_name])
        assert contains_row_with(
            result.json,
            {"source": filename, "target": filename, "status": "UPLOADED"},
        )

    result = runner.invoke_integration(["stage", "list", stage_name])
    expect = snowflake_session.execute_string(f"list @{stage_name}")
    assert result.json == row_from_snowflake_session(expect)

    result = runner.invoke_integration(
        ["stage", "get", stage_name, tmp_path.parent.__str__()]
    )
    assert contains_row_with(result.json, {"file": filename, "status": "DOWNLOADED"})
    assert os.path.isfile(tmp_path.parent / filename)

    result = runner.invoke_integration(["stage", "remove", stage_name, f"/{filename}"])
    assert contains_row_with(
        result.json,
        {"name": f"{stage_name}/{filename}", "result": "removed"},
    )
    expect = snowflake_session.execute_string(f"list @{stage_name}")
    assert not_contains_row_with(
        row_from_snowflake_session(expect), {"name": f"{stage_name}/{filename}"}
    )

    result = runner.invoke_integration(["stage", "drop", stage_name])
    assert contains_row_with(
        result.json,
        {"status": f"{stage_name.upper()} successfully dropped."},
    )
    expect = snowflake_session.execute_string(f"show stages like '%{stage_name}%'")
    assert row_from_snowflake_session(expect) == []
