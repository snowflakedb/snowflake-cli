import pytest
import os

from unittest import mock
from tempfile import NamedTemporaryFile
from tests_integration.snowflake_connector import test_database, snowflake_session
from tests_integration.test_utils import (
    row_from_mock,
    row_from_snowflake_session,
    contains_row_with,
    not_contains_row_with,
)


@pytest.mark.integration
@mock.patch("snowcli.output.decorators.print_db_cursor")
def test_stage(mock_print, runner, snowflake_session, test_database, tmp_path):
    stage_name = "test_stage"

    runner.invoke_with_config_and_integration_connection(
        ["stage", "create", stage_name]
    )
    assert contains_row_with(
        row_from_mock(mock_print),
        {"status": f"Stage area {stage_name.upper()} successfully created."},
    )

    runner.invoke_with_config_and_integration_connection(["stage", "list"])
    expect = snowflake_session.execute_string(f"show stages like '{stage_name}'")
    assert contains_row_with(
        row_from_mock(mock_print), row_from_snowflake_session(expect)[0]
    )

    with NamedTemporaryFile("w+", suffix=".txt") as tmp_file:
        runner.invoke_with_config_and_integration_connection(
            ["stage", "put", tmp_file.name, stage_name]
        )
        filename = os.path.basename(tmp_file.name)
        assert contains_row_with(
            row_from_mock(mock_print),
            {"source": filename, "target": filename, "status": "UPLOADED"},
        )

    runner.invoke_with_config_and_integration_connection(["stage", "list", stage_name])
    result = snowflake_session.execute_string(f"list @{stage_name}")
    assert row_from_mock(mock_print) == row_from_snowflake_session(result)

    runner.invoke_with_config_and_integration_connection(
        ["stage", "get", stage_name, tmp_path.parent.__str__()]
    )
    assert contains_row_with(
        row_from_mock(mock_print), {"file": filename, "status": "DOWNLOADED"}
    )
    assert os.path.isfile(tmp_path.parent / filename)

    runner.invoke_with_config_and_integration_connection(
        ["stage", "remove", stage_name, f"/{filename}"]
    )
    assert contains_row_with(
        row_from_mock(mock_print),
        {"name": f"{stage_name}/{filename}", "result": "removed"},
    )
    result = snowflake_session.execute_string(f"list @{stage_name}")
    assert not_contains_row_with(
        row_from_snowflake_session(result), {"name": f"{stage_name}/{filename}"}
    )

    runner.invoke_with_config_and_integration_connection(["stage", "drop", stage_name])
    assert contains_row_with(
        row_from_mock(mock_print),
        {"status": f"{stage_name.upper()} successfully dropped."},
    )
    result = snowflake_session.execute_string(f"show stages like '%{stage_name}%'")
    assert row_from_snowflake_session(result) == []
