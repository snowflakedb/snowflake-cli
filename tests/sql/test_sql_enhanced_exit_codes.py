from __future__ import annotations

from unittest import mock

import pytest


@pytest.mark.parametrize(
    "command, expected_exit_code",
    (
        pytest.param(
            ("sql", "-q", "select 1"),
            0,
            id="successful invocation",
        ),
        pytest.param(
            ("sql", "-q", "select 1", "--enhanced-exit-codes"),
            0,
            id="successful invocation with enhanced exit codes",
        ),
        pytest.param(
            ("sql", "-q", "select '&foo'"),
            1,
            id="missing variable fails with 1",
        ),
        pytest.param(
            ("sql", "-q", "select '&foo'", "--enhanced-exit-codes"),
            5,
            id="missing variable fails with 5",
        ),
    ),
)
@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_sql_exit_codes(mock_execute, runner, mock_cursor, command, expected_exit_code):
    mock_execute.return_value = (mock_cursor(["row"], []) for _ in range(1))
    resutl = runner.invoke(command)
    assert resutl.exit_code == expected_exit_code
