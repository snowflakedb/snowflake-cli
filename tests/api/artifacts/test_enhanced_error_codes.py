from __future__ import annotations

import pytest
from snowflake.cli.api.cli_global_context import get_cli_context


@pytest.mark.parametrize(
    "command, expected_value",
    (
        pytest.param(("connection", "list"), False, id="enhanced exit codes disabled"),
        pytest.param(
            ("connection", "list", "--enhanced-exit-codes"),
            True,
            id="enhanced exit codes disabled",
        ),
    ),
)
def test_enhanced_exit_codes_context(
    command: tuple[str, ...], expected_value: bool, runner
):
    runner.invoke(command)
    assert get_cli_context().enhanced_exit_codes == expected_value
