from __future__ import annotations

from typing import Tuple

import pytest
from snowflake.cli.api.console.enum import Output


@pytest.mark.parametrize(
    "stack, expected",
    (
        pytest.param((), False, id="empty stack"),
        pytest.param((Output.STEP,), False, id="single step no phase"),
        pytest.param((Output.PHASE,), True, id="single phase"),
        pytest.param((Output.STEP, Output.STEP), False, id="2 steps, no phase"),
        pytest.param((Output.PHASE, Output.STEP), True, id="phase than step"),
        pytest.param(
            (Output.STEP, Output.PHASE, Output.STEP), True, id="step after phase"
        ),
    ),
)
def test_is_in_phase_context(stack: Tuple[Output], expected: bool, cli_console_ctx):
    for item in stack:
        cli_console_ctx.push(item)

    assert cli_console_ctx.is_in_phase is expected
