# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import pytest
from snowflake.cli.api.cli_global_context import get_cli_context


@pytest.mark.parametrize(
    "command, expected_value",
    (
        pytest.param(("sql",), False, id="silent is False"),
        pytest.param(("sql", "--silent"), True, id="silent is True"),
    ),
)
def test_silent_in_global_context(
    command: tuple[str, ...],
    expected_value: bool,
    runner,
):
    runner.invoke(command)

    assert get_cli_context().silent is expected_value


def test_silent_output_help(runner):
    result = runner.invoke(["streamlit", "get-url", "--help"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    expected_message = "Turns off intermediate output to console"
    assert expected_message in result.output, result.output


def test_proper_context_values_for_silent(runner):
    result = runner.invoke(["streamlit", "get-url", "--silent", "--help"])
    assert runner.app is not None
    assert runner.app

    assert result.exit_code == 0, result.output
