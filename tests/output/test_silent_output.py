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
from snowflake.cli.api.feature_flags import FeatureFlag

from tests_common.feature_flag_utils import with_feature_flags


@pytest.mark.parametrize(
    "command, expected_value",
    (
        pytest.param(("sql"), False, id="silent is False"),
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


def test_silent_output_help(runner, snapshot):
    result = runner.invoke(["streamlit", "get-url", "--help"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    snapshot.assert_match(result.output)


def test_proper_context_values_for_silent(runner):
    result = runner.invoke(["streamlit", "get-url", "--silent", "--help"])
    assert runner.app is not None
    assert runner.app

    assert result.exit_code == 0, result.output


def _panel_lines(output: str) -> list[str]:
    """Help output with the Rich panel borders stripped, one content line each."""
    lines = []
    for line in output.splitlines():
        content = line.strip()
        for border in ("│", "|"):
            content = content.removeprefix(border).removesuffix(border)
        lines.append(content.strip())
    return lines


@with_feature_flags({FeatureFlag.ENABLE_COMMAND_DOCS_IN_HELP: True})
@pytest.mark.parametrize("silent", [False, True], ids=["help", "silent-help"])
def test_cortex_complete_help_includes_command_docs(runner, silent):
    args = ["cortex", "complete"]
    if silent:
        args.append("--silent")
    args.append("--help")
    result = runner.invoke(args)

    assert result.exit_code == 0, result.output
    lines = _panel_lines(result.output)
    # Prose is wrapped to the panel, so compare against the unwrapped text.
    prose = " ".join(line for line in lines if line)

    assert "Related topics" in result.output
    assert "Usage notes" in result.output
    assert "Examples" in result.output
    assert "In the simplest use case, the prompt is a single string." in prose
    assert "You can also provide a JSON file with conversation history" in prose
    assert "Ask a question using the default model." in prose
    assert "snow cortex complete" in prose

    # Every path segment of every related link has to survive on a single line;
    # folding at the panel edge used to cut them (".../command-refere" / "nce").
    for href in (
        "/developer-guide/snowflake-cli/index",
        "/developer-guide/snowflake-cli/command-reference/overview",
        "/developer-guide/snowflake-cli/command-reference/cortex-commands/overview",
        "/user-guide/snowflake-cortex/aisql",
    ):
        for segment in href.split("/"):
            if segment:
                assert any(
                    segment in line for line in lines
                ), f"{segment!r} from {href} was split across lines"
