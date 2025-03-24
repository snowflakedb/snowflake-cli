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

import sys
from typing import List

import pytest
from snowflake.cli._app.commands_registration.command_plugins_loader import (
    load_only_builtin_command_plugins,
)
from snowflake.cli.api.constants import PYTHON_3_12
from typer.core import TyperGroup

SNOW_CORTEX_SEARCH = "Performs query search using Cortex Search Services."
SNOW_CORTEX_COMPLETE = "Given a prompt, the command generates"
SNOW_CORTEX_HELP = "cortex [OPTIONS] COMMAND [ARGS]..."


def iter_through_all_commands(command_groups_only: bool = False):
    """
    Generator iterating through all commands.
    Paths are yielded as List[str]
    """
    ignore_plugins = ["helpers", "cortex", "workspace"]

    no_command: List[str] = []
    yield no_command

    def _iter_through_commands(command, path):
        if not command_groups_only or isinstance(command, TyperGroup):
            yield list(path)

        for subpath, subcommand in getattr(command, "commands", {}).items():
            path.append(subpath)
            yield from _iter_through_commands(subcommand, path)
            path.pop()

    builtin_plugins = load_only_builtin_command_plugins()
    for plugin in builtin_plugins:
        spec = plugin.command_spec
        if not plugin.plugin_name in ignore_plugins:
            yield from _iter_through_commands(
                spec.command, spec.full_command_path.path_segments
            )


@pytest.mark.parametrize(
    "command",
    iter_through_all_commands(),
    ids=(".".join(cmd) for cmd in iter_through_all_commands()),
)
def test_help_messages(runner, snapshot, command):
    """
    Check help messages against the snapshot
    """
    result = runner.invoke(command + ["--help"])
    assert result.exit_code == 0, result.output
    assert result.output == snapshot


@pytest.mark.parametrize(
    "command",
    iter_through_all_commands(command_groups_only=True),
    ids=(".".join(cmd) for cmd in iter_through_all_commands(command_groups_only=True)),
)
def test_help_messages_no_help_flag(runner, snapshot, command):
    """
    Check help messages against the snapshot
    """
    result = runner.invoke(command)
    assert result.exit_code == 0
    assert result.output == snapshot


@pytest.mark.parametrize(
    "command",
    iter_through_all_commands(),
    ids=(".".join(cmd) for cmd in iter_through_all_commands()),
)
def test_required_arguments_dont_show_default(runner, snapshot, command):
    result = runner.invoke(command + ["--help"])
    assert result.exit_code == 0

    required_msg = "[required]"
    default_msg = "[default: None]"
    prev_none = False
    for line in result.output.splitlines():
        if required_msg in line:
            assert not (
                prev_none or default_msg in line
            ), "Add show_default=False to required arguments"
        prev_none = default_msg in line


@pytest.mark.skipif(
    sys.version_info < PYTHON_3_12,
    reason="It tests if cortex search command is hidden when run using Python 3.12",
)
def test_cortex_help_messages_for_312(runner):
    result = runner.invoke(["cortex", "--help"])
    assert result.exit_code == 0
    assert SNOW_CORTEX_HELP in result.output
    assert SNOW_CORTEX_COMPLETE in result.output
    assert SNOW_CORTEX_SEARCH not in result.output


@pytest.mark.skipif(
    sys.version_info < PYTHON_3_12,
    reason="It tests if cortex search command is hidden when run using Python 3.12",
)
def test_cortex_help_messages_for_312_no_help_flag(runner):
    result = runner.invoke(["cortex"])
    assert result.exit_code == 0
    assert SNOW_CORTEX_HELP in result.output
    assert SNOW_CORTEX_COMPLETE in result.output
    assert SNOW_CORTEX_SEARCH not in result.output


@pytest.mark.skipif(
    sys.version_info >= PYTHON_3_12,
    reason="Snow Cortex Search should be only visible in Python version 3.11 and older",
)
def test_cortex_help_messages_for_311_and_less(runner):
    result = runner.invoke(["cortex", "--help"])
    assert result.exit_code == 0
    assert SNOW_CORTEX_HELP in result.output
    assert SNOW_CORTEX_COMPLETE in result.output
    assert SNOW_CORTEX_SEARCH in result.output


@pytest.mark.skipif(
    sys.version_info >= PYTHON_3_12,
    reason="Snow Cortex Search should be only visible in Python version 3.11 and older",
)
def test_cortex_help_messages_for_311_and_less_no_help_flag(runner):
    result = runner.invoke(["cortex"])
    assert result.exit_code == 0
    assert SNOW_CORTEX_HELP in result.output
    assert SNOW_CORTEX_COMPLETE in result.output
    assert SNOW_CORTEX_SEARCH in result.output
