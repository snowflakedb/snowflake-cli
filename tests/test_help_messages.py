from __future__ import annotations

from typing import List

import pytest
from snowflake.cli.app.commands_registration.command_plugins_loader import (
    load_only_builtin_command_plugins,
)
from typer.core import TyperGroup


def iter_through_all_commands(command_groups_only: bool = False):
    """
    Generator iterating through all commands.
    Paths are yielded as List[str]
    """
    ignore_plugins = ["render", "cortex"]

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
    assert result.exit_code == 0
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
