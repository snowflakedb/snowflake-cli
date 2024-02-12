from snowflake.cli.app.commands_registration.command_plugins_loader import (
    load_only_builtin_command_plugins,
)
import pytest


def iter_through_all_commands_paths():
    """
    Generator iterating through all commands.
    Paths are yielded as List[str]
    """
    IGNORE_PLUGINS = ["render"]

    def _iter_through_commands(command, path):
        yield list(path)
        for subpath, subcommand in getattr(command, "commands", {}).items():
            path.append(subpath)
            yield from _iter_through_commands(subcommand, path)
            path.pop()

    builtin_plugins = load_only_builtin_command_plugins()
    for plugin in builtin_plugins:
        spec = plugin.command_spec
        if not plugin.plugin_name in IGNORE_PLUGINS:
            yield from _iter_through_commands(
                spec.command, spec.full_command_path.path_segments
            )


@pytest.mark.parametrize(
    "command",
    iter_through_all_commands_paths(),
    ids=(".".join(cmd) for cmd in iter_through_all_commands_paths()),
)
def test_help_messages(runner, snapshot, command):
    """
    Check help messages against the snapshot
    """
    result = runner.invoke(command + ["--help"])
    assert result.exit_code == 0
    assert result.output == snapshot
