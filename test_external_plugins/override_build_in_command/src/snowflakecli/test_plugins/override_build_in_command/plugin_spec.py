from snowflake.cli.api.plugins.command import (
    CommandPath,
    CommandSpec,
    CommandType,
    plugin_hook_impl,
)
from snowflakecli.test_plugins.override_build_in_command import commands


@plugin_hook_impl
def command_spec():
    return CommandSpec(
        parent_command_path=CommandPath(["connection"]),
        command_type=CommandType.SINGLE_COMMAND,
        typer_instance=commands.app,
    )
