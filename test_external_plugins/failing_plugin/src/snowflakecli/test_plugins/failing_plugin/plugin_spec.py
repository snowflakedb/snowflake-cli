from snowflake.cli.api.plugins.command import (
    CommandPath,
    CommandSpec,
    CommandType,
    plugin_hook_impl,
)
from snowflakecli.test_plugins.failing_plugin import commands


@plugin_hook_impl
def command_spec():
    return CommandSpec(
        parent_command_path=CommandPath(["failing"]),
        command_type=CommandType.SINGLE_COMMAND,
        typer_instance=commands.app,
    )
