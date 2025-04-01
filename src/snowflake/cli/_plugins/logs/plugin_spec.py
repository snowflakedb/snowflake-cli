from snowflake.cli._plugins.logs import commands
from snowflake.cli.api.plugins.command import (
    SNOWCLI_ROOT_COMMAND_PATH,
    CommandSpec,
    CommandType,
    plugin_hook_impl,
)


@plugin_hook_impl
def command_spec():
    return CommandSpec(
        parent_command_path=SNOWCLI_ROOT_COMMAND_PATH,
        command_type=CommandType.SINGLE_COMMAND,
        typer_instance=commands.app.create_instance(),
    )
