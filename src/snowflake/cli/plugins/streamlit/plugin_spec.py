from snowflake.cli.api.plugins.command import (
    SNOWCLI_ROOT_COMMAND_PATH,
    CommandSpec,
    CommandType,
    plugin_hook_impl,
)
from snowflake.cli.plugins.streamlit import commands


@plugin_hook_impl
def command_spec():
    return CommandSpec(
        parent_command_path=SNOWCLI_ROOT_COMMAND_PATH,
        command_type=CommandType.COMMAND_GROUP,
        typer_instance=commands.app,
    )
