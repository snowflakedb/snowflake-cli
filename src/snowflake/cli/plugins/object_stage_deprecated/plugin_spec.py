# TODO 3.0: remove this file

from snowflake.cli.api.plugins.command import (
    CommandPath,
    CommandSpec,
    CommandType,
    plugin_hook_impl,
)
from snowflake.cli.plugins.object_stage_deprecated import commands


@plugin_hook_impl
def command_spec():
    return CommandSpec(
        parent_command_path=CommandPath(["object"]),
        command_type=CommandType.COMMAND_GROUP,
        typer_instance=commands.app.create_app(),
    )
