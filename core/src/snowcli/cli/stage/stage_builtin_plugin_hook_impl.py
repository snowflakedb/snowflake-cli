import snowcli
from snowcli.cli.stage import commands
from snowcli.plugin.api import PluginCommandGroupSpec, PluginCommandGroupPath


@snowcli.plugin.api.plugin_hook_impl
def plugin_command_group_spec() -> PluginCommandGroupSpec:
    return PluginCommandGroupSpec(
        path=PluginCommandGroupPath([]), command_group=commands.app
    )
