import snowcli
from snowcli.plugin.api import PluginCommandGroupSpec, PluginCommandGroupPath
from snowcli_connection_plugin import connection


@snowcli.plugin.api.plugin_hook_impl
def plugin_command_group_spec() -> PluginCommandGroupSpec:
    return PluginCommandGroupSpec(
        path=PluginCommandGroupPath([]), command_group=connection.app
    )
