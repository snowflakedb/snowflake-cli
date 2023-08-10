import snowcli
from snowcli.plugin.api import PluginSpec, PluginPath
from snowcli_connection_plugin import connection


@snowcli.plugin.api.plugin_hook_impl
def plugin_spec() -> PluginSpec:
    return PluginSpec(path=PluginPath([]), typer_instance=connection.app)
