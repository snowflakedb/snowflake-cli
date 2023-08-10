import snowcli
from snowcli.plugin.api import PluginSpec, PluginPath
from snowcli_function_plugin import function


@snowcli.plugin.api.plugin_hook_impl
def plugin_spec() -> PluginSpec:
    return PluginSpec(path=PluginPath(["snowpark"]), typer_instance=function.app)
