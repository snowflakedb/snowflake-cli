import snowcli
from snowcli.plugin.api import PluginSpec, PluginPath
from snowcli_say_hello_plugin import say_hello


@snowcli.plugin.api.plugin_hook_impl
def plugin_spec() -> PluginSpec:
    return PluginSpec(path=PluginPath(["snowpark"]), typer_instance=say_hello.app)
