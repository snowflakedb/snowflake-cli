import snowcli
from snowcli.plugin.api import PluginSpec, PluginPath
from snowcli_package_plugin import package


@snowcli.plugin.api.plugin_hook_impl
def plugin_spec() -> PluginSpec:
    return PluginSpec(path=PluginPath(["snowpark"]), typer_instance=package.app)
