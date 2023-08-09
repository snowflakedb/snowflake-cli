import snowcli
from snowcli.plugin.api import PluginCommandGroupSpec, PluginCommandGroupPath
from snowcli_package_plugin import package


@snowcli.plugin.api.plugin_hook_impl
def plugin_command_group_spec() -> PluginCommandGroupSpec:
    return PluginCommandGroupSpec(
        path=PluginCommandGroupPath(["snowpark"]), command_group=package.app
    )
