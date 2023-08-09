import logging
from typing import List

from snowcli.plugin import PluginLoadingMode, LoadedPlugin
from snowcli.plugin.add_plugins_to_typer import AddPluginsToTyper
from snowcli.plugin.load_plugins import LoadPlugins
from snowcli.plugin.verify_plugins import VerifyPlugins

log = logging.getLogger(__name__)


def load_and_register_plugins_in_typer(plugin_loading_mode: PluginLoadingMode):
    loaded_plugins = LoadPlugins(plugin_loading_mode)()
    verified_plugins: List[LoadedPlugin] = []
    for plugin_verification in VerifyPlugins(loaded_plugins)():
        if plugin_verification.is_ok():
            verified_plugins.append(plugin_verification.plugin)
        else:
            log.error(
                f"Cannot register plugin [{plugin_verification.plugin.plugin_name}] "
                f"because of verification error: {plugin_verification.error_str()}"
            )
    AddPluginsToTyper(verified_plugins)()
