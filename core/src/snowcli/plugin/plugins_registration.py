import logging
from typing import List

from snowcli.plugin import (
    ExternalPluginsLoadingMode,
    ExternalLoadedPlugin,
    LoadedPlugin,
)
from snowcli.plugin.add_plugins_to_typer import AddPluginsToTyper
from snowcli.plugin.load_builtin_plugins import LoadBuiltInPlugins
from snowcli.plugin.load_external_plugins import LoadExternalPlugins
from snowcli.plugin.verify_external_plugins import VerifyExternalPlugins

log = logging.getLogger(__name__)


def _cast_to_loaded_plugin(plugin: LoadedPlugin) -> LoadedPlugin:
    return plugin


def load_and_register_plugins_in_typer(
    external_plugins_loading_mode: ExternalPluginsLoadingMode,
):
    loaded_builtin_plugins = LoadBuiltInPlugins()()
    loaded_external_plugins = LoadExternalPlugins(external_plugins_loading_mode)()
    verified_external_plugins: List[ExternalLoadedPlugin] = []
    for plugin_verification in VerifyExternalPlugins(loaded_external_plugins)():
        if plugin_verification.is_ok():
            verified_external_plugins.append(plugin_verification.plugin)
        else:
            log.error(
                f"Cannot register external plugin [{plugin_verification.plugin.plugin_name}] "
                f"because of verification error: {plugin_verification.error_str()}"
            )
    all_plugins_to_register = [
        _cast_to_loaded_plugin(p) for p in loaded_builtin_plugins
    ] + [_cast_to_loaded_plugin(p) for p in verified_external_plugins]
    AddPluginsToTyper(all_plugins_to_register)()
