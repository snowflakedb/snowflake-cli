import logging
from typing import List, Union, Optional

import pkg_resources

from snowcli.plugin import LoadedPlugin, PluginAfterVerification

log = logging.getLogger(__name__)


class VerifyPlugins:
    def __init__(
        self,
        loaded_plugins: List[LoadedPlugin],
    ) -> None:
        self._loaded_plugins = loaded_plugins

    def __call__(self, *args, **kwargs) -> List[PluginAfterVerification]:
        return [self._verify_plugin(plugin) for plugin in self._loaded_plugins]

    def _verify_plugin(self, plugin: LoadedPlugin) -> PluginAfterVerification:
        try:
            self._verify_plugin_dependencies(plugin)
            return PluginAfterVerification(plugin, verification_error=None)
        except Exception as ex:
            log.debug("Plugin verification error", exc_info=True)
            return PluginAfterVerification(plugin, verification_error=ex)

    @staticmethod
    def _verify_plugin_dependencies(plugin: LoadedPlugin) -> None:
        pkg_resources.require(plugin.plugin_package_info.dependencies)
