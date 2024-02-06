from typing import Optional

from snowflake.cli.api.plugins.plugin_config import PluginConfigProvider


class Api:
    def __init__(
        self,
        plugin_config_provider: PluginConfigProvider,
    ):
        self.plugin_config_provider = plugin_config_provider


class ApiNotInitializedError(RuntimeError):
    """There was a try to use CLI's API while it is still not initialized."""


class ApiProvider:
    def __init__(self):
        self._api: Optional[Api] = None

    def register_api(self, api: Api) -> None:
        self._api = api

    def api(self) -> Api:
        if self._api:
            return self._api
        else:
            raise ApiNotInitializedError()


api_provider = ApiProvider()
