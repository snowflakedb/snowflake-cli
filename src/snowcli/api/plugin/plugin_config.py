from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class PluginConfig:
    is_plugin_enabled: bool
    internal_config: Dict[str, Any]


class PluginConfigProvider:
    def get_enabled_plugin_names(self) -> List[str]:
        raise NotImplementedError()

    def get_config(self, plugin_name: str) -> PluginConfig:
        raise NotImplementedError()
