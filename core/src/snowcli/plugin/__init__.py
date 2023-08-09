from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from snowcli.plugin.api import PluginCommandGroupSpec


class PluginLoadingMode(Enum):
    ALL_INSTALLED_PLUGINS = 1
    ONLY_ENABLED_PLUGINS = 2


@dataclass
class PluginPackageInfo:
    package_name: str
    package_version: str
    dependencies: List[str]


@dataclass
class LoadedPlugin:
    plugin_name: str
    is_enabled: bool
    plugin_package_info: PluginPackageInfo
    command_group_spec: PluginCommandGroupSpec


@dataclass
class PluginAfterVerification:
    plugin: LoadedPlugin
    verification_error: Optional[Exception]

    def is_ok(self) -> bool:
        return self.verification_error is None

    def error_str(self) -> Optional[str]:
        error = self.verification_error
        if error:
            return f"{error.__class__.__name__} - {error.__str__()}"
        else:
            return None
