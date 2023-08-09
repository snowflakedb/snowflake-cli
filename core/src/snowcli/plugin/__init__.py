from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from snowcli.plugin.api import PluginCommandGroupSpec


@dataclass
class LoadedPlugin:
    plugin_name: str
    command_group_spec: PluginCommandGroupSpec


@dataclass
class BuiltInLoadedPlugin(LoadedPlugin):
    pass


class ExternalPluginsLoadingMode(Enum):
    ALL_INSTALLED_EXTERNAL_PLUGINS = 1
    ONLY_ENABLED_EXTERNAL_PLUGINS = 2


@dataclass
class ExternalPluginPackageInfo:
    package_name: str
    package_version: str
    dependencies: List[str]


@dataclass
class ExternalLoadedPlugin(LoadedPlugin):
    is_enabled: bool
    plugin_package_info: ExternalPluginPackageInfo


@dataclass
class ExternalPluginAfterVerification:
    plugin: ExternalLoadedPlugin
    verification_error: Optional[Exception]

    def is_ok(self) -> bool:
        return self.verification_error is None

    def error_str(self) -> Optional[str]:
        error = self.verification_error
        if error:
            return f"{error.__class__.__name__} - {error.__str__()}"
        else:
            return None
