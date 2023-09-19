from dataclasses import dataclass

from snowcli.api.plugin.command import CommandSpec


@dataclass
class LoadedCommandPlugin:
    plugin_name: str
    command_spec: CommandSpec


@dataclass
class LoadedBuiltInCommandPlugin(LoadedCommandPlugin):
    pass
