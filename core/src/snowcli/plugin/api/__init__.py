from dataclasses import dataclass
from typing import List

import pluggy
from typer import Typer

plugin_hook_spec = pluggy.HookspecMarker("snowcli.plugin")
plugin_hook_impl = pluggy.HookimplMarker("snowcli.plugin")


@dataclass
class PluginCommandGroupPath:
    path_segments: List[str]


@dataclass
class PluginCommandGroupSpec:
    path: PluginCommandGroupPath
    command_group: Typer
