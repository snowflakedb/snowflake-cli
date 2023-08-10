from dataclasses import dataclass
from typing import List

import pluggy
from typer import Typer

plugin_hook_spec = pluggy.HookspecMarker("snowcli.plugin")
plugin_hook_impl = pluggy.HookimplMarker("snowcli.plugin")


@dataclass
class PluginPath:
    path_segments: List[str]


@dataclass
class PluginSpec:
    path: PluginPath
    typer_instance: Typer
