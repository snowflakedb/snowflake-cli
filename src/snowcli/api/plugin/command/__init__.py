from dataclasses import dataclass
from typing import List

import pluggy
from typer import Typer
from typer.main import get_command

SNOWCLI_COMMAND_PLUGIN_NAMESPACE = "snowcli.command"

plugin_hook_spec = pluggy.HookspecMarker(SNOWCLI_COMMAND_PLUGIN_NAMESPACE)
plugin_hook_impl = pluggy.HookimplMarker(SNOWCLI_COMMAND_PLUGIN_NAMESPACE)


class CommandPath:
    def __init__(self, path_segments: List[str]):
        self._path_segments = tuple(path_segments)

    @property
    def path_segments(self) -> List[str]:
        return list(self._path_segments)

    def __hash__(self):
        return hash(self._path_segments)

    def __eq__(self, other):
        return self._path_segments == other._path_segments

    def __str__(self) -> str:
        return "snow " + " ".join(self.path_segments)


SNOWCLI_ROOT_COMMAND_PATH = CommandPath(path_segments=[])


@dataclass
class CommandSpec:
    parent_command_path: CommandPath
    typer_instance: Typer

    @property
    def full_command_path(self):
        return CommandPath(
            self.parent_command_path.path_segments
            + [get_command(self.typer_instance).name]
        )
