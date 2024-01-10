from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from typing import List

import click
import pluggy
from typer import Typer
from typer.main import get_command

SNOWCLI_COMMAND_PLUGIN_NAMESPACE = "snowflake.cli.plugin.command"

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


class CommandType(Enum):
    SINGLE_COMMAND = "SINGLE_COMMAND"
    COMMAND_GROUP = "COMMAND_GROUP"


@dataclass(frozen=True)
class CommandSpec:
    parent_command_path: CommandPath
    command_type: CommandType
    typer_instance: Typer

    @cached_property
    def command(self) -> click.Command:
        self.typer_instance._add_completion = False  # noqa: SLF001
        return get_command(self.typer_instance)

    @cached_property
    def full_command_path(self) -> CommandPath:
        return CommandPath(self.parent_command_path.path_segments + [self.command.name])
