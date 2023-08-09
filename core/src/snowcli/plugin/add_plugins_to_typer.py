import logging
from typing import List

import click
import typer
from typer.core import TyperGroup

from snowcli.cli.common.snow_cli_global_context import global_context_copy
from snowcli.plugin import LoadedPlugin
from snowcli.plugin.api import PluginCommandGroupSpec, PluginCommandGroupPath

log = logging.getLogger(__name__)


class AddPluginsToTyper:
    def __init__(self, plugins: List[LoadedPlugin]):
        self._plugins = plugins
        self._main_typer_command_group = (
            self._get_main_typer_command_group_from_click_context()
        )

    def __call__(self, *args, **kwargs):
        for plugin in self._plugins:
            try:
                self._add_plugin_command_group(plugin.command_group_spec)
            except Exception as ex:
                log.error(
                    f"Cannot register command group from plugin [{plugin.plugin_name}]: {ex.__str__()}",
                    exc_info=global_context_copy().debug,
                )

    @staticmethod
    def _get_main_typer_command_group_from_click_context() -> TyperGroup:
        main_typer_command_group = click.get_current_context().command
        if isinstance(main_typer_command_group, TyperGroup):
            return main_typer_command_group
        else:
            raise RuntimeError(
                "Invalid main top-level command type. It should be a TyperGroup but it is not."
            )

    def _add_plugin_command_group(
        self,
        command_group_spec: PluginCommandGroupSpec,
    ) -> None:
        parent_group = self._find_typer_group_at_path(
            current_level_group=self._main_typer_command_group,
            remaining_path_segments=command_group_spec.path.path_segments,
            full_plugin_path=command_group_spec.path,
        )
        plugin_group = typer.main.get_group(command_group_spec.command_group)
        parent_group.add_command(plugin_group)

    def _find_typer_group_at_path(
        self,
        current_level_group: TyperGroup,
        remaining_path_segments: List[str],
        full_plugin_path: PluginCommandGroupPath,
    ) -> TyperGroup:
        if remaining_path_segments:
            expected_name = remaining_path_segments[0]
            matching_subgroups = [
                subgroup
                for subgroup in current_level_group.commands.values()
                if isinstance(subgroup, TyperGroup) and subgroup.name == expected_name
            ]
            if matching_subgroups:
                return self._find_typer_group_at_path(
                    current_level_group=matching_subgroups[0],
                    remaining_path_segments=remaining_path_segments[1:],
                    full_plugin_path=full_plugin_path,
                )
            else:
                raise RuntimeError(
                    f"Invalid plugin path [{'/'.join(full_plugin_path.path_segments)}]. "
                    f"Command group [{expected_name}] does not exist."
                )
        else:
            return current_level_group
