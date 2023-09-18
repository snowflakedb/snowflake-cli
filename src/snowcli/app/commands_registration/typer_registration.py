import logging
from typing import List

import click
from typer.core import TyperGroup

from snowcli.app.commands_registration import LoadedCommandPlugin
from snowcli.api.plugin.command import CommandSpec
from snowcli.cli.exception_logging import exception_logging

log = logging.getLogger(__name__)
log_exception = exception_logging(log)


class TyperCommandsRegistration:
    def __init__(self, plugins: List[LoadedCommandPlugin]):
        self._plugins = plugins
        self._main_typer_command_group = (
            self._get_main_typer_command_group_from_click_context()
        )

    def register_commands(self):
        for plugin in self._plugins:
            try:
                self._add_plugin_to_typer(plugin.command_spec)
            except Exception as ex:
                log_exception(
                    f"Cannot register plugin [{plugin.plugin_name}]: {ex.__str__()}", ex
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

    def _add_plugin_to_typer(
        self,
        command_spec: CommandSpec,
    ) -> None:
        parent_group = self._find_typer_group_at_path(
            current_level_group=self._main_typer_command_group,
            remaining_parent_path_segments=command_spec.parent_command_path.path_segments,
            command_spec=command_spec,
        )
        command = command_spec.command
        if command.name in parent_group.commands:
            raise RuntimeError(
                f"Cannot add command [{command_spec.full_command_path}] because it already exists."
            )
        parent_group.add_command(command)

    def _find_typer_group_at_path(
        self,
        current_level_group: TyperGroup,
        remaining_parent_path_segments: List[str],
        command_spec: CommandSpec,
    ) -> TyperGroup:
        if remaining_parent_path_segments:
            expected_name = remaining_parent_path_segments[0]
            matching_subgroups = [
                subgroup
                for subgroup in current_level_group.commands.values()
                if isinstance(subgroup, TyperGroup) and subgroup.name == expected_name
            ]
            if matching_subgroups:
                return self._find_typer_group_at_path(
                    current_level_group=matching_subgroups[0],
                    remaining_parent_path_segments=remaining_parent_path_segments[1:],
                    command_spec=command_spec,
                )
            else:
                raise RuntimeError(
                    f"Invalid command path [{command_spec.full_command_path}]. "
                    f"Command group [{expected_name}] does not exist."
                )
        else:
            return current_level_group


def register_commands_from_plugins(plugins: List[LoadedCommandPlugin]) -> None:
    return TyperCommandsRegistration(plugins).register_commands()
