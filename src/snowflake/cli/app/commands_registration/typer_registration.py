import logging
from typing import List

import click
from snowflake.cli.api.plugins.command import CommandSpec, CommandType
from snowflake.cli.app.commands_registration import LoadedCommandPlugin
from snowflake.cli.app.commands_registration.exception_logging import exception_logging
from typer.core import TyperGroup

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
        command_spec = self._adjust_command_spec_if_required(command_spec)
        parent_group = self._find_typer_group_at_path(
            current_level_group=self._main_typer_command_group,
            remaining_parent_path_segments=command_spec.parent_command_path.path_segments,
            command_spec=command_spec,
        )
        self._validate_command_spec(command_spec, parent_group)
        parent_group.add_command(command_spec.command)

    def _adjust_command_spec_if_required(
        self,
        command_spec: CommandSpec,
    ) -> CommandSpec:
        command_spec = self._add_empty_callback_to_command_spec_if_required(
            command_spec
        )
        return command_spec

    @staticmethod
    def _add_empty_callback_to_command_spec_if_required(
        command_spec: CommandSpec,
    ) -> CommandSpec:
        new_command_spec = command_spec
        is_specified_as_command_group = (
            command_spec.command_type == CommandType.COMMAND_GROUP
        )
        is_typer_group = isinstance(command_spec.command, TyperGroup)
        if is_specified_as_command_group and not is_typer_group:
            typer_instance = command_spec.typer_instance
            typer_instance.callback()(lambda: None)
            new_command_spec = CommandSpec(
                parent_command_path=command_spec.parent_command_path,
                command_type=command_spec.command_type,
                typer_instance=typer_instance,
            )
        return new_command_spec

    @staticmethod
    def _validate_command_spec(
        command_spec: CommandSpec,
        parent_group: TyperGroup,
    ) -> None:
        command = command_spec.command
        command_type = command_spec.command_type
        is_typer_group = isinstance(command, TyperGroup)
        if command.name in parent_group.commands:
            raise RuntimeError(
                f"Cannot add command [{command_spec.full_command_path}] because it already exists."
            )
        if command_type == CommandType.SINGLE_COMMAND and is_typer_group:
            raise RuntimeError(
                f"Cannot add command [{command_spec.full_command_path}] "
                + f"because its command type is {CommandType.SINGLE_COMMAND} "
                + f"while its implementation contains elements "
                + f"making it a TyperGroup ({CommandType.COMMAND_GROUP}) "
                + f"(a callback or multiple nested commands)."
            )
        if command_type == CommandType.COMMAND_GROUP and not is_typer_group:
            raise RuntimeError(
                f"Cannot add command [{command_spec.full_command_path}] "
                + f"because its command type is {CommandType.COMMAND_GROUP} "
                + f"while its implementation is not a TyperGroup."
            )

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
