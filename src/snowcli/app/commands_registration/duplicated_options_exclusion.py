from __future__ import annotations

from dataclasses import dataclass
from typing import List, Union

import click
from click import Command, Parameter
from typer.core import TyperGroup, TyperCommand


@dataclass
class SecondaryOptionNameExclusion:
    command_path: str
    primary_option_name: str
    excluded_secondary_option_name: str


_secondary_option_names_duplicates = [
    SecondaryOptionNameExclusion("snowpark procedure describe", "--password", "-p"),
    SecondaryOptionNameExclusion("snowpark procedure drop", "--password", "-p"),
    SecondaryOptionNameExclusion("snowpark procedure execute", "--password", "-p"),
    SecondaryOptionNameExclusion("snowpark services create", "--password", "-p"),
]


def exclude_duplicated_secondary_option_names():
    _exclude_secondary_option_names(_secondary_option_names_duplicates)


def _exclude_secondary_option_names(exclusions: List[SecondaryOptionNameExclusion]):
    main_command_group = _get_main_typer_command_group_from_click_context()
    for exclusion in exclusions:
        command = _find_command(
            command_group=main_command_group,
            remaining_path=exclusion.command_path.split(" "),
            full_path=exclusion.command_path,
        )
        param = _find_param(
            command=command,
            primary_option_name=exclusion.primary_option_name,
            command_path=exclusion.command_path,
        )
        _remove_option_name_from_param(exclusion.excluded_secondary_option_name, param)


def _get_main_typer_command_group_from_click_context() -> TyperGroup:
    main_typer_command_group = click.get_current_context().command
    if isinstance(main_typer_command_group, TyperGroup):
        return main_typer_command_group
    else:
        raise RuntimeError(
            "Invalid main top-level command type. It should be a TyperGroup but it is not."
        )


def _find_command(
    command_group: TyperGroup,
    remaining_path: List[str],
    full_path: str,
) -> Union[TyperCommand, TyperGroup]:
    if remaining_path:
        new_remaining_path = remaining_path[1:]
        found_command = command_group.commands.get(remaining_path[0])
        if isinstance(found_command, TyperGroup):
            return _find_command(found_command, new_remaining_path, full_path)
        elif isinstance(found_command, TyperCommand) and not new_remaining_path:
            return found_command
        else:
            raise RuntimeError(f"Cannot find command [{full_path}].")
    else:
        return command_group


def _find_param(
    command: Command, primary_option_name: str, command_path: str
) -> Parameter:
    for param in command.params:
        if primary_option_name in param.opts:
            return param
    raise RuntimeError(
        f"Cannot find param [{primary_option_name}] for command [{command_path}]."
    )


def _remove_option_name_from_param(excluded_name: str, param: Parameter):
    if excluded_name in param.opts:
        param.opts.remove(excluded_name)
    if excluded_name in param.secondary_opts:
        param.secondary_opts.remove(excluded_name)
