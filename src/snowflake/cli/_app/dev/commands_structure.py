# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any

from click import Argument, Command, Option, Parameter

_OMIT = object()


def build_command_catalog(command: Command, name: str = "snow") -> dict[str, Any]:
    """Build a JSON-serialisable catalog of commands, arguments, and options.

    Hidden commands and parameters are omitted so the catalog matches ``--help``.
    Boolean options include both names (``--flag`` and ``--no-flag``). Choice
    options include their allowed values.
    """
    node: dict[str, Any] = {
        "name": name,
        "help": _help_text(command),
    }

    arguments, options = _serialize_params(command)
    if arguments:
        node["arguments"] = arguments
    if options:
        node["options"] = options

    children = getattr(command, "commands", None)
    if children:
        commands = [
            build_command_catalog(child, child_name)
            for child_name, child in sorted(children.items())
            if not getattr(child, "hidden", False)
        ]
        if commands:
            node["commands"] = commands

    return node


def _help_text(command: Command) -> str:
    text = command.help or command.short_help or ""
    return text.strip()


def _serialize_params(
    command: Command,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    arguments: list[dict[str, Any]] = []
    options: list[dict[str, Any]] = []
    for param in command.params:
        serialized = _serialize_param(param)
        if serialized is None:
            continue
        if isinstance(param, Argument):
            arguments.append(serialized)
        else:
            options.append(serialized)
    return arguments, options


def _serialize_param(param: Parameter) -> dict[str, Any] | None:
    if getattr(param, "hidden", False):
        return None

    payload: dict[str, Any] = {
        "name": param.name,
        "help": (getattr(param, "help", None) or "").strip(),
        "required": bool(param.required),
    }

    if isinstance(param, Option):
        payload["flags"] = [*param.opts, *param.secondary_opts]
        if param.is_flag:
            payload["is_flag"] = True

    choices = _choice_values(param)
    if choices:
        payload["choices"] = choices

    default = _json_safe(param.default)
    if default is not _OMIT and default is not None:
        payload["default"] = default

    return payload


def _choice_values(param: Parameter) -> list[str] | None:
    choices = getattr(param.type, "choices", None)
    if not isinstance(choices, (list, tuple)) or not choices:
        return None
    values: list[str] = []
    for choice in choices:
        if isinstance(choice, Enum):
            values.append(str(choice.value))
        else:
            values.append(str(choice))
    return values


def _json_safe(value: Any) -> Any:
    if value is _OMIT:
        return _OMIT
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, (list, tuple)):
        converted = [_json_safe(item) for item in value]
        if any(item is _OMIT for item in converted):
            return _OMIT
        return converted
    if callable(value):
        return _OMIT
    return str(value)
