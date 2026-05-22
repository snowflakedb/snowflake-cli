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

"""Plugin interface definitions.

This module provides frozen dataclasses for declaring a plugin's command
surface (what commands exist, their parameters, help text) separately from
the implementation. The command spec is reviewed in Phase 1; the handler
implementation follows in Phase 2.

Typical usage in a plugin's ``interface.py``::

    from snowflake.cli.api.plugins.command.interface import (
        CommandDef, CommandGroupSpec, CommandHandler, ParamDef, ParamKind, REQUIRED,
    )

    MY_SPEC = CommandGroupSpec(
        name="my-plugin",
        help="Does something useful.",
        commands=(
            CommandDef(
                name="run",
                help="Run the thing.",
                handler_method="run",
                requires_connection=True,
                params=(
                    ParamDef(name="name", type=str, kind=ParamKind.ARGUMENT, help="Name"),
                ),
            ),
        ),
    )

    class MyHandler(CommandHandler):
        @abstractmethod
        def run(self, name: str) -> CommandResult: ...
"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Type

import click


class _RequiredSentinel:
    """Sentinel indicating a parameter has no default and is required."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "REQUIRED"

    def __bool__(self) -> bool:
        return False


REQUIRED = _RequiredSentinel()


class ParamKind(Enum):
    """Whether a CLI parameter is a positional argument or a named option."""

    ARGUMENT = "argument"
    OPTION = "option"


@dataclass(frozen=True)
class ParamDef:
    """Declares a single CLI parameter (argument or option).

    Attributes:
        name: Python parameter name (used as the handler method kwarg).
        type: The Python type (``str``, ``FQN``, ``Path``, ...).
        kind: ``ParamKind.ARGUMENT`` or ``ParamKind.OPTION``.
        help: Help text shown in ``--help``.
        cli_names: Explicit CLI names, e.g. ``("--notebook-file", "-f")``.
            Empty means auto-derived from *name*.
        default: Default value. Use ``REQUIRED`` (the default) for required params.
        required: Whether the parameter is required.
        is_flag: ``True`` for boolean flags like ``--replace``.
        show_default: Whether ``--help`` shows the default value.
        hidden: Hide from ``--help`` output.
        click_type: Optional Click ``ParamType`` for custom type parsing.
            Use for types that Typer doesn't handle natively (e.g. ``FQN``
            with ``IdentifierType()``).
    """

    name: str
    type: Type
    kind: ParamKind
    help: str = ""
    cli_names: tuple[str, ...] = ()
    default: Any = REQUIRED
    required: bool = True
    is_flag: bool = False
    show_default: bool = True
    hidden: bool = False
    click_type: Optional[click.ParamType] = None


@dataclass(frozen=True)
class CommandDef:
    """Declares a single CLI command.

    Attributes:
        name: The CLI command name (e.g. ``"execute"``, ``"get-url"``).
        help: Help text shown in ``--help``.
        handler_method: Method name on the ``CommandHandler`` subclass.
        params: Tuple of ``ParamDef`` instances.
        requires_connection: Whether a Snowflake connection is needed.
        require_warehouse: Whether a warehouse must be set.
        is_preview: Mark as a preview feature.
        is_hidden: Hide from ``--help`` output.
        decorators: Names of extra decorators to apply (e.g.
            ``("with_project_definition",)``).
        output_type: Documentation hint for reviewers (not enforced at runtime).
    """

    name: str
    help: str
    handler_method: str
    params: tuple[ParamDef, ...] = ()
    requires_connection: bool = False
    require_warehouse: bool = False
    is_preview: bool = False
    is_hidden: bool = False
    decorators: tuple[str, ...] = ()
    output_type: str = "CommandResult"


@dataclass(frozen=True)
class CommandGroupSpec:
    """Declares a command group — the top-level reviewable spec.

    Attributes:
        name: Group name (e.g. ``"notebook"`` for ``snow notebook``).
        help: Group-level help text.
        parent_path: Where to attach in the CLI tree.
            ``()`` = root level, ``("snowpark",)`` = nested under snowpark.
        commands: Direct child commands.
        subgroups: Nested command groups.
    """

    name: str
    help: str
    parent_path: tuple[str, ...] = ()
    commands: tuple[CommandDef, ...] = ()
    subgroups: tuple["CommandGroupSpec", ...] = ()


@dataclass(frozen=True)
class SingleCommandSpec:
    """Declares a single command (not a group).

    Use this instead of ``CommandGroupSpec`` when the plugin contributes
    exactly one command with no subcommands.

    Attributes:
        parent_path: Where to attach in the CLI tree.
        command: The command definition.
    """

    parent_path: tuple[str, ...] = ()
    command: CommandDef = None  # type: ignore[assignment]


class CommandHandler(ABC):
    """Base class for plugin command handlers.

    Subclass this and declare abstract methods whose names match the
    ``handler_method`` values in your ``CommandGroupSpec`` / ``SingleCommandSpec``.

    The bridge validates at load time that every command in the spec has
    a corresponding callable method on the handler.
    """

    pass
