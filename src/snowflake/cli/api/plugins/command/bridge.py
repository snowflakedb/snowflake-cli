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

"""Bridge: converts a command spec + handler into a standard CommandSpec.

The bridge is the only piece that knows about both the declarative interface
layer and the Typer/Click runtime.  Downstream code (plugin loader, command
registration) sees a plain ``CommandSpec`` and is completely unaware of the
interface/handler split.
"""

from __future__ import annotations

import inspect
from functools import wraps
from typing import Callable

import typer

from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.plugins.command import (
    CommandPath,
    CommandSpec,
    CommandType,
)
from snowflake.cli.api.plugins.command.interface import (
    CommandDef,
    CommandGroupSpec,
    CommandHandler,
    ParamDef,
    ParamKind,
    REQUIRED,
    SingleCommandSpec,
)


# ---------------------------------------------------------------------------
# Decorator registry — maps string names to factory callables
# ---------------------------------------------------------------------------

_DECORATOR_REGISTRY: dict[str, Callable] = {
    "with_project_definition": lambda: with_project_definition(),
}


def register_decorator(name: str, factory: Callable) -> None:
    """Register an additional decorator for use in ``CommandDef.decorators``.

    *factory* is called with no arguments and must return a decorator function.
    """
    _DECORATOR_REGISTRY[name] = factory


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def build_command_spec(
    spec: CommandGroupSpec | SingleCommandSpec,
    handler: CommandHandler,
    *,
    validate: bool = True,
) -> CommandSpec:
    """Convert a declarative spec + handler into a standard ``CommandSpec``.

    The returned object is identical to what a traditional ``plugin_spec.py``
    would produce — the rest of the CLI infrastructure is unchanged.

    Args:
        spec: Declarative command surface.
        handler: Concrete handler implementing the business logic.
        validate: If ``True`` (default), verify the handler satisfies the spec
            before building.  Disable in production for faster startup if the
            interface tests already cover this.
    """
    if validate:
        validate_interface_handler(spec, handler)

    if isinstance(spec, SingleCommandSpec):
        factory = _build_single_command(spec, handler)
        command_type = CommandType.SINGLE_COMMAND
    else:
        factory = _build_command_group(spec, handler)
        command_type = CommandType.COMMAND_GROUP

    return CommandSpec(
        parent_command_path=CommandPath(list(spec.parent_path)),
        command_type=command_type,
        typer_instance=factory.create_instance(),
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class InterfaceValidationError(Exception):
    """Raised when a handler does not satisfy its interface spec."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        msg = "Interface validation failed:\n" + "\n".join(
            f"  - {e}" for e in errors
        )
        super().__init__(msg)


def validate_interface_handler(
    spec: CommandGroupSpec | SingleCommandSpec,
    handler: CommandHandler,
) -> None:
    """Check that *handler* has a callable method for every command in *spec*.

    Raises ``InterfaceValidationError`` with all violations listed.
    """
    errors: list[str] = []
    for cmd in _collect_commands(spec):
        method = getattr(handler, cmd.handler_method, None)
        if method is None:
            errors.append(
                f"Handler missing method '{cmd.handler_method}' "
                f"for command '{cmd.name}'"
            )
        elif not callable(method):
            errors.append(f"Handler.{cmd.handler_method} is not callable")
    if errors:
        raise InterfaceValidationError(errors)


def _collect_commands(
    spec: CommandGroupSpec | SingleCommandSpec,
) -> list[CommandDef]:
    """Recursively collect all ``CommandDef`` objects from a spec tree."""
    if isinstance(spec, SingleCommandSpec):
        return [spec.command]
    commands = list(spec.commands)
    for sub in spec.subgroups:
        commands.extend(_collect_commands(sub))
    return commands


# ---------------------------------------------------------------------------
# Typer construction
# ---------------------------------------------------------------------------


def _build_command_group(
    spec: CommandGroupSpec,
    handler: CommandHandler,
) -> SnowTyperFactory:
    factory = SnowTyperFactory(name=spec.name, help=spec.help)

    for cmd_def in spec.commands:
        _register_command(factory, cmd_def, handler)

    for subgroup in spec.subgroups:
        sub_factory = _build_command_group(subgroup, handler)
        factory.add_typer(sub_factory)

    return factory


def _build_single_command(
    spec: SingleCommandSpec,
    handler: CommandHandler,
) -> SnowTyperFactory:
    factory = SnowTyperFactory(name=spec.command.name)

    _register_command(factory, spec.command, handler)

    return factory


def _register_command(
    factory: SnowTyperFactory,
    cmd_def: CommandDef,
    handler: CommandHandler,
) -> None:
    """Build a Typer command function from a ``CommandDef`` + handler method."""
    handler_method = getattr(handler, cmd_def.handler_method)
    spec_param_names = {p.name for p in cmd_def.params}

    # Build signature parameters for Typer introspection
    sig_params: list[inspect.Parameter] = []
    annotations: dict[str, type] = {}

    for p in cmd_def.params:
        annotations[p.name] = p.type
        default = _make_typer_default(p)
        sig_params.append(
            inspect.Parameter(
                p.name,
                inspect.Parameter.KEYWORD_ONLY,
                default=default,
                annotation=p.type,
            )
        )

    # **options is required by SnowTyper's global_options decorator injection
    sig_params.append(
        inspect.Parameter("options", inspect.Parameter.VAR_KEYWORD)
    )

    # Create the wrapper that Typer will call at runtime
    @wraps(handler_method)
    def command_fn(**kwargs):
        handler_kwargs = {k: v for k, v in kwargs.items() if k in spec_param_names}
        return handler_method(**handler_kwargs)

    # Attach the constructed signature so Typer/Click build the right CLI
    command_fn.__signature__ = inspect.Signature(sig_params)  # type: ignore[attr-defined]
    command_fn.__annotations__ = annotations
    command_fn.__doc__ = cmd_def.help
    command_fn.__name__ = cmd_def.handler_method
    command_fn.__qualname__ = cmd_def.handler_method

    # Apply registered decorators in reverse order (outermost first)
    for dec_name in reversed(cmd_def.decorators):
        dec_factory = _DECORATOR_REGISTRY.get(dec_name)
        if dec_factory is None:
            raise ValueError(
                f"Unknown decorator '{dec_name}' in command '{cmd_def.name}'. "
                f"Register it with register_decorator() first."
            )
        command_fn = dec_factory()(command_fn)

    factory.command(
        name=cmd_def.name,
        requires_connection=cmd_def.requires_connection,
        require_warehouse=cmd_def.require_warehouse,
        preview=cmd_def.is_preview,
        hidden=cmd_def.is_hidden,
    )(command_fn)


def _make_typer_default(p: ParamDef):
    """Create the appropriate ``typer.Argument`` or ``typer.Option`` default."""
    is_required = p.default is REQUIRED

    # Extra kwargs for custom Click type parsing (e.g. IdentifierType for FQN)
    extra: dict = {}
    if p.click_type is not None:
        extra["click_type"] = p.click_type

    if p.kind == ParamKind.ARGUMENT:
        return typer.Argument(
            ... if is_required else p.default,
            help=p.help,
            show_default=p.show_default,
            hidden=p.hidden,
            **extra,
        )

    # OPTION
    cli_names = p.cli_names or (f"--{p.name.replace('_', '-')}",)

    if p.is_flag:
        return typer.Option(
            p.default if not is_required else False,
            *cli_names,
            help=p.help,
            is_flag=True,
            show_default=p.show_default,
            hidden=p.hidden,
            **extra,
        )

    return typer.Option(
        ... if is_required else p.default,
        *cli_names,
        help=p.help,
        show_default=p.show_default,
        hidden=p.hidden,
        **extra,
    )
