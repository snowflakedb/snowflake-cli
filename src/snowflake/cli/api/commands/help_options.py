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

import sys
from contextlib import contextmanager
from typing import TYPE_CHECKING

import click
from snowflake.cli.api.commands.flags import (
    CONNECTION_CONFIGURATION_PANEL,
    DEFAULT_CONTEXT_SETTINGS,
    GLOBAL_CONFIGURATION_PANEL,
)
from snowflake.cli.api.exceptions import CliArgumentError

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

CONDENSED_HELP_FOOTER = (
    "Run `snow --help` for full descriptions, " "or use `--help-all` on this command."
)

_STANDARD_HELP_FLAGS: tuple[str, ...] = tuple(
    DEFAULT_CONTEXT_SETTINGS["help_option_names"]
)
_HELP_ALL_FLAG = "--help-all"


def invocation_args(ctx: click.Context | None) -> list[str]:
    if ctx is not None and ctx.obj and "invocation_args" in ctx.obj:
        return list(ctx.obj["invocation_args"])
    return list(sys.argv)


def conflicting_help_flags(
    ctx: click.Context | None = None,
    argv: list[str] | None = None,
) -> bool:
    args = argv if argv is not None else invocation_args(ctx)
    return any(flag in args for flag in _STANDARD_HELP_FLAGS) and _HELP_ALL_FLAG in args


def raise_if_conflicting_help_flags(ctx: click.Context | None = None) -> None:
    if conflicting_help_flags(ctx):
        raise CliArgumentError(
            "Cannot use --help with --help-all. Use one or the other."
        )


def condensed_shared_options_panel_title(panels: set[str | None]) -> str:
    has_connection = CONNECTION_CONFIGURATION_PANEL in panels
    has_global = GLOBAL_CONFIGURATION_PANEL in panels
    if has_connection and has_global:
        return "Global and connection options"
    if has_connection:
        return "Connection options"
    if has_global:
        return "Global options"
    return ""


def condensed_option_flag_names(params: Iterable[click.Option]) -> str:
    names: list[str] = []
    for param in params:
        long_opts = [opt for opt in param.opts if opt.startswith("--")]
        if long_opts:
            names.append(long_opts[0])
    return ", ".join(names)


_REFERENCE_COMMAND: click.Command | None = None


def is_injected_help_param(param: click.Parameter) -> bool:
    panel = getattr(param, "rich_help_panel", None)
    return panel in (CONNECTION_CONFIGURATION_PANEL, GLOBAL_CONFIGURATION_PANEL)


def show_all_help(ctx: click.Context | None = None) -> bool:
    return bool(ctx is not None and ctx.obj and ctx.obj.get("show_all_help"))


def condensed_usage_suffix(command: click.Command, ctx: click.Context) -> str:
    panels = {
        getattr(param, "rich_help_panel", None) for param in command.get_params(ctx)
    }
    parts: list[str] = []
    if GLOBAL_CONFIGURATION_PANEL in panels:
        parts.append("[global options]")
    if CONNECTION_CONFIGURATION_PANEL in panels:
        parts.append("[connection options]")
    return " ".join(parts)


def has_injected_help_params(command: click.Command, ctx: click.Context) -> bool:
    return any(is_injected_help_param(param) for param in command.get_params(ctx))


def hide_injected_help_params(
    command: click.Command, ctx: click.Context
) -> dict[click.Option, bool]:
    restore: dict[click.Option, bool] = {}
    for param in command.get_params(ctx):
        if not isinstance(param, click.Option):
            continue
        if is_injected_help_param(param) and not param.hidden:
            restore[param] = param.hidden
            param.hidden = True
    return restore


def restore_help_param_visibility(restore: dict[click.Option, bool]) -> None:
    for param, hidden in restore.items():
        param.hidden = hidden


@contextmanager
def hidden_injected_help_params(
    command: click.Command, ctx: click.Context
) -> Generator[None, None, None]:
    restore = hide_injected_help_params(command, ctx)
    try:
        yield
    finally:
        restore_help_param_visibility(restore)


def injected_help_params_reference() -> click.Command:
    global _REFERENCE_COMMAND
    if _REFERENCE_COMMAND is not None:
        return _REFERENCE_COMMAND

    from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
    from typer.main import get_command

    app = SnowTyperFactory(name="_snow_help_ref")

    @app.command("_ref", requires_connection=True)
    def _ref(**options):
        pass

    group = get_command(app.create_instance())
    if isinstance(group, click.Group):
        _REFERENCE_COMMAND = group.commands["_ref"]
    else:
        _REFERENCE_COMMAND = group
    return _REFERENCE_COMMAND


def iter_shared_configuration_params(
    panel: str,
) -> Iterable[click.Option]:
    command = injected_help_params_reference()
    ctx = click.Context(command)
    for param in command.get_params(ctx):
        if not isinstance(param, click.Option):
            continue
        if getattr(param, "rich_help_panel", None) == panel and not param.hidden:
            yield param
