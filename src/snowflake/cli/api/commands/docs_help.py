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

from typing import Iterator, Sequence, cast

import click
import typer
import typer.rich_utils as rich_utils
from click.exceptions import NoSuchOption
from rich.console import Console, ConsoleOptions, Group, RenderableType, RenderResult
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    Example,
    RelatedLink,
    get_command_docs,
)
from snowflake.cli.api.commands.command_docs_rendering import (
    render_paragraph_help,
    render_usage_help,
)
from snowflake.cli.api.commands.flags import (
    CONNECTION_CONFIGURATION_PANEL,
    GLOBAL_CONFIGURATION_PANEL,
)
from snowflake.cli.api.commands.help_options import (
    CONDENSED_HELP_FOOTER,
    condensed_option_flag_names,
    condensed_shared_options_panel_title,
    condensed_usage_suffix,
    has_injected_help_params,
    hide_injected_help_params,
    is_injected_help_param,
    iter_shared_configuration_params,
    raise_if_conflicting_help_flags,
    restore_help_param_visibility,
    show_all_help,
)
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from typer.core import TyperCommand
from typer.rich_utils import (
    ALIGN_OPTIONS_PANEL,
    STYLE_OPTIONS_PANEL_BORDER,
    _get_rich_console,
)

DOCS_BASE_URL = "https://docs.snowflake.com/en"

STYLE_EXAMPLE_COMMAND = "cyan"
STYLE_EXAMPLE_OUTPUT = "dim"


def handle_help_all_option(
    ctx: click.Context,
    param: click.Parameter,
    value: bool,
) -> None:
    if not value or ctx.resilient_parsing:
        return
    if not FeatureFlag.ENABLE_CONDENSED_COMMAND_HELP.is_enabled():
        raise NoSuchOption("--help-all", ctx=ctx)
    raise_if_conflicting_help_flags(ctx)
    ctx.ensure_object(dict)
    ctx.obj["show_all_help"] = True
    typer.echo(ctx.get_help())
    ctx.exit()


class CondensedHelp:
    """Condense per-command help by hiding injected global/connection option panels."""

    _condensed_help_active: bool = False

    def get_usage(self, ctx: click.Context) -> str:
        usage = super().get_usage(ctx)  # type: ignore[misc]
        if getattr(self, "_condensed_help_active", False):
            suffix = condensed_usage_suffix(self, ctx)
            if suffix:
                return f"{usage.rstrip()} {suffix}"
        return usage

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        ctx.ensure_object(dict)
        ctx.obj["invocation_args"] = list(args)
        return super().parse_args(ctx, args)  # type: ignore[misc]

    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        raise_if_conflicting_help_flags(ctx)
        if not FeatureFlag.ENABLE_CONDENSED_COMMAND_HELP.is_enabled():
            super().format_help(ctx, formatter)  # type: ignore[misc]
            self._print_command_docs_panels()
            return

        if show_all_help(ctx) or not has_injected_help_params(self, ctx):
            super().format_help(ctx, formatter)  # type: ignore[misc]
            self._print_command_docs_panels()
            return

        self._condensed_help_active = True
        restore = hide_injected_help_params(self, ctx)
        try:
            super().format_help(ctx, formatter)  # type: ignore[misc]
        finally:
            restore_help_param_visibility(restore)
            self._condensed_help_active = False

        render_condensed_shared_options_panel(cast(click.Command, self), ctx)
        self._print_command_docs_panels()

    def _print_command_docs_panels(self) -> None:
        if (
            getattr(self, "rich_markup_mode", None) is None
            or not FeatureFlag.ENABLE_COMMAND_DOCS_IN_HELP.is_enabled()
        ):
            return

        console = _get_rich_console()
        for panel in _docs_panels(get_command_docs(self)):
            console.print(panel)


class SnowTyperCommand(CondensedHelp, TyperCommand):
    """Renders structured command documentation after the standard help."""


def render_condensed_shared_options_panel(
    command: click.Command,
    ctx: click.Context,
) -> None:
    """Print a concise list of shared options after condensed per-command help."""
    panels = {
        getattr(param, "rich_help_panel", None)
        for param in command.get_params(ctx)
        if is_injected_help_param(param)
    }
    if not panels:
        return

    sections: list[RenderableType] = []
    if CONNECTION_CONFIGURATION_PANEL in panels:
        connection_names = condensed_option_flag_names(
            iter_shared_configuration_params(CONNECTION_CONFIGURATION_PANEL)
        )
        if connection_names:
            sections.append(Text(f"Connection options: {connection_names}"))
    if GLOBAL_CONFIGURATION_PANEL in panels:
        global_names = condensed_option_flag_names(
            iter_shared_configuration_params(GLOBAL_CONFIGURATION_PANEL)
        )
        if global_names:
            sections.append(Text(f"Global options: {global_names}"))

    if not sections:
        return

    sections.extend((Text(""), Text(CONDENSED_HELP_FOOTER)))
    console = _get_rich_console()
    console.print(
        _panel(condensed_shared_options_panel_title(panels), Group(*sections))
    )


def render_shared_configuration_panels(
    ctx: click.Context,
    *,
    markup_mode: str = "markdown",
) -> None:
    """Print global and connection option panels for root ``snow --help``."""
    if not FeatureFlag.ENABLE_CONDENSED_COMMAND_HELP.is_enabled():
        return

    console = _get_rich_console()
    reference = iter_shared_configuration_params(CONNECTION_CONFIGURATION_PANEL)
    connection_params = list(reference)
    global_params = list(iter_shared_configuration_params(GLOBAL_CONFIGURATION_PANEL))

    if connection_params:
        rich_utils._print_options_panel(  # noqa: SLF001
            name=CONNECTION_CONFIGURATION_PANEL,
            params=connection_params,
            ctx=ctx,
            markup_mode=markup_mode,
            console=console,
        )
    if global_params:
        rich_utils._print_options_panel(  # noqa: SLF001
            name=GLOBAL_CONFIGURATION_PANEL,
            params=global_params,
            ctx=ctx,
            markup_mode=markup_mode,
            console=console,
        )


def _docs_panels(docs: CommandDocs) -> Iterator[Panel]:
    if docs.usage_notes:
        yield _panel("Usage notes", render_usage_help(docs.usage_notes))
    if docs.examples:
        yield _panel("Examples", Group(*_example_lines(docs.examples)))
    if docs.related:
        yield _panel("Related topics", _related_list(docs.related))


def _panel(title: str, renderable: RenderableType) -> Panel:
    return rich_utils.Panel(
        renderable,
        border_style=STYLE_OPTIONS_PANEL_BORDER,
        title=title,
        title_align=ALIGN_OPTIONS_PANEL,
    )


def _example_lines(examples: Sequence[Example]) -> Iterator[RenderableType]:
    for position, example in enumerate(examples):
        if position:
            yield Text("")
        if example.description:
            yield render_paragraph_help(example.description)
        if example.command:
            yield Text(
                sanitize_for_terminal(example.command), style=STYLE_EXAMPLE_COMMAND
            )
        if example.output:
            output = sanitize_for_terminal(example.output)
            if output and "\n" in output:
                yield Text("Output:", style=STYLE_EXAMPLE_OUTPUT)
                for line in output.splitlines():
                    yield Text(line, style=STYLE_EXAMPLE_OUTPUT, no_wrap=True)
            else:
                yield Text(
                    f"Output: {output}",
                    style=STYLE_EXAMPLE_OUTPUT,
                )


class _RelatedTopic:
    """A related link: the title, then its URL broken after ``/``."""

    def __init__(self, link: RelatedLink) -> None:
        self._link = link

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:
        title = sanitize_for_terminal(self._link.title) or ""
        url = _absolute_url(sanitize_for_terminal(self._link.href) or "")
        if title:
            yield Text(title)
        for line in _fold_at_slash(url, options.max_width):
            yield Text(line)


def _related_list(related: Sequence[RelatedLink]) -> Table:
    table = Table.grid(padding=(0, 1, 0, 0))
    table.add_column(width=1, no_wrap=True)
    table.add_column(no_wrap=False)
    for link in related:
        table.add_row("•", _RelatedTopic(link))
    return table


def _fold_at_slash(url: str, width: int) -> list[str]:
    """
    Splits a URL into lines of at most ``width`` characters.

    Breaks are taken after ``/`` so path segments stay readable and the URL
    can be reassembled by concatenating the lines. A segment longer than
    ``width`` is the one case that still has to be cut mid-token.
    """
    if width <= 0:
        return [url]

    segments = url.split("/")
    chunks = [f"{segment}/" for segment in segments[:-1]] + segments[-1:]

    lines: list[str] = []
    current = ""
    for chunk in chunks:
        if current and len(current) + len(chunk) > width:
            lines.append(current)
            current = ""
        current += chunk
        while len(current) > width:
            lines.append(current[:width])
            current = current[width:]
    if current:
        lines.append(current)
    return lines or [url]


def _absolute_url(href: str) -> str:
    """Converts site-relative docs links into useful terminal URLs."""
    if href.startswith(("http://", "https://")):
        return href
    return f"{DOCS_BASE_URL}/{href.lstrip('/')}"
