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

from typing import Iterator, Sequence

import click
import typer.rich_utils as rich_utils
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


class SnowTyperCommand(TyperCommand):
    """Renders structured command documentation after the standard help."""

    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        super().format_help(ctx, formatter)

        if (
            self.rich_markup_mode is None
            or not FeatureFlag.ENABLE_COMMAND_DOCS_IN_HELP.is_enabled()
        ):
            return

        console = _get_rich_console()
        for panel in _docs_panels(get_command_docs(self)):
            console.print(panel)


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
        yield Text(sanitize_for_terminal(example.command), style=STYLE_EXAMPLE_COMMAND)
        if example.output:
            yield Text(
                f"Output: {sanitize_for_terminal(example.output)}",
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
