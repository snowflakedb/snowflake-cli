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

from collections.abc import Sequence
from typing import Any

from rich.console import Group, RenderableType
from rich.text import Text as RichText
from snowflake.cli.api.commands.command_docs import ContentBlock, Paragraph, PlainText
from snowflake.cli.api.sanitizers import sanitize_for_terminal


def mdx_escape(value: Any) -> str:
    """Escapes angle brackets so MDX does not parse prose as JSX tags."""
    if value is None:
        return ""
    return str(value).replace("<", "&lt;").replace(">", "&gt;")


def _render_paragraph_mdx(paragraph: Paragraph) -> str:
    return "".join(mdx_escape(part) for part in paragraph.parts)


def render_usage_mdx(blocks: Sequence[ContentBlock]) -> str:
    """Renders structured usage-note blocks as an MDX-ready body."""
    return "\n\n".join(_render_usage_block_mdx(block) for block in blocks)


def _render_usage_block_mdx(block: ContentBlock) -> str:
    if isinstance(block, PlainText):
        return _render_paragraph_mdx(block)
    raise TypeError(f"Unsupported usage-note block: {type(block).__name__}")


def _render_paragraph_help(paragraph: Paragraph) -> RichText:
    value = "".join(str(part) for part in paragraph.parts)
    return RichText(sanitize_for_terminal(value) or "")


def render_usage_help(blocks: Sequence[ContentBlock]) -> RenderableType:
    """Renders structured usage-note blocks as a Rich panel body."""
    renderables: list[RenderableType] = []
    for block in blocks:
        if not isinstance(block, PlainText):
            raise TypeError(f"Unsupported usage-note block: {type(block).__name__}")
        if renderables:
            renderables.append(RichText(""))
        renderables.append(_render_paragraph_help(block))
    return Group(*renderables)
