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
from snowflake.cli.api.commands.command_docs import (
    REFERENCE_TEXT,
    Code,
    ContentBlock,
    Paragraph,
    PlainText,
    Ref,
    Span,
)
from snowflake.cli.api.sanitizers import sanitize_for_terminal

STYLE_INLINE_CODE = "markdown.code"


def mdx_escape(value: Any) -> str:
    """Escapes angle brackets so MDX does not parse prose as JSX tags."""
    if value is None:
        return ""
    return str(value).replace("<", "&lt;").replace(">", "&gt;")


def _render_span_mdx(span: Span) -> str:
    if isinstance(span, Code):
        # No mdx_escape: ``-D "<key>=<value>"`` stays a code span, not ``&lt;...&gt;``.
        return f"`{span.value}`"
    if isinstance(span, Ref):
        return f"%{span.name}%"
    return mdx_escape(span)


def render_paragraph_mdx(paragraph: Paragraph) -> str:
    return "".join(_render_span_mdx(span) for span in paragraph.parts)


def render_usage_mdx(blocks: Sequence[ContentBlock]) -> str:
    """Renders structured usage-note blocks as an MDX-ready body."""
    return "\n\n".join(_render_usage_block_mdx(block) for block in blocks)


def _render_usage_block_mdx(block: ContentBlock) -> str:
    if isinstance(block, PlainText):
        return render_paragraph_mdx(block)
    raise TypeError(f"Unsupported usage-note block: {type(block).__name__}")


def _render_spans_help(spans: Sequence[Span]) -> RichText:
    """Renders inline spans as Rich text for the terminal."""
    rendered = RichText()
    for part in spans:
        if isinstance(part, Code):
            rendered.append(
                sanitize_for_terminal(part.value) or "", style=STYLE_INLINE_CODE
            )
        elif isinstance(part, Ref):
            rendered.append(REFERENCE_TEXT.get(part.name, ""))
        else:
            rendered.append(sanitize_for_terminal(part) or "")
    return rendered


def render_paragraph_help(paragraph: Paragraph) -> RichText:
    return _render_spans_help(paragraph.parts)


def render_usage_help(blocks: Sequence[ContentBlock]) -> RenderableType:
    """Renders structured usage-note blocks as a Rich panel body."""
    renderables: list[RenderableType] = []
    for block in blocks:
        if not isinstance(block, PlainText):
            raise TypeError(f"Unsupported usage-note block: {type(block).__name__}")
        if renderables:
            renderables.append(RichText(""))
        renderables.append(render_paragraph_help(block))
    return Group(*renderables)
