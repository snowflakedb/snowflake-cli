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
from rich.table import Table
from rich.text import Text as RichText
from snowflake.cli.api.commands.command_docs import (
    REFERENCE_TEXT,
    Admonition,
    AdmonitionType,
    BulletList,
    Code,
    ContentBlock,
    Include,
    Paragraph,
    PlainText,
    Ref,
    RelatedLink,
    Span,
)
from snowflake.cli.api.sanitizers import sanitize_for_terminal

STYLE_INLINE_CODE = "markdown.code"
ADMONITION_HELP_LABELS: dict[AdmonitionType, str] = {
    AdmonitionType.NOTE: "Note",
    AdmonitionType.WARNING: "Warning",
    AdmonitionType.TIP: "Tip",
    AdmonitionType.IMPORTANT: "Important",
    AdmonitionType.CAUTION: "Caution",
    AdmonitionType.ATTENTION: "Attention",
    AdmonitionType.DANGER: "Danger",
    AdmonitionType.HINT: "Hint",
    AdmonitionType.ERROR: "Error",
    AdmonitionType.SFEDITION: "Standard Edition Feature",
    AdmonitionType.PREVIEW: "Preview Feature",
    AdmonitionType.NEW: "New Feature",
}
assert set(ADMONITION_HELP_LABELS) == set(AdmonitionType)


def mdx_escape(value: Any) -> str:
    """Escape MDX/JSX-sensitive characters in prose (option help, fallbacks, etc.)."""
    if value is None:
        return ""
    text = str(value)
    text = text.replace("<", "&lt;").replace(">", "&gt;")
    # MDX treats `{expr}` as inline JSX; prod-docs escape literal braces in prose.
    text = text.replace("{", r"\{").replace("}", r"\}")
    return text


def _render_span_mdx(span: Span) -> str:
    if isinstance(span, Code):
        # No mdx_escape: ``-D "<key>=<value>"`` stays a code span, not ``&lt;...&gt;``.
        return f"`{span.value}`"
    if isinstance(span, Ref):
        return f"%{span.name}%"
    if isinstance(span, RelatedLink):
        return f"[{mdx_escape(span.title)}]({span.href})"
    return mdx_escape(span)


def render_paragraph_mdx(paragraph: Paragraph) -> str:
    return "".join(_render_span_mdx(span) for span in paragraph.parts)


def _render_bullet_mdx(item: Paragraph) -> str:
    return f"- {render_paragraph_mdx(item).replace(chr(10), chr(10) + '  ')}"


def _render_admonition_opening_tag(block: Admonition) -> str:
    attrs = [f'type="{block.admonition_type.value}"']
    if block.title is not None:
        attrs.append(f'title="{mdx_escape(block.title)}"')
    if block.title_suffix is not None:
        attrs.append(f'titleSuffix="{mdx_escape(block.title_suffix)}"')
    if block.title_href is not None:
        attrs.append(f'titleHref="{mdx_escape(block.title_href)}"')
    return f"<Admonition {' '.join(attrs)}>"


def _render_admonition_mdx(block: Admonition) -> str:
    return (
        f"{_render_admonition_opening_tag(block)}\n\n"
        f"{render_paragraph_mdx(block)}\n\n"
        "</Admonition>"
    )


def _render_include_mdx(block: Include) -> str:
    return f"<{block.tag} />"


def render_usage_mdx(blocks: Sequence[ContentBlock]) -> str:
    """Renders structured usage-note blocks as an MDX-ready body."""
    return "\n\n".join(_render_usage_block_mdx(block) for block in blocks)


def _render_usage_block_mdx(block: ContentBlock) -> str:
    if isinstance(block, PlainText):
        return render_paragraph_mdx(block)
    if isinstance(block, BulletList):
        return "\n".join(_render_bullet_mdx(item) for item in block.items)
    if isinstance(block, Admonition):
        return _render_admonition_mdx(block)
    if isinstance(block, Include):
        return _render_include_mdx(block)
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
            rendered.append(REFERENCE_TEXT.get(part.name, f"%{part.name}%"))
        elif isinstance(part, RelatedLink):
            rendered.append(
                sanitize_for_terminal(part.title)
                or sanitize_for_terminal(part.href)
                or ""
            )
        else:
            rendered.append(sanitize_for_terminal(part) or "")
    return rendered


def render_paragraph_help(paragraph: Paragraph) -> RichText:
    return _render_spans_help(paragraph.parts)


def _render_bullet_list_help(block: BulletList) -> Table:
    table = Table.grid(padding=(0, 1, 0, 0))
    table.add_column(width=1, no_wrap=True)
    table.add_column(no_wrap=False)
    for item in block.items:
        table.add_row("•", render_paragraph_help(item))
    return table


def _render_admonition_help(block: Admonition) -> RichText:
    label = ADMONITION_HELP_LABELS[block.admonition_type]
    rendered = RichText()
    rendered.append(f"{label}: ", style="bold")
    rendered.append_text(render_paragraph_help(block))
    return rendered


def render_usage_help(blocks: Sequence[ContentBlock]) -> RenderableType:
    """Renders structured usage-note blocks as a Rich panel body."""
    renderables: list[RenderableType] = []
    for block in blocks:
        if isinstance(block, PlainText):
            content: RenderableType = render_paragraph_help(block)
        elif isinstance(block, BulletList):
            content = _render_bullet_list_help(block)
        elif isinstance(block, Admonition):
            content = _render_admonition_help(block)
        elif isinstance(block, Include):
            if block.help_content is None:
                raise TypeError("Include blocks in usage notes must set help_content")
            content = render_usage_help(block.help_content)
        else:
            raise TypeError(f"Unsupported usage-note block: {type(block).__name__}")
        if renderables:
            renderables.append(RichText(""))
        renderables.append(content)
    return Group(*renderables)
