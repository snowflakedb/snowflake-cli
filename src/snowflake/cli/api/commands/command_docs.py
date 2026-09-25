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

from dataclasses import dataclass

from click import Command

DOCS_ATTRIBUTE = "__snowflake_cli_command_docs__"

# Subset of ``sphinx/source/sfvariables.txt`` in snowflake-prod-docs. A new
# ``Ref("name")`` needs a matching entry here for ``--help`` to expand it.
REFERENCE_TEXT: dict[str, str] = {
    "dcm": "DCM Projects",
    "dcm-object": "DCM project",
    "sf-cli": "Snowflake CLI",
}


@dataclass(frozen=True)
class Code:
    """Inline code literal (MDX wraps ``value`` in backticks; do not add your own).

    Angle brackets in ``value`` stay raw so placeholders like ``<key>`` survive.
    Downstream call site: ``code("--target")``.
    """

    value: str

    def __post_init__(self) -> None:
        if "`" in self.value:
            raise ValueError(
                f"code value {self.value!r} must not contain backticks; "
                "MDX wraps the literal in backticks for you"
            )


@dataclass(frozen=True)
class Ref:
    """Prod-docs substitution key (``sfvariables.txt`` ``|name|`` → MDX ``%name%``).

    Not free text. Downstream call site: ``ref("dcm-object")``.
    """

    name: str

    def __post_init__(self) -> None:
        if self.name not in REFERENCE_TEXT:
            raise ValueError(
                f"unknown prod-docs reference {self.name!r}; "
                "add a matching entry to REFERENCE_TEXT in command_docs.py"
            )


@dataclass(frozen=True)
class RelatedLink:
    href: str
    title: str = ""


Span = str | Code | Ref | RelatedLink


@dataclass(frozen=True)
class Paragraph:
    """Inline ``parts`` concatenated with no separator and no ``cleandoc``.

    Wrapping newlines inside a string are kept in both MDX and ``--help``.
    A lone bullet is not a content block.
    """

    parts: tuple[Span, ...]


@dataclass(frozen=True)
class PlainText(Paragraph):
    """A paragraph of inline spans. Used in usage notes and example descriptions."""

    pass


@dataclass(frozen=True)
class Note(Paragraph):
    """A ``ContentBlock`` note: a paragraph of spans (``Code`` / ``Ref``).

    Downstream call site: ``note("The command prompts...", code("--force"), ...)``.
    """

    pass


@dataclass(frozen=True)
class Bullet(Paragraph):
    """One list item: a paragraph of spans (``Code`` / ``Ref``), not a ``ContentBlock``.

    Pass to ``bullet_list``; do not put a lone ``Bullet`` in ``usage_notes``.
    Downstream call site: ``bullet("Exit code ", code("0"), " if all tests pass")``.
    """

    pass


@dataclass(frozen=True)
class BulletList:
    """A ``ContentBlock`` of bullet items for usage notes."""

    items: tuple[Bullet, ...]


ContentBlock = PlainText | BulletList | Note


def plain_text(*parts: Span) -> PlainText:
    return PlainText(parts=parts)


def note(*parts: Span) -> Note:
    return Note(parts=parts)


def bullet(*parts: Span) -> Bullet:
    return Bullet(parts=parts)


def bullet_list(*items: Bullet) -> BulletList:
    return BulletList(items=items)


def code(value: str) -> Code:
    return Code(value=value)


def ref(name: str) -> Ref:
    return Ref(name=name)


def link(href: str, title: str = "") -> RelatedLink:
    return RelatedLink(href=href, title=title)


@dataclass(frozen=True)
class Example:
    """A command example for docs pages and ``--help``.

    ``description`` is a ``PlainText`` paragraph of spans (same ``Code`` / ``Ref``
    as usage notes), not a bare string.
    """

    command: str
    description: PlainText | None = None
    output: str | None = None

    def __post_init__(self) -> None:
        if self.description is not None and not isinstance(self.description, PlainText):
            raise TypeError(
                "description must be a PlainText paragraph, "
                f"not {type(self.description).__name__}"
            )


@dataclass(frozen=True)
class Include:
    """Prod-docs MDX component imported from an ``INCLUDE/`` fragment.

    ``help`` content for terminal rendering will be added in a follow-up change.
    """

    tag: str
    path: str


PUBLIC_PREVIEW = Include(
    tag="PublicPreview",
    path="INCLUDE/text/sidebars/basic/public-preview.mdx",
)
PUBLIC_PREVIEW_NO_GOV = Include(
    tag="PublicPreviewNoGov",
    path="INCLUDE/text/sidebars/basic/public-preview-no-gov.mdx",
)


def unique_includes(
    includes: tuple[Include, ...],
) -> tuple[Include, ...]:
    """Returns ``includes`` with duplicate ``path`` values removed."""
    seen: set[str] = set()
    unique: list[Include] = []
    for include in includes:
        if include.path in seen:
            continue
        seen.add(include.path)
        unique.append(include)
    return tuple(unique)


@dataclass(frozen=True)
class CommandDocs:
    """
    Command documentation declared through ``@app.command(docs=...)``.

    Documentation content is structured so it can be rendered for both MDX
    pages and terminal help.
    """

    related: tuple[RelatedLink, ...] = ()
    banners: tuple[Include, ...] = ()
    usage_notes: tuple[ContentBlock, ...] | None = None
    examples: tuple[Example, ...] | None = None

    def __post_init__(self) -> None:
        self._validate_includes(self.banners, "banners")
        if self.usage_notes is None:
            return
        self._validate_content_blocks(self.usage_notes, "usage_notes")

    def _validate_includes(
        self, includes: tuple[Include, ...], field_name: str
    ) -> None:
        if not isinstance(includes, tuple):
            raise TypeError(
                f"{field_name} must be a tuple of includes, "
                f"not {type(includes).__name__}"
            )
        for include in includes:
            if not isinstance(include, Include):
                raise TypeError(
                    f"{field_name} entries must be Include values, "
                    f"not {type(include).__name__}"
                )

    def _validate_content_blocks(
        self, blocks: tuple[ContentBlock, ...], field_name: str
    ) -> None:
        if not isinstance(blocks, tuple):
            raise TypeError(
                f"{field_name} must be a tuple of content blocks, "
                f"not {type(blocks).__name__}"
            )
        for block in blocks:
            if not isinstance(block, ContentBlock):
                raise TypeError(
                    f"{field_name} entries must be content blocks, "
                    f"not {type(block).__name__}"
                )


def get_command_docs(command: Command) -> CommandDocs:
    """
    Reads the metadata declared through ``@app.command(docs=...)``.

    The metadata is set on the user function. Typer copies ``__dict__`` onto
    ``command.callback``; later wrappers are followed through ``__wrapped__``.

    Commands without ``docs=`` return an empty ``CommandDocs``. Callers that
    need sections (pages, ``--help``) should look at the fields, not at
    whether the object was declared.
    """
    candidate = getattr(command, "callback", None)
    while candidate is not None:
        docs = getattr(candidate, DOCS_ATTRIBUTE, None)
        if isinstance(docs, CommandDocs):
            return docs
        candidate = getattr(candidate, "__wrapped__", None)
    return CommandDocs()
