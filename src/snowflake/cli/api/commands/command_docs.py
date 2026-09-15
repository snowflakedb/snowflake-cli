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
from inspect import cleandoc

from click import Command

DOCS_ATTRIBUTE = "__snowflake_cli_command_docs__"


@dataclass(frozen=True)
class Example:
    command: str
    description: str | None = None
    output: str | None = None


@dataclass(frozen=True)
class RelatedLink:
    href: str
    title: str = ""


@dataclass(frozen=True)
class CommandDocs:
    """
    Command documentation declared through ``@app.command(docs=...)``.

    All text fields are plain prose, not MDX. Docs pages escape them, so
    markup written here is shown literally instead of being rendered.
    """

    related: tuple[RelatedLink, ...] = ()
    usage_notes: str | None = None
    examples: tuple[Example, ...] | None = None

    def __post_init__(self) -> None:
        if self.usage_notes is not None:
            object.__setattr__(self, "usage_notes", cleandoc(self.usage_notes))


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
