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
    related: tuple[RelatedLink, ...] = ()
    usage_notes: str | None = None
    examples: tuple[Example, ...] = ()

    def __post_init__(self) -> None:
        if self.usage_notes is not None:
            object.__setattr__(self, "usage_notes", cleandoc(self.usage_notes))
