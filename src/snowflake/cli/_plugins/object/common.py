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
from typing import Optional

from click import ClickException
from snowflake.cli.api.commands.flags import OverrideableOption
from snowflake.cli.api.project.util import (
    VALID_IDENTIFIER_REGEX,
    is_valid_identifier,
    to_string_literal,
)


@dataclass
class Tag:
    name: str
    value: str

    def __post_init__(self):
        if not is_valid_identifier(self.name):
            raise ValueError("name of a tag must be a valid Snowflake identifier")

    def value_string_literal(self):
        return to_string_literal(self.value)


class TagError(ClickException):
    def __init__(self):
        super().__init__(
            "tag must be in the format <name>=<value> where 'name' is a valid identifier and value is a string"
        )


def _parse_tag(tag: str) -> Tag:
    import re

    identifier_pattern = rf"(?P<tag_name>{VALID_IDENTIFIER_REGEX})"
    value_pattern = r"(?P<tag_value>.+)"
    result = re.fullmatch(rf"{identifier_pattern}={value_pattern}", tag)
    if result is not None:
        try:
            return Tag(result.group("tag_name"), result.group("tag_value"))
        except ValueError:
            raise TagError()
    else:
        raise TagError()


"""
Provides a common interface for all commands that accept a tag option (e.g. when altering the tag of an object).
Parses the input string in the format "name=value" into a Tag object with 'name' and 'value' properties.
"""
TagOption = OverrideableOption(
    None, "--tag", help=f"Tag for the object.", parser=_parse_tag, metavar="NAME=VALUE"
)


def _comment_callback(comment: Optional[str]):
    if comment is None:
        return comment
    return to_string_literal(comment)


"""
Provides a common interface for all commands that accept a comment option (e.g. when creating a new object).
Parses the input string into a string literal.
"""
CommentOption = OverrideableOption(
    None, "--comment", help="Comment for the object.", callback=_comment_callback
)
