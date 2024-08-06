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

from typing import Tuple

import pytest
from snowflake.cli._plugins.object.common import (
    Tag,
    TagError,
    _parse_tag,
)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("tag=value", ("tag", "value")),
        ("_underscore_start=value", ("_underscore_start", "value")),
        ("a=xyz", ("a", "xyz")),
        ("A=123", ("A", "123")),
        ("mixedCase=value", ("mixedCase", "value")),
        ("_=value", ("_", "value")),
        ("tag='this is a value'", ("tag", "'this is a value'")),
        (
            '"tag name!@#"=value',
            ('"tag name!@#"', "value"),
        ),  # quoted identifier allows for spaces and special characters
        (
            "tag==value",
            ("tag", "=value"),
        ),  # This is a strange case which we may not actually want to support
    ],
)
def test_parse_tag_valid(value: str, expected: Tuple[str, str]):
    assert _parse_tag(value) == Tag(*expected)


@pytest.mark.parametrize(
    "value",
    [
        "123_name=value",  # starts with a digit
        "tag name=value",  # space in identifier
        "tag&_name=value",  # special characters in identifier
        "tag",  # no equals sign
        "=value",  # empty identifier
        "a" * 257 + "=value",  # identifier is over 256 characters
        '"tag"name"=value',  # undoubled quote in tag name
    ],
)
def test_parse_tag_invalid(value: str):
    with pytest.raises(TagError):
        _parse_tag(value)
