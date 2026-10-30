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

import pytest
from snowflake.cli._plugins.sql.completion.identifiers import format_catalog_name


@pytest.mark.parametrize(
    "name, expected",
    [
        ("MY_TABLE", "MY_TABLE"),
        ("MiXeD", '"MiXeD"'),
        ("lower", '"lower"'),
        ("SELECT", '"SELECT"'),
        ("has space", '"has space"'),
        ('a"b', '"a""b"'),
        ("foo/bar", '"foo/bar"'),
        ("foo-bar", '"foo-bar"'),
        ("123abc", '"123abc"'),
    ],
)
def test_format_catalog_name(name, expected):
    assert format_catalog_name(name) == expected


def test_format_catalog_name_never_uses_backticks_or_identifier_fn():
    inserted = format_catalog_name("has space")
    assert "`" not in inserted
    assert "IDENTIFIER(" not in inserted
