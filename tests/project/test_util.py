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

from itertools import permutations

import pytest
from snowflake.cli.api.project.util import (
    append_to_identifier,
    escape_like_pattern,
    is_valid_identifier,
    is_valid_object_name,
    is_valid_quoted_identifier,
    is_valid_string_literal,
    is_valid_unquoted_identifier,
    to_identifier,
    to_string_literal,
)

VALID_UNQUOTED_IDENTIFIERS = (
    "_",
    "____",
    "A",
    "a",
    "_aA1",
    "a$1",
    "a" * 255,
)

INVALID_UNQUOTED_IDENTIFIERS = (
    "1a",  # leading digit
    "$a",  # leading dollar sign
    "a#",  # invalid character
    "(a)",  # invalid character
    "",  # empty
    "a" * 256,  # too long
)

VALID_QUOTED_IDENTIFIERS = (
    '""',  # an empty quoted identifier is valid
    '"_"',
    '"a"',
    '"A"',
    '"1"',
    '"a$1"',
    '"a""1"""',
    "\"a'1'\"",
    '""""',
    '"(abc def)"',
)

INVALID_QUOTED_IDENTIFIERS = (
    '"abc',  # unterminated quote
    'abc"',  # missing leading quote
    '"abc"def"',  # improperly escaped inner quote
)


def test_is_valid_unquoted_identifier():
    for id_ in VALID_UNQUOTED_IDENTIFIERS:
        assert is_valid_unquoted_identifier(id_)

    for id_ in VALID_QUOTED_IDENTIFIERS:
        assert not is_valid_unquoted_identifier(id_)

    for id_ in INVALID_UNQUOTED_IDENTIFIERS:
        assert not is_valid_unquoted_identifier(id_)

    for id_ in INVALID_QUOTED_IDENTIFIERS:
        assert not is_valid_unquoted_identifier(id_)


def test_is_valid_quoted_identifier():
    for id_ in VALID_UNQUOTED_IDENTIFIERS:
        assert not is_valid_quoted_identifier(id_)

    for id_ in VALID_QUOTED_IDENTIFIERS:
        assert is_valid_quoted_identifier(id_)

    for id_ in INVALID_UNQUOTED_IDENTIFIERS:
        assert not is_valid_quoted_identifier(id_)

    for id_ in INVALID_QUOTED_IDENTIFIERS:
        assert not is_valid_quoted_identifier(id_)


def test_is_valid_identifier():
    for id_ in VALID_UNQUOTED_IDENTIFIERS:
        assert is_valid_identifier(id_)

    for id_ in VALID_QUOTED_IDENTIFIERS:
        assert is_valid_identifier(id_)

    for id_ in INVALID_UNQUOTED_IDENTIFIERS:
        assert not is_valid_identifier(id_)

    for id_ in INVALID_QUOTED_IDENTIFIERS:
        assert not is_valid_identifier(id_)


def test_is_valid_object_name():
    valid_identifiers = VALID_QUOTED_IDENTIFIERS + VALID_UNQUOTED_IDENTIFIERS
    invalid_identifiers = INVALID_QUOTED_IDENTIFIERS + INVALID_UNQUOTED_IDENTIFIERS

    # any combination of 1, 2, or 3 valid identifiers separated by a '.' is valid
    for num in [1, 2, 3]:
        names = [".".join(p) for p in list(permutations(valid_identifiers, num))]
        for name in names:
            assert is_valid_object_name(name)
            if num > 1:
                assert not is_valid_object_name(name, 0)

    # any combination with at least one invalid identifier is invalid
    for num in [1, 2, 3]:
        valid_permutations = list(permutations(valid_identifiers, num - 1))
        for invalid_identifier in invalid_identifiers:
            for valid_perm in valid_permutations:
                combined_set = (invalid_identifier, *valid_perm)
                names = [".".join(p) for p in list(permutations(combined_set, num))]
                for name in names:
                    assert not is_valid_object_name(name)


def test_is_valid_object_name_disallow_quoted():
    valid_identifiers = VALID_QUOTED_IDENTIFIERS + VALID_UNQUOTED_IDENTIFIERS
    for num in [1, 2, 3]:
        name_tuples = list(permutations(valid_identifiers, num))
        for name_tuple in name_tuples:
            has_quotes = any(t in VALID_QUOTED_IDENTIFIERS for t in name_tuple)
            name = ".".join(name_tuple)
            assert is_valid_object_name(name, max_depth=2, allow_quoted=True)
            if has_quotes:
                assert not is_valid_object_name(name, max_depth=2, allow_quoted=False)
            else:
                assert is_valid_object_name(name, max_depth=2, allow_quoted=False)


def test_to_identifier():
    for id_ in VALID_UNQUOTED_IDENTIFIERS:
        assert to_identifier(id_) == id_
    for id_ in VALID_QUOTED_IDENTIFIERS:
        assert to_identifier(id_) == id_

    assert to_identifier("abc def") == '"abc def"'
    assert to_identifier('abc"def') == '"abc""def"'
    assert to_identifier("abc'def") == '"abc\'def"'
    assert to_identifier("(A)") == '"(A)"'


def test_append_to_identifier():
    assert append_to_identifier("abc", "_suffix") == "abc_suffix"
    assert append_to_identifier("_", "_suffix") == "__suffix"
    assert append_to_identifier('"abc"', "_suffix") == '"abc_suffix"'
    assert append_to_identifier('"abc def"', "_suffix") == '"abc def_suffix"'
    assert append_to_identifier('"abc""def"', "_suffix") == '"abc""def_suffix"'
    assert append_to_identifier("abc", " def ghi") == '"abc def ghi"'


@pytest.mark.parametrize(
    "literal,valid",
    [
        ("abc", False),
        ("'abc'", True),
        ("'_aBc_$'", True),
        ('"abc"', False),
        (r"'abc\'def'", True),
        (r"'abc''def'", True),
        ("'a\bbc'", True),  # escape sequences
        ("'a\fbc'", True),
        ("'a\nbc'", False),
        (r"'a\nbc'", True),
        ("'a\rbc'", True),
        ("'a\tbc'", True),
        ("'a\vbc'", True),
        ("'\xf6'", True),  # unicode escape
        (r"'\'abc'", True),
        (r"'a\'c'", True),
        (r"'abc\''", True),
        ("'abc", False),  # leading unterminated single quote
        ("'a'c'", False),  # nested single quote
        ("abc'", False),  # trailing single quote
    ],
)
def test_is_valid_string_literal(literal, valid):
    assert is_valid_string_literal(literal) == valid


@pytest.mark.parametrize(
    "raw_string,literal",
    [
        ("abc", "'abc'"),
        ("'abc'", r"'\'abc\''"),
        ("_aBc_$", "'_aBc_$'"),
        ('"abc"', "'\"abc\"'"),
        ("a\bbc", r"'a\x08bc'"),  # escape sequences
        ("a\fbc", r"'a\x0cbc'"),
        ("a\nbc", r"'a\nbc'"),
        ("a\rbc", r"'a\rbc'"),
        ("a\tbc", r"'a\tbc'"),
        ("a\vbc", r"'a\x0bbc'"),
        ("\xf6", r"'\xf6'"),  # unicode escapes
        ("'abc", r"'\'abc'"),  # leading unterminated single quote
        ("a'c", r"'a\'c'"),  # nested single quote
        ("abc'", r"'abc\''"),  # trailing single quote
        ('a"bc', "'a\"bc'"),  # double quote
    ],
)
def test_to_string_literal(raw_string, literal):
    assert to_string_literal(raw_string) == literal


@pytest.mark.parametrize(
    "raw_string, escaped",
    [
        (r"underscore_table", r"underscore\\_table"),
        (r"percent%%table", r"percent\\%\\%table"),
        (r"__many__under__scores__", r"\\_\\_many\\_\\_under\\_\\_scores\\_\\_"),
        (r"mixed_underscore%percent", r"mixed\\_underscore\\%percent"),
        (r"regular$table", r"regular$table"),
    ],
)
def test_escape_like_pattern(raw_string, escaped):
    assert escape_like_pattern(raw_string) == escaped
