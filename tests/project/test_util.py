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
    concat_identifiers,
    escape_like_pattern,
    identifier_in_list,
    identifier_to_str,
    is_valid_identifier,
    is_valid_object_name,
    is_valid_quoted_identifier,
    is_valid_string_literal,
    is_valid_unquoted_identifier,
    same_identifiers,
    sanitize_identifier,
    sql_match,
    to_identifier,
    to_quoted_identifier,
    to_string_literal,
    unquote_identifier,
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


@pytest.mark.parametrize(
    "input_value, expected_value",
    [
        # valid unquoted id -> return quoted
        ("Id_1", '"Id_1"'),
        # valid quoted id without special chars -> return the same
        ('"Id_1"', '"Id_1"'),
        # valid quoted id with special chars -> return the same
        ('"Id_""_._end"', '"Id_""_._end"'),
        # unquoted with unsafe chars -> return quoted
        ('Id_""_._end', '"Id_""""_._end"'),
        # looks like quoted identifier but not properly escaped -> requote
        ('"Id"end"', '"""Id""end"""'),
        # blank -> quoted
        ("", '""'),
    ],
)
def test_to_quoted_identifier(input_value, expected_value):
    assert to_quoted_identifier(input_value) == expected_value


@pytest.mark.parametrize(
    "id1, id2, concatenated_value",
    [
        # both unquoted, no special char -> result unquoted
        ("Id_1", "_Id_2", "Id_1_Id_2"),
        # both unquoted, one with special char -> result quoted
        ('Id_1."', "_Id_2", '"Id_1.""_Id_2"'),
        # both unquoted, one with special char -> result quoted
        ("Id_1", '_Id_2."', '"Id_1_Id_2."""'),
        # one quoted, no special chars -> result quoted
        ('"Id_1"', "_Id_2", '"Id_1_Id_2"'),
        # one quoted, no special chars -> result quoted
        ("Id_1", '"_Id_2"', '"Id_1_Id_2"'),
        # both quoted, no special chars -> result quoted
        ('"Id_1"', '"_Id_2"', '"Id_1_Id_2"'),
        # quoted with valid 2 double quotes within -> result quoted
        ('"Id_""_1"', '"_""_Id_2"', '"Id_""_1_""_Id_2"'),
        # quoted with invalid single double quotes within
        # -> result quoted, and original quotes escaped
        ('"Id_"_1"', '"_"_Id_2"', '"""Id_""_1""""_""_Id_2"""'),
        # one quoted with invalid single double quotes within and other properly quoted
        # -> result quoted, double quotes escaped
        ('"Id_"_1"', '"_Id_2"', '"""Id_""_1""_Id_2"'),
        # one quoted with escaped double quotes within
        # another non quoted with double quotes within
        # -> result is quoted, and proper escaping of non quoted
        ('"Id_""_1"', '_Id_"_2', '"Id_""_1_Id_""_2"'),
        # 2 blanks -> result should be quoted to be a valid identifier
        ("", "", '""'),
    ],
)
def test_concat_identifiers(id1, id2, concatenated_value):
    assert concat_identifiers([id1, id2]) == concatenated_value


@pytest.mark.parametrize(
    "identifier, expected_value",
    [
        # valid unquoted id -> return same
        ("Id_1", "Id_1"),
        # valid quoted id without special chars -> return unquoted
        ('"Id_1"', "Id_1"),
        # valid quoted id with special chars -> return unquoted and unescaped "
        ('"Id_""_._end"', 'Id_"_._end'),
        # unquoted with unsafe chars -> return the same without unescaping
        ('Id_""_._end', 'Id_""_._end'),
        # blank identifier -> turns into blank string
        ('""', ""),
    ],
)
def test_identifier_to_str(identifier, expected_value):
    assert identifier_to_str(identifier) == expected_value


@pytest.mark.parametrize(
    "identifier, expected_value",
    [
        # valid unquoted id -> return same
        ("Id_1", "Id_1"),
        # valid quoted id -> remove quotes
        ('"Id""1"', "Id1"),
        # empty string -> return underscore
        ("", "_"),
        # valid string starting with number -> prepend underscore
        ("1ABC", "_1ABC"),
        # valid string starting with number after a special character -> prepend underscore
        ("..1ABC", "_1ABC"),
        # valid string starting with dollar sign -> prepend underscore
        ("$ABC", "_$ABC"),
        # string longer than 255 characters -> truncate to 255
        ("A" * 256, "A" * 255),
        # string longer than 255 characters with special characters -> truncate to 255 after removing special characters
        ("." + "A" * 254 + "BC", "A" * 254 + "B"),
    ],
)
def test_sanitize_identifier(identifier, expected_value):
    assert sanitize_identifier(identifier) == expected_value


def test_same_identifiers():
    # both unquoted, same case:
    assert same_identifiers("abc", "abc")

    # both unquoted, different case:
    assert same_identifiers("abc", "ABC")

    # both quoted, same case:
    assert same_identifiers('"abc"', '"abc"')

    # both quoted, different case:
    assert not same_identifiers('"abc"', '"ABC"')

    # one quoted, one unquoted - unquoted is lowercase:
    assert not same_identifiers("abc", '"abc"')

    # one quoted, one unquoted - unquoted is uppercase:
    assert same_identifiers("abc", '"ABC"')

    # one quoted, one unquoted - unquoted has special characters, so treat it as quoted:
    assert same_identifiers("a.bc", '"a.bc"')

    # blank strings:
    assert same_identifiers("", '""')


def test_sql_match():
    # simple case with a match:
    assert sql_match(pattern="abc", value="abc")

    # simple case that does not match:
    assert not sql_match(pattern="abc", value="def")

    # case with a match but uses '_' as a wildcard:
    assert sql_match(pattern="a_c", value="abc")

    # case with wildcard '_' but does not match due to different length:
    assert not sql_match(pattern="a_c", value="abcd")

    # case that match with wildcard '%':
    assert sql_match(pattern="a%b", value="a123b")

    # case that match with wildcard '%' but 0 characters:
    assert sql_match(pattern="a%b", value="ab")

    # case that does not match because '%' is in the wrong place:
    assert not sql_match(pattern="a%b", value="ab123")

    # case with '%%' that matches everything:
    assert sql_match(pattern="%%", value="abc")

    # case with quoted identifier:
    assert sql_match(pattern="a_c", value='"a_c"')


def test_identifier_in_list():
    # one ID matching:
    assert identifier_in_list("abc", ["def", "abc", "ghi"])

    # id not matching:
    assert not identifier_in_list("abc", ["def", "ghi"])

    # id matching with case insensitivity:
    assert identifier_in_list("AbC", ["dEf", "aBc", "gHi"])

    # id with quotes matching:
    assert identifier_in_list('"ABC"', ["def", "abc", "ghi"])

    # id with quotes not matching due to case:
    assert not identifier_in_list('"abc"', ["DEF", "ABC", "GHI"])
    assert not identifier_in_list('"abc"', ["def", "abc", "ghi"])

    # test with empty list:
    assert not identifier_in_list("abc", [])


@pytest.mark.parametrize(
    "identifier, expected",
    [
        # valid unquoted id -> return upper case version
        ("Id_1", "ID_1"),
        # valid quoted id -> remove quotes and keep case
        ('"Id""1"', 'Id"1'),
        # unquoted id with special characters -> treat it as quoted ID and reserve case
        ("Id.aBc", "Id.aBc"),
        # unquoted id with double quotes inside -> treat is quoted ID
        ('Id"1', 'Id"1'),
        # quoted id with escaped double quotes -> unescape and keep case
        ('"Id""1"', 'Id"1'),
        # empty string -> return the same
        ("", ""),
    ],
)
def test_unquote_identifier(identifier, expected):
    assert unquote_identifier(identifier) == expected
