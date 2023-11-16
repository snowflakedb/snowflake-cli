from snowcli.cli.project.util import (
    append_to_identifier,
    is_valid_identifier,
    is_valid_quoted_identifier,
    is_valid_unquoted_identifier,
    to_identifier,
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
    '"abc"def"',  # improprely escaped inner quote
)


def test_is_valid_unquoted_identifier():
    for id in VALID_UNQUOTED_IDENTIFIERS:
        assert is_valid_unquoted_identifier(id)

    for id in VALID_QUOTED_IDENTIFIERS:
        assert not is_valid_unquoted_identifier(id)

    for id in INVALID_UNQUOTED_IDENTIFIERS:
        assert not is_valid_unquoted_identifier(id)

    for id in INVALID_QUOTED_IDENTIFIERS:
        assert not is_valid_unquoted_identifier(id)


def test_is_valid_quoted_identifier():
    for id in VALID_UNQUOTED_IDENTIFIERS:
        assert not is_valid_quoted_identifier(id)

    for id in VALID_QUOTED_IDENTIFIERS:
        assert is_valid_quoted_identifier(id)

    for id in INVALID_UNQUOTED_IDENTIFIERS:
        assert not is_valid_quoted_identifier(id)

    for id in INVALID_QUOTED_IDENTIFIERS:
        assert not is_valid_quoted_identifier(id)


def test_is_valid_identifier():
    for id in VALID_UNQUOTED_IDENTIFIERS:
        assert is_valid_identifier(id)

    for id in VALID_QUOTED_IDENTIFIERS:
        assert is_valid_identifier(id)

    for id in INVALID_UNQUOTED_IDENTIFIERS:
        assert not is_valid_identifier(id)

    for id in INVALID_QUOTED_IDENTIFIERS:
        assert not is_valid_identifier(id)


def test_to_identifier():
    for id in VALID_UNQUOTED_IDENTIFIERS:
        assert to_identifier(id) == id
    for id in VALID_QUOTED_IDENTIFIERS:
        assert to_identifier(id) == id

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
