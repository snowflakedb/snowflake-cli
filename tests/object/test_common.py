from snowflake.cli.plugins.object.common import _parse_tag, Tag

import pytest

from click import ClickException


INVALID_TAGS = (
    "123_name=value",  # starts with a digit
    "tag name=value",  # space in identifier
    "tag&_name=value",  # special characters in identifier
    "tag",  # no equals sign
    "=value",  # empty identifier
    "a" * 257 + "=value",  # identifier is over 256 characters
    '"tag"name"=value'  # undoubled quote in tag name
)
VALID_TAGS = (
    ("tag=value", ("tag", "value")),
    ("_underscore_start=value", ("_underscore_start", "value")),
    ("a=xyz", ("a", "xyz")),
    ("A=123", ("A", "123")),
    ("mixedCase=value", ("mixedCase", "value")),
    ("_=value", ("_", "value")),
    ("tag='this is a value'", ("tag", "'this is a value'")),
    ("\"tag name!@#\"=value", ("\"tag name!@#\"", "value")),  # quoted identifier allows for spaces and special characters
    ("tag==value", ("tag", "=value"))  # This is a strange case which we may not actually want to support
)


def test_parse_tag():
    for tag in INVALID_TAGS:
        with pytest.raises(ClickException):
            _parse_tag(tag)
    for tag in VALID_TAGS:
        assert Tag(*tag[1]) == _parse_tag(tag[0])