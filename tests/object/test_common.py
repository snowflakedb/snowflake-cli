import uuid
from snowflake.cli.plugins.object.common import (
    _parse_tag,
    Tag,
    object_name_callback,
    TagError,
)
from typing import Tuple
import pytest
from unittest import mock
from click import ClickException


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


@mock.patch("snowflake.cli.plugins.object.common.is_valid_object_name")
def test_object_name_callback(mock_is_valid):
    name = f"id_{uuid.uuid4()}"
    mock_is_valid.return_value = True
    assert object_name_callback(name) == name


@mock.patch("snowflake.cli.plugins.object.common.is_valid_object_name")
def test_object_name_callback_invalid(mock_is_valid):
    name = f"id_{uuid.uuid4()}"
    mock_is_valid.return_value = False
    with pytest.raises(ClickException):
        object_name_callback(name)
