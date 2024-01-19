from dataclasses import dataclass
from snowflake.cli.api.project.util import QUOTED_IDENTIFIER_REGEX, UNQUOTED_IDENTIFIER_REGEX, to_string_literal

from click import ClickException

import typer


@dataclass
class Tag:
    name: str
    value: str


def _parse_tag(tag: str) -> Tag:
    import re
    identifier_pattern = re.compile(f"(?P<tag_name>{UNQUOTED_IDENTIFIER_REGEX}|{QUOTED_IDENTIFIER_REGEX})")
    value_pattern = re.compile(f"(?P<tag_value>.+)")
    match = re.fullmatch(f"{identifier_pattern.pattern}={value_pattern.pattern}", tag)
    if match is not None:
        return Tag(match.group('tag_name'), to_string_literal(match.group('tag_value')))
    else:
        raise ClickException(
            "tag must be in the format <tag_name>=<tag_value> where tag_name is a valid unquoted identifier and tag_value is a string")


def comment_option(object_type: str):
    """
    Provides a common interface for all commands that accept a comment option (e.g. when creating a new object).
    Parses the input string into a string literal.
    """
    return typer.Option(None, "--comment", help=f"Comment for the {object_type}", callback=to_string_literal)


def tag_option(object_type: str):
    """
    Provides a common interface for all commands that accept a tag option (e.g. when altering the tag of an object).
    Parses the input string in the format "tag_name=tag_value" into a Tag object with 'name' and 'value' properties.
    """
    return typer.Option(None, "--tag", help=f"Tag for the {object_type}", parser=_parse_tag)
