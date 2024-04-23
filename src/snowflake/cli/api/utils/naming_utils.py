from __future__ import annotations

import re
from typing import Tuple

from snowflake.cli.api.project.util import (
    VALID_IDENTIFIER_REGEX,
)


def from_qualified_name(name: str) -> Tuple[str, str | None, str | None]:
    """
    Takes in an object name in the form [[database.]schema.]name. Returns a tuple (name, [schema], [database])
    """
    # TODO: Use regex to match object name to a valid identifier or valid identifier (args). Second case is for sprocs and UDFs
    qualifier_pattern = rf"(?:(?P<first_qualifier>{VALID_IDENTIFIER_REGEX})\.)?(?:(?P<second_qualifier>{VALID_IDENTIFIER_REGEX})\.)?(?P<name>.*)"
    result = re.fullmatch(qualifier_pattern, name)

    if result is None:
        raise ValueError(f"'{name}' is not a valid qualified name")

    unqualified_name = result.group("name")
    if result.group("second_qualifier") is not None:
        database = result.group("first_qualifier")
        schema = result.group("second_qualifier")
    else:
        database = None
        schema = result.group("first_qualifier")
    return unqualified_name, schema, database
