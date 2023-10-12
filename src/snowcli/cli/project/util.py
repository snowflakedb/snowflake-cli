import re
import os
from typing import Optional

IDENTIFIER = r'((?:"[^"]*(?:""[^"]*)*")|(?:[A-Za-z_][\w$]{0,254}))'
DB_SCHEMA_AND_NAME = f"{IDENTIFIER}[.]{IDENTIFIER}[.]{IDENTIFIER}"
SCHEMA_AND_NAME = f"{IDENTIFIER}[.]{IDENTIFIER}"
GLOB_REGEX = r"^[a-zA-Z0-9_\-./*?**\p{L}\p{N}]+$"
RELATIVE_PATH = r"^[^/][\p{L}\p{N}_\-.][^/]*$"
QUOTED = r'["](.+)["]'


def clean_identifier(input):
    """
    Removes characters that cannot be used in an unquoted identifier,
    converting to lowercase as well.
    """
    return re.sub(r"[^a-z0-9_$]", "", f"{input}".lower())


def unquote_identifier(identifier: str) -> str:
    """
    Returns a version of this identifier that can be used inside of a URL,
    inside of a string for a LIKE clause, or to match an identifier passed
    back as a value from a SQL statement.
    """
    if match := re.fullmatch(QUOTED, identifier):
        return match.group(1)
    # unquoted identifiers are internally represented as uppercase
    return identifier.upper()


def extract_schema(qualified_name: str):
    """
    Extracts the schema from either a two-part or three-part qualified name
    (i.e. schema.object or database.schema.object). If qualified_name is not
    qualified with a schema, returns None.
    """
    if match := re.fullmatch(DB_SCHEMA_AND_NAME, qualified_name):
        return match.group(2)
    elif match := re.fullmatch(SCHEMA_AND_NAME, qualified_name):
        return match.group(1)
    return None


def generate_user_env(username: str) -> dict:
    return {
        "USER": username,
    }


def first_set_env(*keys: str):
    for k in keys:
        v = os.getenv(k)
        if v:
            return v

    return None


def get_env_username() -> Optional[str]:
    return first_set_env("USER", "USERNAME", "LOGNAME")


def identifier_as_part(identifier: str) -> str:
    """
    Returns a version of this identifier that can be used inside of a URL
    or inside of a string for a LIKE clause.
    """
    if match := re.fullmatch(QUOTED, identifier):
        return match.group(1)
    return identifier.upper()
