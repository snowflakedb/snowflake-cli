import re
import os
from typing import Optional


QUOTED = r'["](.+)["]'


def clean_identifier(input):
    """
    Removes characters that cannot be used in an unquoted identifier,
    converting to lowercase as well.
    """
    return re.sub(r"[^a-z0-9_$]", "", f"{input}".lower())


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
