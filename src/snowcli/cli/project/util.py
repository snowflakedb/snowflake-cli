import re
import os
from typing import List


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


def first_set_env(*keys: List[str]):
    getenv = lambda i: os.getenv(keys[i], getenv(i + 1) if i + 1 < len(keys) else None)
    return getenv(0)


def get_env_username() -> str | None:
    return first_set_env("USER", "USERNAME", "LOGNAME")
