import re
import os
from typing import Optional


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
