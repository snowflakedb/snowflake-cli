from __future__ import annotations

from typing import Any


def try_cast_to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    # Now if value is not string then cast it to str. Simplifies logic for 1 and 0
    if not isinstance(value, str):
        value = str(value)

    know_booleans_mapping = {"true": True, "false": False, "1": True, "0": False}

    if value.lower() not in know_booleans_mapping:
        raise ValueError(f"Could not case {value} to bool value")
    return know_booleans_mapping[value.lower()]
