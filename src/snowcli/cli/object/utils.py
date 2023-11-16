from typing import Dict

from snowcli.cli.constants import ObjectType


def get_plural_name(valid_sf_name: str):
    exceptions: Dict[str, str] = {}
    return exceptions.get(valid_sf_name, f"{valid_sf_name}s")
