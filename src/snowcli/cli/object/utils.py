from typing import Dict

from snowcli.cli.constants import ObjectType
def get_plural_name(object_type: ObjectType):
    exceptions: Dict[str, str] = {}

    return exceptions.get(object_type.value, f"{object_type.value}s")
