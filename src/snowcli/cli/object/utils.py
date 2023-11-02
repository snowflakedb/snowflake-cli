from enum import Enum
from typing import Dict


class ObjectType(str, Enum):
    COMPUTE_POOL = "compute pool"
    DATABASE = "database"
    FUNCTION = "function"
    JOB = "job"
    PROCEDURE = "procedure"
    ROLE = "role"
    SCHEMA = "schema"
    SERVICE = "service"
    STREAMLIT = "streamlit"
    TABLE = "table"
    WAREHOUSE = "warehouse"


def get_plural_name(object_type: ObjectType):
    exceptions: Dict[str, str] = {}

    return exceptions.get(object_type.value, f"{object_type.value}s")
