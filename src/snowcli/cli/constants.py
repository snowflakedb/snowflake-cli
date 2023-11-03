from __future__ import annotations

from enum import Enum

DEPLOYMENT_STAGE = "deployments"


class ObjectType(Enum):
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
