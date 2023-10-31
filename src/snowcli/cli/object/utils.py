from enum import Enum


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
