from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path

DEPLOYMENT_STAGE = "deployments"


@dataclass(frozen=True)
class ObjectNames:
    cli_name: str
    sf_name: str
    sf_plural_name: str

    def __str__(self):
        return self.sf_name


class ObjectType(Enum):
    COMPUTE_POOL = ObjectNames("compute-pool", "compute pool", "compute pools")
    DATABASE = ObjectNames("database", "database", "databases")
    FUNCTION = ObjectNames("function", "function", "functions")
    JOB = ObjectNames("job", "job", "jobs")
    PROCEDURE = ObjectNames("procedure", "procedure", "procedures")
    ROLE = ObjectNames("role", "role", "roles")
    SCHEMA = ObjectNames("schema", "schema", "schemas")
    SERVICE = ObjectNames("service", "service", "services")
    STAGE = ObjectNames("stage", "stage", "stages")
    STREAMLIT = ObjectNames("streamlit", "streamlit", "streamlits")
    TABLE = ObjectNames("table", "table", "tables")
    WAREHOUSE = ObjectNames("warehouse", "warehouse", "warehouses")

    def __str__(self):
        """This makes using this Enum easier in formatted string"""
        return self.value.cli_name


OBJECT_TO_NAMES = {o.value.cli_name: o.value for o in ObjectType}

PACKAGES_DIR = Path(".packages")
