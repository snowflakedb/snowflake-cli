from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path

TEMPLATES_PATH = Path(__file__).parent.parent / "templates"
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
    INTEGRATION = ObjectNames("integration", "integration", "integrations")
    # JOB = ObjectNames("job", "job", "jobs")
    NETWORK_RULE = ObjectNames("network-rule", "network rule", "network rules")
    PROCEDURE = ObjectNames("procedure", "procedure", "procedures")
    ROLE = ObjectNames("role", "role", "roles")
    SCHEMA = ObjectNames("schema", "schema", "schemas")
    SERVICE = ObjectNames("service", "service", "services")
    SECRET = ObjectNames("secret", "secret", "secrets")
    STAGE = ObjectNames("stage", "stage", "stages")
    STREAM = ObjectNames("stream", "stream", "streams")
    STREAMLIT = ObjectNames("streamlit", "streamlit", "streamlits")
    TABLE = ObjectNames("table", "table", "tables")
    TASK = ObjectNames("task", "task", "tasks")
    USER = ObjectNames("user", "user", "users")
    WAREHOUSE = ObjectNames("warehouse", "warehouse", "warehouses")
    VIEW = ObjectNames("view", "view", "views")
    IMAGE_REPOSITORY = ObjectNames(
        "image-repository", "image repository", "image repositories"
    )

    def __str__(self):
        """This makes using this Enum easier in formatted string"""
        return self.value.cli_name


OBJECT_TO_NAMES = {o.value.cli_name: o.value for o in ObjectType}
SUPPORTED_OBJECTS = sorted(OBJECT_TO_NAMES.keys())

PACKAGES_DIR = Path(".packages")
