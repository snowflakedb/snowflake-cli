# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path

TEMPLATES_PATH = Path(__file__).parent.parent / "templates"
DEPLOYMENT_STAGE = "deployments"
PYTHON_3_12 = (3, 12)


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
    EXTERNAL_ACCESS_INTEGRATION = ObjectNames(
        "external-access-integration",
        "external access integration",
        "external access integrations",
    )
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
    GIT_REPOSITORY = ObjectNames("git-repository", "git repository", "git repositories")

    def __str__(self):
        """This makes using this Enum easier in formatted string"""
        return self.value.cli_name


OBJECT_TO_NAMES = {o.value.cli_name: o.value for o in ObjectType}
SUPPORTED_OBJECTS = sorted(OBJECT_TO_NAMES.keys())

# Scope names here must replace spaces with '-'. For example 'compute pool' is 'compute-pool'.
VALID_SCOPES = ["database", "schema", "compute-pool"]

DEFAULT_SIZE_LIMIT_MB = 128

SF_REST_API_URL_PREFIX = "/api/v2"

PROJECT_TEMPLATE_VARIABLE_OPENING = "<%"
PROJECT_TEMPLATE_VARIABLE_CLOSING = "%>"

INIT_TEMPLATE_VARIABLE_OPENING = "<!"
INIT_TEMPLATE_VARIABLE_CLOSING = "!>"

SNOWPARK_SHARED_MIXIN = "snowpark_shared"

DEFAULT_ENV_FILE = "environment.yml"
DEFAULT_PAGES_DIR = "pages"
