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

from pathlib import Path
from typing import List, Optional

import yaml
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.project.schemas.project_definition import (
    ProjectDefinition,
    ProjectProperties,
)
from snowflake.cli.api.project.util import (
    append_to_identifier,
    clean_identifier,
    get_env_username,
    to_identifier,
)
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.utils.definition_rendering import render_definition_template
from snowflake.cli.api.utils.dict_utils import deep_merge_dicts
from snowflake.cli.api.utils.types import Context, Definition

DEFAULT_USERNAME = "unknown_user"


def _get_merged_definitions(paths: List[Path]) -> Optional[Definition]:
    spaths: List[SecurePath] = [SecurePath(p) for p in paths]
    if len(spaths) == 0:
        return None

    with spaths[0].open("r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as base_yml:
        definition = yaml.load(base_yml.read(), Loader=yaml.loader.BaseLoader) or {}

    for override_path in spaths[1:]:
        with override_path.open(
            "r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB
        ) as override_yml:
            overrides = (
                yaml.load(override_yml.read(), Loader=yaml.loader.BaseLoader) or {}
            )
            deep_merge_dicts(definition, overrides)

    return definition


def load_project(
    paths: List[Path], context_overrides: Optional[Context] = None
) -> ProjectProperties:
    """
    Loads project definition, optionally overriding values. Definition values
    are merged in left-to-right order (increasing precedence).
    Templating is also applied after the merging process.
    """
    merged_definitions = _get_merged_definitions(paths)
    return render_definition_template(merged_definitions, context_overrides or {})


def generate_local_override_yml(
    project: ProjectDefinition,
) -> ProjectDefinition:
    """
    Generates defaults for optional keys in the same YAML structure as the project
    schema. The returned YAML object can be saved directly to a file, if desired.
    A connection is made using global context to resolve current role and warehouse.
    """
    conn = cli_context.connection
    user = clean_identifier(get_env_username() or DEFAULT_USERNAME)
    role = conn.role
    warehouse = conn.warehouse

    local: dict = {}
    if project.native_app:
        name = clean_identifier(project.native_app.name)
        app_identifier = to_identifier(name)
        user_app_identifier = append_to_identifier(app_identifier, f"_{user}")
        package_identifier = append_to_identifier(app_identifier, f"_pkg_{user}")
        local["native_app"] = {
            "application": {
                "name": user_app_identifier,
                "role": role,
                "debug": True,
                "warehouse": warehouse,
            },
            "package": {"name": package_identifier, "role": role},
        }

    return project.update_from_dict(local)


def default_app_package(project_name: str):
    user = clean_identifier(get_env_username() or DEFAULT_USERNAME)
    return append_to_identifier(to_identifier(project_name), f"_pkg_{user}")


def default_role():
    conn = cli_context.connection
    return conn.role


def default_application(project_name: str):
    user = clean_identifier(get_env_username() or DEFAULT_USERNAME)
    return append_to_identifier(to_identifier(project_name), f"_{user}")
