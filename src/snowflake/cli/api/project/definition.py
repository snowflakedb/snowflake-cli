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
from click import ClickException
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.project.schemas.project_definition import (
    ProjectProperties,
    YamlOverride,
)
from snowflake.cli.api.project.util import (
    append_to_identifier,
    get_env_username,
    sanitize_identifier,
    to_identifier,
)
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.utils.definition_rendering import (
    raw_project_properties,
    render_definition_template,
)
from snowflake.cli.api.utils.dict_utils import deep_merge_dicts
from snowflake.cli.api.utils.types import Context, Definition
from yaml import MappingNode, SequenceNode

DEFAULT_USERNAME = "unknown_user"


def _get_merged_definitions(paths: List[Path]) -> Optional[Definition]:
    spaths: List[SecurePath] = [SecurePath(p) for p in paths]
    if len(spaths) == 0:
        return None

    loader = yaml.BaseLoader
    loader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _no_duplicates_constructor
    )
    loader.add_constructor("!override", _override_tag)

    with spaths[0].open("r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as base_yml:
        definition = yaml.load(base_yml.read(), Loader=loader) or {}

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
    paths: List[Path],
    context_overrides: Optional[Context] = None,
    render_templates: bool = True,
) -> ProjectProperties:
    """
    Loads project definition, optionally overriding values. Definition values
    are merged in left-to-right order (increasing precedence).
    Templating is also applied after the merging process.
    """
    merged_definitions = _get_merged_definitions(paths)
    if render_templates:
        return render_definition_template(merged_definitions, context_overrides or {})
    else:
        return raw_project_properties(merged_definitions)


def default_app_package(project_name: str):
    user = sanitize_identifier(get_env_username() or DEFAULT_USERNAME).lower()
    return append_to_identifier(to_identifier(project_name), f"_pkg_{user}")


def default_role():
    conn = get_cli_context().connection
    return conn.role


def default_application(project_name: str):
    user = sanitize_identifier(get_env_username() or DEFAULT_USERNAME).lower()
    return append_to_identifier(to_identifier(project_name), f"_{user}")


def _no_duplicates_constructor(loader, node, deep=False):
    """
    Raises error it there are duplicated keys on the same level in the yaml file
    """
    mapping = {}

    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        value = loader.construct_object(value_node, deep=deep)
        if key in mapping.keys():
            raise ClickException(
                f"While loading the project definition file, duplicate key was found: {key}"
            )
        mapping[key] = value
    return loader.construct_mapping(node, deep)


def _override_tag(loader, node, deep=False):
    if isinstance(node, SequenceNode):
        return YamlOverride(data=loader.construct_sequence(node, deep))
    if isinstance(node, MappingNode):
        return YamlOverride(data=loader.construct_mapping(node, deep))
    return node.value
