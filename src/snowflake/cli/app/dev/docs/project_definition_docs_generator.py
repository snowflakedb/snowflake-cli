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

import logging
from pathlib import Path
from typing import Any, Dict

from jinja2 import Environment, FileSystemLoader
from pydantic.json_schema import model_json_schema
from snowflake.cli.api.project.schemas.project_definition import ProjectDefinition
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.app.dev.docs.project_definition_generate_json_schema import (
    ProjectDefinitionGenerateJsonSchema,
)

log = logging.getLogger(__name__)

DEFINITION_DESCRIPTION = "definition_description.rst.jinja2"


def generate_project_definition_docs(root: SecurePath):
    """
    Recursively traverses the generated project definition schema,
    creating a file for each section that mirrors the YAML structure.
    Each file contains the definition for every field within that section.
    """

    list_of_sections = model_json_schema(
        ProjectDefinition, schema_generator=ProjectDefinitionGenerateJsonSchema
    )["result"]
    for section in list_of_sections:
        _render_definition_description(root, section)
    return


def _render_definition_description(root: SecurePath, section: Dict[str, Any]) -> None:
    env = Environment(loader=FileSystemLoader(Path(__file__).parent / "templates"))
    file_path = root / f"project-definition-{section['name']}.txt"
    log.info("Creating %s", file_path)
    template = env.get_template(DEFINITION_DESCRIPTION)
    with file_path.open("w+") as fh:
        fh.write(template.render(section))
