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
from typing import Any, Dict

from pydantic.json_schema import model_json_schema
from snowflake.cli._app.dev.docs.project_definition_generate_json_schema import (
    ProjectDefinitionGenerateJsonSchema,
)
from snowflake.cli._app.dev.docs.template_utils import get_template_environment
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

DEFINITION_DESCRIPTION = "definition_description.rst.jinja2"


def generate_project_definition_docs(
    root: SecurePath, definition: type[UpdatableModel]
) -> None:
    """
    Recursively traverses the generated project definition schema,
    creating a file for each section that mirrors the YAML structure.
    Each file contains the definition for every field within that section.
    """

    root.mkdir(exist_ok=True)
    list_of_sections = model_json_schema(
        definition, schema_generator=ProjectDefinitionGenerateJsonSchema
    )["result"]
    for section in list_of_sections:
        _render_definition_description(root, section)
    return


def _render_definition_description(root: SecurePath, section: Dict[str, Any]) -> None:
    env = get_template_environment()

    # RST files are presumed to be standalone pages in the docs with a matching item in the left nav.
    # Included files, which these are, need to use the .txt extension.
    file_path = root / f"definition_{section['name']}.txt"
    log.info("Creating %s", file_path)
    template = env.get_template(DEFINITION_DESCRIPTION)
    with file_path.open("w+") as fh:
        fh.write(template.render(section))
