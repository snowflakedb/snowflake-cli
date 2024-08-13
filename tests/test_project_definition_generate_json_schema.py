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

from typing import Any, Dict, List, Set

from _pytest.fixtures import fixture
from pydantic.json_schema import GenerateJsonSchema, model_json_schema
from snowflake.cli._app.dev.docs.project_definition_generate_json_schema import (
    ProjectDefinitionGenerateJsonSchema,
)
from snowflake.cli.api.project.schemas.project_definition import DefinitionV11


@fixture
def section_properties_set():
    project_definition_sections = model_json_schema(
        DefinitionV11, schema_generator=ProjectDefinitionGenerateJsonSchema
    )["result"]

    section_properties_set = set()
    for section in project_definition_sections:
        section_properties_set |= set(
            [field["path"] for field in section["properties"]]
        )

    return section_properties_set


def test_generated_json_contains_properties_generated_from_references(
    section_properties_set,
):
    manual_properties_set = {
        "snowpark.functions.external_access_integrations",
        "snowpark.functions.signature.default",
    }

    errors = [
        f"Field `{field}` was not generated in section_properties_set"
        for field in manual_properties_set - section_properties_set
    ]

    assert len(errors) == 0, " ".join(errors)


def test_generated_json_correspond_to_project_definition_model(section_properties_set):
    model_json = model_json_schema(
        DefinitionV11, schema_generator=GenerateJsonSchema, ref_template="{model}"
    )

    def _get_field_references(model_with_type: Dict[str, Any]) -> List[str]:
        if "$ref" in model_with_type:
            return [model_with_type["$ref"]]

        if "type" in model_with_type and model_with_type["type"] == "array":
            return _get_field_references(model_with_type["items"])

        result = []
        if "anyOf" in model_with_type:
            for field_type in model_with_type["anyOf"]:
                result += _get_field_references(field_type)
        return result

    def _get_set_of_model_properties(
        references: Dict[str, Any], definition_model: Dict[str, Any], path: str = ""
    ) -> Set[str]:
        result = set()
        for field_name, field_model in definition_model["properties"].items():
            new_path = field_name if path == "" else path + "." + field_name
            result.add(new_path)
            for field_reference in _get_field_references(field_model):
                if field_reference in references:
                    result |= _get_set_of_model_properties(
                        references, references[field_reference], new_path
                    )

        return result

    model_properties_set = _get_set_of_model_properties(model_json["$defs"], model_json)

    errors = [
        f"Field `{field}` was not properly generated"
        for field in model_properties_set - section_properties_set
    ]

    assert len(errors) == 0, " ".join(errors)
