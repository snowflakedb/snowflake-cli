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

from typing import Any, Dict, List, Tuple

from pydantic.json_schema import GenerateJsonSchema


class ProjectDefinitionGenerateJsonSchema(GenerateJsonSchema):
    def __init__(self, by_alias: bool = True, ref_template: str = ""):
        reference_template = "{model}"
        super().__init__(by_alias, reference_template)
        self._remapped_definitions: Dict[str, Any] = {}

    def generate(self, schema, mode="validation"):
        json_schema = super().generate(schema, mode=mode)
        self._remapped_definitions = json_schema["$defs"]
        return {"result": self._get_fields_sections(json_schema)}

    def _get_fields_sections(
        self, current_definition: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        required_fields: List[Dict[str, Any]] = []
        sections: List[Dict[str, Any]] = []

        for field_name, field_model in current_definition["properties"].items():
            is_required = (
                "required" in current_definition
                and field_name in current_definition["required"]
            )
            fields = self._get_field_with_its_child_fields(
                field_name, field_model, is_required
            )
            if is_required:
                required_fields += fields
            else:
                sections += [
                    {
                        "fields": fields,
                        "title": field_model["title"],
                        "name": field_name,
                    }
                ]

        for section in sections:
            section["fields"] = required_fields + section["fields"]

        return sections

    def _get_fields_with_path(
        self,
        current_definition: Dict[str, Any],
        current_path: str = "",
        deep: int = 0,
        is_array_item: bool = False,
    ) -> List[Dict[str, Any]]:
        required_fields: List[Dict[str, Any]] = []
        optional_fields: List[Dict[str, Any]] = []
        item_index = 0

        for field_name, field_model in current_definition["properties"].items():
            item_index += 1 if is_array_item else 0
            is_required = (
                "required" in current_definition
                and field_name in current_definition["required"]
            )
            fields = self._get_field_with_its_child_fields(
                field_name, field_model, is_required, current_path, item_index, deep
            )
            if is_required:
                required_fields += fields
            else:
                optional_fields += fields
        return required_fields + optional_fields

    def _get_field_with_its_child_fields(
        self,
        field_name: str,
        field_model: Dict[str, Any],
        is_required: bool,
        current_path: str = "",
        item_index: int = 0,
        deep: int = 0,
    ) -> List[Dict[str, Any]]:

        new_current_path = (
            field_name if current_path == "" else current_path + "." + field_name
        )
        child_fields = self._get_child_references(field_model, new_current_path, deep)
        types, is_array = self._get_field_type(field_model)
        field_model["is_array"] = is_array
        new_field = {
            "path": new_current_path,
            "title": field_model["title"],
            "indents": deep,
            "item_index": item_index,
            "required": is_required,
            "name": field_name,
            "is_model": len(child_fields) > 0,
            "types": " | ".join(types),
        }
        fields = [new_field] + child_fields
        return fields

    def _get_child_references(
        self,
        model_with_type: Dict[str, Any],
        current_path: str,
        deep: int = 0,
        array_item: bool = False,
    ) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []

        if "$ref" in model_with_type:
            result += self._get_fields_with_path(
                self._remapped_definitions[model_with_type["$ref"]],
                current_path,
                deep + 1,
                array_item,
            )

        if "type" in model_with_type and model_with_type["type"] == "array":
            result += self._get_child_references(
                model_with_type["items"], current_path, deep, True
            )

        if "AnyOf" in model_with_type:
            for field_type in model_with_type["AnyOf"]:
                result += self._get_child_references(
                    field_type, current_path, deep, array_item
                )
        return result

    def _get_field_type(
        self, model_with_type: Dict[str, Any]
    ) -> Tuple[List[str], bool]:
        types_result: List[str] = []
        is_array_result = False
        if "type" in model_with_type:
            if model_with_type["type"] == "array":
                is_array_result = True
                items_types, _ = self._get_field_type(model_with_type["items"])
                if len(items_types) > 0:
                    types_result += [f"array[{' | '.join(items_types)}]"]

            elif model_with_type["type"] != "null":
                types_result += [model_with_type["type"]]
        elif "AnyOf" in model_with_type:
            for field_type in model_with_type["AnyOf"]:
                types, is_array = self._get_field_type(field_type)
                is_array_result = is_array or is_array_result
                types_result += types
        return types_result, is_array_result
