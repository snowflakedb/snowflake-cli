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
            children_fields = self._get_children_fields(field_model, field_name)

            new_field = self._create_new_field(
                path=field_name,
                title=field_model["title"],
                indents=0,
                item_index=0,
                is_required=is_required,
                field_name=field_name,
                is_model=len(children_fields) > 0,
                types=" | ".join(self._get_field_types(field_model)),
            )
            fields = [new_field] + children_fields

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
            new_current_path = (
                field_name if current_path == "" else current_path + "." + field_name
            )
            children_fields = self._get_children_fields(
                field_model, new_current_path, deep
            )
            new_field = self._create_new_field(
                path=new_current_path,
                title=field_model["title"],
                indents=deep,
                item_index=item_index,
                is_required=is_required,
                field_name=field_name,
                is_model=len(children_fields) > 0,
                types=" | ".join(self._get_field_types(field_model)),
            )
            fields = [new_field] + children_fields
            if is_required:
                required_fields += fields
            else:
                optional_fields += fields
        return required_fields + optional_fields

    def _create_new_field(
        self,
        path: str,
        title: str,
        indents: int,
        item_index: int,
        is_required: bool,
        field_name: str,
        is_model: bool,
        types: str,
    ):
        return {
            "path": path,
            "title": title,
            "indents": indents,
            "item_index": item_index,
            "required": is_required,
            "name": field_name,
            "is_model": is_model,
            "types": types,
        }

    def _get_children_fields(
        self,
        field_model: Dict[str, Any],
        current_path: str,
        deep: int = 0,
    ) -> List[Dict[str, Any]]:
        child_fields: List[Dict[str, Any]] = []
        references: List[Tuple[str, bool]] = self._get_references(field_model)
        for reference, is_array_item in references:
            child_fields += self._get_fields_with_path(
                self._remapped_definitions[reference],
                current_path,
                deep + 1,
                is_array_item,
            )

        return child_fields

    def _get_references(
        self,
        model_with_type: Dict[str, Any],
        is_array_item: bool = False,
    ) -> list[tuple[str, bool]]:
        result: List[Tuple[str, bool]] = []

        if "$ref" in model_with_type:
            return [(model_with_type["$ref"], is_array_item)]

        if "type" in model_with_type and model_with_type["type"] == "array":
            result += self._get_references(model_with_type["items"], True)

        if "anyOf" in model_with_type:
            for field_type in model_with_type["anyOf"]:
                result += self._get_references(field_type, is_array_item)
        return result

    def _get_field_types(self, model_with_type: Dict[str, Any]) -> list[str]:
        types_result: List[str] = []
        if "type" in model_with_type:
            if model_with_type["type"] == "array":
                items_types = self._get_field_types(model_with_type["items"])
                if len(items_types) > 0:
                    types_result += [f"array[{' | '.join(items_types)}]"]

            elif model_with_type["type"] != "null":
                types_result += [model_with_type["type"]]
        elif "anyOf" in model_with_type:
            for field_type in model_with_type["anyOf"]:
                types = self._get_field_types(field_type)
                types_result += types
        return types_result
