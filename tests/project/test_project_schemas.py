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

import pytest
from pydantic import ValidationError
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.schemas.project_definition import (
    ProjectDefinitionV2,
    build_project_definition,
)
from snowflake.cli.api.project.schemas.v1.snowpark.argument import Argument
from snowflake.cli.api.project.schemas.v1.snowpark.callable import FunctionSchema


def test_if_fields_are_updated_correctly_by_assignment(argument_instance: Argument):
    argument_instance.name = "Baz"

    assert argument_instance.name == "Baz"


def test_if_optional_fields_set_to_none_are_correctly_updated_by_assignment(
    argument_instance: Argument,
):
    assert argument_instance.default == None

    argument_instance.default = "Baz"

    assert argument_instance.default == "Baz"


def test_if_model_is_validated_on_update_by_assingment(argument_instance: Argument):
    with pytest.raises(ValidationError) as expected_error:
        argument_instance.name = 42

    assert (
        "Input should be a valid string [type=string_type, input_value=42, input_type=int]"
        in str(expected_error.value)
    )


def test_if_model_is_updated_correctly_from_dict(argument_instance: Argument):
    assert argument_instance.default == None
    update_dict = {"name": "Baz", "default": "Foo"}

    argument_instance.update_from_dict(update_dict)

    assert argument_instance.default == "Foo"
    assert argument_instance.name == "Baz"
    assert argument_instance.arg_type == "Bar"


def test_nested_fields_update(
    function_instance: FunctionSchema, argument_instance: Argument
):
    assert Argument(name="a", type="string") in function_instance.signature
    update_dict = {"signature": [{"name": "Foo", "type": "Bar"}]}

    function_instance.update_from_dict(update_dict)

    assert argument_instance in function_instance.signature


def test_project_schema_is_updated_correctly_from_dict(
    native_app_project_instance: ProjectDefinitionV2,
):
    pkg_model = native_app_project_instance.entities["pkg"]
    assert pkg_model.manifest == "app/manifest.yml"
    assert pkg_model.distribution == "internal"
    assert pkg_model.meta.role == "test_role"

    update_dict = {"distribution": "external", "meta": {"role": "test_role_2"}}
    pkg_model.update_from_dict(update_dict)
    assert pkg_model.manifest == "app/manifest.yml"
    assert pkg_model.distribution == "external"
    assert pkg_model.artifacts[0].src == "app/*"
    assert pkg_model.meta.role == "test_role_2"


def test_project_definition_work_for_int_version():
    p = build_project_definition(definition_version=1)
    assert p.definition_version == "1"


def test_project_definition_fails_for_unknown_version():
    with pytest.raises(SchemaValidationError) as err:
        build_project_definition(definition_version="6.2.3")

    assert "Version 6.2.3 is not supported. Supported versions: 1, 1.1" in str(
        err.value
    )
