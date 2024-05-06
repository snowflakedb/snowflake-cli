import pytest
from pydantic import ValidationError
from snowflake.cli.api.project.schemas.project_definition import ProjectDefinition
from snowflake.cli.api.project.schemas.snowpark.argument import Argument
from snowflake.cli.api.project.schemas.snowpark.callable import FunctionSchema


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
    native_app_project_instance: ProjectDefinition,
):
    assert native_app_project_instance.native_app.name == "napp_test"
    assert native_app_project_instance.native_app.package.distribution == "internal"
    update_dict = {"native_app": {"package": {"distribution": "external"}}}

    native_app_project_instance.update_from_dict(update_dict)
    assert native_app_project_instance.native_app.name == "napp_test"
    assert native_app_project_instance.native_app.package.distribution == "external"
