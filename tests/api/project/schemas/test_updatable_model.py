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
from pydantic import ValidationError, field_validator
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel, context


def test_updatable_model_including_other_models():
    class TestIncludedModel(UpdatableModel):
        c: str

    class TestModel(UpdatableModel):
        a: str
        b: TestIncludedModel

    test_input = {"a": "a_value", "b": {"c": "c_value"}}
    result = TestModel(**test_input)

    assert result.a == "a_value"
    assert result.b is not None
    assert result.b.c == "c_value"


def test_updatable_model_with_sub_class_models():
    class ParentModel(UpdatableModel):
        a: str

    class ChildModel(ParentModel):
        a: str
        b: str

    test_input = {"a": "a_value", "b": "b_value"}
    result = ChildModel(**test_input)

    assert result.a == "a_value"
    assert result.b == "b_value"


def test_updatable_model_with_validators():
    class TestModel(UpdatableModel):
        a: str

        @field_validator("a", mode="before")
        @classmethod
        def validate_a_before(cls, value):
            if value != "expected_value":
                raise ValueError("Invalid Value")
            return value

        @field_validator("a", mode="after")
        @classmethod
        def validate_a_after(cls, value):
            if value != "expected_value":
                raise ValueError("Invalid Value")
            return value

        @field_validator("a", mode="wrap")
        @classmethod
        def validate_a_wrap(cls, value, handler):
            if value != "expected_value":
                raise ValueError("Invalid Value")
            result = handler(value)
            if result != "expected_value":
                raise ValueError("Invalid Value")
            return result

    result = TestModel(a="expected_value")

    assert result.a == "expected_value"

    with pytest.raises(ValidationError) as e:
        TestModel(a="abc")
    assert "Invalid Value" in str(e.value)

    with pytest.raises(ValidationError) as e:
        TestModel(a="<% sometemplate %>")
    assert "Invalid Value" in str(e.value)

    with context({"skip_validation_on_templates": True}):
        with pytest.raises(ValidationError) as e:
            TestModel(a="abc")
        assert "Invalid Value" in str(e.value)

        result = TestModel(a="<% sometemplate %>")
        assert result.a == "<% sometemplate %>"


def test_updatable_model_with_plain_validator():
    class TestModel(UpdatableModel):
        a: str

        @field_validator("a", mode="plain")
        @classmethod
        def validate_a_plain(cls, value):
            if value != "expected_value":
                raise ValueError("Invalid Value")
            return value

    result = TestModel(a="expected_value")
    assert result.a == "expected_value"

    with pytest.raises(ValidationError) as e:
        TestModel(a="abc")
    assert "Invalid Value" in str(e.value)

    with pytest.raises(ValidationError) as e:
        TestModel(a="<% sometemplate %>")
    assert "Invalid Value" in str(e.value)

    with context({"skip_validation_on_templates": True}):
        with pytest.raises(ValidationError) as e:
            TestModel(a="abc")
        assert "Invalid Value" in str(e.value)

        result = TestModel(a="<% sometemplate %>")
        assert result.a == "<% sometemplate %>"


def test_updatable_model_with_int_and_templates():
    class TestModel(UpdatableModel):
        a: int

    result = TestModel(a="123")
    assert result.a == 123

    with pytest.raises(ValidationError) as e:
        TestModel(a="<% sometemplate %>")
    assert "Input should be a valid integer" in str(e.value)

    with context({"skip_validation_on_templates": True}):
        with pytest.raises(ValidationError) as e:
            TestModel(a="abc")
        assert "Input should be a valid integer" in str(e.value)

        result = TestModel(a="<% sometemplate %>")
        assert result.a == "<% sometemplate %>"


def test_updatable_model_with_bool_and_templates():
    class TestModel(UpdatableModel):
        a: bool

    result = TestModel(a="true")
    assert result.a is True

    with pytest.raises(ValidationError) as e:
        TestModel(a="<% sometemplate %>")
    assert "Input should be a valid boolean" in str(e.value)

    with context({"skip_validation_on_templates": True}):
        with pytest.raises(ValidationError) as e:
            TestModel(a="abc")
        assert "Input should be a valid boolean" in str(e.value)

        result = TestModel(a="<% sometemplate %>")
        assert result.a == "<% sometemplate %>"


def test_updatable_model_with_sub_classes_and_template_values():
    class ParentModel(UpdatableModel):
        a: str

    class ChildModel(ParentModel):
        b: int

    result = ChildModel(a="any_value", b="123")
    assert result.b == 123

    with pytest.raises(ValidationError) as e:
        ChildModel(a="any_value", b="<% sometemplate %>")
    assert "Input should be a valid integer" in str(e.value)

    with context({"skip_validation_on_templates": True}):
        with pytest.raises(ValidationError) as e:
            ChildModel(a="any_value", b="abc")
        assert "Input should be a valid integer" in str(e.value)

        result = ChildModel(a="any_value", b="<% sometemplate %>")
        assert result.b == "<% sometemplate %>"


def test_updatable_model_with_sub_classes_and_template_values_and_custom_validator_in_parent():
    class ParentModel(UpdatableModel):
        a: str

        @field_validator("a", mode="before")
        @classmethod
        def validate_a_before(cls, value):
            if value != "expected_value":
                raise ValueError("Invalid Value")
            return value

    class ChildModel(ParentModel):
        b: str

    result = ChildModel(a="expected_value", b="any_value")
    assert result.a == "expected_value"

    with pytest.raises(ValidationError) as e:
        ChildModel(a="<% sometemplate %>", b="any_value")
    assert "Invalid Value" in str(e.value)

    with context({"skip_validation_on_templates": True}):
        with pytest.raises(ValidationError) as e:
            ChildModel(a="abc", b="any_value")
        assert "Invalid Value" in str(e.value)

        result = ChildModel(a="<% sometemplate %>", b="any_value")
        assert result.a == "<% sometemplate %>"


def test_updatable_model_with_sub_classes_and_template_values_and_custom_validator_in_child():
    class ParentModel(UpdatableModel):
        a: str

    class ChildModel(ParentModel):
        b: str

        @field_validator("b", mode="before")
        @classmethod
        def validate_b_before(cls, value):
            if value != "expected_value":
                raise ValueError("Invalid Value")
            return value

    result = ChildModel(a="any_value", b="expected_value")
    assert result.b == "expected_value"

    with pytest.raises(ValidationError) as e:
        ChildModel(a="any_value", b="<% sometemplate %>")
    assert "Invalid Value" in str(e.value)

    with context({"skip_validation_on_templates": True}):
        with pytest.raises(ValidationError) as e:
            ChildModel(a="any_value", b="abc")
        assert "Invalid Value" in str(e.value)

        result = ChildModel(a="any_value", b="<% sometemplate %>")
        assert result.b == "<% sometemplate %>"
