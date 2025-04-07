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

import os
from unittest import mock

import pytest
from snowflake.cli.api.exceptions import InvalidTemplateError
from snowflake.cli.api.utils.definition_rendering import render_definition_template
from snowflake.cli.api.utils.templating_functions import get_templating_functions


def test_template_unknown_function():
    definition = {
        "definition_version": "1.1",
        "env": {
            "value": "<% fn.unknown_func('hello') %>",
        },
    }

    with pytest.raises(InvalidTemplateError) as err:
        render_definition_template(definition, {})

    assert "Could not find template variable fn.unknown_func" in err.value.message


def test_available_templating_functions():
    result = get_templating_functions()
    assert sorted(result.keys()) == sorted(
        [
            "id_to_str",
            "str_to_id",
            "concat_ids",
            "get_username",
            "sanitize_id",
        ]
    )


@pytest.mark.parametrize(
    "input_list, expected_output",
    [
        # test concatenate a constant with a variable -> quoted
        (["'first_'", "ctx.definition_version"], '"first_1.1"'),
        # test concatenate valid unquoted values  -> non-quoted
        (["'first_'", "'second'"], "first_second"),
        # test concatenate unquoted ids with unsafe chars -> quoted
        (["'first.'", "'second'"], '"first.second"'),
        # all safe chars, one with quoted id -> quoted
        (["'first_'", "'second_'", "'\"third\"'"], '"first_second_third"'),
        # one word, unsafe chars -> quoted
        (["'first.'"], '"first."'),
        # one word, safe chars -> non-quoted
        (["'first'"], "first"),
        # blank input -> quoted blank output
        (["''", "''"], '""'),
    ],
)
def test_concat_ids_with_valid_values(input_list, expected_output):
    input_list_str = ", ".join(input_list)
    definition = {
        "definition_version": "1.1",
        "env": {
            "value": f"<% fn.concat_ids({input_list_str}) %>",
        },
    }

    result = render_definition_template(definition, {}).project_context
    env = result.get("ctx", {}).get("env", {})
    assert env.get("value") == expected_output


def test_concat_ids_with_no_args():
    definition = {
        "definition_version": "1.1",
        "env": {
            "value": "<% fn.concat_ids() %>",
        },
    }

    with pytest.raises(InvalidTemplateError) as err:
        render_definition_template(definition, {})

    assert "concat_ids requires at least 1 argument(s)" in err.value.message


def test_concat_ids_with_non_string_arg():
    definition = {
        "definition_version": "1.1",
        "env": {
            "value": "<% fn.concat_ids(123) %>",
        },
    }

    with pytest.raises(InvalidTemplateError) as err:
        render_definition_template(definition, {})

    assert "concat_ids only accepts String values" in err.value.message


@pytest.mark.parametrize(
    "input_val, expected_output",
    [
        # unquoted safe -> unchanged
        ("first", "first"),
        # unquoted unsafe -> unchanged
        ("first.second", "first.second"),
        # looks like quoted but invalid -> unchanged
        ('"first"second"', '"first"second"'),
        # valid quoted -> unquoted
        ('"first""second"', 'first"second'),
        # unquoted blank -> blank
        ("", ""),
        # quoted blank -> blank
        ('""', ""),
    ],
)
def test_id_to_str_valid_values(input_val, expected_output):
    definition = {
        "definition_version": "1.1",
        "env": {
            "input_value": input_val,
            "output_value": "<% fn.id_to_str(ctx.env.input_value) %>",
        },
    }

    result = render_definition_template(definition, {}).project_context
    env = result.get("ctx", {}).get("env", {})
    assert env.get("output_value") == expected_output


def test_id_to_str_with_no_args():
    definition = {
        "definition_version": "1.1",
        "env": {
            "value": "<% fn.id_to_str() %>",
        },
    }

    with pytest.raises(InvalidTemplateError) as err:
        render_definition_template(definition, {})

    assert "id_to_str requires at least 1 argument(s)" in err.value.message


def test_id_to_str_with_two_args():
    definition = {
        "definition_version": "1.1",
        "env": {
            "value": "<% fn.id_to_str('a', 'b') %>",
        },
    }

    with pytest.raises(InvalidTemplateError) as err:
        render_definition_template(definition, {})

    assert "id_to_str supports at most 1 argument(s)" in err.value.message


def test_id_to_str_with_non_string_arg():
    definition = {
        "definition_version": "1.1",
        "env": {
            "value": "<% fn.id_to_str(123) %>",
        },
    }

    with pytest.raises(InvalidTemplateError) as err:
        render_definition_template(definition, {})

    assert "id_to_str only accepts String values" in err.value.message


@pytest.mark.parametrize(
    "input_val, expected_output",
    [
        # unquoted safe -> unchanged
        ("first", "first"),
        # unquoted unsafe -> quoted
        ("first.second", '"first.second"'),
        # looks like quoted but invalid -> quote it and escape
        ('"first"second"', '"""first""second"""'),
        # valid quoted -> unchanged
        ('"first""second"', '"first""second"'),
        # blank -> quoted blank
        ("", '""'),
        # quoted blank -> unchanged
        ('""', '""'),
    ],
)
def test_str_to_id_valid_values(input_val, expected_output):
    definition = {
        "definition_version": "1.1",
        "env": {
            "input_value": input_val,
            "output_value": "<% fn.str_to_id(ctx.env.input_value) %>",
        },
    }

    result = render_definition_template(definition, {}).project_context
    env = result.get("ctx", {}).get("env", {})
    assert env.get("output_value") == expected_output


def test_str_to_id_with_no_args():
    definition = {
        "definition_version": "1.1",
        "env": {
            "value": "<% fn.str_to_id() %>",
        },
    }

    with pytest.raises(InvalidTemplateError) as err:
        render_definition_template(definition, {})

    assert "str_to_id requires at least 1 argument(s)" in err.value.message


def test_str_to_id_with_two_args():
    definition = {
        "definition_version": "1.1",
        "env": {
            "value": "<% fn.str_to_id('a', 'b') %>",
        },
    }

    with pytest.raises(InvalidTemplateError) as err:
        render_definition_template(definition, {})

    assert "str_to_id supports at most 1 argument(s)" in err.value.message


def test_str_to_id_with_non_string_arg():
    definition = {
        "definition_version": "1.1",
        "env": {
            "value": "<% fn.str_to_id(123) %>",
        },
    }

    with pytest.raises(InvalidTemplateError) as err:
        render_definition_template(definition, {})

    assert "str_to_id only accepts String values" in err.value.message


@pytest.mark.parametrize(
    "os_environ, expected_output",
    [
        ({"USER": "test_user"}, "test_user"),
        ({"USERNAME": "test_user"}, "test_user"),
        ({}, ""),
    ],
)
def test_get_username_valid_values(os_environ, expected_output):
    definition = {
        "definition_version": "1.1",
        "env": {
            "output_value": "<% fn.get_username() %>",
        },
    }

    with mock.patch.dict(os.environ, os_environ, clear=True):
        result = render_definition_template(definition, {}).project_context

    env = result.get("ctx", {}).get("env", {})
    assert env.get("output_value") == expected_output


@mock.patch.dict(os.environ, {}, clear=True)
def test_get_username_with_fallback_value():
    definition = {
        "definition_version": "1.1",
        "env": {
            "output_value": "<% fn.get_username('fallback_user') %>",
        },
    }

    result = render_definition_template(definition, {}).project_context

    env = result.get("ctx", {}).get("env", {})
    assert env.get("output_value") == "fallback_user"


def test_get_username_with_two_args_should_fail():
    definition = {
        "definition_version": "1.1",
        "env": {
            "value": "<% fn.get_username('a', 'b') %>",
        },
    }

    with pytest.raises(InvalidTemplateError) as err:
        render_definition_template(definition, {})

    assert "get_username supports at most 1 argument(s)" in err.value.message


@pytest.mark.parametrize(
    "input_value, expected_output",
    [
        ("test_value", "test_value"),
        (" T'EST_Va l.u-e", "TEST_Value"),
        ("", "_"),
        ('""', "_"),
        ("_val.ue", "_value"),
        ("1val.ue", "_1value"),
        ('"some_id"', "some_id"),
        ("a." + "b" * 254 + "c", "a" + "b" * 254),
    ],
)
def test_sanitize_id_valid_values(input_value, expected_output):
    definition = {
        "definition_version": "1.1",
        "env": {
            "input_value": input_value,
            "output_value": "<% fn.sanitize_id(ctx.env.input_value) %>",
        },
    }

    result = render_definition_template(definition, {}).project_context

    env = result.get("ctx", {}).get("env", {})
    assert env.get("output_value") == expected_output


def test_sanitize_id_with_no_args():
    definition = {
        "definition_version": "1.1",
        "env": {
            "value": "<% fn.sanitize_id() %>",
        },
    }

    with pytest.raises(InvalidTemplateError) as err:
        render_definition_template(definition, {})

    assert "sanitize_id requires at least 1 argument(s)" in err.value.message


def test_sanitize_id_with_two_args():
    definition = {
        "definition_version": "1.1",
        "env": {
            "value": "<% fn.sanitize_id('a', 'b') %>",
        },
    }

    with pytest.raises(InvalidTemplateError) as err:
        render_definition_template(definition, {})

    assert "sanitize_id supports at most 1 argument(s)" in err.value.message
