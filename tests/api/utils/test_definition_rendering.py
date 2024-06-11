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
from pathlib import Path
from tempfile import NamedTemporaryFile
from textwrap import dedent
from unittest import mock

import pytest
from jinja2 import UndefinedError
from snowflake.cli.api.exceptions import CycleDetectedError, InvalidTemplate
from snowflake.cli.api.project.definition import load_project
from snowflake.cli.api.utils.definition_rendering import render_definition_template


@mock.patch.dict(os.environ, {}, clear=True)
def test_resolve_variables_in_project_no_cross_variable_dependencies():
    definition = {
        "definition_version": "1.1",
        "env": {
            "number": 1,
            "string": "foo",
            "boolean": True,
        },
    }

    result = render_definition_template(definition)

    assert result == {
        "definition_version": "1.1",
        "env": {"number": 1, "string": "foo", "boolean": True},
    }


@mock.patch.dict(os.environ, {}, clear=True)
def test_resolve_variables_in_project_cross_variable_dependencies():
    definition = {
        "definition_version": "1.1",
        "env": {
            "A": 42,
            "B": "b=<% ctx.env.A %>",
            "C": "<% ctx.env.B %> and <% ctx.env.A %>",
        },
    }
    result = render_definition_template(definition)

    assert result == {
        "definition_version": "1.1",
        "env": {"A": 42, "B": "b=42", "C": "b=42 and 42"},
    }


@mock.patch.dict(os.environ, {}, clear=True)
def test_no_resolve_in_version_1_0():
    definition = {
        "definition_version": "1.0",
        "env": {
            "A": 42,
            "B": "b=<% ctx.env.A %>",
            "C": "<% ctx.env.B %> and <% ctx.env.A %>",
        },
    }
    result = render_definition_template(definition)

    assert result == {
        "definition_version": "1.0",
        "env": {
            "A": 42,
            "B": "b=<% ctx.env.A %>",
            "C": "<% ctx.env.B %> and <% ctx.env.A %>",
        },
    }


@mock.patch.dict(os.environ, {}, clear=True)
def test_resolve_variables_in_project_cross_project_dependencies():
    definition = {
        "definition_version": "1.1",
        "streamlit": {"name": "my_app"},
        "env": {"app": "name of streamlit is <% ctx.streamlit.name %>"},
    }
    result = render_definition_template(definition)
    assert result == {
        "definition_version": "1.1",
        "streamlit": {"name": "my_app"},
        "env": {
            "app": "name of streamlit is my_app",
        },
    }


@mock.patch.dict(
    os.environ,
    {
        "lowercase": "new_lowercase_value",
        "UPPERCASE": "new_uppercase_value",
        "should_be_replace_by_env": "test succeeded",
        "value_from_env": "this comes from os.environ",
    },
    clear=True,
)
def test_resolve_variables_in_project_environment_variables_precedence():
    definition = {
        "definition_version": "1.1",
        "env": {
            "should_be_replace_by_env": "test failed",
            "test_variable": "<% ctx.env.lowercase %> and <% ctx.env.UPPERCASE %>",
            "test_variable_2": "<% ctx.env.value_from_env %>",
        },
    }
    result = render_definition_template(definition)

    assert result == {
        "definition_version": "1.1",
        "env": {
            "UPPERCASE": "new_uppercase_value",
            "lowercase": "new_lowercase_value",
            "should_be_replace_by_env": "test succeeded",
            "test_variable": "new_lowercase_value and new_uppercase_value",
            "test_variable_2": "this comes from os.environ",
            "value_from_env": "this comes from os.environ",
        },
    }


@mock.patch.dict(os.environ, {"env_var": "<% ctx.definition_version %>"}, clear=True)
def test_env_variables_do_not_get_resolved():
    definition = {
        "definition_version": "1.1",
        "native_app": {
            "name": "test_source_<% ctx.env.env_var %>",
        },
        "env": {
            "reference_to_name": "<% ctx.native_app.name %>",
        },
    }
    result = render_definition_template(definition)

    assert result == {
        "definition_version": "1.1",
        "native_app": {
            "name": "test_source_<% ctx.definition_version %>",
        },
        "env": {
            "reference_to_name": "test_source_<% ctx.definition_version %>",
            "env_var": "<% ctx.definition_version %>",
        },
    }


@pytest.mark.parametrize(
    "definition",
    [
        {"definition_version": "1.1", "env": {"A": "<% ctx.env.A %>"}},
        {
            "definition_version": "1.1",
            "env": {"A": "<% ctx.env.B %>", "B": "<% ctx.env.A %>"},
        },
        {
            "definition_version": "1.1",
            "env": {
                "A": "<% ctx.env.B %>",
                "B": "<% ctx.env.C %>",
                "C": "<% ctx.env.D %>",
                "D": "<% ctx.env.A %>",
            },
        },
        {
            "definition_version": "1.1",
            "native_app": {"name": "test_<% ctx.env.A %>"},
            "env": {"A": "<% ctx.native_app.name %>"},
        },
        {
            "definition_version": "1.1",
            "native_app": {"name": "test_<% ctx.native_app.name %>"},
        },
        {
            "definition_version": "1.1",
            "native_app": {
                "name": "test_<% ctx.native_app.source_stage %>",
                "source_stage": "stage <% ctx.native_app.name %>",
            },
        },
    ],
)
def test_resolve_variables_error_on_cycle(definition):
    with pytest.raises(CycleDetectedError) as err:
        render_definition_template(definition)

    assert err.value.message.startswith("Cycle detected in templating variable ")


@pytest.mark.parametrize(
    "definition, error_var",
    [
        (
            {
                "definition_version": "1.1",
                "native_app": {
                    "name": "app_name",
                    "artifacts": [{"src": "src/add.py", "dest": "add.py"}],
                },
                "env": {"A": "<% ctx.native_app.artifacts %>"},
            },
            "ctx.native_app.artifacts",
        ),
        (
            {
                "definition_version": "1.1",
                "native_app": {
                    "name": "app_name",
                    "artifacts": [{"src": "src/add.py", "dest": "add.py"}],
                },
                "env": {"A": "<% ctx.native_app %>"},
            },
            "ctx.native_app",
        ),
    ],
)
def test_resolve_variables_reference_non_scalar(definition, error_var):
    with pytest.raises(UndefinedError) as err:
        render_definition_template(definition)

    assert (
        err.value.message
        == f"Template variable {error_var} does not contain a valid value"
    )


@mock.patch.dict(os.environ, {"blank_env": ""}, clear=True)
def test_resolve_variables_blank_is_ok():
    definition = {
        "definition_version": "1.1",
        "native_app": {
            "name": "<% ctx.env.blank_default_env %>",
            "source_stage": "",
            "deploy_root": "<% ctx.env.blank_env %>",
        },
        "env": {
            "blank_default_env": "",
            "refers_to_blank": "<% ctx.native_app.source_stage %>",
        },
    }
    result = render_definition_template(definition)

    assert result == {
        "definition_version": "1.1",
        "native_app": {"name": "", "source_stage": "", "deploy_root": ""},
        "env": {
            "blank_env": "",
            "blank_default_env": "",
            "refers_to_blank": "",
        },
    }


@pytest.mark.parametrize(
    "env, msg",
    [
        ({"app": "<% bdbdbd %>"}, "Could not find template variable bdbdbd"),
        (
            {"app": "<% ctx.streamlit.name %>"},
            "Could not find template variable ctx.streamlit.name",
        ),
        ({"app": "<% ctx.foo %>"}, "Could not find template variable ctx.foo"),
    ],
)
def test_resolve_variables_fails_if_referencing_unknown_variable(env, msg):
    definition = {
        "definition_version": "1.1",
        "env": env,
    }
    with pytest.raises(UndefinedError) as err:
        render_definition_template(definition)
    assert msg in str(err.value)


@mock.patch.dict(os.environ, {}, clear=True)
def test_unquoted_template_usage_in_strings_yaml():
    text = """\
    definition_version: "1.1"
    env:
        value: "Snowflake is great!"
        single_line: <% ctx.env.value %>
        flow_multiline_quoted: "this is
            multiline string with template <% ctx.env.value %>"
        flow_multiline_not_quoted: this is
            multiline string with template <% ctx.env.value %>
        block_multiline: |
            this is multiline string 
            with template <% ctx.env.value %>
    """

    with NamedTemporaryFile(suffix=".yml") as file:
        p = Path(file.name)
        p.write_text(dedent(text))
        project_definition = load_project([p]).project_definition

    assert project_definition.env == {
        "block_multiline": "this is multiline string \nwith template Snowflake is great!\n",
        "flow_multiline_not_quoted": "this is multiline string with template Snowflake is great!",
        "flow_multiline_quoted": "this is multiline string with template Snowflake is great!",
        "single_line": "Snowflake is great!",
        "value": "Snowflake is great!",
    }


@mock.patch.dict(os.environ, {"var_with_yml": "    - app/*\n    - src\n"}, clear=True)
def test_injected_yml_in_env_should_not_be_expanded():
    definition = {
        "definition_version": "1.1",
        "env": {
            "test_env": "<% ctx.env.var_with_yml %>",
        },
    }
    result = render_definition_template(definition)

    assert result == {
        "definition_version": "1.1",
        "env": {
            "test_env": "    - app/*\n    - src\n",
            "var_with_yml": "    - app/*\n    - src\n",
        },
    }


@pytest.mark.parametrize(
    "template_value",
    [
        "<% ctx.env.0 %>",
        "<% ctx.env[0] %>",
        "<% ctx.0.env %>",
        "<% ctx[definition_version] %>",
    ],
)
@mock.patch.dict(os.environ, {}, clear=True)
def test_invalid_templating_syntax(template_value):
    definition = {
        "definition_version": "1.1",
        "env": {
            "test_env": template_value,
        },
    }
    with pytest.raises(InvalidTemplate) as err:
        render_definition_template(definition)

    assert err.value.message == f"Unexpected templating syntax in {template_value}"
