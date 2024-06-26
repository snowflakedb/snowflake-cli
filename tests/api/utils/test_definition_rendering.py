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
from textwrap import dedent
from unittest import mock

import pytest
from snowflake.cli.api.exceptions import CycleDetectedError, InvalidTemplate
from snowflake.cli.api.project.definition import load_project
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.utils.definition_rendering import render_definition_template
from snowflake.cli.api.utils.models import ProjectEnvironment

from tests.nativeapp.utils import NATIVEAPP_MODULE


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

    result = render_definition_template(definition, {}).project_context

    assert result == {
        "ctx": {
            "definition_version": "1.1",
            "env": ProjectEnvironment(
                {"number": 1, "string": "foo", "boolean": True}, {}
            ),
        }
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
    result = render_definition_template(definition, {}).project_context

    assert result == {
        "ctx": {
            "definition_version": "1.1",
            "env": ProjectEnvironment({"A": 42, "B": "b=42", "C": "b=42 and 42"}, {}),
        }
    }


@mock.patch.dict(os.environ, {}, clear=True)
def test_env_not_supported_in_version_1():
    definition = {
        "definition_version": "1",
        "env": {
            "A": "42",
            "B": "b=<% ctx.env.A %>",
            "C": "<% ctx.env.B %> and <% ctx.env.A %>",
        },
    }
    with pytest.raises(SchemaValidationError):
        render_definition_template(definition, {})


@mock.patch.dict(os.environ, {"A": "value"}, clear=True)
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
def test_no_resolve_and_warning_in_version_1(warning_mock):
    definition = {
        "definition_version": "1",
        "native_app": {"name": "test_source_<% ctx.env.A %>", "artifacts": []},
    }
    result = render_definition_template(definition, {}).project_context

    assert result == {
        "ctx": {
            "definition_version": "1",
            "native_app": {"name": "test_source_<% ctx.env.A %>", "artifacts": []},
            "env": ProjectEnvironment({}, {}),
        }
    }
    warning_mock.assert_called_once_with(
        "Ignoring template pattern in project definition file. "
        "Update 'definition_version' to 1.1 or later in snowflake.yml to enable template expansion."
    )


@mock.patch.dict(os.environ, {"A": "value"}, clear=True)
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
def test_partial_invalid_template_in_version_1(warning_mock):
    definition = {
        "definition_version": "1",
        "native_app": {"name": "test_source_<% ctx.env.A", "artifacts": []},
    }
    result = render_definition_template(definition, {}).project_context

    assert result == {
        "ctx": {
            "definition_version": "1",
            "native_app": {"name": "test_source_<% ctx.env.A", "artifacts": []},
            "env": ProjectEnvironment({}, {}),
        }
    }
    # we still want to warn if there was an incorrect attempt to use templating
    warning_mock.assert_called_once_with(
        "Ignoring template pattern in project definition file. "
        "Update 'definition_version' to 1.1 or later in snowflake.yml to enable template expansion."
    )


@mock.patch.dict(os.environ, {"A": "value"}, clear=True)
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
def test_no_warning_in_version_1_1(warning_mock):
    definition = {
        "definition_version": "1.1",
        "native_app": {"name": "test_source_<% ctx.env.A %>", "artifacts": []},
    }
    result = render_definition_template(definition, {}).project_context

    assert result == {
        "ctx": {
            "definition_version": "1.1",
            "native_app": {"name": "test_source_value", "artifacts": []},
            "env": ProjectEnvironment({}, {}),
        }
    }
    warning_mock.assert_not_called()


@mock.patch.dict(os.environ, {"A": "value"}, clear=True)
def test_invalid_template_in_version_1_1():
    definition = {
        "definition_version": "1.1",
        "native_app": {"name": "test_source_<% ctx.env.A", "artifacts": []},
    }
    with pytest.raises(InvalidTemplate) as err:
        render_definition_template(definition, {})

    assert err.value.message.startswith(
        "Error parsing template from project definition file. "
        "Value: 'test_source_<% ctx.env.A'. "
        "Error: unexpected end of template, expected 'end of print statement'."
    )


@mock.patch.dict(os.environ, {}, clear=True)
def test_resolve_variables_in_project_cross_project_dependencies():
    definition = {
        "definition_version": "1.1",
        "streamlit": {"name": "my_app"},
        "env": {"app": "name of streamlit is <% ctx.streamlit.name %>"},
    }
    result = render_definition_template(definition, {}).project_context
    assert result == {
        "ctx": {
            "definition_version": "1.1",
            "streamlit": {"name": "my_app"},
            "env": ProjectEnvironment(
                {
                    "app": "name of streamlit is my_app",
                },
                {},
            ),
        }
    }


@mock.patch.dict(
    os.environ,
    {
        "lowercase": "new_lowercase_value",
        "UPPERCASE": "new_uppercase_value",
        "should_be_replaced_by_env": "test succeeded",
        "value_from_env": "this comes from os.environ",
    },
    clear=True,
)
def test_resolve_variables_in_project_environment_variables_precedence():
    definition = {
        "definition_version": "1.1",
        "env": {
            "should_be_replaced_by_env": "test failed",
            "test_variable": "<% ctx.env.lowercase %> and <% ctx.env.UPPERCASE %>",
            "test_variable_2": "<% ctx.env.value_from_env %>",
        },
    }
    result = render_definition_template(definition, {}).project_context

    assert result == {
        "ctx": {
            "definition_version": "1.1",
            "env": ProjectEnvironment(
                {
                    "should_be_replaced_by_env": "test failed",
                    "test_variable": "new_lowercase_value and new_uppercase_value",
                    "test_variable_2": "this comes from os.environ",
                },
                {},
            ),
        }
    }
    assert result["ctx"]["env"]["lowercase"] == "new_lowercase_value"
    assert result["ctx"]["env"]["UPPERCASE"] == "new_uppercase_value"
    assert result["ctx"]["env"]["should_be_replaced_by_env"] == "test succeeded"
    assert result["ctx"]["env"]["value_from_env"] == "this comes from os.environ"


@mock.patch.dict(os.environ, {"env_var": "<% ctx.definition_version %>"}, clear=True)
def test_env_variables_do_not_get_resolved():
    definition = {
        "definition_version": "1.1",
        "native_app": {"name": "test_source_<% ctx.env.env_var %>", "artifacts": []},
        "env": {
            "reference_to_name": "<% ctx.native_app.name %>",
        },
    }
    result = render_definition_template(definition, {}).project_context

    assert result == {
        "ctx": {
            "definition_version": "1.1",
            "native_app": {
                "name": "test_source_<% ctx.definition_version %>",
                "artifacts": [],
            },
            "env": ProjectEnvironment(
                {
                    "reference_to_name": "test_source_<% ctx.definition_version %>",
                },
                {},
            ),
        }
    }

    assert result["ctx"]["env"]["env_var"] == "<% ctx.definition_version %>"


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
            "native_app": {"name": "test_<% ctx.env.A %>", "artifacts": []},
            "env": {"A": "<% ctx.native_app.name %>"},
        },
        {
            "definition_version": "1.1",
            "native_app": {"name": "test_<% ctx.native_app.name %>", "artifacts": []},
        },
        {
            "definition_version": "1.1",
            "native_app": {
                "name": "test_<% ctx.native_app.source_stage %>",
                "artifacts": [],
                "source_stage": "stage <% ctx.native_app.name %>",
            },
        },
    ],
)
def test_resolve_variables_error_on_cycle(definition):
    with pytest.raises(CycleDetectedError) as err:
        render_definition_template(definition, {})

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
    with pytest.raises(InvalidTemplate) as err:
        render_definition_template(definition, {})

    assert (
        err.value.message
        == f"Template variable {error_var} does not have a scalar value"
    )


@mock.patch.dict(os.environ, {"blank_env": ""}, clear=True)
def test_resolve_variables_blank_is_ok():
    definition = {
        "definition_version": "1.1",
        "native_app": {
            "name": "<% ctx.env.blank_default_env %>",
            "deploy_root": "<% ctx.env.blank_env %>",
            "artifacts": [],
        },
        "env": {
            "blank_default_env": "",
        },
    }
    result = render_definition_template(definition, {}).project_context

    assert result == {
        "ctx": {
            "definition_version": "1.1",
            "native_app": {"name": "", "deploy_root": "", "artifacts": []},
            "env": ProjectEnvironment(
                {
                    "blank_default_env": "",
                },
                {},
            ),
        }
    }

    assert result["ctx"]["env"]["blank_env"] == ""


@pytest.mark.parametrize(
    "env, msg",
    [
        ({"app": "<% bdbdbd %>"}, "Could not find template variable bdbdbd"),
        (
            {"app": "<% ctx.streamlit.name %>"},
            "Could not find template variable ctx.streamlit.name",
        ),
        ({"app": "<% ctx.foo %>"}, "Could not find template variable ctx.foo"),
        ({"app": "<% ctx.env.get %>"}, "Could not find template variable ctx.env.get"),
        (
            {"app": "<% ctx.env.__class__ %>"},
            "Could not find template variable ctx.env.__class__",
        ),
        (
            {"app": "<% ctx.native_app.__class__ %>"},
            "Could not find template variable ctx.native_app.__class__",
        ),
        (
            {"app": "<% ctx.native_app.schema %>"},
            "Could not find template variable ctx.native_app.schema",
        ),
    ],
)
def test_resolve_variables_fails_if_referencing_unknown_variable(env, msg):
    definition = {
        "definition_version": "1.1",
        "native_app": {"name": "test_name", "artifacts": []},
        "env": env,
    }
    with pytest.raises(InvalidTemplate) as err:
        render_definition_template(definition, {})
    assert msg == err.value.message


@mock.patch.dict(os.environ, {}, clear=True)
def test_unquoted_template_usage_in_strings_yaml(named_temporary_file):
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

    with named_temporary_file(suffix=".yml") as p:
        p.write_text(dedent(text))
        project_definition = load_project([p]).project_definition

    assert project_definition.env == ProjectEnvironment(
        {
            "block_multiline": "this is multiline string \nwith template Snowflake is great!\n",
            "flow_multiline_not_quoted": "this is multiline string with template Snowflake is great!",
            "flow_multiline_quoted": "this is multiline string with template Snowflake is great!",
            "single_line": "Snowflake is great!",
            "value": "Snowflake is great!",
        },
        {},
    )


@mock.patch.dict(os.environ, {"var_with_yml": "    - app/*\n    - src\n"}, clear=True)
def test_injected_yml_in_env_should_not_be_expanded():
    definition = {
        "definition_version": "1.1",
        "env": {
            "test_env": "<% ctx.env.var_with_yml %>",
        },
    }
    result = render_definition_template(definition, {}).project_context

    assert result == {
        "ctx": {
            "definition_version": "1.1",
            "env": ProjectEnvironment(
                {
                    "test_env": "    - app/*\n    - src\n",
                },
                {},
            ),
        }
    }

    assert result["ctx"]["env"]["var_with_yml"] == "    - app/*\n    - src\n"


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
        render_definition_template(definition, {})

    assert err.value.message == f"Unexpected templating syntax in {template_value}"


def test_invalid_type_for_env_section():
    definition = {
        "definition_version": "1.1",
        "env": ["test_env", "array_val1"],
    }
    with pytest.raises(InvalidTemplate) as err:
        render_definition_template(definition, {})

    assert (
        err.value.message
        == "env section in project definition file should be a mapping"
    )


def test_invalid_type_for_env_variable():
    definition = {
        "definition_version": "1.1",
        "env": {
            "test_env": ["array_val1"],
        },
    }
    with pytest.raises(InvalidTemplate) as err:
        render_definition_template(definition, {})

    assert (
        err.value.message
        == "Variable test_env in env section of project definition file should be a scalar"
    )


@mock.patch.dict(os.environ, {"env_var_test": "value_from_os_env"}, clear=True)
def test_env_priority_from_cli_and_os_env_and_project_env():
    definition = {
        "definition_version": "1.1",
        "env": {
            "env_var_test": "value_from_definition_file",
            "final_value": "<% ctx.env.env_var_test %>",
        },
    }
    result = render_definition_template(
        definition, {"ctx": {"env": {"env_var_test": "value_from_cli_override"}}}
    ).project_context

    assert result == {
        "ctx": {
            "definition_version": "1.1",
            "env": ProjectEnvironment(
                {
                    "env_var_test": "value_from_definition_file",
                    "final_value": "value_from_cli_override",
                },
                {"env_var_test": "value_from_cli_override"},
            ),
        }
    }

    assert result["ctx"]["env"]["env_var_test"] == "value_from_cli_override"


@mock.patch.dict(os.environ, {}, clear=True)
def test_values_env_from_only_overrides():
    definition = {
        "definition_version": "1.1",
        "env": {
            "final_value": "<% ctx.env.env_var_test %>",
        },
    }
    result = render_definition_template(
        definition, {"ctx": {"env": {"env_var_test": "value_from_cli_override"}}}
    ).project_context

    assert result == {
        "ctx": {
            "definition_version": "1.1",
            "env": ProjectEnvironment(
                {"final_value": "value_from_cli_override"},
                {"env_var_test": "value_from_cli_override"},
            ),
        }
    }

    assert result["ctx"]["env"]["env_var_test"] == "value_from_cli_override"


@mock.patch.dict(os.environ, {}, clear=True)
def test_cli_env_var_blank():
    definition = {
        "definition_version": "1.1",
    }
    result = render_definition_template(
        definition, {"ctx": {"env": {"env_var_test": ""}}}
    ).project_context

    assert result == {
        "ctx": {
            "definition_version": "1.1",
            "env": ProjectEnvironment(
                {},
                {"env_var_test": ""},
            ),
        }
    }

    assert result["ctx"]["env"]["env_var_test"] == ""


@mock.patch.dict(os.environ, {}, clear=True)
def test_cli_env_var_does_not_expand_with_templating():
    definition = {
        "definition_version": "1.1",
    }
    result = render_definition_template(
        definition, {"ctx": {"env": {"env_var_test": "<% ctx.env.something %>"}}}
    ).project_context

    assert result == {
        "ctx": {
            "definition_version": "1.1",
            "env": ProjectEnvironment(
                {},
                {"env_var_test": "<% ctx.env.something %>"},
            ),
        }
    }

    assert result["ctx"]["env"]["env_var_test"] == "<% ctx.env.something %>"


@mock.patch.dict(os.environ, {"os_env_var": "os_env_var_value"}, clear=True)
def test_os_env_and_override_envs_in_version_1():
    definition = {
        "definition_version": "1",
    }

    override_ctx = {"ctx": {"env": {"override_env": "override_env_value"}}}
    result = render_definition_template(definition, override_ctx).project_context

    assert result == {
        "ctx": {
            "definition_version": "1",
            "env": ProjectEnvironment(
                {},
                {"override_env": "override_env_value"},
            ),
        }
    }

    assert result["ctx"]["env"]["override_env"] == "override_env_value"
    assert result["ctx"]["env"]["os_env_var"] == "os_env_var_value"
