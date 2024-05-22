import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from textwrap import dedent
from unittest import mock

import pytest
from click import ClickException
from jinja2 import UndefinedError
from snowflake.cli.api.project.definition import load_project_definition
from snowflake.cli.api.project.schemas.project_definition import (
    ProjectDefinition,
)
from snowflake.cli.api.project.schemas.streamlit.streamlit import Streamlit
from snowflake.cli.api.utils.rendering import (
    _add_project_context,
    snowflake_sql_jinja_render,
)


def test_rendering_with_data():
    assert snowflake_sql_jinja_render("&{ foo }", data={"foo": "bar"}) == "bar"


@pytest.mark.parametrize(
    "text, output",
    [
        # Green path
        ("&{ foo }", "bar"),
        # Using $ as sf variable and basic jinja for server side
        ("${{ foo }}", "${{ foo }}"),
        ("$&{ foo }{{ var }}", "$bar{{ var }}"),
        ("${{ &{ foo } }}", "${{ bar }}"),
        # Using $ as sf variable and client side rendering
        ("$&{ foo }", "$bar"),
    ],
)
def test_rendering(text, output):
    assert snowflake_sql_jinja_render(text, data={"foo": "bar"}) == output


@pytest.mark.parametrize(
    "text",
    [
        """
    {% for item in navigation %}
        <li><a href="{{ item.href }}">{{ item.caption }}</a></li>
    {% endfor %}
    """,
        """{% if loop.index is divisibleby 3 %}""",
        """
    {% if True %}
        yay
    {% endif %}
    """,
    ],
)
def test_that_common_logic_block_are_ignored(text):
    assert snowflake_sql_jinja_render(text) == text


def test_that_common_comments_are_respected():
    # Make sure comment are ignored
    assert snowflake_sql_jinja_render("{# note a comment &{ foo } #}") == ""
    # Make sure comment's work together with templates
    assert (
        snowflake_sql_jinja_render("{# note a comment #}&{ foo }", data={"foo": "bar"})
        == "bar"
    )


def test_that_undefined_variables_raise_error():
    with pytest.raises(UndefinedError):
        snowflake_sql_jinja_render("&{ foo }")


def test_contex_can_access_environment_variable():
    assert snowflake_sql_jinja_render("&{ ctx.env.USER }") == os.environ.get("USER")


def test_resolve_variables_in_project_no_cross_variable_dependencies():
    pdf = ProjectDefinition(
        definition_version="1.1",
        env={
            "number": 1,
            "string": "foo",
            "boolean": True,
        },
    )
    result = _add_project_context(project_definition=pdf)
    assert result == {
        "ctx": ProjectDefinition(
            definition_version="1.1",
            native_app=None,
            snowpark=None,
            streamlit=None,
            env={"number": 1, "string": "foo", "boolean": True},
        )
    }


def test_resolve_variables_in_project_cross_variable_dependencies():
    pdf = ProjectDefinition(
        definition_version="1.1",
        env={
            "A": 42,
            "B": "b=<% ctx.env.A %>",
            "C": "<% ctx.env.B %> and <% ctx.env.A %>",
        },
    )
    result = _add_project_context(project_definition=pdf)
    assert result == {
        "ctx": ProjectDefinition(
            definition_version="1.1",
            native_app=None,
            snowpark=None,
            streamlit=None,
            env={"A": 42, "B": "b=42", "C": "b=42 and 42"},
        )
    }


def test_resolve_variables_in_project_cross_project_dependencies():
    pdf = ProjectDefinition(
        definition_version="1.1",
        streamlit=Streamlit(name="my_app"),
        env={"app": "name of streamlit is <% ctx.streamlit.name %>"},
    )
    result = _add_project_context(project_definition=pdf)
    assert result == {
        "ctx": ProjectDefinition(
            definition_version="1.1",
            native_app=None,
            snowpark=None,
            streamlit=Streamlit(
                name="my_app",
                stage="streamlit",
                query_warehouse="streamlit",
                main_file="streamlit_app.py",
                env_file=None,
                pages_dir=None,
                additional_source_files=None,
            ),
            env={"app": "name of streamlit is my_app"},
        )
    }


@mock.patch.dict(
    os.environ,
    {
        "lowercase": "new_lowercase_value",
        "UPPERCASE": "new_uppercase_value",
        "should_be_replace_by_env": "test succeeded",
        "value_from_env": "this comes from os.environ",
    },
)
def test_resolve_variables_in_project_environment_variables_precedence():
    pdf = ProjectDefinition(
        definition_version="1.1",
        env={
            "should_be_replace_by_env": "test failed",
            "test_variable": "<% ctx.env.lowercase %> and <% ctx.env.UPPERCASE %>",
            "test_variable_2": "<% ctx.env.value_from_env %>",
        },
    )
    result = _add_project_context(project_definition=pdf)

    assert result == {
        "ctx": ProjectDefinition(
            definition_version="1.1",
            native_app=None,
            snowpark=None,
            streamlit=None,
            env={
                "should_be_replace_by_env": "test succeeded",
                "test_variable": "new_lowercase_value and new_uppercase_value",
                "test_variable_2": "this comes from os.environ",
            },
        )
    }


@pytest.mark.parametrize(
    "env, cycle",
    [
        ({"A": "<% ctx.env.A %>"}, "A"),
        ({"A": "<% ctx.env.B %>", "B": "<% ctx.env.A %>"}, "A -> B"),
        (
            {
                "A": "<% ctx.env.B %>",
                "B": "<% ctx.env.C %>",
                "C": "<% ctx.env.D %>",
                "D": "<% ctx.env.A %>",
            },
            "A -> B -> C -> D",
        ),
    ],
)
def test_resolve_variables_error_on_cycle(env, cycle):
    pdf = ProjectDefinition(
        definition_version="1.1",
        env=env,
    )
    with pytest.raises(ClickException) as err:
        _add_project_context(project_definition=pdf)

    assert err.value.message == f"Cycle detected between variables: {cycle}"


@pytest.mark.parametrize(
    "env, msg",
    [
        ({"app": "<% bdbdbd %>"}, "'bdbdbd' is undefined"),
        ({"app": "<% ctx.streamlit.name %>"}, "'None' has no attribute 'name'"),
        ({"app": "<% ctx.foo %>"}, "has no attribute 'foo'"),
    ],
)
def test_resolve_variables_fails_if_referencing_unknown_variable(env, msg):
    pdf = ProjectDefinition(
        definition_version="1.1",
        env=env,
    )
    with pytest.raises(UndefinedError) as err:
        _add_project_context(project_definition=pdf)
    assert msg in str(err.value)


def tests_unquoted_template_usage_in_strings_yaml():
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
        definition = load_project_definition([p])

    _add_project_context(project_definition=definition)
    assert definition.env == {
        "block_multiline": "this is multiline string \nwith template Snowflake is great!\n",
        "flow_multiline_not_quoted": "this is multiline string with template Snowflake is great!",
        "flow_multiline_quoted": "this is multiline string with template Snowflake is great!",
        "single_line": "Snowflake is great!",
        "value": "Snowflake is great!",
    }
