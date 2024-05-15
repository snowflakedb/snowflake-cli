import os

import pytest
from jinja2 import UndefinedError
from snowflake.cli.api.project.schemas.project_definition import (
    ProjectDefinition,
)
from snowflake.cli.api.project.schemas.streamlit.streamlit import Streamlit
from snowflake.cli.api.utils.rendering import (
    _add_project_context,
    snowflake_cli_jinja_render,
)


def test_rendering_with_data():
    assert snowflake_cli_jinja_render("&{ foo }", data={"foo": "bar"}) == "bar"


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
    assert snowflake_cli_jinja_render(text, data={"foo": "bar"}) == output


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
    assert snowflake_cli_jinja_render(text) == text


def test_that_common_comments_are_respected():
    # Make sure comment are ignored
    assert snowflake_cli_jinja_render("{# note a comment &{ foo } #}") == ""
    # Make sure comment's work together with templates
    assert (
        snowflake_cli_jinja_render("{# note a comment #}&{ foo }", data={"foo": "bar"})
        == "bar"
    )


def test_that_undefined_variables_raise_error():
    with pytest.raises(UndefinedError):
        snowflake_cli_jinja_render("&{ foo }")


def test_contex_can_access_environment_variable():
    assert snowflake_cli_jinja_render("&{ ctx.env.USER }") == os.environ.get("USER")


def test_resolve_variables_in_project_no_cross_variable_dependencies():
    pdf = ProjectDefinition(
        env={
            "number": 1,
            "string": "foo",
            "boolean": True,
        }
    )
    result = _add_project_context({}, project_definition=pdf)
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
        env={
            "A": 42,
            "B": "b=&{ ctx.env.A }",
            "C": "&{ ctx.env.B } and &{ ctx.env.A }",
        }
    )
    result = _add_project_context({}, project_definition=pdf)
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
        streamlit=Streamlit(name="my_app"),
        env={"app": "name of streamlit is &{ ctx.streamlit.name }"},
    )
    result = _add_project_context({}, project_definition=pdf)
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
