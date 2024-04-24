import os

import pytest
from jinja2 import UndefinedError
from snowflake.cli.api.utils.rendering import snowflake_cli_jinja_render


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
