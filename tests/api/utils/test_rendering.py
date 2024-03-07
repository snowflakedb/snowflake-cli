import pytest
from snowflake.cli.api.utils.rendering import snowflake_cli_jinja_render


def test_rendering_with_data():
    assert snowflake_cli_jinja_render("%{ foo }", data={"foo": "bar"}) == "bar"


@pytest.mark.parametrize(
    "text, output",
    [
        # Green path
        ("%{ foo }", "bar"),
        # Using $ as sf variable and basic jinja for server side
        ("${{ foo }}", "${{ foo }}"),
        ("$%{ foo }{{ var }}", "$bar{{ var }}"),
        ("${{ %{ foo } }}", "${{ bar }}"),
        # Using $ as sf variable and client side rendering
        ("$%{ foo }", "$bar"),
    ],
)
def test_rendering(text, output):
    assert snowflake_cli_jinja_render(text, data={"foo": "bar"}) == output
