import pytest


def test_global(runner):
    result = runner.invoke(["-h"])

    assert result.exit_code == 0

    assert "Snowflake CLI tool for developers" in result.output


@pytest.mark.parametrize(
    "namespace, expected",
    [
        ("object", "Manages Snowflake objects"),
        ("snowpark", "Manages procedures and functions."),
        ("streamlit", " Manages a Streamlit app in Snowflake."),
    ],
)
def test_namespace(namespace, expected, runner):
    result = runner.invoke([namespace, "-h"])

    assert result.exit_code == 0

    assert expected in result.output
